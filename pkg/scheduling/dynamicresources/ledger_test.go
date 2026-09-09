/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicresources_test

import (
	"unique"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling/dynamicresources"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("AllocationLedger", func() {
	var emptyState dynamicresources.AllocatedDeviceState

	BeforeEach(func() {
		ExpectApplied(ctx, env.Client, &resourcev1.DeviceClass{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
			Spec: resourcev1.DeviceClassSpec{
				Selectors: []resourcev1.DeviceSelector{
					{CEL: &resourcev1.CELDeviceSelector{Expression: `device.driver == "gpu.example.com"`}},
				},
			},
		})
		emptyState = dynamicresources.AllocatedDeviceState{
			ExclusiveDevices: sets.New[cloudprovider.DeviceID](),
		}
	})

	It("preserves exclusive allocations, release indexes, claim metadata, and snapshot isolation", func() {
		slices := []dynamicresources.ResourceSlice{
			makeAPISlice("devices", "gpu.example.com", "pool-a", withAllNodes(),
				withGeneration(1, 1), withAPIDevices("gpu-0", "gpu-1")),
		}
		sourceNodeClaim := makeNodeClaimWithID("nc-source", "it-1")
		sourceClaim := makeClaim("source-claim", exactRequest("request", "gpu", 1))
		source := dynamicresources.NewAllocator(slices, emptyState, nil, env.Client, nil)
		result, err := source.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{sourceClaim})
		Expect(err).ToNot(HaveOccurred())
		result.Allocation.Commit(ctx)

		ledger := source.ExportLedger()
		Expect(ledger.DeepCopy()).ToNot(BeNil())

		// AllocationTracker can be seeded independently, and retains the inverse index needed for release.
		tracker := dynamicresources.NewAllocationTracker(emptyState)
		tracker.ApplyLedger(ledger)
		otherNodeClaim := makeNodeClaimWithID("nc-other", "it-1")
		Expect(tracker.IsAllocated(deviceID("gpu.example.com", "pool-a", "gpu-0"), otherNodeClaim, unique.Make("it-1"))).To(BeTrue())
		tracker.ReleaseInstanceTypes(ctx, unique.Make("nc-source"), unique.Make("it-1"))
		Expect(tracker.IsAllocated(deviceID("gpu.example.com", "pool-a", "gpu-0"), otherNodeClaim, unique.Make("it-1"))).To(BeFalse())

		candidate := dynamicresources.NewAllocator(slices, emptyState, nil, env.Client, nil)
		candidate.ApplyLedger(ledger)

		// The claim allocation metadata is sufficient to classify the original claim as already allocated.
		classified, err := candidate.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{sourceClaim})
		Expect(err).ToNot(HaveOccurred())
		Expect(classified.Allocation).To(BeNil())
		Expect(candidate.ResourceClaimAllocationMetadataForClaim(resourceClaimKey(sourceClaim))).ToNot(BeNil())

		// A rejected solve against a seeded copy must not mutate the canonical ledger.
		impossibleClaim := makeClaim("impossible", exactRequest("request", "gpu", 2))
		_, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{impossibleClaim})
		Expect(err).To(HaveOccurred())
		candidate.ReleaseInstanceType(ctx, unique.Make("nc-source"), unique.Make("it-1"))

		pristine := dynamicresources.NewAllocator(slices, emptyState, nil, env.Client, nil)
		pristine.ApplyLedger(ledger)
		_, err = pristine.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{impossibleClaim})
		Expect(err).To(HaveOccurred())
		metadata := pristine.ResourceClaimAllocationMetadataForClaim(resourceClaimKey(sourceClaim))
		Expect(metadata.Devices[unique.Make("it-1")]).To(HaveLen(1))
	})

	It("reapplies shared counter consumption over the current cluster baseline", func() {
		slices := []dynamicresources.ResourceSlice{
			makeAPISlice("counters", "gpu.example.com", "pool-a",
				withSharedCounters(counterSet("slots", map[string]resource.Quantity{
					"gpu": resource.MustParse("100"),
				})),
				withGeneration(1, 2),
			),
			makeAPISlice("devices", "gpu.example.com", "pool-a", withAllNodes(),
				withGeneration(1, 2),
				withDevicesConsumingCounters(
					deviceConsumingCounter("gpu-0", "slots", map[string]resource.Quantity{"gpu": resource.MustParse("40")}),
					deviceConsumingCounter("gpu-1", "slots", map[string]resource.Quantity{"gpu": resource.MustParse("40")}),
					deviceConsumingCounter("gpu-2", "slots", map[string]resource.Quantity{"gpu": resource.MustParse("40")}),
				),
			),
		}
		sourceNodeClaim := makeNodeClaimWithID("nc-source", "it-1")
		source := dynamicresources.NewAllocator(slices, emptyState, nil, env.Client, nil)
		result, err := source.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{
			makeClaim("source-claim", exactRequest("request", "gpu", 1)),
		})
		Expect(err).ToNot(HaveOccurred())
		result.Allocation.Commit(ctx)
		ledger := source.ExportLedger()

		currentState := dynamicresources.AllocatedDeviceState{
			ExclusiveDevices: sets.New(deviceID("gpu.example.com", "pool-a", "gpu-1").DeviceID),
		}
		candidate := dynamicresources.NewAllocator(slices, currentState, nil, env.Client, nil)
		candidate.ApplyLedger(ledger)
		otherNodeClaim := makeNodeClaimWithID("nc-other", "it-1")
		claim := makeClaim("other-claim", exactRequest("request", "gpu", 1))

		// Current baseline (40) + ledger (40) leaves too little for another 40-unit device.
		_, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{claim})
		Expect(err).To(HaveOccurred())

		// The copied inverse map releases only the ledger's consumption; the current baseline remains.
		candidate.ReleaseInstanceType(ctx, unique.Make("nc-source"), unique.Make("it-1"))
		result, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{claim})
		Expect(err).ToNot(HaveOccurred())
		Expect(result).ToNot(BeNil())
	})

	It("reapplies global consumable capacity over the current cluster baseline", func() {
		const capacityName resourcev1.QualifiedName = "gpu.example.com/vram"
		slices := []dynamicresources.ResourceSlice{
			makeAPISlice("devices", "gpu.example.com", "pool-a", withAllNodes(), withGeneration(1, 1),
				withDevicesConsumingCounters(resourcev1.Device{
					Name:                     "gpu-0",
					AllowMultipleAllocations: ptr.To(true),
					Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
						capacityName: {Value: resource.MustParse("100Gi")},
					},
				}),
			),
		}
		sourceNodeClaim := makeNodeClaimWithID("nc-source", "it-1")
		source := dynamicresources.NewAllocator(slices, emptyState, nil, env.Client, nil)
		result, err := source.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{
			makeClaim("source-claim", exactRequestWithCapacity("request", "gpu", 1, map[resourcev1.QualifiedName]resource.Quantity{
				capacityName: resource.MustParse("60Gi"),
			})),
		})
		Expect(err).ToNot(HaveOccurred())
		result.Allocation.Commit(ctx)

		currentState := dynamicresources.AllocatedDeviceState{
			ExclusiveDevices: sets.New[cloudprovider.DeviceID](),
			ConsumedCapacity: map[cloudprovider.DeviceID]map[resourcev1.QualifiedName]resource.Quantity{
				deviceID("gpu.example.com", "pool-a", "gpu-0").DeviceID: {
					capacityName: resource.MustParse("10Gi"),
				},
			},
		}
		candidate := dynamicresources.NewAllocator(slices, currentState, nil, env.Client, nil)
		candidate.ApplyLedger(source.ExportLedger())
		otherNodeClaim := makeNodeClaimWithID("nc-other", "it-1")
		claim := makeClaim("other-claim", exactRequestWithCapacity("request", "gpu", 1, map[resourcev1.QualifiedName]resource.Quantity{
			capacityName: resource.MustParse("40Gi"),
		}))

		_, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{claim})
		Expect(err).To(HaveOccurred())
		candidate.ReleaseInstanceType(ctx, unique.Make("nc-source"), unique.Make("it-1"))
		result, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{claim})
		Expect(err).ToNot(HaveOccurred())
		Expect(result).ToNot(BeNil())
	})

	It("keeps template allocations NodeClaim-local", func() {
		template := makeTemplate("gpu.example.com", "template-pool", "gpu-0")
		sourceNodeClaim := makeNodeClaimWithTemplatesAndID("nc-source", "it-1", template)
		source := dynamicresources.NewAllocator(nil, emptyState, nil, env.Client, nil)
		result, err := source.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{
			makeClaim("source-claim", exactRequest("request", "gpu", 1)),
		})
		Expect(err).ToNot(HaveOccurred())
		result.Allocation.Commit(ctx)

		candidate := dynamicresources.NewAllocator(nil, emptyState, nil, env.Client, nil)
		candidate.ApplyLedger(source.ExportLedger())
		_, err = candidate.Allocate(ctx, sourceNodeClaim, []*resourcev1.ResourceClaim{
			makeClaim("same-nodeclaim", exactRequest("request", "gpu", 1)),
		})
		Expect(err).To(HaveOccurred())

		otherNodeClaim := makeNodeClaimWithTemplatesAndID("nc-other", "it-1", template)
		result, err = candidate.Allocate(ctx, otherNodeClaim, []*resourcev1.ResourceClaim{
			makeClaim("other-nodeclaim", exactRequest("request", "gpu", 1)),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(result).ToNot(BeNil())
	})
})

func resourceClaimKey(claim *resourcev1.ResourceClaim) types.NamespacedName {
	return types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}
}
