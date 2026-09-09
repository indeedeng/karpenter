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

package disruption_test

import (
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Drift/DRA batching", func() {
	var nodePool *v1.NodePool
	var replicaSet *appsv1.ReplicaSet

	BeforeEach(func() {
		if driftDRAServerMinor() < 34 {
			Skip("DRA is only available in K8s versions >= 1.34.x")
		}
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{IgnoreDRARequests: lo.ToPtr(false)}))
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{fake.NewInstanceType("no-dra-devices")}
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter: v1.MustParseNillableDuration("Never"),
					Budgets:          []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		replicaSet = test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, replicaSet, test.DeviceClassWithSelector("gpu", test.GPUDriver))
	})

	It("carries accepted exclusive-device allocations into later drift solves", func() {
		ExpectApplied(ctx, env.Client, test.ClusterWideSlice("exclusive-target", test.GPUDriver, "gpu-0"))
		firstNodeClaim, firstNode := newDriftDRACandidate(nodePool, replicaSet, 0, 1, nil)
		_, secondNode := newDriftDRACandidate(nodePool, replicaSet, 1, 1, nil)

		commands := computeDriftDRACommands(nodePool, firstNode, secondNode)

		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates[0].NodeClaim.Name).To(Equal(firstNodeClaim.Name))
		expectReplacementOnlyCommand(commands[0])
	})

	It("carries accepted multi-allocatable capacity into later drift solves", func() {
		if driftDRAServerMinor() < 36 {
			Skip("Consumable capacity requires K8s versions >= 1.36.x")
		}
		ExpectApplied(ctx, env.Client, test.SharedCapacitySlice("capacity-target", test.GPUDriver, "gpu-0", "16Gi"))
		firstNodeClaim, firstNode := newDriftDRACandidate(nodePool, replicaSet, 0, 1, test.CapacityRequest("10Gi"))
		_, secondNode := newDriftDRACandidate(nodePool, replicaSet, 1, 1, test.CapacityRequest("10Gi"))

		commands := computeDriftDRACommands(nodePool, firstNode, secondNode)

		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates[0].NodeClaim.Name).To(Equal(firstNodeClaim.Name))
		expectReplacementOnlyCommand(commands[0])
	})

	It("carries accepted shared-counter consumption into the second drift candidate", func() {
		counterSlices := driftDRACounterSlices("counter-target", "slots", "gpu", "60",
			driftDRACounterDevice("gpu-0", "slots", "gpu", "40"),
			driftDRACounterDevice("gpu-1", "slots", "gpu", "40"),
			driftDRACounterDevice("gpu-2", "slots", "gpu", "40"),
		)
		for i := range counterSlices {
			ExpectApplied(ctx, env.Client, &counterSlices[i])
		}
		firstNodeClaim, firstNode := newDriftDRACandidate(nodePool, replicaSet, 0, 1, nil)
		_, secondNode := newDriftDRACandidate(nodePool, replicaSet, 1, 1, nil)

		commands := computeDriftDRACommands(nodePool, firstNode, secondNode)

		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates[0].NodeClaim.Name).To(Equal(firstNodeClaim.Name))
		expectReplacementOnlyCommand(commands[0])
	})

	It("does not leak a rejected solve into the next drift candidate", func() {
		ExpectApplied(ctx, env.Client, test.ClusterWideSlice("isolation-target", test.GPUDriver, "gpu-0", "gpu-1"))
		firstNodeClaim, firstNode := newDriftDRACandidate(nodePool, replicaSet, 0, 1, nil)
		_, rejectedNode := newDriftDRACandidate(nodePool, replicaSet, 1, 2, nil)
		thirdNodeClaim, thirdNode := newDriftDRACandidate(nodePool, replicaSet, 2, 1, nil)

		commands := computeDriftDRACommands(nodePool, firstNode, rejectedNode, thirdNode)

		Expect(commands).To(HaveLen(2))
		Expect(commands[0].Candidates[0].NodeClaim.Name).To(Equal(firstNodeClaim.Name))
		Expect(commands[1].Candidates[0].NodeClaim.Name).To(Equal(thirdNodeClaim.Name))
		expectReplacementOnlyCommand(commands[0])
		expectReplacementOnlyCommand(commands[1])
	})
})

func newDriftDRACandidate(nodePool *v1.NodePool, replicaSet *appsv1.ReplicaSet, index int, count int64, capacity map[resourcev1.QualifiedName]resource.Quantity) (*v1.NodeClaim, *corev1.Node) {
	GinkgoHelper()
	nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("drift-dra-nodeclaim-%d", index),
			Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: "no-dra-devices",
			},
		},
		Status: v1.NodeClaimStatus{
			ProviderID: test.RandomProviderID(),
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse("4"),
				corev1.ResourcePods: resource.MustParse("10"),
			},
		},
	})
	nodeClaim.Status.Conditions = append(nodeClaim.Status.Conditions, status.Condition{
		Type:               v1.ConditionTypeDrifted,
		Status:             metav1.ConditionTrue,
		Reason:             v1.ConditionTypeDrifted,
		Message:            v1.ConditionTypeDrifted,
		LastTransitionTime: metav1.Time{Time: env.Clock.Now().Add(-time.Hour + time.Duration(index)*time.Minute)},
	})
	node.Labels[corev1.LabelHostname] = node.Name
	ExpectApplied(ctx, env.Client, nodeClaim, node)

	claimName := fmt.Sprintf("drift-dra-claim-%d", index)
	pod := test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "ReplicaSet",
				Name:               replicaSet.Name,
				UID:                replicaSet.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}},
		},
		ResourceClaims:          []corev1.PodResourceClaim{test.PodResourceClaimReference("gpu", claimName)},
		ContainerResourceClaims: []corev1.ResourceClaim{{Name: "gpu"}},
	})
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, node)

	request := test.ExactDeviceRequestWithCapacity("req", "gpu", count, capacity)
	pool := test.NodeLocalPoolName(test.GPUDriver, node.Name)
	allocationResults := make([]resourcev1.DeviceRequestAllocationResult, 0, count)
	sourceDevices := make([]resourcev1.Device, 0, count)
	for deviceIndex := range count {
		deviceName := fmt.Sprintf("source-%d", deviceIndex)
		allocationResults = append(allocationResults, resourcev1.DeviceRequestAllocationResult{
			Request:          request.Name,
			Driver:           test.GPUDriver,
			Pool:             pool,
			Device:           deviceName,
			ConsumedCapacity: capacity,
		})
		device := resourcev1.Device{Name: deviceName}
		if len(capacity) != 0 {
			device.AllowMultipleAllocations = lo.ToPtr(true)
			device.Capacity = lo.MapValues(capacity, func(value resource.Quantity, _ resourcev1.QualifiedName) resourcev1.DeviceCapacity {
				return resourcev1.DeviceCapacity{Value: value}
			})
		}
		sourceDevices = append(sourceDevices, device)
	}
	claim := test.ResourceClaim(resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: pod.Namespace},
		Spec: resourcev1.ResourceClaimSpec{
			Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{request}},
		},
		Status: resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{Results: allocationResults},
			},
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{test.PodConsumer(pod)},
		},
	})
	sourceSlice := test.ResourceSlice(resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("drift-dra-source-%d", index),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       node.Name,
				UID:        node.UID,
			}},
		},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   test.GPUDriver,
			NodeName: lo.ToPtr(node.Name),
			Pool: resourcev1.ResourcePool{
				Name:               pool,
				Generation:         1,
				ResourceSliceCount: 1,
			},
			Devices: sourceDevices,
		},
	})
	ExpectApplied(ctx, env.Client, claim, sourceSlice)
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
	return nodeClaim, ExpectExists(ctx, env.Client, node)
}

func computeDriftDRACommands(nodePool *v1.NodePool, nodes ...*corev1.Node) []disruption.Command {
	GinkgoHelper()
	ExpectDeviceAllocationReconciled(ctx, env.Client, draController)
	drift := disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock, queue.NodePoolBackoff())
	candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, drift.ShouldDisrupt, drift.Class(), queue)
	Expect(err).To(Succeed())
	Expect(candidates).To(HaveLen(len(nodes)))
	commands, err := drift.ComputeCommands(ctx, map[string]int{nodePool.Name: len(nodes)}, candidates...)
	Expect(err).To(Succeed())
	return commands
}

func expectReplacementOnlyCommand(command disruption.Command) {
	GinkgoHelper()
	Expect(command.Decision()).To(Equal(disruption.ReplaceDecision))
	Expect(command.Results.ExistingNodes).To(BeEmpty())
	Expect(command.Results.NewNodeClaims).To(HaveLen(1))
}

func driftDRACounterSlices(pool, counterSet, counter, total string, devices ...resourcev1.Device) []resourcev1.ResourceSlice {
	counterSets := []resourcev1.CounterSet{{
		Name: counterSet,
		Counters: map[string]resourcev1.Counter{
			counter: {Value: resource.MustParse(total)},
		},
	}}
	if driftDRAServerMinor() < 35 {
		return []resourcev1.ResourceSlice{
			*test.ResourceSlice(resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: pool},
				Spec: resourcev1.ResourceSliceSpec{
					Driver:         test.GPUDriver,
					AllNodes:       lo.ToPtr(true),
					Pool:           resourcev1.ResourcePool{Name: pool, Generation: 1, ResourceSliceCount: 1},
					SharedCounters: counterSets,
					Devices:        devices,
				},
			}),
		}
	}

	resourcePool := resourcev1.ResourcePool{
		Name:               pool,
		Generation:         1,
		ResourceSliceCount: 2,
	}
	return []resourcev1.ResourceSlice{
		*test.ResourceSlice(resourcev1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-counters", pool)},
			Spec: resourcev1.ResourceSliceSpec{
				Driver:         test.GPUDriver,
				Pool:           resourcePool,
				SharedCounters: counterSets,
			},
		}),
		*test.ResourceSlice(resourcev1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-devices", pool)},
			Spec: resourcev1.ResourceSliceSpec{
				Driver:   test.GPUDriver,
				AllNodes: lo.ToPtr(true),
				Pool:     resourcePool,
				Devices:  devices,
			},
		}),
	}
}

func driftDRAServerMinor() int {
	GinkgoHelper()
	serverVersion, err := env.KubernetesInterface.Discovery().ServerVersion()
	Expect(err).ToNot(HaveOccurred())
	parsedVersion, err := version.ParseGeneric(serverVersion.GitVersion)
	Expect(err).ToNot(HaveOccurred())
	return int(parsedVersion.Minor())
}

func driftDRACounterDevice(name, counterSet, counter, amount string) resourcev1.Device {
	return resourcev1.Device{
		Name: name,
		ConsumesCounters: []resourcev1.DeviceCounterConsumption{{
			CounterSet: counterSet,
			Counters: map[string]resourcev1.Counter{
				counter: {Value: resource.MustParse(amount)},
			},
		}},
	}
}
