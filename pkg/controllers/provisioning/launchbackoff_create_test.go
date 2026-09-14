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

package provisioning_test

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type ambiguousNodeClaimCreateClient struct {
	client.Client
}

func (c *ambiguousNodeClaimCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if _, ok := obj.(*v1.NodeClaim); ok {
		return apierrors.NewTimeoutError("ambiguous NodeClaim create", 1)
	}
	return nil
}

var _ = ginkgo.Describe("Launch Backoff Create", func() {
	ginkgo.It("should retain the annotated reservation when an ambiguous create persisted the NodeClaim", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			FeatureGates: test.FeatureGates{LaunchBackoff: lo.ToPtr(true)},
		}))
		offering := cloudprovider.OfferingKey{
			InstanceType: "ambiguous-instance-type",
			CapacityType: v1.CapacityTypeOnDemand,
			Zone:         "test-zone-1",
		}
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{fake.NewInstanceType(offering.InstanceType,
			fake.WithOfferings(cloudprovider.Offering{
				Available: true,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  offering.CapacityType,
					corev1.LabelTopologyZone: offering.Zone,
				}),
				Price: 1.0,
			}),
		)}
		nodePool := test.NodePool()
		pod := test.UnschedulablePod(test.PodOptions{
			NodeSelector: map[string]string{corev1.LabelTopologyZone: offering.Zone},
		})
		ExpectApplied(ctx, env.Client, nodePool)

		tracker := launchbackoff.NewTracker(env.Clock)
		tracker.Fail(ctx, "seed", offering)
		env.Clock.Step(launchbackoff.ProbeInterval)
		kubeClient := &ambiguousNodeClaimCreateClient{Client: env.Client}
		localProvisioner := provisioning.NewProvisioner(
			kubeClient,
			events.NewRecorder(&record.FakeRecorder{}),
			cloudProvider,
			cluster,
			env.Clock,
			deviceallocation.NewController(kubeClient),
			virtualpods.NewVirtualPodCache(kubeClient),
			tracker,
		)
		results := ExpectProvisionedResults(ctx, kubeClient, cluster, cloudProvider, localProvisioner, pod)
		Expect(results.NewNodeClaims).To(HaveLen(1))
		reservations, err := localProvisioner.ReserveNodeClaims(ctx, results.NewNodeClaims)
		Expect(err).ToNot(HaveOccurred())
		Expect(reservations.Admitted).To(HaveLen(1))
		reservationID := reservations.Admitted[0].Annotations[v1.LaunchBackoffReservationAnnotationKey]
		Expect(reservationID).ToNot(BeEmpty())

		_, err = localProvisioner.Create(ctx, reservations.Admitted[0])
		Expect(apierrors.IsTimeout(err)).To(BeTrue())

		nodeClaims := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
		Expect(nodeClaims.Items).To(HaveLen(1))
		Expect(nodeClaims.Items[0].Annotations[v1.LaunchBackoffReservationAnnotationKey]).To(Equal(reservationID))
		Expect(tracker.Reserve(ctx, "competing", []cloudprovider.OfferingKey{offering}).Admitted).To(BeFalse())
	})
})
