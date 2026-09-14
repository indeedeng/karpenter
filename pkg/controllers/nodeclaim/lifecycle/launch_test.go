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

package lifecycle_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	nodeclaimlifecycle "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	launchbackoffstate "sigs.k8s.io/karpenter/pkg/state/launchbackoff"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Launch", func() {
	var nodePool *v1.NodePool
	BeforeEach(func() {
		nodePool = test.NodePool()
	})
	DescribeTable(
		"Launch",
		func(isNodeClaimManaged bool) {
			nodeClaimOpts := []v1.NodeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey: nodePool.Name,
					},
				},
			}}
			if !isNodeClaimManaged {
				nodeClaimOpts = append(nodeClaimOpts, v1.NodeClaim{
					Spec: v1.NodeClaimSpec{
						NodeClassRef: &v1.NodeClassReference{
							Group: "karpenter.test.sh",
							Kind:  "UnmanagedNodeClass",
							Name:  "default",
						},
					},
				})
			}
			nodeClaim := test.NodeClaim(nodeClaimOpts...)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

			Expect(cloudProvider.CreateCalls).To(HaveLen(lo.Ternary(isNodeClaimManaged, 1, 0)))
			Expect(cloudProvider.CreatedNodeClaims).To(HaveLen(lo.Ternary(isNodeClaimManaged, 1, 0)))
			if isNodeClaimManaged {
				_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
				Expect(err).ToNot(HaveOccurred())
			}
		},
		Entry("should launch an instance when a new NodeClaim is created", true),
		Entry("should ignore NodeClaims which aren't managed by this Karpenter instance", false),
	)
	It("should add the Launched status condition after creating the NodeClaim", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(ExpectStatusConditionExists(nodeClaim, v1.ConditionTypeLaunched).Status).To(Equal(metav1.ConditionTrue))
	})
	It("should delete the nodeclaim if InsufficientCapacity is returned from the cloudprovider", func() {
		cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("all instance types were unavailable"))
		nodeClaim := test.NodeClaim()
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should delete the nodeclaim if NodeClassNotReady is returned from the cloudprovider", func() {
		cloudProvider.NextCreateErr = cloudprovider.NewNodeClassNotReadyError(fmt.Errorf("nodeClass isn't ready"))
		nodeClaim := test.NodeClaim()
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should set nodeClaim status condition from the condition message received if error returned is CreateError", func() {
		conditionReason := "CustomReason"
		conditionMessage := "instance creation failed"
		cloudProvider.NextCreateErr = cloudprovider.NewCreateError(fmt.Errorf("error launching instance"), conditionReason, conditionMessage)
		nodeClaim := test.NodeClaim()
		ExpectApplied(ctx, env.Client, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		condition := ExpectStatusConditionExists(nodeClaim, v1.ConditionTypeLaunched)
		Expect(condition.Status).To(Equal(metav1.ConditionUnknown))
		Expect(condition.Reason).To(Equal(conditionReason))
		Expect(condition.Message).To(Equal(conditionMessage))
	})

	Context("Launch Backoff", func() {
		const reservationTTL = time.Second

		var (
			nodeClaim     *v1.NodeClaim
			reservationID string
			offering      cloudprovider.OfferingKey
			alternate     cloudprovider.OfferingKey
		)

		BeforeEach(func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
				FeatureGates: test.FeatureGates{LaunchBackoff: lo.ToPtr(true)},
			}))
			launchBackoff = launchbackoffstate.NewTracker(env.Clock, reservationTTL)
			nodeClaimController = nodeclaimlifecycle.NewController(env.Clock, env.Client, cloudProvider, recorder, npState, nil, launchBackoff)
			reservationID = "reservation"
			offering = cloudprovider.OfferingKey{
				InstanceType: "large",
				CapacityType: v1.CapacityTypeSpot,
				Zone:         "test-zone-1a",
			}
			alternate = cloudprovider.OfferingKey{
				InstanceType: "small",
				CapacityType: v1.CapacityTypeOnDemand,
				Zone:         "test-zone-1b",
			}
			nodeClaim = test.NodeClaim(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{v1.NodePoolLabelKey: nodePool.Name},
					Annotations: map[string]string{v1.LaunchBackoffReservationAnnotationKey: reservationID},
				},
			})
		})
		AfterEach(func() {
			launchBackoff = launchbackoffstate.NewTracker(env.Clock)
			nodeClaimController = nodeclaimlifecycle.NewController(env.Clock, env.Client, cloudProvider, recorder, npState, nil, launchBackoff)
			ctx = options.ToContext(ctx, test.Options())
		})

		reserve := func(keys ...cloudprovider.OfferingKey) {
			GinkgoHelper()
			for i, key := range keys {
				launchBackoff.Fail(ctx, fmt.Sprintf("seed-%d", i), key)
			}
			env.Clock.Step(launchbackoffstate.ProbeInterval)
			result := launchBackoff.Reserve(ctx, reservationID, keys)
			Expect(result.Admitted).To(BeTrue())
			Expect(result.DebitedOfferings).To(Equal(len(keys)))
		}

		It("should bind a reservation to the NodeClaim before launching", func() {
			reserve(offering)
			cloudProvider.NextCreateErr = cloudprovider.NewCreateError(fmt.Errorf("transient error"), "LaunchFailed", "transient error")
			ExpectApplied(ctx, env.Client, nodeClaim)

			_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
			env.Clock.Step(2 * reservationTTL)
			launchBackoff.Cleanup()

			Expect(launchBackoff.Reserve(ctx, "competing", []cloudprovider.OfferingKey{offering}).Admitted).To(BeFalse())
			launchBackoff.Release(reservationID)
			Expect(launchBackoff.Reserve(ctx, "after-release", []cloudprovider.OfferingKey{offering}).Admitted).To(BeTrue())
		})
		It("should release a reservation when the NodeClaim is deleted before launch", func() {
			reserve(offering)
			nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
			ExpectApplied(ctx, env.Client, nodeClaim)
			Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.Reserve(ctx, "after-deletion", []cloudprovider.OfferingKey{offering}).Admitted).To(BeTrue())
		})
		It("should settle an attributed insufficient capacity failure against only the refused offering", func() {
			reserve(offering, alternate)
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"), offering)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.Budgets()).To(Equal(map[cloudprovider.OfferingKey]launchbackoffstate.OfferingBudget{
				offering:  {Burst: 1, Unavailable: true},
				alternate: {Burst: 1, Unavailable: false},
			}))
		})
		It("should settle an unattributed insufficient capacity failure against every reserved offering", func() {
			reserve(offering, alternate)
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"))
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)

			Expect(launchBackoff.Budgets()).To(Equal(map[cloudprovider.OfferingKey]launchbackoffstate.OfferingBudget{
				offering:  {Burst: 1, Unavailable: true},
				alternate: {Burst: 1, Unavailable: true},
			}))
		})
		It("should ramp the landed offering and refund the other reserved offerings on success", func() {
			probe, err := cloudProvider.Create(ctx, nodeClaim.DeepCopy())
			Expect(err).To(Succeed())
			landed := cloudprovider.OfferingKey{
				InstanceType: probe.Labels[corev1.LabelInstanceTypeStable],
				CapacityType: probe.Labels[v1.CapacityTypeLabelKey],
				Zone:         probe.Labels[corev1.LabelTopologyZone],
			}
			cloudProvider.Reset()
			reserve(landed, alternate)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.Budgets()).To(Equal(map[cloudprovider.OfferingKey]launchbackoffstate.OfferingBudget{
				landed:    {Burst: 2, Unavailable: true},
				alternate: {Burst: 1, Unavailable: false},
			}))
			refunded := launchBackoff.Reserve(ctx, "refunded", []cloudprovider.OfferingKey{alternate})
			Expect(refunded.Admitted).To(BeTrue())
			Expect(refunded.DebitedOfferings).To(Equal(1))
		})
		It("should record nothing while the gate is disabled", func() {
			ctx = options.ToContext(ctx, test.Options())
			Expect(launchBackoff.Reserve(ctx, reservationID, []cloudprovider.OfferingKey{offering}).Admitted).To(BeTrue())
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"), offering)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.Budgets()).To(BeEmpty())
		})
	})
})
