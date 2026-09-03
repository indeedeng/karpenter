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

	"github.com/awslabs/operatorpkg/object"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
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
		var nodeClaim *v1.NodeClaim

		BeforeEach(func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
				FeatureGates: test.FeatureGates{LaunchBackoff: lo.ToPtr(true)},
			}))
			// The owner reference has to carry the UID the API server assigned, since that is the key
			// the tracker budgets a NodePool by.
			ExpectApplied(ctx, env.Client, nodePool)
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			nodeClaim = test.NodeClaim(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: object.GVK(&v1.NodePool{}).GroupVersion().String(),
						Kind:       object.GVK(&v1.NodePool{}).Kind,
						Name:       nodePool.Name,
						UID:        nodePool.UID,
					}},
				},
			})
		})
		AfterEach(func() {
			launchBackoff.Delete(nodePool.UID)
			ctx = options.ToContext(ctx, test.Options())
		})

		It("should constrain the owning nodepool on an insufficient capacity failure", func() {
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("all instance types were unavailable"))
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.IsConstrained(ctx, nodePool.UID)).To(BeTrue())
		})
		It("should back off the offerings the provider attributed the failure to", func() {
			offering := cloudprovider.OfferingKey{InstanceType: "large", CapacityType: v1.CapacityTypeSpot, Zone: "test-zone-1a"}
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"), offering)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.IsAvailable(offering)).To(BeFalse())
		})
		It("should back off the pool the cloud provider actually refused", func() {
			// The one spec that closes the loop through a provider rather than a hand-built error:
			// the fake attributes the failure to the pool the launch landed in, and core backs off
			// exactly that pool.
			//
			// Where a launch lands is discovered rather than hardcoded, so this does not depend on
			// the fake's instance type fixture.
			probe, err := cloudProvider.Create(ctx, nodeClaim.DeepCopy())
			Expect(err).To(Succeed())
			landed := cloudprovider.OfferingKey{
				InstanceType: probe.Labels[corev1.LabelInstanceTypeStable],
				CapacityType: probe.Labels[v1.CapacityTypeLabelKey],
				Zone:         probe.Labels[corev1.LabelTopologyZone],
			}
			cloudProvider.CapacityUnavailable = sets.New(landed)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.IsAvailable(landed)).To(BeFalse())
			Expect(launchBackoff.IsConstrained(ctx, nodePool.UID)).To(BeTrue())
		})
		It("should record the failure before deleting the nodeclaim", func() {
			// The delete returns early on error, and it is the only thing standing between the
			// failure and the record of it.
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"))
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)

			Expect(launchBackoff.IsConstrained(ctx, nodePool.UID)).To(BeTrue())
		})
		It("should ramp the allowance of a constrained nodepool on a successful launch", func() {
			launchBackoff.FailPool(ctx, nodePool.UID)
			Expect(launchBackoff.Burst(nodePool.UID)).To(Equal(1))
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.Burst(nodePool.UID)).To(Equal(2))
		})
		It("should record nothing while the gate is disabled", func() {
			// Read back through a gate-on context, since a gate-off read answers false regardless of
			// what is stored and would pass even if the failure had been recorded.
			gateOn := ctx
			ctx = options.ToContext(ctx, test.Options())
			// Offering entries are keyed globally and outlive the spec that recorded them, so this
			// key has to be one no other spec touches.
			offering := cloudprovider.OfferingKey{InstanceType: "gated", CapacityType: v1.CapacityTypeSpot, Zone: "test-zone-1a"}
			cloudProvider.NextCreateErr = cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no capacity"), offering)
			ExpectApplied(ctx, env.Client, nodeClaim)

			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)

			Expect(launchBackoff.IsConstrained(gateOn, nodePool.UID)).To(BeFalse())
			Expect(launchBackoff.IsAvailable(offering)).To(BeTrue())
		})
	})
})
