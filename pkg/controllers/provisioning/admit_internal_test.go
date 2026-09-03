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

package provisioning

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	scheduler "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
)

var _ = Describe("Launch Admission", func() {
	var provisioner *Provisioner
	var tracker *launchbackoff.Tracker
	var fakeClock *clocktesting.FakeClock
	var gated context.Context
	var poolA, poolB types.UID

	// One instance type with two offerings, so that a spec can fail one and leave the other untried.
	instanceTypes := func() cloudprovider.InstanceTypes {
		newOffering := func(capacityType, zone string) cloudprovider.Offering {
			return cloudprovider.Offering{
				Available: true,
				Price:     1.0,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  capacityType,
					corev1.LabelTopologyZone: zone,
				}),
			}
		}
		return cloudprovider.InstanceTypes{fake.NewInstanceType("large", fake.WithOfferings(
			newOffering(v1.CapacityTypeSpot, "zone-a"),
			newOffering(v1.CapacityTypeOnDemand, "zone-a"),
		))}
	}

	nodeClaim := func(nodePoolUID types.UID) *scheduler.NodeClaim {
		return &scheduler.NodeClaim{NodeClaimTemplate: scheduler.NodeClaimTemplate{
			NodePoolName:        string(nodePoolUID),
			NodePoolUUID:        nodePoolUID,
			InstanceTypeOptions: instanceTypes(),
		}}
	}

	uids := func(nodeClaims []*scheduler.NodeClaim) []types.UID {
		out := []types.UID{}
		for _, nc := range nodeClaims {
			out = append(out, nc.NodePoolUUID)
		}
		return out
	}

	BeforeEach(func() {
		fakeClock = clocktesting.NewFakeClock(time.Now())
		tracker = launchbackoff.NewTracker(fakeClock)
		provisioner = &Provisioner{launchBackoff: tracker}
		gated = options.ToContext(context.Background(), &options.Options{
			FeatureGates: options.FeatureGates{LaunchBackoff: true},
		})
		poolA, poolB = types.UID("pool-a"), types.UID("pool-b")
	})

	It("should admit everything when nothing has failed", func() {
		nodeClaims := []*scheduler.NodeClaim{nodeClaim(poolA), nodeClaim(poolA), nodeClaim(poolB)}

		Expect(provisioner.admit(gated, nodeClaims)).To(HaveLen(3))
	})
	It("should admit only the probe allowance on a constrained nodepool", func() {
		tracker.FailPool(gated, poolA)
		nodeClaims := []*scheduler.NodeClaim{nodeClaim(poolA), nodeClaim(poolA), nodeClaim(poolA)}

		Expect(provisioner.admit(gated, nodeClaims)).To(HaveLen(1))
	})
	It("should not let one nodepool's shortage throttle another", func() {
		tracker.FailPool(gated, poolA)
		nodeClaims := []*scheduler.NodeClaim{nodeClaim(poolA), nodeClaim(poolA), nodeClaim(poolB), nodeClaim(poolB)}

		Expect(uids(provisioner.admit(gated, nodeClaims))).To(ConsistOf(poolA, poolB, poolB))
	})
	It("should offer non-risky nodeclaims the probe before risky ones", func() {
		// Ordering is the whole point of the partition. Given one probe and both kinds waiting, the
		// launch with somewhere untried to go is the one worth spending it on.
		tracker.FailPool(gated, poolA)
		risky := nodeClaim(poolA)
		risky.InstanceTypeOptions = cloudprovider.InstanceTypes{
			fake.NewInstanceType("exhausted", fake.WithOfferings(
				cloudprovider.Offering{Available: true, Price: 1.0, Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  v1.CapacityTypeSpot,
					corev1.LabelTopologyZone: "zone-a",
				})},
			)),
		}
		tracker.Fail(gated, cloudprovider.OfferingKey{InstanceType: "exhausted", CapacityType: v1.CapacityTypeSpot, Zone: "zone-a"})
		healthy := nodeClaim(poolA)

		// Risky first in input order, so a result of the healthy one can only come from the reorder.
		admitted := provisioner.admit(gated, []*scheduler.NodeClaim{risky, healthy})

		Expect(admitted).To(ConsistOf(healthy))
	})
	It("should throttle a risky nodeclaim on a nodepool that has not failed", func() {
		// Offering state is shared across nodepools, so a pool with a clean record of its own can
		// still have launches with nowhere untried to go.
		for _, key := range []cloudprovider.OfferingKey{
			{InstanceType: "large", CapacityType: v1.CapacityTypeSpot, Zone: "zone-a"},
			{InstanceType: "large", CapacityType: v1.CapacityTypeOnDemand, Zone: "zone-a"},
		} {
			tracker.Fail(gated, key)
		}
		fakeClock.Step(launchbackoff.BaseDelay * 2)
		nodeClaims := []*scheduler.NodeClaim{nodeClaim(poolA), nodeClaim(poolA), nodeClaim(poolA)}

		Expect(tracker.IsConstrained(gated, poolA)).To(BeFalse())
		Expect(provisioner.admit(gated, nodeClaims)).To(HaveLen(launchbackoff.RiskyBurst))
	})
	It("should admit everything while the gate is disabled", func() {
		tracker.FailPool(gated, poolA)
		ungated := options.ToContext(context.Background(), &options.Options{})
		nodeClaims := []*scheduler.NodeClaim{nodeClaim(poolA), nodeClaim(poolA), nodeClaim(poolA)}

		Expect(provisioner.admit(ungated, nodeClaims)).To(HaveLen(3))
	})
})
