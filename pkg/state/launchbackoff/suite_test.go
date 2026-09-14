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

package launchbackoff_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
)

var (
	ctx       context.Context
	fakeClock *clocktesting.FakeClock
	tracker   *launchbackoff.Tracker
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LaunchBackoff")
}

func withGate(launchBackoff bool) context.Context {
	return options.ToContext(context.Background(), &options.Options{
		FeatureGates: options.FeatureGates{LaunchBackoff: launchBackoff},
	})
}

var _ = BeforeEach(func() {
	ctx = withGate(true)
	fakeClock = clocktesting.NewFakeClock(time.Now())
	tracker = launchbackoff.NewTracker(fakeClock)
})

func key(instanceType, capacityType, zone string) cloudprovider.OfferingKey {
	return cloudprovider.OfferingKey{InstanceType: instanceType, CapacityType: capacityType, Zone: zone}
}

func offering(capacityType, zone string) cloudprovider.Offering {
	return cloudprovider.Offering{
		Available: true,
		Price:     1.0,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  capacityType,
			corev1.LabelTopologyZone: zone,
		}),
	}
}

func unavailableOffering(capacityType, zone string) cloudprovider.Offering {
	result := offering(capacityType, zone)
	result.Available = false
	return result
}

func sharesBackingArray(a, b []*cloudprovider.InstanceType) bool {
	return len(a) == len(b) && len(a) > 0 && &a[0] == &b[0]
}

func availableKeys(instanceType *cloudprovider.InstanceType) []cloudprovider.OfferingKey {
	var result []cloudprovider.OfferingKey
	for _, candidate := range instanceType.Offerings {
		if candidate.Available {
			result = append(result, candidate.Key(instanceType.Name))
		}
	}
	return result
}

var _ = Describe("Offering reservations", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")
	spotB := key("large", v1.CapacityTypeSpot, "zone-b")

	It("refills lazily without allowing reads to spend the refill", func() {
		tracker.Fail(ctx, "initial-failure", spotA)
		nextRefill := fakeClock.Now().Add(launchbackoff.ProbeInterval)

		Expect(tracker.IsAvailable(spotA)).To(BeFalse())
		Expect(tracker.NextEligible(spotA)).To(Equal(nextRefill))

		fakeClock.Step(launchbackoff.ProbeInterval)
		for range 5 {
			Expect(tracker.IsAvailable(spotA)).To(BeTrue())
			Expect(tracker.NextEligible(spotA)).To(Equal(fakeClock.Now()))
		}

		Expect(tracker.Reserve(ctx, "probe", []cloudprovider.OfferingKey{spotA})).To(Equal(launchbackoff.ReservationResult{
			Admitted:         true,
			NextEligible:     fakeClock.Now(),
			DebitedOfferings: 1,
		}))
		Expect(tracker.Reserve(ctx, "blocked", []cloudprovider.OfferingKey{spotA})).To(Equal(launchbackoff.ReservationResult{
			Admitted:     false,
			NextEligible: fakeClock.Now().Add(launchbackoff.ProbeInterval),
		}))
	})

	It("refills from burst even when reservations from the previous generation never settle", func() {
		tracker.Fail(ctx, "initial-failure", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "ramp", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		tracker.Succeed(ctx, "ramp", spotA)
		Expect(tracker.Budgets()[spotA].Burst).To(Equal(2))

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "stale-1", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		Expect(tracker.Reserve(ctx, "stale-2", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		Expect(tracker.Reserve(ctx, "exhausted", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeFalse())

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "fresh-1", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		Expect(tracker.Reserve(ctx, "fresh-2", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		Expect(tracker.Reserve(ctx, "still-exhausted", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeFalse())
	})

	It("ramps only the landed offering through 1, 2, 4, 8, and unrestricted", func() {
		tracker.Fail(ctx, "initial-failure", spotA)
		Expect(tracker.Budgets()[spotA]).To(Equal(launchbackoff.OfferingBudget{Burst: 1, Unavailable: true}))

		for i, expectedBurst := range []int{2, 4, 8} {
			fakeClock.Step(launchbackoff.ProbeInterval)
			id := fmt.Sprintf("success-%d", i)
			Expect(tracker.Reserve(ctx, id, []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
			tracker.Succeed(ctx, id, spotA)
			Expect(tracker.Budgets()[spotA].Burst).To(Equal(expectedBurst))
		}

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "release", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		tracker.Succeed(ctx, "release", spotA)

		Expect(tracker.Budgets()).ToNot(HaveKey(spotA))
		for i := range 20 {
			result := tracker.Reserve(ctx, fmt.Sprintf("unrestricted-%d", i), []cloudprovider.OfferingKey{spotA})
			Expect(result.Admitted).To(BeTrue())
			Expect(result.DebitedOfferings).To(BeZero())
		}
	})

	It("treats repeated failures in one closed window as idempotent", func() {
		tracker.Fail(ctx, "failure-0", spotA)
		nextRefill := tracker.NextEligible(spotA)

		for i := range 50 {
			tracker.Fail(ctx, fmt.Sprintf("failure-%d", i+1), spotA)
		}

		Expect(tracker.NextEligible(spotA)).To(Equal(nextRefill))
		Expect(tracker.Budgets()[spotA]).To(Equal(launchbackoff.OfferingBudget{Burst: 1, Unavailable: true}))
	})

	It("pessimistically debits every launchable tracked candidate and refunds alternatives", func() {
		tracker.Fail(ctx, "failure-a", spotA)
		tracker.Fail(ctx, "failure-b", spotB)
		fakeClock.Step(launchbackoff.ProbeInterval)

		result := tracker.Reserve(ctx, "flexible", []cloudprovider.OfferingKey{spotA, spotB, spotA})
		Expect(result.Admitted).To(BeTrue())
		Expect(result.DebitedOfferings).To(Equal(2), "duplicate candidates must not be double-debited")
		Expect(tracker.IsAvailable(spotA)).To(BeFalse())
		Expect(tracker.IsAvailable(spotB)).To(BeFalse())

		tracker.Succeed(ctx, "flexible", spotA)

		Expect(tracker.Budgets()[spotA].Burst).To(Equal(2))
		Expect(tracker.IsAvailable(spotA)).To(BeFalse(), "the landed key keeps its debit")
		Expect(tracker.IsAvailable(spotB)).To(BeTrue(), "the non-landed candidate is refunded")
	})

	It("refunds every pessimistic debit when a reservation is released", func() {
		tracker.Fail(ctx, "failure-a", spotA)
		tracker.Fail(ctx, "failure-b", spotB)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "omitted", []cloudprovider.OfferingKey{spotA, spotB}).DebitedOfferings).To(Equal(2))

		tracker.Release("omitted")
		tracker.Release("omitted")

		Expect(tracker.IsAvailable(spotA)).To(BeTrue())
		Expect(tracker.IsAvailable(spotB)).To(BeTrue())
	})

	It("admits through a launchable alternative when another candidate is exhausted", func() {
		tracker.Fail(ctx, "failure-a", spotA)
		tracker.Fail(ctx, "failure-b", spotB)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "consume-a", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())

		result := tracker.Reserve(ctx, "use-b", []cloudprovider.OfferingKey{spotA, spotB})

		Expect(result.Admitted).To(BeTrue())
		Expect(result.DebitedOfferings).To(Equal(1))
		Expect(result.NextEligible).To(Equal(fakeClock.Now()))
	})

	It("clamps attributed failures and refunds every other candidate", func() {
		tracker.Fail(ctx, "failure-a", spotA)
		tracker.Fail(ctx, "failure-b", spotB)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "attempt", []cloudprovider.OfferingKey{spotA, spotB}).DebitedOfferings).To(Equal(2))

		tracker.Fail(ctx, "attempt", spotA)

		Expect(tracker.IsAvailable(spotA)).To(BeFalse())
		Expect(tracker.IsAvailable(spotB)).To(BeTrue())
		Expect(tracker.Budgets()[spotA].Burst).To(Equal(1))
	})

	It("clamps every saved candidate for an unattributed failure during recovery", func() {
		tracker.Fail(ctx, "seed", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "attempt", []cloudprovider.OfferingKey{spotA, spotB}).Admitted).To(BeTrue())

		tracker.Fail(ctx, "attempt")

		Expect(tracker.UnavailableOfferings()).To(ConsistOf(spotA, spotB))
		Expect(tracker.Budgets()[spotB].Burst).To(Equal(1), "an unrestricted saved candidate must become tracked")
	})

	It("rolls back an atomic batch when every replacement cannot be admitted", func() {
		tracker.Fail(ctx, "failure", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)

		results, admitted := tracker.ReserveBatch(ctx, map[string][]cloudprovider.OfferingKey{
			"replacement-a": {spotA},
			"replacement-b": {spotA},
		})

		Expect(admitted).To(BeFalse())
		Expect(results).To(HaveLen(2))
		Expect(tracker.Reserve(ctx, "after-rollback", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
	})

	It("orders constrained batch requests before flexible requests", func() {
		tracker.Fail(ctx, "failure-a", spotA)
		tracker.Fail(ctx, "failure-b", spotB)
		fakeClock.Step(launchbackoff.ProbeInterval)

		results, admitted := tracker.ReserveBatch(ctx, map[string][]cloudprovider.OfferingKey{
			"flexible":    {spotA, spotB},
			"constrained": {spotA},
		})

		Expect(admitted).To(BeTrue())
		Expect(results["constrained"].DebitedOfferings).To(Equal(1))
		Expect(results["flexible"].DebitedOfferings).To(Equal(1))
	})
})

var _ = Describe("Snapshots", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")

	It("reports immutable budget and unavailable-offering snapshots", func() {
		Expect(tracker.Budgets()).To(BeEmpty())
		Expect(tracker.UnavailableOfferings()).To(BeEmpty())

		tracker.Fail(ctx, "failure", spotA)
		snapshot := tracker.Budgets()
		Expect(snapshot).To(Equal(map[cloudprovider.OfferingKey]launchbackoff.OfferingBudget{
			spotA: {Burst: 1, Unavailable: true},
		}))
		Expect(tracker.UnavailableOfferings()).To(ConsistOf(spotA))

		snapshot[spotA] = launchbackoff.OfferingBudget{Burst: 99}
		Expect(tracker.Budgets()[spotA]).To(Equal(launchbackoff.OfferingBudget{Burst: 1, Unavailable: true}))

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Budgets()[spotA]).To(Equal(launchbackoff.OfferingBudget{Burst: 1, Unavailable: false}))
		Expect(tracker.UnavailableOfferings()).To(BeEmpty())
	})
})

var _ = Describe("Feature gate", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")

	It("records, filters, and debits nothing while disabled", func() {
		disabled := withGate(false)
		tracker.Fail(disabled, "ignored", spotA)
		Expect(tracker.Empty()).To(BeTrue())

		tracker.Fail(ctx, "enabled", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)
		result := tracker.Reserve(disabled, "disabled-reservation", []cloudprovider.OfferingKey{spotA})
		Expect(result.Admitted).To(BeTrue())
		Expect(result.DebitedOfferings).To(BeZero())
		Expect(tracker.Reserve(ctx, "enabled-reservation", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())

		instanceTypes := []*cloudprovider.InstanceType{
			fake.NewInstanceType("large", fake.WithOfferings(offering(v1.CapacityTypeSpot, "zone-a"))),
		}
		Expect(sharesBackingArray(launchbackoff.FilterUnavailable(disabled, instanceTypes, tracker), instanceTypes)).To(BeTrue())
	})
})

var _ = Describe("FilterUnavailable", func() {
	var instanceTypes []*cloudprovider.InstanceType

	BeforeEach(func() {
		instanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType("large", fake.WithOfferings(
				offering(v1.CapacityTypeSpot, "zone-a"),
				offering(v1.CapacityTypeSpot, "zone-b"),
				offering(v1.CapacityTypeOnDemand, "zone-a"),
			)),
			fake.NewInstanceType("small", fake.WithOfferings(
				offering(v1.CapacityTypeSpot, "zone-a"),
				unavailableOffering(v1.CapacityTypeOnDemand, "zone-a"),
			)),
		}
	})

	It("returns the original slice on every fast path", func() {
		Expect(sharesBackingArray(launchbackoff.FilterUnavailable(ctx, instanceTypes, nil), instanceTypes)).To(BeTrue())
		Expect(sharesBackingArray(launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker), instanceTypes)).To(BeTrue())

		tracker.Fail(ctx, "failure", key("large", v1.CapacityTypeSpot, "zone-a"))
		Expect(sharesBackingArray(launchbackoff.FilterUnavailable(withGate(false), instanceTypes, tracker), instanceTypes)).To(BeTrue())
	})

	It("deep copies only affected instance types before allocatable offerings are memoized", func() {
		backedOff := key("large", v1.CapacityTypeSpot, "zone-a")
		tracker.Fail(ctx, "failure", backedOff)

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(filtered).ToNot(BeIdenticalTo(instanceTypes))
		Expect(filtered[0]).ToNot(BeIdenticalTo(instanceTypes[0]))
		Expect(filtered[0].Offerings[0]).ToNot(BeIdenticalTo(instanceTypes[0].Offerings[0]))
		Expect(filtered[1]).To(BeIdenticalTo(instanceTypes[1]))
		Expect(availableKeys(filtered[0])).To(ConsistOf(
			key("large", v1.CapacityTypeSpot, "zone-b"),
			key("large", v1.CapacityTypeOnDemand, "zone-a"),
		))
		Expect(availableKeys(instanceTypes[0])).To(HaveLen(3), "provider-owned instance types must remain unchanged")

		var allocatable cloudprovider.Offerings
		for _, group := range filtered[0].AllocatableOfferingsList() {
			allocatable = append(allocatable, group.Offerings...)
		}
		Expect(allocatable).To(HaveLen(2))
		for _, candidate := range allocatable {
			Expect(candidate.Key(filtered[0].Name)).ToNot(Equal(backedOff))
		}
	})

	It("removes requirement values represented only by backed-off offerings", func() {
		tracker.Fail(ctx, "spot-zone-b", key("large", v1.CapacityTypeSpot, "zone-b"))
		tracker.Fail(ctx, "on-demand-zone-a", key("large", v1.CapacityTypeOnDemand, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(filtered[0].Requirements.Get(corev1.LabelTopologyZone).Values()).To(ConsistOf("zone-a"))
		Expect(filtered[0].Requirements.Get(v1.CapacityTypeLabelKey).Values()).To(ConsistOf(v1.CapacityTypeSpot))
		Expect(instanceTypes[0].Requirements.Get(corev1.LabelTopologyZone).Values()).To(ConsistOf("zone-a", "zone-b"))
		Expect(instanceTypes[0].Requirements.Get(v1.CapacityTypeLabelKey).Values()).To(ConsistOf(v1.CapacityTypeSpot, v1.CapacityTypeOnDemand))
	})

	It("does not copy an instance type when the provider already marked the tracked offering unavailable", func() {
		providerUnavailable := key("small", v1.CapacityTypeOnDemand, "zone-a")
		tracker.Fail(ctx, "failure", providerUnavailable)

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(filtered[1]).To(BeIdenticalTo(instanceTypes[1]))
		Expect(availableKeys(filtered[1])).To(ConsistOf(key("small", v1.CapacityTypeSpot, "zone-a")))
	})

	It("tolerates nil instance types", func() {
		tracker.Fail(ctx, "failure", key("large", v1.CapacityTypeSpot, "zone-a"))
		Expect(launchbackoff.FilterUnavailable(ctx, []*cloudprovider.InstanceType{nil}, tracker)).To(Equal([]*cloudprovider.InstanceType{nil}))
	})
})

var _ = Describe("CandidateOfferings", func() {
	It("derives unique provider-available candidates without mutating final NodeClaim requirements", func() {
		instanceTypes := []*cloudprovider.InstanceType{
			fake.NewInstanceType("large", fake.WithOfferings(
				offering(v1.CapacityTypeSpot, "zone-a"),
				offering(v1.CapacityTypeSpot, "zone-a"),
				offering(v1.CapacityTypeSpot, "zone-b"),
				offering(v1.CapacityTypeOnDemand, "zone-a"),
			)),
			nil,
			fake.NewInstanceType("small", fake.WithOfferings(
				offering(v1.CapacityTypeSpot, "zone-a"),
				unavailableOffering(v1.CapacityTypeSpot, "zone-b"),
			)),
			fake.NewInstanceType("excluded", fake.WithOfferings(offering(v1.CapacityTypeSpot, "zone-a"))),
		}
		nodeClaim := &v1.NodeClaim{
			Spec: v1.NodeClaimSpec{
				Requirements: []v1.NodeSelectorRequirementWithMinValues{
					{
						Key:       corev1.LabelInstanceTypeStable,
						Operator:  corev1.NodeSelectorOpIn,
						Values:    []string{"large", "small"},
						MinValues: ptr.To(2),
					},
					{
						Key:      v1.CapacityTypeLabelKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{v1.CapacityTypeSpot},
					},
					{
						Key:      corev1.LabelTopologyZone,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"zone-a"},
					},
				},
			},
		}
		before := nodeClaim.DeepCopy()

		candidates := launchbackoff.CandidateOfferings(nodeClaim, instanceTypes)

		Expect(candidates).To(ConsistOf(
			key("large", v1.CapacityTypeSpot, "zone-a"),
			key("small", v1.CapacityTypeSpot, "zone-a"),
		))
		Expect(nodeClaim).To(Equal(before))
		Expect(nodeClaim.Spec.Requirements[0].MinValues).ToNot(BeNil())
		Expect(*nodeClaim.Spec.Requirements[0].MinValues).To(Equal(2))
	})

	It("returns no candidates for a nil NodeClaim", func() {
		Expect(launchbackoff.CandidateOfferings(nil, nil)).To(BeNil())
	})
})

var _ = Describe("Concurrency", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")

	It("allows only one caller to spend a single refill", func() {
		tracker.Fail(ctx, "failure", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)

		var wg sync.WaitGroup
		var mu sync.Mutex
		admitted := 0
		for i := range 100 {
			wg.Add(1)
			go func(id int) {
				defer GinkgoRecover()
				defer wg.Done()
				if tracker.Reserve(ctx, fmt.Sprintf("concurrent-%d", id), []cloudprovider.OfferingKey{spotA}).Admitted {
					mu.Lock()
					admitted++
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()

		Expect(admitted).To(Equal(1))
	})

	It("makes a concurrent failure dominate success from the same epoch", func() {
		tracker.Fail(ctx, "initial-failure", spotA)
		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "ramp", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		tracker.Succeed(ctx, "ramp", spotA)

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Reserve(ctx, "success", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())
		Expect(tracker.Reserve(ctx, "failure", []cloudprovider.OfferingKey{spotA}).Admitted).To(BeTrue())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			tracker.Succeed(ctx, "success", spotA)
		}()
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			tracker.Fail(ctx, "failure", spotA)
		}()
		wg.Wait()

		Expect(tracker.Budgets()[spotA]).To(Equal(launchbackoff.OfferingBudget{Burst: 1, Unavailable: true}))
	})
})
