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
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

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
	nodePool  types.UID
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LaunchBackoff")
}

// withGate returns a context carrying the LaunchBackoff feature gate. Building the options
// directly rather than through pkg/test keeps this package free of a dependency on the test
// harness, which will import the controllers that consume the tracker.
func withGate(launchBackoff bool) context.Context {
	return options.ToContext(context.Background(), &options.Options{
		FeatureGates: options.FeatureGates{LaunchBackoff: launchBackoff},
	})
}

var _ = BeforeEach(func() {
	ctx = withGate(true)
	fakeClock = clocktesting.NewFakeClock(time.Now())
	tracker = launchbackoff.NewTracker(fakeClock)
	nodePool = types.UID("nodepool-a")
})

func key(instanceType, capacityType, zone string) cloudprovider.OfferingKey {
	return cloudprovider.OfferingKey{InstanceType: instanceType, CapacityType: capacityType, Zone: zone}
}

// offering builds an available offering for the given capacity type and zone.
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

// unavailableOffering builds an offering the provider itself has already excluded.
func unavailableOffering(capacityType, zone string) cloudprovider.Offering {
	o := offering(capacityType, zone)
	o.Available = false
	return o
}

// sharesBackingArray reports whether two slices are the same slice, which is how the filter's
// zero-copy fast path is distinguished from an equal-valued copy.
func sharesBackingArray(a, b []*cloudprovider.InstanceType) bool {
	return len(a) == len(b) && len(a) > 0 && &a[0] == &b[0]
}

// availableKeys returns the keys of an instance type's still-available offerings.
func availableKeys(it *cloudprovider.InstanceType) []cloudprovider.OfferingKey {
	keys := []cloudprovider.OfferingKey{}
	for _, o := range it.Offerings {
		if o.Available {
			keys = append(keys, o.Key(it.Name))
		}
	}
	return keys
}

var _ = Describe("Offering backoff", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")
	spotB := key("large", v1.CapacityTypeSpot, "zone-b")

	It("should treat an offering with no history as available and not failed", func() {
		Expect(tracker.IsAvailable(spotA)).To(BeTrue())
		Expect(tracker.HasFailed(spotA)).To(BeFalse())
		Expect(tracker.NextEligible(spotA)).To(Equal(fakeClock.Now()))
	})
	It("should mark only the failed offering unavailable", func() {
		tracker.Fail(ctx, spotA)

		Expect(tracker.IsAvailable(spotA)).To(BeFalse())
		Expect(tracker.IsAvailable(spotB)).To(BeTrue())
		Expect(tracker.HasFailed(spotB)).To(BeFalse())
	})
	It("should open the first window within the jitter range of BaseDelay", func() {
		tracker.Fail(ctx, spotA)

		// Equal jitter: the window lands in [BaseDelay/2, BaseDelay).
		Expect(tracker.NextEligible(spotA)).To(BeTemporally(">=", fakeClock.Now().Add(launchbackoff.BaseDelay/2)))
		Expect(tracker.NextEligible(spotA)).To(BeTemporally("<", fakeClock.Now().Add(launchbackoff.BaseDelay)))
	})
	It("should become available again once the window elapses, while retaining failure history", func() {
		tracker.Fail(ctx, spotA)
		fakeClock.Step(launchbackoff.BaseDelay)

		// Probe-eligible: allowed to try, but still known to have failed. Admission ordering
		// depends on these two being different questions.
		Expect(tracker.IsAvailable(spotA)).To(BeTrue())
		Expect(tracker.HasFailed(spotA)).To(BeTrue())
		Expect(tracker.NextEligible(spotA)).To(Equal(fakeClock.Now()))
	})
	It("should escalate the window on a failure in a later window", func() {
		tracker.Fail(ctx, spotA)
		first := tracker.NextEligible(spotA).Sub(fakeClock.Now())

		fakeClock.Step(launchbackoff.BaseDelay)
		tracker.Fail(ctx, spotA)
		second := tracker.NextEligible(spotA).Sub(fakeClock.Now())

		Expect(second).To(BeNumerically(">=", launchbackoff.BaseDelay))
		Expect(second).To(BeNumerically("<", 2*launchbackoff.BaseDelay))
		Expect(second).To(BeNumerically(">", first))
	})
	It("should not escalate on repeated failures inside the same window", func() {
		tracker.Fail(ctx, spotA)
		until := tracker.NextEligible(spotA)

		// A large scale-out produces many concurrent failures for one offering. Escalation must
		// track failed windows, or a single batch would jump straight to the ceiling.
		for range 20 {
			tracker.Fail(ctx, spotA)
		}

		Expect(tracker.NextEligible(spotA)).To(Equal(until))
		Expect(tracker.NextEligible(spotA)).To(BeTemporally("<", fakeClock.Now().Add(launchbackoff.BaseDelay)))
	})
	It("should cap the window at MaxDelay", func() {
		for range 12 {
			tracker.Fail(ctx, spotA)
			fakeClock.Step(launchbackoff.MaxDelay)
		}
		tracker.Fail(ctx, spotA)

		Expect(tracker.NextEligible(spotA)).To(BeTemporally(">=", fakeClock.Now().Add(launchbackoff.MaxDelay/2)))
		Expect(tracker.NextEligible(spotA)).To(BeTemporally("<=", fakeClock.Now().Add(launchbackoff.MaxDelay)))
	})
	It("should clear history on a successful launch", func() {
		tracker.Fail(ctx, spotA)
		tracker.Succeed(ctx, spotA)

		Expect(tracker.IsAvailable(spotA)).To(BeTrue())
		Expect(tracker.HasFailed(spotA)).To(BeFalse())
	})
	It("should restart escalation after a success rather than resuming the old level", func() {
		tracker.Fail(ctx, spotA)
		fakeClock.Step(launchbackoff.BaseDelay)
		tracker.Fail(ctx, spotA)
		tracker.Succeed(ctx, spotA)
		tracker.Fail(ctx, spotA)

		Expect(tracker.NextEligible(spotA)).To(BeTemporally("<", fakeClock.Now().Add(launchbackoff.BaseDelay)))
	})
	It("should discard history that has been quiet for MaxDelay", func() {
		tracker.Fail(ctx, spotA)
		Expect(tracker.HasFailed(spotA)).To(BeTrue())

		fakeClock.Step(launchbackoff.BaseDelay + launchbackoff.MaxDelay)

		// Without decay, an offering that failed once and was never requested again would come
		// back at the ceiling on its next failure, months later.
		Expect(tracker.HasFailed(spotA)).To(BeFalse())
		Expect(tracker.Empty()).To(BeFalse(), "expiry should be lazy so that reads do not mutate")

		tracker.GC()
		Expect(tracker.Empty()).To(BeTrue())

		tracker.Fail(ctx, spotA)
		Expect(tracker.NextEligible(spotA)).To(BeTemporally("<", fakeClock.Now().Add(launchbackoff.BaseDelay)))
	})
	It("should not mutate state on reads", func() {
		tracker.Fail(ctx, spotA)
		until := tracker.NextEligible(spotA)

		for range 5 {
			tracker.IsAvailable(spotA)
			tracker.HasFailed(spotA)
			tracker.NextEligible(spotA)
			tracker.IsConstrained(ctx, nodePool)
		}

		Expect(tracker.NextEligible(spotA)).To(Equal(until))
	})
})

var _ = Describe("NodePool aggregate budget", func() {
	It("should be unconstrained and admit freely with no history", func() {
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())
		for range 100 {
			Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		}
	})
	It("should engage at an allowance of one on failure", func() {
		tracker.FailPool(ctx, nodePool)

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeTrue())
		Expect(tracker.Burst(nodePool)).To(Equal(1))
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeFalse())
	})
	It("should refill the allowance each window", func() {
		tracker.FailPool(ctx, nodePool)
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeFalse())

		fakeClock.Step(launchbackoff.ProbeInterval)

		Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeFalse())
	})
	It("should not reset the window on a repeated failure inside it", func() {
		tracker.FailPool(ctx, nodePool)
		fakeClock.Step(launchbackoff.ProbeInterval)
		tracker.SucceedPool(ctx, nodePool)
		Expect(tracker.Burst(nodePool)).To(Equal(2))

		tracker.FailPool(ctx, nodePool)
		Expect(tracker.Burst(nodePool)).To(Equal(1))
		next := tracker.NextAdmit(nodePool, false)
		tracker.FailPool(ctx, nodePool)

		Expect(tracker.NextAdmit(nodePool, false)).To(Equal(next), "a second failure in the window should not extend it")
	})
	It("should ramp the allowance geometrically as launches succeed", func() {
		tracker.FailPool(ctx, nodePool)
		Expect(tracker.Burst(nodePool)).To(Equal(1))

		for _, expected := range []int{2, 4, 8} {
			tracker.SucceedPool(ctx, nodePool)
			Expect(tracker.Burst(nodePool)).To(Equal(expected))
		}

		// A recovering pool must not jump from one launch per window to unlimited, or the burst
		// that caused the original ICE storm simply happens again.
		fakeClock.Step(launchbackoff.ProbeInterval)
		admitted := 0
		for range 100 {
			if tracker.Admit(ctx, nodePool, false) {
				admitted++
			}
		}
		Expect(admitted).To(Equal(launchbackoff.BurstMax))
	})
	It("should release the pool once the allowance would exceed BurstMax", func() {
		tracker.FailPool(ctx, nodePool)
		for range 4 {
			tracker.SucceedPool(ctx, nodePool)
		}

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())
		Expect(tracker.Burst(nodePool)).To(Equal(0))
		for range 100 {
			Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		}
	})
	It("should ignore a success on a pool that is not constrained", func() {
		tracker.SucceedPool(ctx, nodePool)

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())
		Expect(tracker.Burst(nodePool)).To(Equal(0))
	})
	It("should track budgets per NodePool", func() {
		other := types.UID("nodepool-b")
		tracker.FailPool(ctx, nodePool)

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeTrue())
		Expect(tracker.IsConstrained(ctx, other)).To(BeFalse())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeFalse())
		Expect(tracker.Admit(ctx, other, false)).To(BeTrue())
	})
	It("should drop budget state for a deleted NodePool", func() {
		tracker.FailPool(ctx, nodePool)
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeTrue())

		tracker.Delete(nodePool)

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())
	})
	It("should keep a constrained NodePool while its window is live", func() {
		tracker.FailPool(ctx, nodePool)
		fakeClock.Step(launchbackoff.ProbeInterval)
		tracker.GC()

		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeTrue())
	})
	It("should reclaim a constrained NodePool that has gone quiet", func() {
		tracker.FailPool(ctx, nodePool)
		fakeClock.Step(launchbackoff.ProbeInterval + launchbackoff.MaxDelay)
		tracker.GC()

		// This is what makes a deleted NodePool's state go away, since the informer cannot
		// supply the UID once the object is gone. After MaxDelay with nothing trying to launch,
		// "recently failed" is no longer true of a NodePool that still exists either.
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())
	})
})

var _ = Describe("NodePool risky budget", func() {
	It("should throttle risky NodeClaims on a pool that has never failed", func() {
		// Offering entries are shared across NodePools, so a pool with no failure history of
		// its own can still have NodeClaims with nowhere healthy to land.
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())

		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())
	})
	It("should not throttle non-risky NodeClaims on an unconstrained pool", func() {
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())

		for range 100 {
			Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())
		}
	})
	It("should refill the risky allowance each window without ramping", func() {
		for range 3 {
			Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
			Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())
			fakeClock.Step(launchbackoff.ProbeInterval)
		}
	})
	It("should keep throttling risky NodeClaims after the pool is released", func() {
		tracker.FailPool(ctx, nodePool)
		for range 4 {
			tracker.SucceedPool(ctx, nodePool)
		}
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeFalse())

		// The aggregate budget is gone but a NodeClaim whose every usable offering has failed
		// still has nowhere to land, so it must stay bounded.
		fakeClock.Step(launchbackoff.ProbeInterval)
		admitted := 0
		for range 100 {
			if tracker.Admit(ctx, nodePool, true) {
				admitted++
			}
		}
		Expect(admitted).To(Equal(launchbackoff.RiskyBurst))
	})
	It("should consume nothing when it rejects on the risky gate", func() {
		tracker.FailPool(ctx, nodePool)
		for range 3 {
			tracker.SucceedPool(ctx, nodePool)
		}
		Expect(tracker.Burst(nodePool)).To(Equal(8))
		fakeClock.Step(launchbackoff.ProbeInterval)

		// Spend the single risky allowance, then have a risky NodeClaim rejected.
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())

		// The rejected claim must not have burned aggregate allowance on a launch that never
		// happened. Seven of the eight remain: the admitted risky claim took one.
		admitted := 0
		for range 100 {
			if tracker.Admit(ctx, nodePool, false) {
				admitted++
			}
		}
		Expect(admitted).To(Equal(launchbackoff.BurstMax - 1))
	})
	It("should consume nothing when it rejects on the aggregate gate", func() {
		tracker.FailPool(ctx, nodePool)
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeTrue())

		// Aggregate is exhausted, so this risky claim is rejected and must leave the untouched
		// risky allowance for the next window's probe.
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())

		fakeClock.Step(launchbackoff.ProbeInterval)
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
	})
	It("should require both gates for a risky NodeClaim on a constrained pool", func() {
		tracker.FailPool(ctx, nodePool)

		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeFalse())
		Expect(tracker.Admit(ctx, nodePool, false)).To(BeFalse())
	})
})

var _ = Describe("NextAdmit", func() {
	It("should return now for a NodeClaim that is not gated", func() {
		Expect(tracker.NextAdmit(nodePool, false)).To(Equal(fakeClock.Now()))
	})
	It("should return the aggregate window for a non-risky NodeClaim", func() {
		tracker.FailPool(ctx, nodePool)

		Expect(tracker.NextAdmit(nodePool, false)).To(Equal(fakeClock.Now().Add(launchbackoff.ProbeInterval)))
	})
	It("should ignore the risky window for a non-risky NodeClaim", func() {
		// Arrange a risky window that closes later than the aggregate one.
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())
		fakeClock.Step(launchbackoff.ProbeInterval / 2)
		tracker.FailPool(ctx, nodePool)

		aggregate := fakeClock.Now().Add(launchbackoff.ProbeInterval)
		Expect(tracker.NextAdmit(nodePool, false)).To(Equal(aggregate))
		// Waking at the aggregate gate would admit nothing for a risky claim still inside its
		// own window, so it must report the later of the two.
		Expect(tracker.NextAdmit(nodePool, true)).To(BeTemporally(">=", aggregate))
	})
	It("should return the risky window for a risky NodeClaim on an unconstrained pool", func() {
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())

		Expect(tracker.NextAdmit(nodePool, true)).To(Equal(fakeClock.Now().Add(launchbackoff.ProbeInterval)))
		Expect(tracker.NextAdmit(nodePool, false)).To(Equal(fakeClock.Now()))
	})
	It("should return the later gate for a risky NodeClaim on a constrained pool", func() {
		tracker.FailPool(ctx, nodePool)
		fakeClock.Step(launchbackoff.ProbeInterval / 2)
		Expect(tracker.Admit(ctx, nodePool, true)).To(BeTrue())

		Expect(tracker.NextAdmit(nodePool, true)).To(Equal(fakeClock.Now().Add(launchbackoff.ProbeInterval)))
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
				offering(v1.CapacityTypeOnDemand, "zone-a"),
			)),
		}
	})

	It("should return the input untouched when the tracker is nil", func() {
		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, nil)

		Expect(sharesBackingArray(filtered, instanceTypes)).To(BeTrue())
	})
	It("should return the input untouched when no offering has failed", func() {
		// The overwhelming majority of reconciles hit this path, so it must not copy anything.
		tracker.FailPool(ctx, nodePool)

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(sharesBackingArray(filtered, instanceTypes)).To(BeTrue())
	})
	It("should clear Available on backed-off offerings only", func() {
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(availableKeys(filtered[0])).To(ConsistOf(
			key("large", v1.CapacityTypeSpot, "zone-b"),
			key("large", v1.CapacityTypeOnDemand, "zone-a"),
		))
		// Backoff is scoped to the offering, not the instance type or the zone: spot in zone-a
		// failing for "large" says nothing about "small" or about on-demand.
		Expect(availableKeys(filtered[1])).To(ConsistOf(
			key("small", v1.CapacityTypeSpot, "zone-a"),
			key("small", v1.CapacityTypeOnDemand, "zone-a"),
		))
	})
	It("should not mutate the provider's instance types", func() {
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		// The provider caches these across NodePools and reconciles. Mutating them in place
		// would leak one NodePool's backoff into every other consumer.
		Expect(availableKeys(instanceTypes[0])).To(HaveLen(3))
	})
	It("should preserve the identity of unaffected instance types", func() {
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(filtered[0]).ToNot(BeIdenticalTo(instanceTypes[0]))
		Expect(filtered[1]).To(BeIdenticalTo(instanceTypes[1]))
	})
	It("should be reflected in the copy's precomputed allocatable offerings", func() {
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		// Allocatables are memoized behind a sync.Once over the available offerings, so this is
		// the assertion that the filter actually reaches instance type selection rather than
		// being read after the memoization has already been taken.
		var offerings cloudprovider.Offerings
		for _, group := range filtered[0].AllocatableOfferingsList() {
			offerings = append(offerings, group.Offerings...)
		}
		Expect(offerings).To(HaveLen(2))
		for _, o := range offerings {
			Expect(o.Key(filtered[0].Name)).ToNot(Equal(key("large", v1.CapacityTypeSpot, "zone-a")))
		}
	})
	It("should restore an offering once its window elapses", func() {
		k := key("large", v1.CapacityTypeSpot, "zone-a")
		tracker.Fail(ctx, k)
		Expect(availableKeys(launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)[0])).ToNot(ContainElement(k))

		fakeClock.Step(launchbackoff.BaseDelay)

		Expect(availableKeys(launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)[0])).To(ContainElement(k))
	})
	It("should leave an offering the provider already marked unavailable alone", func() {
		instanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType("large", fake.WithOfferings(
				unavailableOffering(v1.CapacityTypeSpot, "zone-a"),
				offering(v1.CapacityTypeOnDemand, "zone-a"),
			)),
		}
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		// Nothing to clear, so no copy is warranted either.
		Expect(filtered[0]).To(BeIdenticalTo(instanceTypes[0]))
		Expect(availableKeys(filtered[0])).To(ConsistOf(key("large", v1.CapacityTypeOnDemand, "zone-a")))
	})
	It("should clear every offering of a fully backed-off instance type", func() {
		tracker.Fail(ctx, key("small", v1.CapacityTypeSpot, "zone-a"))
		tracker.Fail(ctx, key("small", v1.CapacityTypeOnDemand, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, instanceTypes, tracker)

		Expect(availableKeys(filtered[1])).To(BeEmpty())
	})
	It("should tolerate nil instance types", func() {
		tracker.Fail(ctx, key("large", v1.CapacityTypeSpot, "zone-a"))

		filtered := launchbackoff.FilterUnavailable(ctx, []*cloudprovider.InstanceType{nil}, tracker)

		Expect(filtered).To(HaveLen(1))
		Expect(filtered[0]).To(BeNil())
	})
})

var _ = Describe("Feature gate", func() {
	spotA := key("large", v1.CapacityTypeSpot, "zone-a")

	It("should record nothing while disabled", func() {
		off := withGate(false)

		tracker.Fail(off, spotA)
		tracker.FailPool(off, nodePool)

		Expect(tracker.Empty()).To(BeTrue())
		Expect(tracker.IsAvailable(spotA)).To(BeTrue())
	})
	It("should answer permissively while disabled even with state already recorded", func() {
		// State can outlive a gate flip in tests and in a rollback. Answering from it would
		// throttle a cluster that has turned the feature off.
		tracker.Fail(ctx, spotA)
		tracker.FailPool(ctx, nodePool)
		Expect(tracker.IsConstrained(ctx, nodePool)).To(BeTrue())

		off := withGate(false)

		Expect(tracker.IsConstrained(off, nodePool)).To(BeFalse())
		for range 100 {
			Expect(tracker.Admit(off, nodePool, true)).To(BeTrue())
		}
	})
	It("should not filter instance types while disabled", func() {
		instanceTypes := []*cloudprovider.InstanceType{
			fake.NewInstanceType("large", fake.WithOfferings(offering(v1.CapacityTypeSpot, "zone-a"))),
		}
		tracker.Fail(ctx, spotA)

		filtered := launchbackoff.FilterUnavailable(withGate(false), instanceTypes, tracker)

		Expect(sharesBackingArray(filtered, instanceTypes)).To(BeTrue())
	})
})

var _ = Describe("Concurrency", func() {
	It("should record concurrent failures for one offering as a single window", func() {
		k := key("large", v1.CapacityTypeSpot, "zone-a")
		var wg sync.WaitGroup
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tracker.Fail(ctx, k)
			}()
		}
		wg.Wait()

		// Idempotence inside a window has to hold under the concurrency it exists to handle:
		// launch failures for one offering arrive from many workers at once.
		Expect(tracker.NextEligible(k)).To(BeTemporally("<", fakeClock.Now().Add(launchbackoff.BaseDelay)))
	})
	It("should not admit more than the allowance under concurrent callers", func() {
		tracker.FailPool(ctx, nodePool)
		for range 3 {
			tracker.SucceedPool(ctx, nodePool)
		}
		fakeClock.Step(launchbackoff.ProbeInterval)

		// Two provisioners share the tracker, so the budget is only meaningful if concurrent
		// Admit calls cannot both consume the same allowance.
		var mu sync.Mutex
		var wg sync.WaitGroup
		admitted := 0
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if tracker.Admit(ctx, nodePool, false) {
					mu.Lock()
					admitted++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		Expect(admitted).To(Equal(launchbackoff.BurstMax))
	})
})
