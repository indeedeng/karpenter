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

package launchbackoff

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

var _ = Describe("Tracker state invariants", func() {
	var (
		ctx context.Context
		clk *clocktesting.FakeClock
		t   *Tracker
		k   cloudprovider.OfferingKey
	)

	BeforeEach(func() {
		ctx = options.ToContext(context.Background(), &options.Options{
			FeatureGates: options.FeatureGates{LaunchBackoff: true},
		})
		clk = clocktesting.NewFakeClock(time.Now())
		t = NewTracker(clk)
		k = cloudprovider.OfferingKey{
			InstanceType: "large",
			CapacityType: v1.CapacityTypeSpot,
			Zone:         "zone-a",
		}
	})

	It("creates a closed burst-one entry on the first attributed failure", func() {
		t.Fail(ctx, "failure", k)

		entry := t.offerings[k]
		Expect(entry.incarnation).To(Equal(uint64(1)))
		Expect(entry.burst).To(Equal(1))
		Expect(entry.remaining).To(BeZero())
		Expect(entry.nextRefill).To(Equal(clk.Now().Add(ProbeInterval)))
		Expect(entry.generation).To(BeZero())
		Expect(entry.epoch).To(Equal(uint64(1)))
		Expect(entry.lastFailure).ToNot(BeNil())
		Expect(*entry.lastFailure).To(BeZero())
	})

	It("does not move the window or epoch for duplicate failures in one generation", func() {
		t.Fail(ctx, "failure-1", k)
		entry := t.offerings[k]
		nextRefill := entry.nextRefill
		epoch := entry.epoch

		for range 20 {
			t.Fail(ctx, "duplicate", k)
		}

		Expect(entry.nextRefill).To(Equal(nextRefill))
		Expect(entry.epoch).To(Equal(epoch))
		Expect(entry.burst).To(Equal(1))
		Expect(entry.remaining).To(BeZero())
	})

	It("ignores success and refund outcomes stamped with an older generation", func() {
		t.Fail(ctx, "initial-failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "ramp", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Succeed(ctx, "ramp", k)
		Expect(t.offerings[k].burst).To(Equal(2))

		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "stale-success", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		Expect(t.Reserve(ctx, "stale-release", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		oldGeneration := t.offerings[k].generation

		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "current", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		Expect(t.offerings[k].generation).To(Equal(oldGeneration + 1))
		Expect(t.offerings[k].remaining).To(Equal(1))

		t.Succeed(ctx, "stale-success", k)
		t.Release("stale-release")

		Expect(t.offerings[k].burst).To(Equal(2))
		Expect(t.offerings[k].remaining).To(Equal(1))
		Expect(t.reservations).ToNot(HaveKey("stale-success"))
		Expect(t.reservations).ToNot(HaveKey("stale-release"))
	})

	It("makes a newer failure epoch dominate older success and refund outcomes", func() {
		t.Fail(ctx, "initial-failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "ramp", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Succeed(ctx, "ramp", k)

		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "stale-success", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		Expect(t.Reserve(ctx, "stale-release", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		oldEpoch := t.offerings[k].epoch

		t.Fail(ctx, "new-failure", k)
		Expect(t.offerings[k].epoch).To(Equal(oldEpoch + 1))

		t.Succeed(ctx, "stale-success", k)
		t.Release("stale-release")

		Expect(t.offerings[k].burst).To(Equal(1))
		Expect(t.offerings[k].remaining).To(BeZero())
		Expect(t.offerings[k].nextRefill).To(Equal(clk.Now().Add(ProbeInterval)))
	})

	It("uses incarnation to prevent ABA refunds after entry deletion and recreation", func() {
		t.nextIncarnation = 1
		t.offerings[k] = &offeringEntry{
			incarnation: 1,
			burst:       1,
			remaining:   1,
			nextRefill:  clk.Now().Add(ProbeInterval),
			generation:  0,
			epoch:       1,
			lastActive:  clk.Now(),
		}
		Expect(t.Reserve(ctx, "old", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Bind("old", types.UID("old-nodeclaim"))
		oldStamp := t.reservations["old"].keys[k]

		clk.Step(EntryTTL)
		t.Cleanup()
		Expect(t.offerings).ToNot(HaveKey(k))
		Expect(t.reservations).To(HaveKey("old"), "binding must keep the old settlement alive")

		t.Fail(ctx, "new-failure", k)
		newEntry := t.offerings[k]
		Expect(newEntry.incarnation).ToNot(Equal(oldStamp.incarnation))
		Expect(newEntry.generation).To(Equal(oldStamp.generation))
		Expect(newEntry.epoch).To(Equal(oldStamp.epoch))

		t.Release("old")

		Expect(newEntry.remaining).To(BeZero(), "the stale debit must not refund the recreated entry")
	})

	It("expires and refunds unbound reservations at their TTL", func() {
		const reservationTTL = time.Minute
		t = NewTracker(clk, reservationTTL)
		t.Fail(ctx, "failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "unbound", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		Expect(t.offerings[k].remaining).To(BeZero())

		clk.Step(reservationTTL)
		t.Cleanup()

		Expect(t.reservations).ToNot(HaveKey("unbound"))
		Expect(t.offerings[k].remaining).To(Equal(1))
	})

	It("keeps bound reservations beyond the unbound TTL until settlement", func() {
		const reservationTTL = time.Minute
		t = NewTracker(clk, reservationTTL)
		t.Fail(ctx, "failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "bound", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Bind("bound", types.UID("nodeclaim"))

		clk.Step(reservationTTL)
		t.Cleanup()

		Expect(t.reservations).To(HaveKey("bound"))
		Expect(t.reservations["bound"].expiresAt).To(BeZero())
		Expect(t.reservations["bound"].nodeClaim).To(Equal(types.UID("nodeclaim")))
		Expect(t.offerings[k].remaining).To(BeZero())

		t.Release("bound")
		Expect(t.reservations).ToNot(HaveKey("bound"))
		Expect(t.offerings[k].remaining).To(Equal(1))
	})

	It("releases orphaned bound reservations without racing a newer binding", func() {
		t.Fail(ctx, "failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "bound", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Bind("bound", types.UID("old-nodeclaim"))
		observed := t.BoundReservations()

		t.Bind("bound", types.UID("new-nodeclaim"))
		t.ReleaseOrphanedBoundReservations(observed, sets.New[types.UID]())
		Expect(t.reservations).To(HaveKey("bound"))

		t.ReleaseOrphanedBoundReservations(t.BoundReservations(), sets.New[types.UID]())
		Expect(t.reservations).ToNot(HaveKey("bound"))
		Expect(t.offerings[k].remaining).To(Equal(1))
	})

	It("discards budgets and reservations together across restart", func() {
		t.Fail(ctx, "failure", k)
		clk.Step(ProbeInterval)
		Expect(t.Reserve(ctx, "bound", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
		t.Bind("bound", types.UID("nodeclaim"))

		restarted := NewTracker(clk)
		Expect(restarted.Empty()).To(BeTrue())
		Expect(restarted.BoundReservations()).To(BeEmpty())
		Expect(restarted.Reserve(ctx, "after-restart", []cloudprovider.OfferingKey{k}).Admitted).To(BeTrue())
	})

	It("does not allocate reservation bookkeeping on the empty-tracker fast path", func() {
		result := t.Reserve(ctx, "healthy", []cloudprovider.OfferingKey{k})

		Expect(result.Admitted).To(BeTrue())
		Expect(result.DebitedOfferings).To(BeZero())
		Expect(t.reservations).To(BeEmpty())
	})
})
