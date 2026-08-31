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

// Package launchbackoff bounds how often Karpenter re-attempts a launch that failed for
// insufficient capacity. It holds two kinds of state, both written only from observed launch
// outcomes:
//
//   - Per-offering backoff, which marks a capacity pool unavailable for an exponentially
//     growing window after an ICE. This is a filter: it decides whether an offering may be
//     tried at all, not how fast.
//   - Per-NodePool launch budgets, which decide how fast. An aggregate budget engages while a
//     pool is constrained by a recent failure; a risky budget always applies to NodeClaims
//     whose every usable offering has a failure history, including on a released pool.
//
// The tracker is shared, mutable state constructed once and injected by pointer into several
// independently concurrent controllers, so every method is safe for concurrent use. Reads
// never mutate: only Fail, Succeed, FailPool, SucceedPool, Admit, Delete, and GC change
// anything.
package launchbackoff

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

const (
	// BaseDelay is the nominal first backoff window for an offering. It is not chosen to
	// outlast any provider's own ICE cache: usability is the intersection of provider and core
	// availability, so the longer gate binds and at low levels that is the provider. What core
	// windows buy is escalation past a provider's fixed TTL at higher levels, and per-offering
	// isolation that a per-NodePool budget cannot express.
	BaseDelay = 30 * time.Second
	// MaxDelay is the ceiling on a single offering window, and also the idle period after
	// which an entry is discarded entirely.
	MaxDelay = 10 * time.Minute
	// ProbeInterval is the window length for both NodePool budgets. It is independent of
	// BaseDelay so that a pool with hundreds of backed-off offerings cannot emit hundreds of
	// launches when their windows line up.
	ProbeInterval = 30 * time.Second
	// BurstMax is the aggregate allowance a recovering pool must exceed before it is released
	// outright. It caps wasted launches after a spurious recovery.
	BurstMax = 8
	// RiskyBurst is how many launches per window are allowed for NodeClaims with no healthy
	// offering to land on. This budget deliberately does not ramp: recovery is expressed by
	// offering entries clearing, which makes such NodeClaims stop being risky.
	RiskyBurst = 1
	// maxLevel caps escalation so that the window shift cannot overflow. The window is capped
	// at MaxDelay well before this.
	maxLevel = 20
)

// offeringEntry is the backoff state for one capacity pool. An entry exists only for an
// offering that has failed at least once.
type offeringEntry struct {
	// level counts consecutive failed windows, not individual failures.
	level int
	// until is when the offering becomes eligible again. The entry itself is discarded at
	// until+MaxDelay, which is what keeps level meaning "recent" history.
	until time.Time
}

// poolEntry holds both launch budgets for one NodePool.
type poolEntry struct {
	// constrained reports whether the aggregate budget is engaged.
	constrained bool
	// burst is the allowance ceiling at the current recovery level. It changes only on
	// SucceedPool and FailPool, never on admission, so it graphs as a recovery ramp.
	burst int
	// remaining is the allowance left in the current window. Distinct from burst: consuming
	// burst directly would erode the ceiling.
	remaining int
	nextAdmit time.Time

	riskyRemaining int
	nextRiskyAdmit time.Time
}

type Tracker struct {
	mu        sync.RWMutex
	clock     clock.Clock
	offerings map[cloudprovider.OfferingKey]*offeringEntry
	pools     map[types.UID]*poolEntry
}

func NewTracker(clk clock.Clock) *Tracker {
	return &Tracker{
		clock:     clk,
		offerings: map[cloudprovider.OfferingKey]*offeringEntry{},
		pools:     map[types.UID]*poolEntry{},
	}
}

// enabled reports whether the LaunchBackoff feature gate is on.
//
// The gate is read per call rather than captured at construction so that it stays overridable
// through the context, which is how the rest of the codebase gates behavior and how tests
// flip features per spec. Every method that decides or mutates checks this, which keeps the
// gate out of the call sites: with the feature off a Tracker answers as though nothing has
// ever failed and records nothing.
func enabled(ctx context.Context) bool {
	return options.FromContext(ctx).FeatureGates.LaunchBackoff
}

// expired reports whether an entry has been eligible for long enough to discard. Expiry is
// evaluated lazily so that reads stay non-mutating; GC does the actual deletion.
func (t *Tracker) expired(e *offeringEntry, now time.Time) bool {
	return !now.Before(e.until.Add(MaxDelay))
}

// live returns the entry for a key if one exists and has not expired.
func (t *Tracker) live(key cloudprovider.OfferingKey, now time.Time) (*offeringEntry, bool) {
	e, ok := t.offerings[key]
	if !ok || t.expired(e, now) {
		return nil, false
	}
	return e, true
}

// Empty reports whether the tracker holds no offering state at all. FilterUnavailable uses
// this to return the provider's slice untouched on a cluster that has never failed a launch.
func (t *Tracker) Empty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.offerings) == 0
}

// IsAvailable reports whether core will allow an attempt on this offering. True when the
// offering has no history, when its history has expired, or when its window has elapsed.
//
// This is a clock comparison, not a reservation: it does not and cannot limit how many
// NodeClaims are launched onto a newly eligible offering. That bound comes from the risky
// budget in Admit.
func (t *Tracker) IsAvailable(key cloudprovider.OfferingKey) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	e, ok := t.live(key, now)
	if !ok {
		return true
	}
	return !now.Before(e.until)
}

// HasFailed reports whether this offering has unexpired failure history, whether or not its
// window has elapsed. This is a different question from IsAvailable, which is also true for
// an offering whose window just elapsed: admission ordering needs "never failed", not
// "allowed to try".
func (t *Tracker) HasFailed(key cloudprovider.OfferingKey) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, ok := t.live(key, t.clock.Now())
	return ok
}

// NextEligible returns when this offering becomes eligible for another attempt. For an
// offering with no live history, or one whose window has already elapsed, that is now.
func (t *Tracker) NextEligible(key cloudprovider.OfferingKey) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	e, ok := t.live(key, now)
	if !ok || !now.Before(e.until) {
		return now
	}
	return e.until
}

// Fail records an observed insufficient-capacity failure for an offering. It is idempotent
// inside a window: a burst of in-flight failures for the same key arms backoff once, so
// escalation tracks failed windows rather than individual attempts.
func (t *Tracker) Fail(ctx context.Context, key cloudprovider.OfferingKey) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock.Now()
	e, ok := t.live(key, now)
	if !ok {
		// Absent or expired: start over rather than resuming an old escalation.
		t.offerings[key] = &offeringEntry{level: 1, until: now.Add(window(1))}
		return
	}
	if now.Before(e.until) {
		return
	}
	if e.level < maxLevel {
		e.level++
	}
	e.until = now.Add(window(e.level))
}

// Succeed records an observed successful launch, clearing the offering's history. A NodeClaim
// that was risky because of this key stops being risky on the next scheduling loop, which is
// how recovery propagates without the risky budget needing a ramp.
func (t *Tracker) Succeed(ctx context.Context, key cloudprovider.OfferingKey) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.offerings, key)
}

// window returns the equal-jittered backoff for a given escalation level.
func window(level int) time.Duration {
	w := BaseDelay << (level - 1)
	if w > MaxDelay || w <= 0 {
		w = MaxDelay
	}
	half := w / 2
	if half <= 0 {
		return w
	}
	//nolint:gosec // jitter does not need a cryptographic source
	return half + time.Duration(rand.Int63n(int64(half)))
}

// IsConstrained reports whether a NodePool's aggregate budget is engaged. It is read-only:
// disruption peeks with this so that an opportunistic replacement never consumes a probe that
// pending pods are waiting for. Deliberately not named CanAdmit, because it is not a
// prediction of what Admit would return — it refuses a constrained pool even when window
// allowance remains.
func (t *Tracker) IsConstrained(ctx context.Context, nodePoolUID types.UID) bool {
	if !enabled(ctx) {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	p, ok := t.pools[nodePoolUID]
	return ok && p.constrained
}

// FailPool engages the aggregate budget for a NodePool at its floor. Idempotent inside a
// window, matching Fail.
func (t *Tracker) FailPool(ctx context.Context, nodePoolUID types.UID) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock.Now()
	p, ok := t.pools[nodePoolUID]
	if !ok {
		p = &poolEntry{}
		t.pools[nodePoolUID] = p
	}
	if p.constrained && now.Before(p.nextAdmit) {
		return
	}
	p.constrained = true
	p.burst = 1
	p.remaining = 1
	p.nextAdmit = now.Add(ProbeInterval)
}

// SucceedPool ramps a constrained NodePool's allowance. Doubling rather than releasing on the
// first success bounds the overshoot to BurstMax when capacity has only marginally returned,
// while still reaching full speed in a few windows when it genuinely has.
func (t *Tracker) SucceedPool(ctx context.Context, nodePoolUID types.UID) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	p, ok := t.pools[nodePoolUID]
	if !ok || !p.constrained {
		return
	}
	if p.burst*2 > BurstMax {
		p.constrained = false
		p.burst = 0
		p.remaining = 0
		return
	}
	p.burst *= 2
}

// rollover refills the windows that gate this admission and whose deadline has passed. Caller
// holds the write lock.
//
// Only applicable gates are touched: refreshing the risky window on every ordinary admission
// would keep a long-since-recovered NodePool's entry perpetually young, so GC would never
// reclaim it or its metric series.
func (t *Tracker) rollover(p *poolEntry, now time.Time, risky bool) {
	if p.constrained && !now.Before(p.nextAdmit) {
		p.remaining = p.burst
		p.nextAdmit = now.Add(ProbeInterval)
	}
	if risky && !now.Before(p.nextRiskyAdmit) {
		p.riskyRemaining = RiskyBurst
		p.nextRiskyAdmit = now.Add(ProbeInterval)
	}
}

// Admit decides whether one NodeClaim may be created for a NodePool, consuming allowance when
// it says yes. risky must be true when every usable offering in the NodeClaim's compatible set
// has failure history, meaning it has nowhere healthy to land.
//
// Every applicable gate is evaluated before any is consumed. Decrementing the aggregate
// allowance and then rejecting on the risky gate would burn a constrained pool's window on a
// launch that never happens, which one caller can use up on behalf of another.
func (t *Tracker) Admit(ctx context.Context, nodePoolUID types.UID, risky bool) bool {
	if !enabled(ctx) {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock.Now()
	p, ok := t.pools[nodePoolUID]
	if !ok {
		if !risky {
			// Nothing to enforce, and no reason to allocate state for a healthy pool.
			return true
		}
		// A pool that has never failed a launch itself can still have risky NodeClaims,
		// because offering entries are shared across NodePools.
		p = &poolEntry{}
		t.pools[nodePoolUID] = p
	}
	t.rollover(p, now, risky)

	if p.constrained && p.remaining == 0 {
		return false
	}
	if risky && p.riskyRemaining == 0 {
		return false
	}
	if p.constrained {
		p.remaining--
	}
	if risky {
		p.riskyRemaining--
	}
	return true
}

// NextAdmit returns when a NodeClaim rejected by Admit could next be admitted. It takes the
// same risky argument and returns the latest of the gates that apply, because waking at an
// earlier gate that was not what blocked this NodeClaim buys a loop that admits nothing.
func (t *Tracker) NextAdmit(nodePoolUID types.UID, risky bool) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	p, ok := t.pools[nodePoolUID]
	if !ok {
		return now
	}
	next := now
	if p.constrained && p.nextAdmit.After(next) {
		next = p.nextAdmit
	}
	if risky && p.nextRiskyAdmit.After(next) {
		next = p.nextRiskyAdmit
	}
	return next
}

// Burst reports a NodePool's current aggregate ceiling, for metrics. Zero means unconstrained.
func (t *Tracker) Burst(nodePoolUID types.UID) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if p, ok := t.pools[nodePoolUID]; ok && p.constrained {
		return p.burst
	}
	return 0
}

// Delete drops all budget state for a NodePool. Called when the NodePool is deleted so that
// entries and their metric series do not outlive it.
func (t *Tracker) Delete(nodePoolUID types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.pools, nodePoolUID)
}

// GC discards state that has gone quiet: offerings eligible for longer than MaxDelay, and
// NodePools whose budget windows have gone untouched for as long. Without this an offering
// that fails once and is never requested again keeps its level forever, so its next failure
// months later would start at the MaxDelay ceiling and its metric series would never be
// dropped.
//
// Quiet is the only deletion signal for NodePools, including deleted ones. A NodePool that no
// longer exists stops being admitted to, so its windows stop advancing and it ages out here.
// Reacting to the deletion event directly is not an option: the informer only has the object
// name once it is gone, and this map is keyed by UID precisely so that deleting and recreating
// a NodePool under the same name does not inherit its backoff.
//
// A constrained NodePool is reclaimed on the same terms rather than pinned, because after
// MaxDelay with nothing asking to launch, "this pool recently failed" is no longer true. The
// next failure re-arms it immediately, and this is the same staleness rule the offering
// windows use.
func (t *Tracker) GC() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock.Now()
	for key, e := range t.offerings {
		if t.expired(e, now) {
			delete(t.offerings, key)
		}
	}
	for uid, p := range t.pools {
		quietSince := p.nextRiskyAdmit
		if p.nextAdmit.After(quietSince) {
			quietSince = p.nextAdmit
		}
		if !now.Before(quietSince.Add(MaxDelay)) {
			delete(t.pools, uid)
		}
	}
}
