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

// Package launchbackoff bounds insufficient-capacity retries with a refill-window budget
// for each cloud-provider offering. NodeClaims reserve compatible offerings before creation
// and settle the reservation after the provider reports the actual outcome.
package launchbackoff

import (
	"context"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

const (
	ProbeInterval = 30 * time.Second
	BurstMax      = 8
	EntryTTL      = 10 * time.Minute

	defaultUnboundReservationTTL = 10 * time.Minute
)

type offeringEntry struct {
	incarnation uint64
	burst       int
	remaining   int
	nextRefill  time.Time
	generation  uint64
	epoch       uint64
	lastRamp    *uint64
	lastFailure *uint64
	lastActive  time.Time
}

type reservationStamp struct {
	incarnation uint64
	generation  uint64
	epoch       uint64
	debited     bool
}

type reservation struct {
	keys      map[cloudprovider.OfferingKey]reservationStamp
	expiresAt time.Time
	nodeClaim types.UID
}

type ReservationResult struct {
	Admitted         bool
	NextEligible     time.Time
	DebitedOfferings int
}

type OfferingBudget struct {
	Burst       int
	Unavailable bool
}

type Tracker struct {
	mu                    sync.RWMutex
	clock                 clock.Clock
	offerings             map[cloudprovider.OfferingKey]*offeringEntry
	reservations          map[string]*reservation
	nextIncarnation       uint64
	unboundReservationTTL time.Duration
}

func NewTracker(clk clock.Clock, unboundReservationTTL ...time.Duration) *Tracker {
	ttl := defaultUnboundReservationTTL
	if len(unboundReservationTTL) != 0 {
		ttl = unboundReservationTTL[0]
	}
	return &Tracker{
		clock:                 clk,
		offerings:             map[cloudprovider.OfferingKey]*offeringEntry{},
		reservations:          map[string]*reservation{},
		unboundReservationTTL: ttl,
	}
}

func enabled(ctx context.Context) bool {
	return options.FromContext(ctx).FeatureGates.LaunchBackoff
}

func (t *Tracker) Empty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.offerings) == 0
}

// IsAvailable is intentionally read-only. A due refill is treated as available, while the
// first Reserve that reaches it performs the actual refill under the write lock.
func (t *Tracker) IsAvailable(key cloudprovider.OfferingKey) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, ok := t.offerings[key]
	return !ok || entry.remaining > 0 || !t.clock.Now().Before(entry.nextRefill)
}

func (t *Tracker) NextEligible(key cloudprovider.OfferingKey) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	entry, ok := t.offerings[key]
	if !ok || entry.remaining > 0 || !now.Before(entry.nextRefill) {
		return now
	}
	return entry.nextRefill
}

func (t *Tracker) Reserve(ctx context.Context, id string, candidates []cloudprovider.OfferingKey) ReservationResult {
	if !enabled(ctx) {
		return ReservationResult{Admitted: true, NextEligible: t.clock.Now()}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cleanupLocked()
	return t.reserveLocked(id, candidates)
}

// ReserveBatch commits reservations only when every request can be admitted. Requests with
// fewer currently launchable candidates are evaluated first to avoid consuming a flexible
// request's allowance before a constrained replacement.
func (t *Tracker) ReserveBatch(ctx context.Context, candidates map[string][]cloudprovider.OfferingKey) (map[string]ReservationResult, bool) {
	if !enabled(ctx) {
		results := make(map[string]ReservationResult, len(candidates))
		for id := range candidates {
			results[id] = ReservationResult{Admitted: true, NextEligible: t.clock.Now()}
		}
		return results, true
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cleanupLocked()
	type request struct {
		id         string
		candidates []cloudprovider.OfferingKey
		launchable int
	}
	requests := make([]request, 0, len(candidates))
	for id, keys := range candidates {
		requests = append(requests, request{id: id, candidates: keys, launchable: t.launchableLocked(keys)})
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].launchable == requests[j].launchable {
			return requests[i].id < requests[j].id
		}
		return requests[i].launchable < requests[j].launchable
	})

	results := make(map[string]ReservationResult, len(candidates))
	var committed []string
	for _, request := range requests {
		result := t.reserveLocked(request.id, request.candidates)
		results[request.id] = result
		if !result.Admitted {
			for _, id := range committed {
				t.releaseLocked(id)
			}
			for _, batchRequest := range requests {
				batchResult := results[batchRequest.id]
				batchResult.Admitted = false
				batchResult.DebitedOfferings = 0
				batchResult.NextEligible = result.NextEligible
				results[batchRequest.id] = batchResult
			}
			return results, false
		}
		committed = append(committed, request.id)
	}
	return results, true
}

func (t *Tracker) reserveLocked(id string, candidates []cloudprovider.OfferingKey) ReservationResult {
	now := t.clock.Now()
	if len(t.offerings) == 0 {
		return ReservationResult{Admitted: true, NextEligible: now}
	}

	keys := uniqueKeys(candidates)
	if len(keys) == 0 {
		return ReservationResult{Admitted: false, NextEligible: now.Add(ProbeInterval)}
	}

	stamps := make(map[cloudprovider.OfferingKey]reservationStamp, len(keys))
	admitted := false
	nextEligible := time.Time{}
	debited := 0
	for _, key := range keys {
		entry, ok := t.offerings[key]
		if !ok {
			stamps[key] = reservationStamp{}
			admitted = true
			continue
		}
		t.refillLocked(entry, now)
		entry.lastActive = now
		stamp := reservationStamp{
			incarnation: entry.incarnation,
			generation:  entry.generation,
			epoch:       entry.epoch,
		}
		if entry.remaining > 0 {
			entry.remaining--
			stamp.debited = true
			admitted = true
			debited++
		} else if nextEligible.IsZero() || entry.nextRefill.Before(nextEligible) {
			nextEligible = entry.nextRefill
		}
		stamps[key] = stamp
	}
	if !admitted {
		return ReservationResult{Admitted: false, NextEligible: nextEligible}
	}
	t.reservations[id] = &reservation{
		keys:      stamps,
		expiresAt: now.Add(t.unboundReservationTTL),
	}
	return ReservationResult{Admitted: true, NextEligible: now, DebitedOfferings: debited}
}

func (t *Tracker) launchableLocked(candidates []cloudprovider.OfferingKey) int {
	now := t.clock.Now()
	launchable := 0
	for _, key := range uniqueKeys(candidates) {
		entry, ok := t.offerings[key]
		if !ok {
			launchable++
			continue
		}
		t.refillLocked(entry, now)
		if entry.remaining > 0 {
			launchable++
		}
	}
	return launchable
}

func (t *Tracker) Bind(id string, nodeClaim types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.reservations[id]; ok {
		r.nodeClaim = nodeClaim
		r.expiresAt = time.Time{}
	}
}

func (t *Tracker) Release(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.releaseLocked(id)
}

func (t *Tracker) releaseLocked(id string) {
	r, ok := t.reservations[id]
	if !ok {
		return
	}
	for key, stamp := range r.keys {
		t.refundLocked(key, stamp)
	}
	delete(t.reservations, id)
}

func (t *Tracker) Fail(ctx context.Context, id string, keys ...cloudprovider.OfferingKey) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock.Now()
	t.cleanupLocked()
	r, hasReservation := t.reservations[id]
	failed := uniqueKeys(keys)
	if len(failed) == 0 && hasReservation {
		failed = make([]cloudprovider.OfferingKey, 0, len(r.keys))
		for key := range r.keys {
			failed = append(failed, key)
		}
	}
	failedSet := make(map[cloudprovider.OfferingKey]struct{}, len(failed))
	for _, key := range failed {
		failedSet[key] = struct{}{}
	}
	if hasReservation {
		for key, stamp := range r.keys {
			if _, ok := failedSet[key]; !ok {
				t.refundLocked(key, stamp)
			}
		}
		delete(t.reservations, id)
	}
	for _, key := range failed {
		t.failLocked(key, now)
	}
}

func (t *Tracker) failLocked(key cloudprovider.OfferingKey, now time.Time) {
	entry, ok := t.offerings[key]
	if !ok {
		t.nextIncarnation++
		generation := uint64(0)
		t.offerings[key] = &offeringEntry{
			incarnation: t.nextIncarnation,
			burst:       1,
			remaining:   0,
			nextRefill:  now.Add(ProbeInterval),
			epoch:       1,
			lastFailure: &generation,
			lastActive:  now,
		}
		return
	}
	t.refillLocked(entry, now)
	entry.lastActive = now
	if entry.lastFailure == nil || *entry.lastFailure != entry.generation {
		entry.epoch++
		generation := entry.generation
		entry.lastFailure = &generation
	}
	if entry.burst != 1 || entry.remaining != 0 {
		entry.burst = 1
		entry.remaining = 0
		entry.nextRefill = now.Add(ProbeInterval)
	}
}

func (t *Tracker) Succeed(ctx context.Context, id string, landed cloudprovider.OfferingKey) {
	if !enabled(ctx) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cleanupLocked()
	r, ok := t.reservations[id]
	if !ok {
		return
	}
	for key, stamp := range r.keys {
		if key != landed {
			t.refundLocked(key, stamp)
		}
	}
	stamp, trackedCandidate := r.keys[landed]
	entry, trackedEntry := t.offerings[landed]
	if trackedCandidate && trackedEntry &&
		entry.incarnation == stamp.incarnation &&
		entry.generation == stamp.generation &&
		entry.epoch == stamp.epoch &&
		(entry.lastFailure == nil || *entry.lastFailure != entry.generation) &&
		(entry.lastRamp == nil || *entry.lastRamp != entry.generation) {
		entry.lastActive = t.clock.Now()
		if entry.burst*2 > BurstMax {
			delete(t.offerings, landed)
		} else {
			entry.burst *= 2
			generation := entry.generation
			entry.lastRamp = &generation
		}
	}
	delete(t.reservations, id)
}

func (t *Tracker) refundLocked(key cloudprovider.OfferingKey, stamp reservationStamp) {
	if !stamp.debited {
		return
	}
	entry, ok := t.offerings[key]
	if !ok ||
		entry.incarnation != stamp.incarnation ||
		entry.generation != stamp.generation ||
		entry.epoch != stamp.epoch {
		return
	}
	if entry.remaining < entry.burst {
		entry.remaining++
	}
}

func (t *Tracker) refillLocked(entry *offeringEntry, now time.Time) {
	if now.Before(entry.nextRefill) {
		return
	}
	entry.remaining = entry.burst
	entry.nextRefill = now.Add(ProbeInterval)
	entry.generation++
}

func (t *Tracker) Cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanupLocked()
}

// BoundReservations returns a snapshot used to reconcile reservations against live NodeClaims.
// ReleaseOrphanedBoundReservations checks the snapshot again before releasing so a reservation
// bound after this call cannot be removed based on an older API-server list.
func (t *Tracker) BoundReservations() map[string]types.UID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := map[string]types.UID{}
	for id, r := range t.reservations {
		if r.nodeClaim != "" {
			result[id] = r.nodeClaim
		}
	}
	return result
}

// ReleaseOrphanedBoundReservations releases reservations from the observed snapshot whose
// NodeClaim UIDs are no longer live.
func (t *Tracker) ReleaseOrphanedBoundReservations(observed map[string]types.UID, liveNodeClaims sets.Set[types.UID]) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, observedUID := range observed {
		r, ok := t.reservations[id]
		if ok && r.nodeClaim == observedUID && !liveNodeClaims.Has(observedUID) {
			t.releaseLocked(id)
		}
	}
}

// GC is retained as a compatibility alias for existing controller wiring while callers move
// to the reservation-oriented name.
func (t *Tracker) GC() {
	t.Cleanup()
}

func (t *Tracker) cleanupLocked() {
	now := t.clock.Now()
	for id, r := range t.reservations {
		if r.nodeClaim == "" && !r.expiresAt.IsZero() && !now.Before(r.expiresAt) {
			t.releaseLocked(id)
		}
	}
	for key, entry := range t.offerings {
		if !now.Before(entry.lastActive.Add(EntryTTL)) {
			delete(t.offerings, key)
		}
	}
}

func (t *Tracker) Budgets() map[cloudprovider.OfferingKey]OfferingBudget {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	result := make(map[cloudprovider.OfferingKey]OfferingBudget, len(t.offerings))
	for key, entry := range t.offerings {
		result[key] = OfferingBudget{
			Burst:       entry.burst,
			Unavailable: entry.remaining == 0 && now.Before(entry.nextRefill),
		}
	}
	return result
}

func (t *Tracker) UnavailableOfferings() []cloudprovider.OfferingKey {
	budgets := t.Budgets()
	keys := make([]cloudprovider.OfferingKey, 0, len(budgets))
	for key, budget := range budgets {
		if budget.Unavailable {
			keys = append(keys, key)
		}
	}
	return keys
}

func uniqueKeys(keys []cloudprovider.OfferingKey) []cloudprovider.OfferingKey {
	seen := make(map[cloudprovider.OfferingKey]struct{}, len(keys))
	result := make([]cloudprovider.OfferingKey, 0, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, key)
	}
	return result
}
