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

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// FilterUnavailable returns the instance types with backed-off offerings marked unavailable.
//
// It reuses the existing Available flag rather than introducing a parallel notion of
// usability, so every consumer that already respects Available — instance type selection,
// price ordering, topology domain counting — honors backoff without changing. Usability is
// therefore the intersection of the provider's view and core's: neither can resurrect an
// offering the other has excluded.
//
// The result must be produced before anything calls AllocatableOfferingsList or Allocatable on
// these instance types, because those memoize the available offerings behind a sync.Once. This
// is also why an affected instance type is deep copied rather than mutated: the provider's
// cached instance types are shared across NodePools and reconciles, and a copy resets the
// memoization so precompute observes the filtered flags.
//
// A disabled feature gate, a nil tracker, or one holding no offering state returns the input
// slice unmodified. That is the common case — a cluster that has not failed a launch recently —
// and it must cost nothing. The gate is checked once here rather than per offering, so the
// per-offering reads below stay lock-cheap.
func FilterUnavailable(ctx context.Context, instanceTypes []*cloudprovider.InstanceType, t *Tracker) []*cloudprovider.InstanceType {
	if t == nil || !enabled(ctx) || t.Empty() {
		return instanceTypes
	}
	filtered := make([]*cloudprovider.InstanceType, len(instanceTypes))
	for i, it := range instanceTypes {
		filtered[i] = filterInstanceType(it, t)
	}
	return filtered
}

// IsRisky reports whether a launch drawing on these instance types has nowhere untried to go —
// every offering still usable has failed within recent memory. Such a launch is the one most likely
// to fail again, so it is charged to the risky budget and admitted at most RiskyBurst per window
// even on a NodePool the aggregate budget has released.
//
// This asks "has it ever failed" rather than "is it available now", because an offering whose window
// has elapsed is precisely a probe candidate: it is usable again, but nothing has confirmed capacity
// came back. Treating it as healthy is what would let a recovered pool launch a whole pending batch
// into a zone that is still dead.
//
// Reports false for an empty offering set. Vacuous truth would classify a NodeClaim with nothing to
// evaluate — a static one, say — as risky and throttle it for no reason.
func (t *Tracker) IsRisky(ctx context.Context, instanceTypes []*cloudprovider.InstanceType) bool {
	if !enabled(ctx) || t.Empty() {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	usable := 0
	for _, it := range instanceTypes {
		if it == nil {
			continue
		}
		for _, o := range it.Offerings {
			if !o.Available {
				continue
			}
			usable++
			if _, ok := t.live(o.Key(it.Name), now); !ok {
				return false
			}
		}
	}
	return usable > 0
}

// filterInstanceType returns it unchanged when none of its offerings are backed off, so that
// unaffected instance types keep their identity and their already-computed allocatables.
func filterInstanceType(it *cloudprovider.InstanceType, t *Tracker) *cloudprovider.InstanceType {
	if it == nil {
		return nil
	}
	var backedOff []int
	for i, o := range it.Offerings {
		if o.Available && !t.IsAvailable(o.Key(it.Name)) {
			backedOff = append(backedOff, i)
		}
	}
	if len(backedOff) == 0 {
		return it
	}
	filtered := it.DeepCopy()
	for _, i := range backedOff {
		filtered.Offerings[i].Available = false
	}
	return filtered
}
