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
	"sort"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// Preview predicts ReserveBatch admissions against a point-in-time copy of offering budgets
// without holding reservations. Admitted batches debit the preview, so later batches contend
// for the budget that earlier ones will consume when they actually reserve.
//
// Unbound reservations that would expire before the real reservation are not refunded, so a
// preview can reject a batch that ReserveBatch would admit, but not the reverse.
type Preview struct {
	remaining map[cloudprovider.OfferingKey]int
}

func (t *Tracker) Preview() *Preview {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock.Now()
	remaining := make(map[cloudprovider.OfferingKey]int, len(t.offerings))
	for key, entry := range t.offerings {
		if !now.Before(entry.lastActive.Add(EntryTTL)) {
			continue
		}
		if now.Before(entry.nextRefill) {
			remaining[key] = entry.remaining
		} else {
			remaining[key] = entry.burst
		}
	}
	return &Preview{remaining: remaining}
}

// AdmitBatch reports whether every request would be admitted, debiting the preview only when
// the whole batch is admitted.
func (p *Preview) AdmitBatch(requests [][]cloudprovider.OfferingKey) bool {
	if len(p.remaining) == 0 {
		return true
	}
	type request struct {
		keys       []cloudprovider.OfferingKey
		launchable int
	}
	ordered := make([]request, 0, len(requests))
	for _, keys := range requests {
		keys = uniqueKeys(keys)
		ordered = append(ordered, request{keys: keys, launchable: p.launchable(keys)})
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].launchable < ordered[j].launchable })

	debits := map[cloudprovider.OfferingKey]int{}
	for _, r := range ordered {
		admitted := false
		for _, key := range r.keys {
			remaining, tracked := p.remaining[key]
			if !tracked {
				admitted = true
				continue
			}
			if remaining-debits[key] > 0 {
				debits[key]++
				admitted = true
			}
		}
		if !admitted {
			return false
		}
	}
	for key, n := range debits {
		p.remaining[key] -= n
	}
	return true
}

func (p *Preview) launchable(keys []cloudprovider.OfferingKey) int {
	launchable := 0
	for _, key := range keys {
		if remaining, tracked := p.remaining[key]; !tracked || remaining > 0 {
			launchable++
		}
	}
	return launchable
}
