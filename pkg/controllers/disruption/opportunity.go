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

package disruption

import "math"

// EstimatedOpportunitySavings returns the existing optimistic command savings
// estimate only when every source and replacement price used by that estimate is
// present. It is intentionally metrics-only: consolidation behavior continues to
// use EstimatedSavings unchanged.
func (c Command) EstimatedOpportunitySavings() (float64, bool) {
	for _, candidate := range c.Candidates {
		if candidate == nil || !candidate.PriceKnown {
			return 0, false
		}
	}
	for _, nodeClaim := range c.Results.NewNodeClaims {
		if nodeClaim == nil || len(nodeClaim.InstanceTypeOptions) == 0 {
			return 0, false
		}
		offerings := nodeClaim.InstanceTypeOptions[0].Offerings.
			Available().
			Compatible(nodeClaim.Requirements)
		if len(offerings) == 0 {
			return 0, false
		}
	}
	savings := c.EstimatedSavings()
	if math.IsNaN(savings) || math.IsInf(savings, 0) {
		return 0, false
	}
	return savings, true
}

func completelyPricedSourceCost(candidates []*Candidate) (cost float64, priced int) {
	for _, candidate := range candidates {
		if candidate == nil || !candidate.PriceKnown {
			continue
		}
		cost += candidate.Price
		priced++
	}
	return cost, priced
}
