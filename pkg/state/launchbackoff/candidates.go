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
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// CandidateOfferings expands a finalized NodeClaim against the provider's unmodified
// instance types. Callers must not pass the copies returned by FilterUnavailable: those
// intentionally hide exhausted core budgets which the emitted NodeClaim may still represent.
func CandidateOfferings(nodeClaim *v1.NodeClaim, instanceTypes []*cloudprovider.InstanceType) []cloudprovider.OfferingKey {
	if nodeClaim == nil {
		return nil
	}
	requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	var candidates []cloudprovider.OfferingKey
	for _, instanceType := range instanceTypes {
		if instanceType == nil ||
			instanceType.Requirements.Intersects(requirements) != nil {
			continue
		}
		for _, offering := range instanceType.Offerings {
			if !offering.Available ||
				!requirements.IsCompatible(offering.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
				continue
			}
			candidates = append(candidates, offering.Key(instanceType.Name))
		}
	}
	return uniqueKeys(candidates)
}
