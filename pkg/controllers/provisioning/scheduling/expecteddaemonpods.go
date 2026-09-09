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

package scheduling

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// expectedDaemonPods are DaemonSet pods that are not bound to a NodeClaim but are expected to run on it. Topology
// counts them in the NodeClaim's hostname domain for pod affinity and anti-affinity terms. It is read-only once built
// because NodeClaim.CanAdd evaluates trial NodeClaims in parallel.
type expectedDaemonPods struct {
	// instanceTypes is every instance type name of the NodeClaimTemplate.
	instanceTypes sets.Set[string]
	// pods maps each expected pod to the instance type names on which it is compatible.
	pods map[*corev1.Pod]sets.Set[string]
}

// buildExpectedDaemonPods returns, per NodeClaimTemplate, the expected pods that are compatible with at least one of the
// template's instance types. Templates without any compatible expected pod are omitted.
func buildExpectedDaemonPods(daemonOverheadGroups map[*NodeClaimTemplate][]DaemonOverheadGroup, expected []*corev1.Pod) map[*NodeClaimTemplate]*expectedDaemonPods {
	if len(expected) == 0 {
		return nil
	}
	expectedSet := sets.New(expected...)
	result := map[*NodeClaimTemplate]*expectedDaemonPods{}
	for template, groups := range daemonOverheadGroups {
		e := &expectedDaemonPods{instanceTypes: sets.New[string](), pods: map[*corev1.Pod]sets.Set[string]{}}
		for _, group := range groups {
			names := sets.New[string]()
			for _, it := range group.InstanceTypes {
				names.Insert(it.Name)
			}
			e.instanceTypes = e.instanceTypes.Union(names)
			for _, p := range group.Pods {
				if !expectedSet.Has(p) {
					continue
				}
				if _, ok := e.pods[p]; !ok {
					e.pods[p] = sets.New[string]()
				}
				e.pods[p] = e.pods[p].Union(names)
			}
		}
		if len(e.pods) > 0 {
			result[template] = e
		}
	}
	return result
}

// instanceTypesSelectedBy returns the instance type names on which at least one expected pod selected by tg runs.
func (e *expectedDaemonPods) instanceTypesSelectedBy(tg *TopologyGroup) sets.Set[string] {
	result := sets.New[string]()
	for p, instanceTypes := range e.pods {
		if tg.selects(p) {
			result = result.Union(instanceTypes)
		}
	}
	return result
}

// adjustDomains counts expected pods in a new NodeClaim's hostname domain for the pod affinity or anti-affinity term
// tg. It returns the adjusted domains and the instance type names the term allows, or nil when the expected pods
// don't constrain instance types.
func (e *expectedDaemonPods) adjustDomains(tg *TopologyGroup, podDomains, nodeDomains, domains *scheduling.Requirement) (*scheduling.Requirement, sets.Set[string]) {
	if e == nil || tg.Key != corev1.LabelHostname || nodeDomains.Operator() != corev1.NodeSelectorOpIn || nodeDomains.Len() != 1 {
		return domains, nil
	}
	hostname := nodeDomains.Values()[0]
	switch tg.Type {
	case TopologyTypePodAffinity:
		if domains.Len() > 0 || !podDomains.Has(hostname) {
			return domains, nil
		}
		allowed := e.instanceTypesSelectedBy(tg)
		if allowed.Len() == 0 {
			return domains, nil
		}
		return scheduling.NewRequirement(tg.Key, corev1.NodeSelectorOpIn, hostname), allowed
	case TopologyTypePodAntiAffinity:
		if domains.Len() == 0 {
			return domains, nil
		}
		blocked := e.instanceTypesSelectedBy(tg)
		if blocked.Len() == 0 {
			return domains, nil
		}
		return domains, e.instanceTypes.Difference(blocked)
	default:
		return domains, nil
	}
}
