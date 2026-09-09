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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling/dynamicresources"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// BatchLedger contains only state committed by accepted scheduling simulations. Each
// simulation is seeded from copies of this state so failed simulations cannot leak.
type BatchLedger struct {
	reservationManager     *ReservationManager
	replacementConsumption map[string]corev1.ResourceList
	topologyPlacements     []batchTopologyPlacement
	removedNodeNames       sets.Set[string]
	removedPods            []*corev1.Pod
	dra                    *dynamicresources.AllocationLedger
}

type batchTopologyPlacement struct {
	hostname     string
	pod          *corev1.Pod
	taints       []corev1.Taint
	requirements scheduling.Requirements
}

func NewBatchLedger() *BatchLedger {
	return &BatchLedger{
		replacementConsumption: map[string]corev1.ResourceList{},
		removedNodeNames:       sets.New[string](),
	}
}

func (l *BatchLedger) reservationManagerFor(instanceTypes map[string][]*cloudprovider.InstanceType) *ReservationManager {
	if l.reservationManager == nil {
		return NewReservationManager(instanceTypes)
	}
	return l.reservationManager.Clone()
}

func (l *BatchLedger) seedScheduler(s *Scheduler) {
	if s.allocator != nil {
		s.allocator.ApplyLedger(l.dra)
	}
	for nodePoolName, consumed := range l.replacementConsumption {
		s.remainingResources[nodePoolName] = resources.Subtract(s.remainingResources[nodePoolName], consumed)
	}
	for _, placement := range l.topologyPlacements {
		s.topology.Register(corev1.LabelHostname, placement.hostname)
		s.topology.Record(placement.pod, placement.taints, placement.requirements, scheduling.AllowUndefinedWellKnownLabels)
	}
}

// Commit records an accepted solve. The scheduler and its mutable state are discarded
// after this call; only the accepted deltas are retained for the next fresh solve.
func (l *BatchLedger) Commit(scheduler *Scheduler, results Results, removedNodeNames []string, removedPods []*corev1.Pod) {
	l.reservationManager = scheduler.reservationManager.Clone()
	if scheduler.allocator != nil {
		l.dra = scheduler.allocator.ExportLedger()
	}
	l.CommitRemoval(removedNodeNames, removedPods)
	for _, nodeClaim := range results.NewNodeClaims {
		maxResources := maxInstanceResources(nodeClaim.InstanceTypeOptions)
		l.replacementConsumption[nodeClaim.NodePoolName] = resources.Merge(l.replacementConsumption[nodeClaim.NodePoolName], maxResources)

		requirements := scheduling.NewRequirements(nodeClaim.Requirements.Values()...)
		requirements.Add(scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, nodeClaim.hostname))
		for _, pod := range nodeClaim.Pods {
			l.topologyPlacements = append(l.topologyPlacements, batchTopologyPlacement{
				hostname:     nodeClaim.hostname,
				pod:          pod.DeepCopy(),
				taints:       append([]corev1.Taint(nil), nodeClaim.Spec.Taints...),
				requirements: requirements,
			})
		}
	}
}

func (l *BatchLedger) CommitRemoval(removedNodeNames []string, removedPods []*corev1.Pod) {
	l.removedNodeNames.Insert(removedNodeNames...)
	for _, pod := range removedPods {
		l.removedPods = append(l.removedPods, pod.DeepCopy())
	}
}

func (l *BatchLedger) RemovedNodeNames() sets.Set[string] {
	return l.removedNodeNames.Clone()
}

func (l *BatchLedger) RemovedPods() []*corev1.Pod {
	return lo.Map(l.removedPods, func(pod *corev1.Pod, _ int) *corev1.Pod {
		return pod.DeepCopy()
	})
}
