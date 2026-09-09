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

package dynamicresources

import (
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// AllocationLedger is an opaque snapshot of committed, in-flight DRA allocation state.
// It deliberately excludes the cluster allocation baseline supplied through AllocatedDeviceState.
type AllocationLedger struct {
	tracker                 allocationTrackerLedger
	claimAllocationMetadata map[ResourceClaimID]*ResourceClaimAllocationMetadata
}

type allocationTrackerLedger struct {
	inflightClusterAllocations            map[DeviceID]*InflightAllocationMetadata
	inflightClusterAllocationsByNodeClaim map[NodeClaimID]map[InstanceTypeID]sets.Set[DeviceID]
	inflightTemplateAllocations           map[NodeClaimID]map[InstanceTypeID]sets.Set[DeviceID]
	inflightConsumedCapacity              map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity
	consumedCapacityByNodeClaimIT         map[NodeClaimID]map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity
	templateConsumedCapacity              map[NodeClaimID]map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity
	countersByNodeClaimIT                 map[NodeClaimID]map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter
	templateRemainingCounters             map[NodeClaimID]map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter
}

// ExportLedger returns a deep copy of the allocator's committed, in-flight allocation state.
func (a *Allocator) ExportLedger() *AllocationLedger {
	return (&AllocationLedger{
		tracker:                 a.allocationTracker.exportLedger(),
		claimAllocationMetadata: a.claimAllocationMetadata,
	}).DeepCopy()
}

// ApplyLedger seeds a fresh allocator with a deep copy of committed, in-flight allocation state.
// The allocator's cluster allocation baseline remains the one supplied when it was constructed.
func (a *Allocator) ApplyLedger(ledger *AllocationLedger) {
	if ledger == nil {
		return
	}
	snapshot := ledger.DeepCopy()
	a.allocationTracker.applyLedger(snapshot.tracker)
	a.claimAllocationMetadata = snapshot.claimAllocationMetadata
}

// ApplyLedger seeds a fresh allocation tracker with a deep copy of committed, in-flight allocation state.
// The tracker's cluster allocation baseline remains the one supplied when it was constructed.
func (at *AllocationTracker) ApplyLedger(ledger *AllocationLedger) {
	if ledger == nil {
		return
	}
	at.applyLedger(ledger.DeepCopy().tracker)
}

// DeepCopy returns an independent copy of the ledger.
func (l *AllocationLedger) DeepCopy() *AllocationLedger {
	if l == nil {
		return nil
	}
	return &AllocationLedger{
		tracker: allocationTrackerLedger{
			inflightClusterAllocations:            copyInflightAllocations(l.tracker.inflightClusterAllocations),
			inflightClusterAllocationsByNodeClaim: copyDeviceAllocationsByNodeClaim(l.tracker.inflightClusterAllocationsByNodeClaim),
			inflightTemplateAllocations:           copyDeviceAllocationsByNodeClaim(l.tracker.inflightTemplateAllocations),
			inflightConsumedCapacity:              copyDeviceCapacity(l.tracker.inflightConsumedCapacity),
			consumedCapacityByNodeClaimIT:         copyCapacityByNodeClaimIT(l.tracker.consumedCapacityByNodeClaimIT),
			templateConsumedCapacity:              copyCapacityByNodeClaimIT(l.tracker.templateConsumedCapacity),
			countersByNodeClaimIT:                 copyCountersByNodeClaimIT(l.tracker.countersByNodeClaimIT),
			templateRemainingCounters:             copyCountersByNodeClaimIT(l.tracker.templateRemainingCounters),
		},
		claimAllocationMetadata: copyClaimAllocationMetadata(l.claimAllocationMetadata),
	}
}

func (at *AllocationTracker) exportLedger() allocationTrackerLedger {
	return allocationTrackerLedger{
		inflightClusterAllocations:            at.InflightClusterAllocations,
		inflightClusterAllocationsByNodeClaim: at.InflightClusterAllocationsByNodeClaim,
		inflightTemplateAllocations:           at.InflightTemplateAllocations,
		inflightConsumedCapacity:              at.InflightConsumedCapacity,
		consumedCapacityByNodeClaimIT:         at.consumedCapacityByNodeClaimIT,
		templateConsumedCapacity:              at.templateConsumedCapacity,
		countersByNodeClaimIT:                 at.countersByNodeClaimIT,
		templateRemainingCounters:             at.templateRemainingCounters,
	}
}

func (at *AllocationTracker) applyLedger(ledger allocationTrackerLedger) {
	at.InflightClusterAllocations = ledger.inflightClusterAllocations
	at.InflightClusterAllocationsByNodeClaim = ledger.inflightClusterAllocationsByNodeClaim
	at.InflightTemplateAllocations = ledger.inflightTemplateAllocations
	at.InflightConsumedCapacity = ledger.inflightConsumedCapacity
	at.consumedCapacityByNodeClaimIT = ledger.consumedCapacityByNodeClaimIT
	at.templateConsumedCapacity = ledger.templateConsumedCapacity
	at.countersByNodeClaimIT = ledger.countersByNodeClaimIT
	at.templateRemainingCounters = ledger.templateRemainingCounters

	// RemainingCounters is initialized from the current cluster baseline, not copied from the
	// ledger. Deduct ledger consumption from any pools that have already been initialized.
	for poolKey := range at.RemainingCounters {
		at.deductInflightCountersForPool(poolKey)
	}
}

func copyInflightAllocations(src map[DeviceID]*InflightAllocationMetadata) map[DeviceID]*InflightAllocationMetadata {
	dst := make(map[DeviceID]*InflightAllocationMetadata, len(src))
	for deviceID, metadata := range src {
		if metadata == nil {
			dst[deviceID] = nil
			continue
		}
		dst[deviceID] = &InflightAllocationMetadata{
			NodeClaimID:   metadata.NodeClaimID,
			InstanceTypes: copySet(metadata.InstanceTypes),
		}
	}
	return dst
}

func copyDeviceAllocationsByNodeClaim(src map[NodeClaimID]map[InstanceTypeID]sets.Set[DeviceID]) map[NodeClaimID]map[InstanceTypeID]sets.Set[DeviceID] {
	dst := make(map[NodeClaimID]map[InstanceTypeID]sets.Set[DeviceID], len(src))
	for nodeClaimID, allocationsByIT := range src {
		dst[nodeClaimID] = make(map[InstanceTypeID]sets.Set[DeviceID], len(allocationsByIT))
		for instanceTypeID, deviceIDs := range allocationsByIT {
			dst[nodeClaimID][instanceTypeID] = copySet(deviceIDs)
		}
	}
	return dst
}

func copySet[T comparable](src sets.Set[T]) sets.Set[T] {
	dst := make(sets.Set[T], len(src))
	for value := range src {
		dst.Insert(value)
	}
	return dst
}

func copyDeviceCapacity(src map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity) map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity {
	dst := make(map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity, len(src))
	for deviceID, capacity := range src {
		dst[deviceID] = copyCapacity(capacity)
	}
	return dst
}

func copyCapacity(src map[resourcev1.QualifiedName]resource.Quantity) map[resourcev1.QualifiedName]resource.Quantity {
	dst := make(map[resourcev1.QualifiedName]resource.Quantity, len(src))
	for name, quantity := range src {
		dst[name] = quantity.DeepCopy()
	}
	return dst
}

func copyCapacityByNodeClaimIT(src map[NodeClaimID]map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity) map[NodeClaimID]map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity {
	dst := make(map[NodeClaimID]map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity, len(src))
	for nodeClaimID, capacityByIT := range src {
		dst[nodeClaimID] = make(map[InstanceTypeID]map[DeviceID]map[resourcev1.QualifiedName]resource.Quantity, len(capacityByIT))
		for instanceTypeID, capacityByDevice := range capacityByIT {
			dst[nodeClaimID][instanceTypeID] = copyDeviceCapacity(capacityByDevice)
		}
	}
	return dst
}

func copyCountersByPool(src map[PoolKey]map[string]map[string]resourcev1.Counter) map[PoolKey]map[string]map[string]resourcev1.Counter {
	dst := make(map[PoolKey]map[string]map[string]resourcev1.Counter, len(src))
	for poolKey, counterSets := range src {
		dst[poolKey] = copyCounterSets(counterSets)
	}
	return dst
}

func copyCountersByNodeClaimIT(src map[NodeClaimID]map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter) map[NodeClaimID]map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter {
	dst := make(map[NodeClaimID]map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter, len(src))
	for nodeClaimID, countersByIT := range src {
		dst[nodeClaimID] = make(map[InstanceTypeID]map[PoolKey]map[string]map[string]resourcev1.Counter, len(countersByIT))
		for instanceTypeID, countersByPool := range countersByIT {
			dst[nodeClaimID][instanceTypeID] = copyCountersByPool(countersByPool)
		}
	}
	return dst
}

func copyClaimAllocationMetadata(src map[ResourceClaimID]*ResourceClaimAllocationMetadata) map[ResourceClaimID]*ResourceClaimAllocationMetadata {
	dst := make(map[ResourceClaimID]*ResourceClaimAllocationMetadata, len(src))
	for claimID, metadata := range src {
		if metadata == nil {
			dst[claimID] = nil
			continue
		}
		copied := &ResourceClaimAllocationMetadata{
			NodeClaimID:             metadata.NodeClaimID,
			ContributedRequirements: make(map[InstanceTypeID]scheduling.Requirements, len(metadata.ContributedRequirements)),
			TotalRequirements:       deepCopyRequirements(metadata.TotalRequirements),
			UsedTemplateDevices:     metadata.UsedTemplateDevices,
			Devices:                 make(map[InstanceTypeID][]DeviceAllocationResult, len(metadata.Devices)),
		}
		for instanceTypeID, requirements := range metadata.ContributedRequirements {
			copied.ContributedRequirements[instanceTypeID] = deepCopyRequirements(requirements)
		}
		for instanceTypeID, results := range metadata.Devices {
			copied.Devices[instanceTypeID] = make([]DeviceAllocationResult, len(results))
			for i, result := range results {
				copied.Devices[instanceTypeID][i] = DeviceAllocationResult{
					DeviceID: result.DeviceID,
				}
				if result.ConsumedCapacity != nil {
					copied.Devices[instanceTypeID][i].ConsumedCapacity = copyCapacity(result.ConsumedCapacity)
				}
			}
		}
		dst[claimID] = copied
	}
	return dst
}

func deepCopyRequirements(src scheduling.Requirements) scheduling.Requirements {
	dst := scheduling.NewRequirements()
	for _, requirement := range src {
		dst.Add(requirement.DeepCopy())
	}
	return dst
}
