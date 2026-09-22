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
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
)

type SimulationPhase uint8

const (
	SimulationPhaseTopologyBuild SimulationPhase = iota
	SimulationPhaseSchedulerBuild
	SimulationPhasePodDataPreparation
	SimulationPhaseScheduling
	SimulationPhaseResultFinalization
	SimulationPhaseSolve
	SimulationPhaseVolumeTopologyLookup
	SimulationPhaseDynamicResourceSetup
)

// SimulationStats is an immutable, identity-free snapshot of scheduler work.
// All counts describe one scheduling simulation.
type SimulationStats struct {
	Phases   SimulationPhaseDurations
	Topology SimulationTopologyStats
	Inputs   SimulationInputStats
	Work     SimulationWorkStats
	Results  SimulationResultStats
}

type SimulationPhaseDurations struct {
	TopologyBuild        time.Duration
	SchedulerBuild       time.Duration
	PodDataPreparation   time.Duration
	Scheduling           time.Duration
	ResultFinalization   time.Duration
	Solve                time.Duration
	VolumeTopologyLookup time.Duration
	DynamicResourceSetup time.Duration
}

type SimulationTopologyStats struct {
	SpreadGroups              uint64
	AffinityGroups            uint64
	AntiAffinityGroups        uint64
	InverseAntiAffinityGroups uint64
	DistinctKeys              uint64
}

type SimulationInputStats struct {
	Pods                   uint64
	TopologyStateNodes     uint64
	AccountingStateNodes   uint64
	NodePools              uint64
	NodePoolTemplates      uint64
	InstanceTypes          uint64
	DaemonSetPods          uint64
	AdditionalExcludedPods uint64
}

type SimulationWorkStats struct {
	PodAttempts             uint64
	PreferenceRelaxations   uint64
	ExistingNodeChecks      uint64
	InflightNodeClaimChecks uint64
	NodePoolTemplateChecks  uint64
	TopologyMatchChecks     uint64
	TopologyAPIReads        uint64
}

type SimulationResultStats struct {
	NewNodeClaims       uint64
	ExistingNodes       uint64
	PodsOnNewNodeClaims uint64
	PodsOnExistingNodes uint64
	PodErrors           uint64
}

// SimulationReport collects scheduler statistics when explicitly supplied through
// WithSimulationReport. A report is safe to snapshot while scheduler work is in progress.
type SimulationReport struct {
	topologyBuildNanos        atomic.Int64
	schedulerBuildNanos       atomic.Int64
	podDataPreparationNanos   atomic.Int64
	schedulingNanos           atomic.Int64
	resultFinalizationNanos   atomic.Int64
	solveNanos                atomic.Int64
	volumeTopologyLookupNanos atomic.Int64
	dynamicResourceSetupNanos atomic.Int64

	spreadGroups              atomic.Uint64
	affinityGroups            atomic.Uint64
	antiAffinityGroups        atomic.Uint64
	inverseAntiAffinityGroups atomic.Uint64
	distinctTopologyKeys      atomic.Uint64

	inputPods                 atomic.Uint64
	topologyStateNodes        atomic.Uint64
	accountingStateNodes      atomic.Uint64
	nodePools                 atomic.Uint64
	nodePoolTemplates         atomic.Uint64
	instanceTypes             atomic.Uint64
	daemonSetPods             atomic.Uint64
	additionalExcludedPods    atomic.Uint64
	podAttempts               atomic.Uint64
	preferenceRelaxations     atomic.Uint64
	existingNodeChecks        atomic.Uint64
	inflightNodeClaimChecks   atomic.Uint64
	nodePoolTemplateChecks    atomic.Uint64
	topologyMatchChecks       atomic.Uint64
	topologyAPIReads          atomic.Uint64
	resultNewNodeClaims       atomic.Uint64
	resultExistingNodes       atomic.Uint64
	resultPodsOnNewNodeClaims atomic.Uint64
	resultPodsOnExistingNodes atomic.Uint64
	resultPodErrors           atomic.Uint64
}

func NewSimulationReport() *SimulationReport {
	return &SimulationReport{}
}

// SimulationReportFromOptions returns the report selected by the supplied options,
// or nil when reporting is disabled.
func SimulationReportFromOptions(opts ...Options) *SimulationReport {
	return option.Resolve(opts...).simulationReport
}

// RecordPhase accumulates one measured phase duration.
func (r *SimulationReport) RecordPhase(phase SimulationPhase, duration time.Duration) {
	if r == nil {
		return
	}
	var destination *atomic.Int64
	switch phase {
	case SimulationPhaseTopologyBuild:
		destination = &r.topologyBuildNanos
	case SimulationPhaseSchedulerBuild:
		destination = &r.schedulerBuildNanos
	case SimulationPhasePodDataPreparation:
		destination = &r.podDataPreparationNanos
	case SimulationPhaseScheduling:
		destination = &r.schedulingNanos
	case SimulationPhaseResultFinalization:
		destination = &r.resultFinalizationNanos
	case SimulationPhaseSolve:
		destination = &r.solveNanos
	case SimulationPhaseVolumeTopologyLookup:
		destination = &r.volumeTopologyLookupNanos
	case SimulationPhaseDynamicResourceSetup:
		destination = &r.dynamicResourceSetupNanos
	default:
		return
	}
	destination.Add(int64(duration))
}

// Stats returns an immutable snapshot. Subsequent scheduler work does not change
// the returned value.
func (r *SimulationReport) Stats() SimulationStats {
	if r == nil {
		return SimulationStats{}
	}
	return SimulationStats{
		Phases: SimulationPhaseDurations{
			TopologyBuild:        time.Duration(r.topologyBuildNanos.Load()),
			SchedulerBuild:       time.Duration(r.schedulerBuildNanos.Load()),
			PodDataPreparation:   time.Duration(r.podDataPreparationNanos.Load()),
			Scheduling:           time.Duration(r.schedulingNanos.Load()),
			ResultFinalization:   time.Duration(r.resultFinalizationNanos.Load()),
			Solve:                time.Duration(r.solveNanos.Load()),
			VolumeTopologyLookup: time.Duration(r.volumeTopologyLookupNanos.Load()),
			DynamicResourceSetup: time.Duration(r.dynamicResourceSetupNanos.Load()),
		},
		Topology: SimulationTopologyStats{
			SpreadGroups:              r.spreadGroups.Load(),
			AffinityGroups:            r.affinityGroups.Load(),
			AntiAffinityGroups:        r.antiAffinityGroups.Load(),
			InverseAntiAffinityGroups: r.inverseAntiAffinityGroups.Load(),
			DistinctKeys:              r.distinctTopologyKeys.Load(),
		},
		Inputs: SimulationInputStats{
			Pods:                   r.inputPods.Load(),
			TopologyStateNodes:     r.topologyStateNodes.Load(),
			AccountingStateNodes:   r.accountingStateNodes.Load(),
			NodePools:              r.nodePools.Load(),
			NodePoolTemplates:      r.nodePoolTemplates.Load(),
			InstanceTypes:          r.instanceTypes.Load(),
			DaemonSetPods:          r.daemonSetPods.Load(),
			AdditionalExcludedPods: r.additionalExcludedPods.Load(),
		},
		Work: SimulationWorkStats{
			PodAttempts:             r.podAttempts.Load(),
			PreferenceRelaxations:   r.preferenceRelaxations.Load(),
			ExistingNodeChecks:      r.existingNodeChecks.Load(),
			InflightNodeClaimChecks: r.inflightNodeClaimChecks.Load(),
			NodePoolTemplateChecks:  r.nodePoolTemplateChecks.Load(),
			TopologyMatchChecks:     r.topologyMatchChecks.Load(),
			TopologyAPIReads:        r.topologyAPIReads.Load(),
		},
		Results: SimulationResultStats{
			NewNodeClaims:       r.resultNewNodeClaims.Load(),
			ExistingNodes:       r.resultExistingNodes.Load(),
			PodsOnNewNodeClaims: r.resultPodsOnNewNodeClaims.Load(),
			PodsOnExistingNodes: r.resultPodsOnExistingNodes.Load(),
			PodErrors:           r.resultPodErrors.Load(),
		},
	}
}

func (r *SimulationReport) addDuration(destination *atomic.Int64, started time.Time) {
	destination.Add(int64(time.Since(started)))
}

func (r *SimulationReport) setResults(results Results) {
	if r == nil {
		return
	}
	var podsOnNewNodeClaims, podsOnExistingNodes uint64
	for _, nodeClaim := range results.NewNodeClaims {
		podsOnNewNodeClaims += uint64(len(nodeClaim.Pods))
	}
	for _, node := range results.ExistingNodes {
		podsOnExistingNodes += uint64(len(node.Pods))
	}
	r.resultNewNodeClaims.Store(uint64(len(results.NewNodeClaims)))
	r.resultExistingNodes.Store(uint64(len(results.ExistingNodes)))
	r.resultPodsOnNewNodeClaims.Store(podsOnNewNodeClaims)
	r.resultPodsOnExistingNodes.Store(podsOnExistingNodes)
	r.resultPodErrors.Store(uint64(len(results.PodErrors)))
}
