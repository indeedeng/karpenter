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
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSimulationReportStatsAreThreadSafeSnapshots(t *testing.T) {
	report := NewSimulationReport()
	const (
		workers    = 8
		increments = 1_000
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range increments {
				report.podAttempts.Add(1)
				report.preferenceRelaxations.Add(1)
				report.existingNodeChecks.Add(1)
				report.inflightNodeClaimChecks.Add(1)
				report.nodePoolTemplateChecks.Add(1)
				report.topologyMatchChecks.Add(1)
				report.topologyAPIReads.Add(1)
			}
		}()
	}
	wg.Wait()

	report.RecordPhase(SimulationPhaseScheduling, 3*time.Second)
	report.setResults(Results{
		NewNodeClaims: []*NodeClaim{
			{Pods: []*corev1.Pod{{}, {}}},
		},
		ExistingNodes: []*ExistingNode{
			{Pods: []*corev1.Pod{{}}},
		},
		PodErrors: map[*corev1.Pod]error{{}: nil},
	})

	got := report.Stats()
	wantWork := uint64(workers * increments)
	if got.Work.PodAttempts != wantWork ||
		got.Work.PreferenceRelaxations != wantWork ||
		got.Work.ExistingNodeChecks != wantWork ||
		got.Work.InflightNodeClaimChecks != wantWork ||
		got.Work.NodePoolTemplateChecks != wantWork ||
		got.Work.TopologyMatchChecks != wantWork ||
		got.Work.TopologyAPIReads != wantWork {
		t.Fatalf("expected all work counters to be %d, got %#v", wantWork, got.Work)
	}
	if got.Phases.Scheduling != 3*time.Second {
		t.Fatalf("expected scheduling duration 3s, got %s", got.Phases.Scheduling)
	}
	if got.Results.NewNodeClaims != 1 || got.Results.PodsOnNewNodeClaims != 2 ||
		got.Results.ExistingNodes != 1 || got.Results.PodsOnExistingNodes != 1 ||
		got.Results.PodErrors != 1 {
		t.Fatalf("unexpected result stats %#v", got.Results)
	}

	report.podAttempts.Add(1)
	if got.Work.PodAttempts != wantWork {
		t.Fatalf("previous snapshot changed after report mutation, got %d", got.Work.PodAttempts)
	}
}

func TestSimulationReportCapturesBoundedActiveTopologyStats(t *testing.T) {
	report := NewSimulationReport()
	topology := &Topology{
		report: report,
		topologyGroups: map[uint64]*TopologyGroup{
			1: {Type: TopologyTypeSpread, Key: corev1.LabelTopologyZone, owners: map[types.UID]struct{}{"spread": {}}},
			2: {Type: TopologyTypePodAffinity, Key: corev1.LabelHostname, owners: map[types.UID]struct{}{"affinity": {}}},
			3: {Type: TopologyTypePodAntiAffinity, Key: corev1.LabelTopologyZone, owners: map[types.UID]struct{}{"anti": {}}},
			4: {Type: TopologyTypeSpread, Key: "inactive", owners: map[types.UID]struct{}{}},
		},
		inverseTopologyGroups: map[uint64]*TopologyGroup{
			5: {Type: TopologyTypePodAntiAffinity, Key: "rack", owners: map[types.UID]struct{}{"inverse": {}}},
			6: {Type: TopologyTypePodAntiAffinity, Key: "inactive", owners: map[types.UID]struct{}{}},
		},
	}

	topology.captureInitialTopologyStats()
	got := report.Stats().Topology
	if got.SpreadGroups != 1 || got.AffinityGroups != 1 || got.AntiAffinityGroups != 1 ||
		got.InverseAntiAffinityGroups != 1 || got.DistinctKeys != 3 {
		t.Fatalf("unexpected topology stats %#v", got)
	}
}

func TestSimulationReportIsOptIn(t *testing.T) {
	report := NewSimulationReport()
	if got := SimulationReportFromOptions(); got != nil {
		t.Fatalf("expected reporting to be disabled by default, got %p", got)
	}
	if got := SimulationReportFromOptions(WithSimulationReport(report)); got != report {
		t.Fatalf("expected configured report %p, got %p", report, got)
	}
}
