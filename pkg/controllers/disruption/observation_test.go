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

import (
	"errors"
	"testing"
	"time"

	prometheusmodel "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clocktesting "k8s.io/utils/clock/testing"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

func TestPassObservationOutcome(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*PassObservation)
		success   bool
		err       error
		want      string
	}{
		{name: "no candidates", want: PassOutcomeNoCandidates},
		{name: "unchanged", configure: func(o *PassObservation) { o.unchanged = true }, want: PassOutcomeUnchanged},
		{name: "budget blocked", configure: func(o *PassObservation) {
			o.eligible.Insert("candidate")
			o.budgetBlocked.Insert("candidate")
		}, want: PassOutcomeBudgetBlocked},
		{name: "validation failed", configure: func(o *PassObservation) {
			o.eligible.Insert("candidate")
			o.validationFailed = true
		}, want: PassOutcomeValidationFailed},
		{name: "timeout", configure: func(o *PassObservation) {
			o.eligible.Insert("candidate")
			o.timedOut = true
		}, want: PassOutcomeTimeout},
		{name: "selected", configure: func(o *PassObservation) { o.selected = 1 }, success: true, want: PassOutcomeSelected},
		{name: "partial selected", configure: func(o *PassObservation) {
			o.selected = 1
			o.queueRejected = 1
		}, err: errors.New("queue"), want: PassOutcomePartialSelected},
		{name: "queue rejected", configure: func(o *PassObservation) { o.queueRejected = 1 }, err: errors.New("queue"), want: PassOutcomeQueueRejected},
		{name: "error", err: errors.New("failed"), want: PassOutcomeError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observation := NewPassObservation(&SingleNodeConsolidation{}, clocktesting.NewFakeClock(time.Unix(1, 0)))
			if tt.configure != nil {
				tt.configure(observation)
			}
			if got := observation.passOutcome(tt.success, tt.err); got != tt.want {
				t.Fatalf("passOutcome() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEstimatedOpportunitySavingsRequiresCompleteSourcePricing(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*Candidate
		want       float64
		complete   bool
	}{
		{
			name:       "known source price",
			candidates: []*Candidate{{Price: 0.42, PriceKnown: true}},
			want:       0.42,
			complete:   true,
		},
		{
			name:       "unknown source price",
			candidates: []*Candidate{{Price: 0.42}},
			complete:   false,
		},
		{
			name:       "known zero source price",
			candidates: []*Candidate{{PriceKnown: true}},
			complete:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, complete := (Command{Candidates: tt.candidates}).EstimatedOpportunitySavings()
			if complete != tt.complete {
				t.Fatalf("complete = %v, want %v", complete, tt.complete)
			}
			if got != tt.want {
				t.Fatalf("savings = %v, want %v", got, tt.want)
			}
		})
	}
	_, complete := (Command{
		Candidates: []*Candidate{{Price: 0.42, PriceKnown: true}},
		Results: scheduling.Results{
			NewNodeClaims: []*scheduling.NodeClaim{{}},
		},
	}).EstimatedOpportunitySavings()
	if complete {
		t.Fatal("replacement without a priced instance type was reported as completely priced")
	}
}

func TestUnevaluatedReasonPrecedence(t *testing.T) {
	observation := NewPassObservation(&SingleNodeConsolidation{}, clocktesting.NewFakeClock(time.Unix(1, 0)))
	observation.budgetBlocked = sets.New("budget")
	observation.searchLimited = sets.New("search")
	observation.selectedEarly = sets.New("selected")
	observation.policySkipped = sets.New("policy")
	observation.timedOut = true

	tests := map[string]string{
		"budget":   UnevaluatedReasonBudget,
		"search":   UnevaluatedReasonSearchLimit,
		"selected": UnevaluatedReasonSelectedEarly,
		"policy":   UnevaluatedReasonPolicy,
		"timeout":  UnevaluatedReasonTimeout,
	}
	for key, want := range tests {
		if got := observation.unevaluatedReason(key, nil); got != want {
			t.Fatalf("unevaluatedReason(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestObserveSimulationReportEmitsBoundedMetrics(t *testing.T) {
	SimulationPhaseDurationSeconds.Reset()
	SimulationTopologyConstraintCount.Reset()
	SimulationTopologyKeyCount.Reset()
	SimulationInputCount.Reset()
	SimulationWorkCount.Reset()
	SimulationResultCount.Reset()
	t.Cleanup(func() {
		SimulationPhaseDurationSeconds.Reset()
		SimulationTopologyConstraintCount.Reset()
		SimulationTopologyKeyCount.Reset()
		SimulationInputCount.Reset()
		SimulationWorkCount.Reset()
		SimulationResultCount.Reset()
	})

	observation := NewPassObservation(&SingleNodeConsolidation{}, clocktesting.NewFakeClock(time.Unix(1, 0)))
	observeSimulationReport(observation, scheduling.SimulationStats{
		Phases: scheduling.SimulationPhaseDurations{Solve: 2 * time.Second},
		Topology: scheduling.SimulationTopologyStats{
			SpreadGroups: 3,
			DistinctKeys: 2,
		},
		Inputs: scheduling.SimulationInputStats{Pods: 5},
		Work: scheduling.SimulationWorkStats{
			PodAttempts: 7,
		},
		Results: scheduling.SimulationResultStats{NewNodeClaims: 1},
	})

	assertHistogramSample(t,
		"karpenter_voluntary_disruption_simulation_phase_duration_seconds",
		map[string]string{methodLabel: MethodSingle, phaseLabel: SimulationPhaseSolve},
		1,
		2,
	)
	assertHistogramSample(t,
		"karpenter_voluntary_disruption_simulation_topology_constraint_count",
		map[string]string{methodLabel: MethodSingle, kindLabel: TopologyConstraintKindSpread},
		1,
		3,
	)
	assertHistogramSample(t,
		"karpenter_voluntary_disruption_simulation_work_count",
		map[string]string{methodLabel: MethodSingle, operationLabel: SimulationOperationPodAttempt},
		1,
		7,
	)
}

func TestPassObservationPublishesCandidateFunnelAndCleansStalePools(t *testing.T) {
	Candidates.Reset()
	OldestEligibleTimestampSeconds.Reset()
	LastEvaluatedTimestampSeconds.Reset()
	PassesTotal.Reset()
	CandidatesEvaluatedPerPass.Reset()
	candidateSnapshotStore = metrics.NewStore()
	observedNodePools.names = sets.New[string]()
	t.Cleanup(func() {
		Candidates.Reset()
		OldestEligibleTimestampSeconds.Reset()
		LastEvaluatedTimestampSeconds.Reset()
		PassesTotal.Reset()
		CandidatesEvaluatedPerPass.Reset()
		candidateSnapshotStore = metrics.NewStore()
		observedNodePools.names = sets.New[string]()
	})

	clk := clocktesting.NewFakeClock(time.Unix(100, 0))
	method := &SingleNodeConsolidation{}
	first := testObservationCandidate("first", "pool-a")
	observation := NewPassObservation(method, clk)
	observation.SetCandidates(CandidateSet{
		Possible:  []*Candidate{first},
		Eligible:  []*Candidate{first},
		NodePools: []string{"pool-a"},
	})
	observation.RecordBudgetEligible(first)
	observation.Complete(false, nil)

	for stage := range map[string]struct{}{
		CandidateStagePossible:       {},
		CandidateStageEligible:       {},
		CandidateStageBudgetEligible: {},
	} {
		metric, found := findMetric(t,
			"karpenter_voluntary_disruption_candidates",
			map[string]string{methodLabel: MethodSingle, metrics.NodePoolLabel: "pool-a", stageLabel: stage},
		)
		if !found {
			t.Fatalf("candidate stage %q not found", stage)
		}
		if metric.GetGauge().GetValue() != 1 {
			t.Fatalf("candidate stage %q = %v, want 1", stage, metric.GetGauge().GetValue())
		}
	}

	second := testObservationCandidate("second", "pool-b")
	next := NewPassObservation(method, clk)
	next.SetCandidates(CandidateSet{
		Possible:  []*Candidate{second},
		Eligible:  []*Candidate{second},
		NodePools: []string{"pool-b"},
	})
	next.RecordBudgetEligible(second)
	next.Complete(false, nil)

	if _, found := findMetric(t,
		"karpenter_voluntary_disruption_candidates",
		map[string]string{methodLabel: MethodSingle, metrics.NodePoolLabel: "pool-a"},
	); found {
		t.Fatal("stale pool-a candidate series was not removed")
	}
}

func testObservationCandidate(name, pool string) *Candidate {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)}}
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	return &Candidate{
		StateNode: &state.StateNode{NodeClaim: nodeClaim},
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: pool}},
	}
}

func assertHistogramSample(t *testing.T, name string, labels map[string]string, wantCount uint64, wantSum float64) {
	t.Helper()
	metric, found := findMetric(t, name, labels)
	if !found {
		t.Fatalf("metric %s with labels %v not found", name, labels)
	}
	if metric.GetHistogram().GetSampleCount() != wantCount {
		t.Fatalf("%s sample count = %d, want %d", name, metric.GetHistogram().GetSampleCount(), wantCount)
	}
	if metric.GetHistogram().GetSampleSum() != wantSum {
		t.Fatalf("%s sample sum = %v, want %v", name, metric.GetHistogram().GetSampleSum(), wantSum)
	}
}

func findMetric(t *testing.T, name string, labels map[string]string) (*prometheusmodel.Metric, bool) {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gathering metrics, %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if metricHasLabels(metric, labels) {
				return metric, true
			}
		}
	}
	return nil, false
}

func metricHasLabels(metric *prometheusmodel.Metric, labels map[string]string) bool {
	remaining := make(map[string]string, len(labels))
	for key, value := range labels {
		remaining[key] = value
	}
	for _, pair := range metric.Label {
		if value, ok := remaining[pair.GetName()]; ok && value == pair.GetValue() {
			delete(remaining, pair.GetName())
		}
	}
	return len(remaining) == 0
}
