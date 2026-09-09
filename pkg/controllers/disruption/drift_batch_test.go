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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestDriftBatchesEmptyCandidatesWithinBudget(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	candidates := []*Candidate{
		newDriftBatchTestCandidate(0, nodePool),
		newDriftBatchTestCandidate(1, nodePool),
		newDriftBatchTestCandidate(2, nodePool),
	}
	budgets := map[string]int{nodePool.Name: 2}

	commands, err := (&Drift{}).ComputeCommands(context.Background(), budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(commands))
	}
	if budgets[nodePool.Name] != 0 {
		t.Fatalf("expected budget to be exhausted, got %d", budgets[nodePool.Name])
	}
	for _, command := range commands {
		if len(command.Candidates) != 1 {
			t.Fatalf("expected one candidate per command, got %d", len(command.Candidates))
		}
		if command.Decision() != DeleteDecision {
			t.Fatalf("expected delete command, got %q", command.Decision())
		}
	}
}

func TestDriftCapsEmptyCandidateBatch(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	candidates := make([]*Candidate, 0, DriftMaxBatchSize+1)
	for i := range DriftMaxBatchSize + 1 {
		candidates = append(candidates, newDriftBatchTestCandidate(i, nodePool))
	}
	budgets := map[string]int{nodePool.Name: len(candidates)}

	commands, err := (&Drift{}).ComputeCommands(context.Background(), budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != DriftMaxBatchSize {
		t.Fatalf("expected %d commands, got %d", DriftMaxBatchSize, len(commands))
	}
}

func TestDriftCapsBatchAcrossNodePools(t *testing.T) {
	nodePools := []*v1.NodePool{
		{ObjectMeta: metav1.ObjectMeta{Name: "first"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "second"}},
	}
	candidates := make([]*Candidate, 0, DriftMaxBatchSize+5)
	for i := range DriftMaxBatchSize + 5 {
		candidates = append(candidates, newDriftBatchTestCandidate(i, nodePools[i%len(nodePools)]))
	}
	budgets := map[string]int{
		nodePools[0].Name: len(candidates),
		nodePools[1].Name: len(candidates),
	}

	commands, err := (&Drift{}).ComputeCommands(context.Background(), budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != DriftMaxBatchSize {
		t.Fatalf("expected %d commands across NodePools, got %d", DriftMaxBatchSize, len(commands))
	}
	if decremented := 2*len(candidates) - budgets[nodePools[0].Name] - budgets[nodePools[1].Name]; decremented != DriftMaxBatchSize {
		t.Fatalf("expected exactly %d budget decrements, got %d", DriftMaxBatchSize, decremented)
	}
}

func TestDriftTracksBudgetsIndependentlyAcrossNodePools(t *testing.T) {
	firstNodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "first"}}
	secondNodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "second"}}
	candidates := []*Candidate{
		newDriftBatchTestCandidate(0, firstNodePool),
		newDriftBatchTestCandidate(1, firstNodePool),
		newDriftBatchTestCandidate(2, secondNodePool),
		newDriftBatchTestCandidate(3, secondNodePool),
	}
	budgets := map[string]int{firstNodePool.Name: 1, secondNodePool.Name: 2}

	commands, err := (&Drift{}).ComputeCommands(context.Background(), budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(commands))
	}
	if budgets[firstNodePool.Name] != 0 || budgets[secondNodePool.Name] != 0 {
		t.Fatalf("expected both budgets to be exhausted, got %v", budgets)
	}
}

func TestDriftZeroTimeoutStopsAfterFirstAcceptedCommand(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	empty := newDriftBatchTestCandidate(0, nodePool)
	nonEmpty := newDriftBatchTestCandidate(1, nodePool)
	nonEmpty.reschedulablePods = []*corev1.Pod{{}}
	budgets := map[string]int{nodePool.Name: 2}

	commands, err := (&Drift{}).computeCommands(context.Background(), 0, budgets, empty, nonEmpty)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("expected one empty command, got %d", len(commands))
	}
	if commands[0].Candidates[0] != empty {
		t.Fatal("expected the empty candidate to be selected")
	}
}

func TestDriftZeroTimeoutStopsAfterSkippedCandidates(t *testing.T) {
	skippedNodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "skipped"}}
	acceptedNodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "accepted"}}
	candidates := []*Candidate{
		newDriftBatchTestCandidate(0, skippedNodePool),
		newDriftBatchTestCandidate(1, acceptedNodePool),
		newDriftBatchTestCandidate(2, acceptedNodePool),
	}
	budgets := map[string]int{skippedNodePool.Name: 0, acceptedNodePool.Name: 2}

	commands, err := (&Drift{}).computeCommands(context.Background(), 0, budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("expected one accepted command after skips, got %d", len(commands))
	}
	if commands[0].Candidates[0].NodePool.Name != acceptedNodePool.Name {
		t.Fatalf("expected command from accepted NodePool, got %q", commands[0].Candidates[0].NodePool.Name)
	}
	if budgets[acceptedNodePool.Name] != 1 {
		t.Fatalf("expected one accepted budget decrement, got %d", budgets[acceptedNodePool.Name])
	}
}

func TestDriftDeadlineReturnsEarlierAcceptedCommands(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	empty := newDriftBatchTestCandidate(0, nodePool)
	nonEmpty := newDriftBatchTestCandidate(1, nodePool)
	nonEmpty.reschedulablePods = []*corev1.Pod{{}}
	budgets := map[string]int{nodePool.Name: 2}
	drift := &Drift{
		simulateReplacementFn: func(ctx context.Context, _ *Candidate, _ *scheduling.BatchLedger) (scheduling.Results, *scheduling.Scheduler, error) {
			<-ctx.Done()
			return scheduling.Results{}, nil, ctx.Err()
		},
	}

	commands, err := drift.computeCommands(context.Background(), 10*time.Millisecond, budgets, empty, nonEmpty)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 1 || commands[0].Candidates[0] != empty {
		t.Fatalf("expected only the earlier empty command, got %v", commands)
	}
	if budgets[nodePool.Name] != 1 {
		t.Fatalf("expected only the accepted command to consume budget, got %d", budgets[nodePool.Name])
	}
}

func TestDriftNonDeadlineSimulationErrorFailsPass(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	empty := newDriftBatchTestCandidate(0, nodePool)
	nonEmpty := newDriftBatchTestCandidate(1, nodePool)
	nonEmpty.reschedulablePods = []*corev1.Pod{{}}
	expectedErr := errors.New("simulation failed")
	drift := &Drift{
		simulateReplacementFn: func(context.Context, *Candidate, *scheduling.BatchLedger) (scheduling.Results, *scheduling.Scheduler, error) {
			return scheduling.Results{}, nil, expectedErr
		},
	}

	commands, err := drift.computeCommands(context.Background(), time.Minute, map[string]int{nodePool.Name: 2}, empty, nonEmpty)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected simulation error, got %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected failed pass to discard commands, got %d", len(commands))
	}
}

func TestDriftDiscardsCandidateMarkedDeletingDuringSimulation(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	candidate := newDriftBatchTestCandidate(0, nodePool)
	candidate.NodeClaim.Status.ProviderID = "provider-id"
	candidate.reschedulablePods = []*corev1.Pod{{}}
	cluster := state.NewCluster(clock.RealClock{}, nil, nil)
	cluster.UpdateNodeClaim(candidate.NodeClaim)
	budgets := map[string]int{nodePool.Name: 1}
	drift := &Drift{
		cluster: cluster,
		simulateReplacementFn: func(context.Context, *Candidate, *scheduling.BatchLedger) (scheduling.Results, *scheduling.Scheduler, error) {
			cluster.MarkForDeletion(candidate.ProviderID())
			return scheduling.Results{}, nil, nil
		},
	}

	commands, err := drift.computeCommands(context.Background(), time.Minute, budgets, candidate)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected deleting candidate to be discarded, got %d commands", len(commands))
	}
	if budgets[nodePool.Name] != 1 {
		t.Fatalf("expected deleting candidate not to consume budget, got %d", budgets[nodePool.Name])
	}
}

func TestDriftExpiredDeadlineReturnsNoNewCommands(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	candidates := []*Candidate{
		newDriftBatchTestCandidate(0, nodePool),
		newDriftBatchTestCandidate(1, nodePool),
	}
	budgets := map[string]int{nodePool.Name: len(candidates)}

	commands, err := (&Drift{}).computeCommands(context.Background(), -time.Second, budgets, candidates...)
	if err != nil {
		t.Fatalf("computing commands, %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("expected no commands after deadline, got %d", len(commands))
	}
	if budgets[nodePool.Name] != len(candidates) {
		t.Fatalf("expected budget to remain unchanged, got %d", budgets[nodePool.Name])
	}
}

func newDriftBatchTestCandidate(index int, nodePool *v1.NodePool) *Candidate {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nodeclaim-%d", index)}}
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
	stateNode := state.NewNode()
	stateNode.NodeClaim = nodeClaim
	return &Candidate{
		StateNode: stateNode,
		NodePool:  nodePool,
	}
}
