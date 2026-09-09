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
	"slices"
	"sort"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	DriftTimeoutDuration = time.Minute
	DriftMaxBatchSize    = 100
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient            client.Client
	cluster               *state.Cluster
	provisioner           *provisioning.Provisioner
	recorder              events.Recorder
	clock                 clock.Clock
	backoff               *NodePoolBackoff
	simulateReplacementFn func(context.Context, *Candidate, *scheduling.BatchLedger) (scheduling.Results, *scheduling.Scheduler, error)
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder, clk clock.Clock, backoff *NodePoolBackoff) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		clock:       clk,
		backoff:     backoff,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return !c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	return d.computeCommands(ctx, DriftTimeoutDuration, disruptionBudgetMapping, candidates...)
}

func (d *Drift) computeCommands(ctx context.Context, timeout time.Duration, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	legacyMode := timeout == 0
	if !legacyMode {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Register a zero-valued back-off counter for every NodePool with a drift candidate so the
	// metric is visible (at 0) for healthy pools rather than being absent until the first back-off.
	// Add(0) is idempotent: it only ensures the series exists and never clobbers an incremented value.
	for _, nodePoolName := range lo.Uniq(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })) {
		DriftBackoffsTotal.Add(0, map[string]string{metrics.NodePoolLabel: nodePoolName})
	}

	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	emptyCandidates, nonEmptyCandidates := lo.FilterReject(candidates, func(c *Candidate, _ int) bool {
		return len(c.reschedulablePods) == 0
	})

	// Prioritize empty candidates since we want them to get priority over non-empty candidates if the budget is constrained.
	// Disrupting empty candidates first also helps reduce the overall churn because if a non-empty candidate is disrupted first,
	// the pods from that node can reschedule on the empty nodes and will need to move again when those nodes get disrupted.
	var commands []Command
	var simulator *driftReplacementSimulator
	ledger := scheduling.NewBatchLedger()
	orderedCandidates := slices.Concat(emptyCandidates, nonEmptyCandidates)
	for i, candidate := range orderedCandidates {
		if len(commands) == DriftMaxBatchSize {
			break
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			logDriftDeadline(ctx, i, len(orderedCandidates), len(commands))
			break
		}
		if d.cluster != nil && !d.cluster.IsNodeActive(candidate.ProviderID()) {
			log.FromContext(ctx).V(1).Info("skipping drift candidate that started deleting", "NodeClaim", candidate.NodeClaim.Name)
			continue
		}
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Skip candidates whose NodePool is currently backed off after repeated unrecoverable
		// drift replacement failures. Healthy pools and pools whose back-off window has elapsed
		// fall through to normal selection. This is a read-only check; the queue is the only
		// place that mutates back-off state (Fail/Reset).
		if d.backoff != nil && d.backoff.IsBackedOff(candidate.NodePool.Name) {
			level, until := d.backoff.Snapshot(candidate.NodePool.Name)
			d.recorder.Publish(disruptionevents.NodePoolDriftBackoff(candidate.NodePool, until, level))
			continue
		}
		if len(candidate.reschedulablePods) == 0 {
			commands = append(commands, newDriftCommand(candidate, scheduling.Results{}))
			ledger.CommitRemoval([]string{candidate.Name()}, nil)
			disruptionBudgetMapping[candidate.NodePool.Name]--
			if legacyMode {
				break
			}
			continue
		}
		if simulator == nil && d.simulateReplacementFn == nil {
			var err error
			simulator, err = newDriftReplacementSimulator(ctx, d.kubeClient, d.cluster, d.provisioner, d.clock, d.recorder)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					logDriftDeadline(ctx, i, len(orderedCandidates), len(commands))
					break
				}
				return []Command{}, err
			}
		}
		simulateReplacement := d.simulateReplacementFn
		if simulateReplacement == nil {
			simulateReplacement = simulator.simulate
		}
		results, scheduler, err := simulateReplacement(ctx, candidate, ledger)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				logDriftDeadline(ctx, i+1, len(orderedCandidates), len(commands))
				break
			}
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		if d.cluster != nil && !d.cluster.IsNodeActive(candidate.ProviderID()) {
			continue
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}

		commands = append(commands, newDriftCommand(candidate, results))
		acceptedPods := lo.FlatMap(results.NewNodeClaims, func(nodeClaim *scheduling.NodeClaim, _ int) []*corev1.Pod {
			return nodeClaim.Pods
		})
		ledger.Commit(scheduler, results, []string{candidate.Name()}, acceptedPods)
		disruptionBudgetMapping[candidate.NodePool.Name]--
		if legacyMode {
			break
		}
	}
	return commands, nil
}

func logDriftDeadline(ctx context.Context, candidatesEvaluated, candidateCount, commandCount int) {
	log.FromContext(ctx).V(1).Info("drift scheduling simulation timed out",
		"candidates_evaluated", candidatesEvaluated,
		"commands", commandCount,
		"candidates_remaining", candidateCount-candidatesEvaluated,
	)
}

func newDriftCommand(candidate *Candidate, results scheduling.Results) Command {
	return Command{
		Candidates:          []*Candidate{candidate},
		Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
		Results:             results,
		PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
	}
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
