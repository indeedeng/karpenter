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
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

type passObservationContextKey struct{}
type simulationStageContextKey struct{}

type opportunityObservation struct {
	command     Command
	disposition string
}

// PassObservation contains bounded, ephemeral state for one disruption method
// invocation. Object identities are used only for in-process deduplication and
// are never exported as metric labels.
type PassObservation struct {
	mu     sync.Mutex
	method Method
	clock  clock.Clock

	candidatesSet  bool
	nodePools      []string
	possible       sets.Set[string]
	eligible       sets.Set[string]
	budgetEligible sets.Set[string]
	evaluated      sets.Set[string]
	candidates     map[string]*Candidate

	budgetBlocked sets.Set[string]
	searchLimited sets.Set[string]
	selectedEarly sets.Set[string]
	policySkipped sets.Set[string]

	timedOut         bool
	unchanged        bool
	validationFailed bool
	selected         int
	queueRejected    int
	opportunities    []opportunityObservation
}

func NewPassObservation(method Method, clk clock.Clock) *PassObservation {
	return &PassObservation{
		method:         method,
		clock:          clk,
		possible:       sets.New[string](),
		eligible:       sets.New[string](),
		budgetEligible: sets.New[string](),
		evaluated:      sets.New[string](),
		candidates:     map[string]*Candidate{},
		budgetBlocked:  sets.New[string](),
		searchLimited:  sets.New[string](),
		selectedEarly:  sets.New[string](),
		policySkipped:  sets.New[string](),
	}
}

func WithPassObservation(ctx context.Context, observation *PassObservation) context.Context {
	return context.WithValue(ctx, passObservationContextKey{}, observation)
}

func passObservationFromContext(ctx context.Context) *PassObservation {
	observation, _ := ctx.Value(passObservationContextKey{}).(*PassObservation)
	return observation
}

func WithSimulationStage(ctx context.Context, stage string) context.Context {
	return context.WithValue(ctx, simulationStageContextKey{}, stage)
}

func simulationStageFromContext(ctx context.Context) string {
	stage, _ := ctx.Value(simulationStageContextKey{}).(string)
	if stage == "" {
		return SimulationStageEvaluation
	}
	return stage
}

func (o *PassObservation) SetCandidates(candidateSet CandidateSet) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.candidatesSet = true
	o.nodePools = append([]string(nil), candidateSet.NodePools...)
	for _, candidate := range candidateSet.Possible {
		key := candidateKey(candidate)
		o.possible.Insert(key)
		o.candidates[key] = candidate
	}
	for _, candidate := range candidateSet.Eligible {
		key := candidateKey(candidate)
		o.eligible.Insert(key)
		o.candidates[key] = candidate
	}
	cleanupRemovedNodePoolGaugeSeries(candidateSet.NodePools)
}

func (o *PassObservation) RecordBudgetEligible(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.budgetEligible.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) RecordEvaluation(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.evaluated.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) MarkBudgetBlocked(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.budgetBlocked.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) MarkSearchLimited(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.searchLimited.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) MarkSelectedEarly(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.selectedEarly.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) MarkPolicySkipped(candidates ...*Candidate) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.policySkipped.Insert(candidateKeys(candidates...).UnsortedList()...)
}

func (o *PassObservation) MarkTimeout() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timedOut = true
}

func (o *PassObservation) MarkUnchanged() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unchanged = true
}

func (o *PassObservation) MarkValidationFailed() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.validationFailed = true
}

func (o *PassObservation) RecordOpportunity(command Command, disposition string) {
	if o == nil || !isConsolidationMethod(o.method) {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opportunities = append(o.opportunities, opportunityObservation{command: command, disposition: disposition})
}

func (o *PassObservation) RecordSelected(command Command) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.selected++
	if isConsolidationMethod(o.method) {
		o.opportunities = append(o.opportunities, opportunityObservation{command: command, disposition: OpportunityDispositionSelected})
	}
	o.mu.Unlock()
	recordSelectedCandidates(o.method, command)
	recordSelectedPodPlacements(o.method, command)
}

func (o *PassObservation) RecordQueueRejected(command Command) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queueRejected++
	if isConsolidationMethod(o.method) {
		o.opportunities = append(o.opportunities, opportunityObservation{command: command, disposition: OpportunityDispositionQueueRejected})
	}
}

func (o *PassObservation) Complete(success bool, retErr error) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	outcome := o.passOutcome(success, retErr)
	labels := o.methodLabels()
	PassesTotal.Inc(withLabel(labels, outcomeLabel, outcome))
	LastEvaluatedTimestampSeconds.Set(float64(o.clock.Now().Unix()), labels)
	evaluated := o.eligible.Intersection(o.evaluated)
	CandidatesEvaluatedPerPass.Observe(float64(evaluated.Len()), withLabel(labels, outcomeLabel, outcome))
	if o.timedOut {
		TimeoutCandidateCount.Observe(float64(o.eligible.Len()), withLabel(labels, kindLabel, TimeoutCandidateKindEligible))
		TimeoutCandidateCount.Observe(float64(evaluated.Len()), withLabel(labels, kindLabel, TimeoutCandidateKindEvaluated))
		TimeoutCandidateCount.Observe(float64(o.eligible.Difference(evaluated).Len()), withLabel(labels, kindLabel, TimeoutCandidateKindUnevaluated))
	}
	if o.candidatesSet {
		o.emitCandidateSnapshot()
		if isConsolidationMethod(o.method) {
			o.emitOpportunitySnapshot(retErr)
		}
	}
}

func (o *PassObservation) passOutcome(success bool, retErr error) string {
	switch {
	case o.selected > 0 && (retErr != nil || o.queueRejected > 0):
		return PassOutcomePartialSelected
	case o.selected > 0 || success:
		return PassOutcomeSelected
	case o.queueRejected > 0:
		return PassOutcomeQueueRejected
	case o.timedOut:
		return PassOutcomeTimeout
	case retErr != nil:
		return PassOutcomeError
	case o.validationFailed:
		return PassOutcomeValidationFailed
	case o.unchanged:
		return PassOutcomeUnchanged
	case o.eligible.Len() == 0:
		return PassOutcomeNoCandidates
	case o.budgetEligible.Len() == 0 && o.budgetBlocked.Len() > 0:
		return PassOutcomeBudgetBlocked
	default:
		return PassOutcomeNoCommand
	}
}

func (o *PassObservation) emitCandidateSnapshot() {
	labels := o.methodLabels()
	possibleByPool := o.countByNodePool(o.possible)
	eligibleByPool := o.countByNodePool(o.eligible)
	budgetEligibleByPool := o.countByNodePool(o.budgetEligible)
	oldestByPool := map[string]time.Time{}
	for key := range o.eligible {
		candidate := o.candidates[key]
		if candidate == nil || candidate.NodeClaim == nil {
			continue
		}
		conditionType := v1.ConditionTypeConsolidatable
		if o.method.Reason() == v1.DisruptionReasonDrifted {
			conditionType = v1.ConditionTypeDrifted
		}
		condition := candidate.NodeClaim.StatusConditions().Get(conditionType)
		if condition == nil || !condition.IsTrue() || condition.LastTransitionTime.IsZero() {
			continue
		}
		pool := candidate.NodePool.Name
		if oldest, ok := oldestByPool[pool]; !ok || condition.LastTransitionTime.Time.Before(oldest) {
			oldestByPool[pool] = condition.LastTransitionTime.Time
		}
	}
	snapshot := []*metrics.StoreMetric{}
	for _, pool := range o.nodePools {
		poolLabels := withLabel(labels, metrics.NodePoolLabel, pool)
		snapshot = append(snapshot,
			&metrics.StoreMetric{
				GaugeMetric: Candidates,
				Value:       float64(possibleByPool[pool]),
				Labels:      withLabel(poolLabels, stageLabel, CandidateStagePossible),
			},
			&metrics.StoreMetric{
				GaugeMetric: Candidates,
				Value:       float64(eligibleByPool[pool]),
				Labels:      withLabel(poolLabels, stageLabel, CandidateStageEligible),
			},
			&metrics.StoreMetric{
				GaugeMetric: Candidates,
				Value:       float64(budgetEligibleByPool[pool]),
				Labels:      withLabel(poolLabels, stageLabel, CandidateStageBudgetEligible),
			},
		)
		if oldest, ok := oldestByPool[pool]; ok {
			snapshot = append(snapshot, &metrics.StoreMetric{
				GaugeMetric: OldestEligibleTimestampSeconds,
				Value:       float64(oldest.Unix()),
				Labels:      poolLabels,
			})
		}
	}
	candidateSnapshotStore.Update(o.method.Name(), snapshot)
}

func (o *PassObservation) emitOpportunitySnapshot(retErr error) {
	labels := o.methodLabels()
	ConsolidationOpportunityLatestMaxBlockedOptimisticEstimatedSavingsPrice.DeletePartialMatch(labels)
	ConsolidationOpportunityUnevaluatedSourceCostPrice.DeletePartialMatch(labels)
	ConsolidationOpportunityEvaluationCoverageRatio.DeletePartialMatch(labels)
	ConsolidationOpportunityPricingCoverageRatio.DeletePartialMatch(labels)
	ConsolidationOpportunityUnpricedCandidateCount.DeletePartialMatch(labels)

	blockedMax := map[string]map[string]float64{}
	for _, opportunity := range o.opportunities {
		savings, complete := opportunity.command.EstimatedOpportunitySavings()
		if !complete {
			continue
		}
		ConsolidationOpportunityOptimisticEstimatedSavingsPrice.Observe(savings, withLabel(labels, dispositionLabel, opportunity.disposition))
		if savings <= 0 || !isBlockedOpportunityDisposition(opportunity.disposition) {
			continue
		}
		pool := candidateNodePoolScope(opportunity.command.Candidates...)
		if blockedMax[pool] == nil {
			blockedMax[pool] = map[string]float64{}
		}
		blockedMax[pool][opportunity.disposition] = math.Max(blockedMax[pool][opportunity.disposition], savings)
	}

	eligibleByPool := o.candidatesByNodePool(o.eligible)
	reasons := []string{
		UnevaluatedReasonBudget,
		UnevaluatedReasonTimeout,
		UnevaluatedReasonSearchLimit,
		UnevaluatedReasonSelectedEarly,
		UnevaluatedReasonPolicy,
		UnevaluatedReasonError,
	}
	dispositions := []string{
		OpportunityDispositionPolicyRejected,
		OpportunityDispositionValidationFailed,
		OpportunityDispositionQueueRejected,
		OpportunityDispositionError,
	}
	for _, pool := range o.nodePools {
		poolLabels := withLabel(labels, metrics.NodePoolLabel, pool)
		candidates := eligibleByPool[pool]
		sourceCost, priced := completelyPricedSourceCost(candidates)
		evaluatedCost := 0.0
		unpriced := len(candidates) - priced
		unevaluatedByReason := map[string]float64{}
		for _, candidate := range candidates {
			key := candidateKey(candidate)
			if o.evaluated.Has(key) {
				if candidate.PriceKnown {
					evaluatedCost += candidate.Price
				}
				continue
			}
			if !candidate.PriceKnown {
				continue
			}
			reason := o.unevaluatedReason(key, retErr)
			unevaluatedByReason[reason] += candidate.Price
		}
		evaluationCoverage := 0.0
		if sourceCost > 0 {
			evaluationCoverage = math.Min(1, evaluatedCost/sourceCost)
		}
		pricingCoverage := 1.0
		if len(candidates) > 0 {
			pricingCoverage = float64(priced) / float64(len(candidates))
		}
		ConsolidationOpportunityEvaluationCoverageRatio.Set(evaluationCoverage, poolLabels)
		ConsolidationOpportunityPricingCoverageRatio.Set(pricingCoverage, poolLabels)
		ConsolidationOpportunityUnpricedCandidateCount.Set(float64(unpriced), poolLabels)
		for _, reason := range reasons {
			ConsolidationOpportunityUnevaluatedSourceCostPrice.Set(unevaluatedByReason[reason], withLabel(poolLabels, metrics.ReasonLabel, reason))
		}
		for _, disposition := range dispositions {
			ConsolidationOpportunityLatestMaxBlockedOptimisticEstimatedSavingsPrice.Set(
				blockedMax[pool][disposition],
				withLabel(poolLabels, dispositionLabel, disposition),
			)
		}
	}
	if byDisposition, ok := blockedMax[MetricLabelMultiple]; ok {
		poolLabels := withLabel(labels, metrics.NodePoolLabel, MetricLabelMultiple)
		for _, disposition := range dispositions {
			ConsolidationOpportunityLatestMaxBlockedOptimisticEstimatedSavingsPrice.Set(
				byDisposition[disposition],
				withLabel(poolLabels, dispositionLabel, disposition),
			)
		}
	}
}

func (o *PassObservation) unevaluatedReason(key string, retErr error) string {
	switch {
	case o.budgetBlocked.Has(key):
		return UnevaluatedReasonBudget
	case o.searchLimited.Has(key):
		return UnevaluatedReasonSearchLimit
	case o.selectedEarly.Has(key):
		return UnevaluatedReasonSelectedEarly
	case o.policySkipped.Has(key):
		return UnevaluatedReasonPolicy
	case o.timedOut:
		return UnevaluatedReasonTimeout
	case retErr != nil:
		return UnevaluatedReasonError
	default:
		return UnevaluatedReasonPolicy
	}
}

func (o *PassObservation) countByNodePool(keys sets.Set[string]) map[string]int {
	result := map[string]int{}
	for key := range keys {
		if candidate := o.candidates[key]; candidate != nil && candidate.NodePool != nil {
			result[candidate.NodePool.Name]++
		}
	}
	return result
}

func (o *PassObservation) candidatesByNodePool(keys sets.Set[string]) map[string][]*Candidate {
	result := map[string][]*Candidate{}
	for key := range keys {
		if candidate := o.candidates[key]; candidate != nil && candidate.NodePool != nil {
			result[candidate.NodePool.Name] = append(result[candidate.NodePool.Name], candidate)
		}
	}
	return result
}

func (o *PassObservation) methodLabels() map[string]string {
	return map[string]string{methodLabel: o.method.Name()}
}

func isConsolidationMethod(method Method) bool {
	return method != nil && (method.Name() == MethodSingle || method.Name() == MethodMulti)
}

func isBlockedOpportunityDisposition(disposition string) bool {
	switch disposition {
	case OpportunityDispositionPolicyRejected,
		OpportunityDispositionValidationFailed,
		OpportunityDispositionQueueRejected,
		OpportunityDispositionError:
		return true
	default:
		return false
	}
}

func candidateKeys(candidates ...*Candidate) sets.Set[string] {
	result := sets.New[string]()
	for _, candidate := range candidates {
		if candidate != nil {
			result.Insert(candidateKey(candidate))
		}
	}
	return result
}

func candidateKey(candidate *Candidate) string {
	if candidate == nil {
		return MetricLabelNone
	}
	if candidate.NodeClaim != nil && candidate.NodeClaim.UID != "" {
		return string(candidate.NodeClaim.UID)
	}
	pool := MetricLabelNone
	if candidate.NodePool != nil && candidate.NodePool.Name != "" {
		pool = candidate.NodePool.Name
	}
	return pool + "/" + candidate.Name()
}

func candidateNodePoolScope(candidates ...*Candidate) string {
	nodePools := sets.New[string]()
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || candidate.NodePool.Name == "" {
			nodePools.Insert(MetricLabelNone)
			continue
		}
		nodePools.Insert(candidate.NodePool.Name)
	}
	switch nodePools.Len() {
	case 0:
		return MetricLabelNone
	case 1:
		return nodePools.UnsortedList()[0]
	default:
		return MetricLabelMultiple
	}
}

func withLabel(labels map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(labels)+1)
	for label, labelValue := range labels {
		result[label] = labelValue
	}
	result[key] = value
	return result
}

func recordSelectedCandidates(method Method, command Command) {
	counts := map[string]int{}
	for _, candidate := range command.Candidates {
		if candidate != nil && candidate.NodePool != nil {
			counts[candidate.NodePool.Name]++
		}
	}
	for pool, count := range counts {
		SelectedCandidatesTotal.Add(float64(count), map[string]string{
			methodLabel:           method.Name(),
			metrics.NodePoolLabel: pool,
			decisionLabel:         string(command.Decision()),
		})
	}
}

func recordSelectedPodPlacements(method Method, command Command) {
	candidatePodPools := map[string]string{}
	for _, candidate := range command.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		for _, pod := range candidate.reschedulablePods {
			candidatePodPools[podKey(pod)] = candidate.NodePool.Name
		}
	}
	podErrors := sets.New[string]()
	for pod := range command.Results.PodErrors {
		podErrors.Insert(podKey(pod))
	}
	seen := sets.New[string]()
	counts := map[string]map[string]int{}
	record := func(pod *corev1.Pod, destination string) {
		key := podKey(pod)
		pool, ok := candidatePodPools[key]
		if !ok || seen.Has(key) || podErrors.Has(key) {
			return
		}
		seen.Insert(key)
		if counts[pool] == nil {
			counts[pool] = map[string]int{}
		}
		counts[pool][destination]++
	}
	for _, node := range command.Results.ExistingNodes {
		for _, pod := range node.Pods {
			record(pod, SimulationDestinationExistingNode)
		}
	}
	for _, nodeClaim := range command.Results.NewNodeClaims {
		for _, pod := range nodeClaim.Pods {
			record(pod, SimulationDestinationNewNodeClaim)
		}
	}
	for pool, byDestination := range counts {
		for destination, count := range byDestination {
			SimulationPodPlacementsTotal.Add(float64(count), map[string]string{
				methodLabel:           method.Name(),
				metrics.NodePoolLabel: pool,
				destinationLabel:      destination,
			})
		}
	}
}

func podKey(pod *corev1.Pod) string {
	if pod == nil {
		return MetricLabelNone
	}
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

func measureValidationStage(ctx context.Context, stage string) func(...error) {
	observation := passObservationFromContext(ctx)
	if observation == nil {
		return func(...error) {}
	}
	started := time.Now()
	return func(errs ...error) {
		var err error
		if len(errs) > 0 {
			err = errs[0]
		}
		outcome := validationOutcome(err)
		ValidationDurationSeconds.Observe(time.Since(started).Seconds(), map[string]string{
			methodLabel:  observation.method.Name(),
			stageLabel:   stage,
			outcomeLabel: outcome,
		})
	}
}

func validationOutcome(err error) string {
	switch {
	case err == nil:
		return ValidationOutcomeSuccess
	case errors.Is(err, errCandidateDeleting):
		return ValidationOutcomeCandidateDeleting
	case errors.Is(err, context.DeadlineExceeded):
		return ValidationOutcomeTimeout
	case IsValidationError(err):
		return ValidationOutcomeInvalidated
	default:
		return ValidationOutcomeError
	}
}

var observedNodePools = struct {
	sync.Mutex
	names sets.Set[string]
}{names: sets.New[string]()}

var candidateSnapshotStore = metrics.NewStore()

func cleanupRemovedNodePoolGaugeSeries(nodePools []string) {
	observedNodePools.Lock()
	defer observedNodePools.Unlock()
	current := sets.New(nodePools...)
	for pool := range observedNodePools.names.Difference(current) {
		labels := map[string]string{metrics.NodePoolLabel: pool}
		Candidates.DeletePartialMatch(labels)
		OldestEligibleTimestampSeconds.DeletePartialMatch(labels)
		ConsolidationOpportunityLatestMaxBlockedOptimisticEstimatedSavingsPrice.DeletePartialMatch(labels)
		ConsolidationOpportunityUnevaluatedSourceCostPrice.DeletePartialMatch(labels)
		ConsolidationOpportunityEvaluationCoverageRatio.DeletePartialMatch(labels)
		ConsolidationOpportunityPricingCoverageRatio.DeletePartialMatch(labels)
		ConsolidationOpportunityUnpricedCandidateCount.DeletePartialMatch(labels)
	}
	observedNodePools.names = current
}
