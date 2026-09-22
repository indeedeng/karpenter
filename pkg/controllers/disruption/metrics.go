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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"

	methodLabel      = "method"
	stageLabel       = "stage"
	outcomeLabel     = "outcome"
	dispositionLabel = "disposition"
	sourceLabel      = "source"
	destinationLabel = "destination"
	operationLabel   = "operation"
	kindLabel        = "kind"
	phaseLabel       = "phase"

	MetricLabelMultiple = "<multiple>"
	MetricLabelNone     = "<none>"

	MethodEmpty       = "empty"
	MethodDrift       = "drift"
	MethodStaticDrift = "static_drift"
	MethodSingle      = "single"
	MethodMulti       = "multi"

	CandidateStagePossible       = "possible"
	CandidateStageEligible       = "eligible"
	CandidateStageBudgetEligible = "budget_eligible"

	SimulationStageEvaluation = "evaluation"
	SimulationStageValidation = "validation"

	ValidationStageDelay                  = "delay"
	ValidationStageCandidateRefreshBefore = "candidate_refresh_before"
	ValidationStageSimulation             = "simulation"
	ValidationStageCandidateRefreshAfter  = "candidate_refresh_after"
	ValidationStageTotal                  = "total"

	PassOutcomeNoCandidates     = "no_candidates"
	PassOutcomeNoCommand        = "no_command"
	PassOutcomeUnchanged        = "unchanged"
	PassOutcomeBudgetBlocked    = "budget_blocked"
	PassOutcomeValidationFailed = "validation_failed"
	PassOutcomeSelected         = "selected"
	PassOutcomePartialSelected  = "partial_selected"
	PassOutcomeQueueRejected    = "queue_rejected"
	PassOutcomeTimeout          = "timeout"
	PassOutcomeError            = "error"

	SimulationOutcomeSchedulable       = "schedulable"
	SimulationOutcomeUnschedulable     = "unschedulable"
	SimulationOutcomeCandidateDeleting = "candidate_deleting"
	SimulationOutcomeTimeout           = "timeout"
	SimulationOutcomeError             = "error"

	ValidationOutcomeSuccess           = "success"
	ValidationOutcomeCandidateDeleting = "candidate_deleting"
	ValidationOutcomeInvalidated       = "invalidated"
	ValidationOutcomeTimeout           = "timeout"
	ValidationOutcomeError             = "error"

	OpportunityDispositionSelected         = "selected"
	OpportunityDispositionSuperseded       = "superseded"
	OpportunityDispositionPolicyRejected   = "policy_rejected"
	OpportunityDispositionValidationFailed = "validation_failed"
	OpportunityDispositionQueueRejected    = "queue_rejected"
	OpportunityDispositionError            = "error"

	UnevaluatedReasonBudget        = "budget"
	UnevaluatedReasonTimeout       = "timeout"
	UnevaluatedReasonSearchLimit   = "search_limit"
	UnevaluatedReasonSelectedEarly = "selected_early"
	UnevaluatedReasonPolicy        = "policy"
	UnevaluatedReasonError         = "error"

	SimulationOperationPodAttempt             = "pod_attempt"
	SimulationOperationPreferenceRelaxation   = "preference_relaxation"
	SimulationOperationExistingNodeCheck      = "existing_node_check"
	SimulationOperationInflightNodeClaimCheck = "inflight_nodeclaim_check"
	SimulationOperationNodePoolTemplateCheck  = "nodepool_template_check"
	SimulationOperationTopologyMatchCheck     = "topology_match_check"
	SimulationOperationTopologyAPIRead        = "topology_api_read"

	SimulationPhaseTopologyBuild        = "topology_build"
	SimulationPhaseSchedulerBuild       = "scheduler_build"
	SimulationPhasePodDataPreparation   = "pod_data_preparation"
	SimulationPhaseScheduling           = "scheduling"
	SimulationPhaseResultFinalization   = "result_finalization"
	SimulationPhaseSolve                = "solve"
	SimulationPhaseVolumeTopologyLookup = "volume_topology_lookup"
	SimulationPhaseDynamicResourceSetup = "dynamic_resource_setup"

	SimulationPodSourceCandidate = "candidate"
	SimulationPodSourcePending   = "pending"
	SimulationPodSourceDeleting  = "deleting"
	SimulationPodSourceExcluded  = "excluded"
	SimulationPodSourceTotal     = "total"

	SimulationDestinationExistingNode = "existing_node"
	SimulationDestinationNewNodeClaim = "new_nodeclaim"

	TimeoutCandidateKindEligible    = "eligible"
	TimeoutCandidateKindEvaluated   = "evaluated"
	TimeoutCandidateKindUnevaluated = "unevaluated"

	TopologyConstraintKindSpread              = "spread"
	TopologyConstraintKindAffinity            = "affinity"
	TopologyConstraintKindAntiAffinity        = "anti_affinity"
	TopologyConstraintKindInverseAntiAffinity = "inverse_anti_affinity"

	SimulationInputKindPod                   = "pod"
	SimulationInputKindTopologyStateNode     = "topology_state_node"
	SimulationInputKindAccountingStateNode   = "accounting_state_node"
	SimulationInputKindNodePool              = "nodepool"
	SimulationInputKindNodePoolTemplate      = "nodepool_template"
	SimulationInputKindInstanceType          = "instance_type"
	SimulationInputKindDaemonSetPod          = "daemonset_pod"
	SimulationInputKindAdditionalExcludedPod = "additional_excluded_pod"
	SimulationInputKindPriorRemovedCandidate = "prior_removed_candidate"

	SimulationResultKindNewNodeClaim      = "new_nodeclaim"
	SimulationResultKindExistingNode      = "existing_node"
	SimulationResultKindPodOnNewNodeClaim = "pod_on_new_nodeclaim"
	SimulationResultKindPodOnExistingNode = "pod_on_existing_node"
	SimulationResultKindPodError          = "pod_error"
)

var (
	disruptionDurationBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
	}
	disruptionCountBuckets = []float64{
		0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}
	disruptionProviderPriceBuckets = []float64{
		-1000, -500, -250, -100, -50, -25, -10, -5, -1, 0, 1, 5, 10, 25, 50, 100, 250, 500, 1000,
	}
)

func init() {
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: MultiNodeConsolidationType})
	ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: SingleNodeConsolidationType})
}

var (
	PassesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "passes_total",
			Help:      "[ALPHA] Number of completed voluntary disruption method passes, including every non-canceled terminal return. Labeled by bounded method and outcome.",
		},
		[]string{methodLabel, outcomeLabel},
	)
	LastEvaluatedTimestampSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "last_evaluated_timestamp_seconds",
			Help:      "[ALPHA] Unix timestamp of the latest completed voluntary disruption method pass. This is a cluster-wide freshness snapshot labeled only by bounded method.",
		},
		[]string{methodLabel},
	)
	SimulationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_duration_seconds",
			Help:      "[ALPHA] Monotonic wall-clock duration in seconds of a complete disruption scheduling simulation. Labeled by bounded method, evaluation or validation stage, and outcome.",
			Buckets:   disruptionDurationBuckets,
		},
		[]string{methodLabel, stageLabel, outcomeLabel},
	)
	SimulationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulations_total",
			Help:      "[ALPHA] Number of disruption scheduling simulations. Labeled by bounded method, NodePool scope, evaluation or validation stage, and outcome; shared or absent NodePool scope uses the documented sentinels.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, stageLabel, outcomeLabel},
	)
	CandidateBatchSize = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "candidate_batch_size",
			Help:      "[ALPHA] Number of candidate nodes supplied to each disruption scheduling simulation. Labeled by bounded method and evaluation or validation stage.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, stageLabel},
	)
	CandidatesEvaluatedPerPass = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "candidates_evaluated_per_pass",
			Help:      "[ALPHA] Number of distinct candidates included in evaluation-stage scheduling simulations during a completed method pass. Labeled by bounded method and terminal outcome.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, outcomeLabel},
	)
	SimulationPodCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_pod_count",
			Help:      "[ALPHA] Number of distinct pods supplied to a disruption scheduling simulation. Labeled by bounded method, evaluation or validation stage, and pod source.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, stageLabel, sourceLabel},
	)
	SimulationPodPlacementsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_pod_placements_total",
			Help:      "[ALPHA] Number of candidate pods placed by selected simulations, labeled by bounded method, NodePool scope, and existing-node or new-NodeClaim destination.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, destinationLabel},
	)
	ValidationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "validation_duration_seconds",
			Help:      "[ALPHA] Monotonic wall-clock duration in seconds of bounded validation stages, including delay, candidate refresh before simulation, simulation, candidate refresh after simulation, and total validation. Labeled by method, stage, and outcome.",
			Buckets:   disruptionDurationBuckets,
		},
		[]string{methodLabel, stageLabel, outcomeLabel},
	)
	Candidates = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "candidates",
			Help:      "[ALPHA] Latest completed-pass candidate snapshot by bounded method, NodePool, and stage. Possible candidates passed common construction, eligible candidates also passed the method predicate, and budget-eligible candidates also had disruption budget.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, stageLabel},
	)
	SelectedCandidatesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "selected_candidates_total",
			Help:      "[ALPHA] Number of candidates in validated commands accepted by the disruption queue. Labeled by bounded method, NodePool, and delete or replace decision.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, decisionLabel},
	)
	OldestEligibleTimestampSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "oldest_eligible_timestamp_seconds",
			Help:      "[ALPHA] Unix timestamp of the oldest candidate's durable disruption eligibility condition in the latest completed-pass snapshot. Labeled by bounded method and NodePool; use last_evaluated_timestamp_seconds to assess freshness.",
		},
		[]string{methodLabel, metrics.NodePoolLabel},
	)
	TimeoutCandidateCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "timeout_candidate_count",
			Help:      "[ALPHA] Candidate-count distribution for timed-out method passes, split into eligible, distinctly evaluated, and unevaluated bounded kinds.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, kindLabel},
	)
	SimulationTopologyConstraintCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_topology_constraint_count",
			Help:      "[ALPHA] Number of active, deduplicated topology groups immediately after scheduler construction, split by bounded topology kind and including inverse anti-affinity.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, kindLabel},
	)
	SimulationTopologyKeyCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_topology_key_count",
			Help:      "[ALPHA] Number of distinct topology keys referenced by active, deduplicated topology groups immediately after scheduler construction. Raw topology keys are never labels.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel},
	)
	SimulationPhaseDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_phase_duration_seconds",
			Help:      "[ALPHA] Monotonic wall-clock duration in seconds of bounded simulation phases. Reused state is observed only when work is performed, not once per simulation.",
			Buckets:   disruptionDurationBuckets,
		},
		[]string{methodLabel, phaseLabel},
	)
	SimulationInputCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_input_count",
			Help:      "[ALPHA] Count of simulation inputs and constructed scheduler state by bounded kind.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, kindLabel},
	)
	SimulationWorkCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_work_count",
			Help:      "[ALPHA] Exact work-unit count performed by a simulation, emitted once after completion and split by bounded operation.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, operationLabel},
	)
	SimulationResultCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_result_count",
			Help:      "[ALPHA] Count of scheduling simulation results by bounded kind.",
			Buckets:   disruptionCountBuckets,
		},
		[]string{methodLabel, kindLabel},
	)
	ConsolidationOpportunityOptimisticEstimatedSavingsPrice = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_optimistic_estimated_savings_price",
			Help:      "[ALPHA] Optimistic estimated savings for completely priced consolidation opportunities, in the cloud provider's Offering.Price units. Currency and billing period are provider-defined; values are not realized billing savings. Labeled by bounded terminal disposition.",
			Buckets:   disruptionProviderPriceBuckets,
		},
		[]string{methodLabel, dispositionLabel},
	)
	ConsolidationOpportunityLatestMaxBlockedOptimisticEstimatedSavingsPrice = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_latest_max_blocked_optimistic_estimated_savings_price",
			Help:      "[ALPHA] Largest completely priced positive optimistic estimated consolidation savings blocked in the latest completed-pass snapshot, in provider-defined Offering.Price units. Superseded overlapping opportunities are excluded.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, dispositionLabel},
	)
	ConsolidationOpportunityUnevaluatedSourceCostPrice = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_unevaluated_source_cost_price",
			Help:      "[ALPHA] Completely priced eligible source cost omitted from simulations in the latest completed-pass snapshot, in provider-defined Offering.Price units. This is unevaluated cost, not achievable savings, and is labeled by bounded reason.",
		},
		[]string{methodLabel, metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	ConsolidationOpportunityEvaluationCoverageRatio = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_evaluation_coverage_ratio",
			Help:      "[ALPHA] Ratio from zero to one of completely priced eligible source cost included in simulations in the latest completed-pass snapshot. Labeled by bounded method and NodePool.",
		},
		[]string{methodLabel, metrics.NodePoolLabel},
	)
	ConsolidationOpportunityPricingCoverageRatio = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_pricing_coverage_ratio",
			Help:      "[ALPHA] Ratio from zero to one of eligible candidates with a known provider source price in the latest completed-pass snapshot. Labeled by bounded method and NodePool.",
		},
		[]string{methodLabel, metrics.NodePoolLabel},
	)
	ConsolidationOpportunityUnpricedCandidateCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_opportunity_unpriced_candidate_count",
			Help:      "[ALPHA] Number of eligible candidates lacking a known provider source price in the latest completed-pass snapshot. Labeled by bounded method and NodePool.",
		},
		[]string{methodLabel, metrics.NodePoolLabel},
	)
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	CandidateDiscoveryDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "candidate_discovery_duration_seconds",
			Help:      "Duration of dynamic drift candidate discovery in seconds.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	DriftReplacementSimulationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "drift_replacement_simulation_duration_seconds",
			Help:      "Duration of each complete drift replacement simulation, including scheduler construction and solving.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{},
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]string{metrics.NodePoolLabel, decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]string{metrics.ReasonLabel},
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]string{ConsolidationTypeLabel},
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel},
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]string{decisionLabel, metrics.ReasonLabel, ConsolidationTypeLabel},
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]string{decisionLabel, metrics.NodePoolLabel, policyLabel},
	)
	DriftBackoffsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "drift_backoffs_total",
			Help:      "The number of times a NodePool entered or escalated drift replacement back-off after an unrecoverable failure. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel},
	)
)
