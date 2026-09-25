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
	terminateFirstReasonLabel    = "terminate_first_reason"
	methodLabel                  = "method"
	stageLabel                   = "stage"
	outcomeLabel                 = "outcome"
	kindLabel                    = "kind"

	// FailureReasonLabel records why a disruption command was discarded during validation.
	FailureReasonLabel = "failure_reason"

	// A candidate was no longer a valid disruption candidate when re-checked.
	ValidationFailureReasonChurn = "churn"
	// A pod was nominated to schedule onto a candidate.
	ValidationFailureReasonNominated = "nominated"
	// Disrupting a candidate would no longer fit within its NodePool's disruption budget.
	ValidationFailureReasonBudget = "budget"
	// The re-run scheduling simulation could not place all of the candidates' pods.
	ValidationFailureReasonUnschedulable = "unschedulable"
	// The command expected a replacement, but the re-run scheduling simulation placed all pods on existing capacity.
	ValidationFailureReasonNoNewNodeClaim = "no_new_nodeclaim"
	// The re-run scheduling simulation required more than one new NodeClaim.
	ValidationFailureReasonMultipleNodeClaims = "multiple_nodeclaims"
	// The command expected no replacement, but the re-run scheduling simulation required a new NodeClaim.
	ValidationFailureReasonUnexpectedReplacement = "unexpected_replacement"
	// The command's replacement instance types were no longer a subset of those the re-run scheduling simulation allows.
	ValidationFailureReasonInstanceTypesNotSubset = "instance_types_not_subset"

	SimulationSessionStageBuild = "build"
	SimulationSessionStageFork  = "fork"

	SimulationSessionResultCreated  = "created"
	SimulationSessionResultFallback = "fallback"
	SimulationSessionResultStale    = "stale"

	SimulationInputKindTopologyStateNode = "topology_state_node"
	SimulationInputKindNodePool          = "nodepool"
	SimulationInputKindNodePoolTemplate  = "nodepool_template"
	SimulationInputKindInstanceType      = "instance_type"
	SimulationInputKindDaemonSetPod      = "daemonset_pod"
	SimulationInputKindTopologyKey       = "topology_key"
)

var (
	disruptionDurationBuckets = []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
	}
	disruptionCountBuckets = []float64{
		0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}
)

var (
	MultiNodeConsolidationType = opmetrics.Value{
		Name: "multi",
		Help: "Consolidation that considers removing multiple nodes at once.",
	}
	SingleNodeConsolidationType = opmetrics.Value{
		Name: "single",
		Help: "Consolidation that considers removing a single node.",
	}
	EmptyConsolidationType = opmetrics.Value{
		Name: "empty",
		Help: "Consolidation that removes empty nodes.",
	}
)

var (
	ConsolidationType = opmetrics.Label{
		Name:   ConsolidationTypeLabel,
		Help:   "The consolidation algorithm that produced the decision.",
		Values: []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType, EmptyConsolidationType},
	}
	// DecisionDim is the `decision` dimension for the voluntary-disruption decision
	// counters, whose value is the command's action.
	DecisionDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "The disruption decision taken for the candidate(s).",
		Values: []opmetrics.Value{
			{
				Name: string(NoOpDecision),
				Help: "No disruption action was taken.",
			},
			{
				Name: string(ReplaceDecision),
				Help: "The candidate(s) were replaced with more efficient capacity.",
			},
			{
				Name: string(DeleteDecision),
				Help: "The candidate(s) were deleted without replacement.",
			},
			{
				Name: string(TerminateFirstDecision),
				Help: "The candidate(s) were deleted without staging a replacement first; reactive provisioning refills afterward.",
			},
			{
				Name: string(RebootDecision),
				Help: "The candidate(s) were rebooted in place.",
			},
		},
	}
	// ApprovalDim is the `decision` dimension for the balanced-consolidation move
	// metrics, which score each candidate move and record whether it was approved or
	// rejected — a disjoint value set from DecisionDim, so it is a separate Label.
	ApprovalDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "Whether a scored balanced-consolidation move was approved or rejected.",
		Values: []opmetrics.Value{
			{
				Name: string(ApprovedDecision),
				Help: "The move's cost savings justified the pod disruption; it was approved.",
			},
			{
				Name: string(RejectedDecision),
				Help: "The move's cost savings did not justify the pod disruption; it was rejected.",
			},
		},
	}
	Policy = opmetrics.Label{
		Name: policyLabel,
		Help: "The NodePool consolidation policy in effect for the move.",
	}
	SimulationMethod = opmetrics.Label{
		Name:   methodLabel,
		Help:   "The consolidation method that owns the simulation session.",
		Values: []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType, EmptyConsolidationType},
	}
	SimulationStage = opmetrics.Label{
		Name: stageLabel,
		Help: "The simulation session stage.",
		Values: []opmetrics.Value{
			{Name: SimulationSessionStageBuild, Help: "Building the pass-scoped immutable scheduler inputs."},
			{Name: SimulationSessionStageFork, Help: "Forking mutable scheduler state for a single simulation."},
		},
	}
	SimulationOutcome = opmetrics.Label{
		Name: outcomeLabel,
		Help: "The result of creating a simulation session.",
		Values: []opmetrics.Value{
			{Name: SimulationSessionResultCreated, Help: "The session was created."},
			{Name: SimulationSessionResultFallback, Help: "The session couldn't be created, so simulations fell back to building scheduler inputs each time."},
			{Name: SimulationSessionResultStale, Help: "The session's inputs went stale during the pass."},
		},
	}
	SimulationInputKind = opmetrics.Label{
		Name: kindLabel,
		Help: "The kind of immutable scheduler input prepared for a simulation session.",
		Values: []opmetrics.Value{
			{Name: SimulationInputKindTopologyStateNode, Help: "State nodes tracked for topology."},
			{Name: SimulationInputKindNodePool, Help: "NodePools."},
			{Name: SimulationInputKindNodePoolTemplate, Help: "NodeClaim templates built from NodePools."},
			{Name: SimulationInputKindInstanceType, Help: "Instance types."},
			{Name: SimulationInputKindDaemonSetPod, Help: "DaemonSet pods."},
			{Name: SimulationInputKindTopologyKey, Help: "Topology keys."},
		},
	}
	FailureReason = opmetrics.Label{
		Name: FailureReasonLabel,
		Help: "Why the disruption command was discarded during validation.",
		Values: []opmetrics.Value{
			{Name: ValidationFailureReasonChurn, Help: "A candidate was no longer a valid disruption candidate when re-checked."},
			{Name: ValidationFailureReasonNominated, Help: "A pod was nominated to schedule onto a candidate."},
			{Name: ValidationFailureReasonBudget, Help: "Disrupting a candidate would no longer fit within its NodePool's disruption budget."},
			{Name: ValidationFailureReasonUnschedulable, Help: "The re-run scheduling simulation could not place all of the candidates' pods."},
			{Name: ValidationFailureReasonNoNewNodeClaim, Help: "The command expected a replacement, but the re-run scheduling simulation placed all pods on existing capacity."},
			{Name: ValidationFailureReasonMultipleNodeClaims, Help: "The re-run scheduling simulation required more than one new NodeClaim."},
			{Name: ValidationFailureReasonUnexpectedReplacement, Help: "The command expected no replacement, but the re-run scheduling simulation required a new NodeClaim."},
			{Name: ValidationFailureReasonInstanceTypesNotSubset, Help: "The command's replacement instance types were no longer a subset of those the re-run scheduling simulation allows."},
		},
	}
	TerminateFirstReasonDim = opmetrics.Label{
		Name: terminateFirstReasonLabel,
		Help: "Why the terminate-first command couldn't stage its replacement before terminating the node.",
		Values: []opmetrics.Value{
			{
				Name: string(TerminateFirstNoReservedCapacity),
				Help: "The replacement has no reserved capacity to launch into except the slot the node holds in a full capacity reservation.",
			},
			{
				Name: string(TerminateFirstStaticAtLimit),
				Help: "The node's static NodePool is at its node limit, so the replacement can't launch alongside it.",
			},
		},
	}
)

func init() {
	// Initialize the consolidation_type series that can time out to 0. Only the
	// multi- and single-node algorithms run a bounded search that can hit a timeout;
	// empty-node consolidation does not, so it is not pre-initialized here.
	for _, ct := range []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType} {
		ConsolidationTimeoutsTotal.Add(0, map[string]string{ConsolidationTypeLabel: ct.Name})
	}
}

var (
	SimulationSessionDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_session_duration_seconds",
			Help:      "[ALPHA] Monotonic wall-clock duration in seconds of pass-scoped simulation session build and per-simulation mutable fork stages.",
			Buckets:   disruptionDurationBuckets,
		},
		[]opmetrics.Label{SimulationMethod, SimulationStage},
		opmetrics.Alpha,
	)
	SimulationSessionTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_session_total",
			Help:      "[ALPHA] Number of pass-scoped simulation sessions by bounded creation, stale, or legacy-fallback result.",
		},
		[]opmetrics.Label{SimulationMethod, SimulationOutcome},
		opmetrics.Alpha,
	)
	SimulationSessionSharedInputCount = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "simulation_session_shared_input_count",
			Help:      "[ALPHA] Count of immutable scheduler inputs prepared once for a pass-scoped simulation session, split by bounded kind.",
			Buckets:   disruptionCountBuckets,
		},
		[]opmetrics.Label{SimulationMethod, SimulationInputKind},
		opmetrics.Alpha,
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
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
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
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
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
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.GA,
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{metrics.NodePool, DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	TerminateFirstDecisionsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "terminate_first_decisions_total",
			Help:      "Number of terminate-first disruption decisions performed. Labeled by nodepool name, reason, and why the replacement couldn't be staged first.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason, TerminateFirstReasonDim},
		opmetrics.Alpha,
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]opmetrics.Label{metrics.DisruptionReason},
		opmetrics.Beta,
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Beta,
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type and failure reason.",
		},
		[]opmetrics.Label{ConsolidationType, FailureReason},
		opmetrics.Alpha,
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.GA,
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.Alpha,
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	// NodeClaimsUnhealthyDisruptedTotal preserves the per-condition/per-image breakdown the retired node.health
	// controller emitted, which the reason-labeled karpenter_nodeclaims_disrupted_total loses. Labeled by the repair
	// condition, the owning NodePool, the capacity type, and the image ID.
	NodeClaimsUnhealthyDisruptedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeClaimSubsystem,
			Name:      "unhealthy_disrupted_total",
			Help:      "Number of unhealthy nodeclaims disrupted in total by node repair. Labeled by the condition the node was disrupted on, the owning nodepool, the capacity type, the image ID, and the termination mode.",
		},
		[]opmetrics.Label{RepairCondition, metrics.NodePool, metrics.CapacityType, ImageID, metrics.TerminationMode},
		opmetrics.Alpha,
	)
	DriftBackoffsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "drift_backoffs_total",
			Help:      "The number of times a NodePool entered or escalated drift replacement back-off after an unrecoverable failure. Labeled by NodePool.",
		},
		[]opmetrics.Label{metrics.NodePool},
		opmetrics.Alpha,
	)
)

const (
	conditionLabel = "condition"
	imageIDLabel   = "image_id"
)

var (
	RepairCondition = opmetrics.Label{
		Name: conditionLabel,
		Help: "The node status condition type that triggered node repair disruption.",
	}
	ImageID = opmetrics.Label{
		Name: imageIDLabel,
		Help: "The image ID of the node that was disrupted.",
	}
)
