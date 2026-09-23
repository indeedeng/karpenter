# Disruption Performance Metrics

## Status

This document defines alpha Prometheus contracts for voluntary disruption performance. Alpha metrics are always registered, and their names, labels, accuracy, and runtime cost may change before stabilization. Every alpha metric has `[ALPHA]` at the beginning of its HELP text.

The implementation is strictly additive. Existing disruption metrics retain their names, labels, HELP text, and semantics.

## Scope

The metrics cover:

- cluster-wide progress through voluntary disruption methods;
- scheduling simulation latency, inputs, work, topology complexity, and results;
- candidate funnel, selection, timeout, and eligibility freshness snapshots; and
- optimistic consolidation opportunity estimates and their evaluation and pricing coverage.

Expiration and node repair are excluded. Their lifecycle and health metrics remain separate contracts.

Instrumentation is observational. It must not run an additional scheduling simulation, change candidate ordering, change a scheduling result, or perform provider lookups solely to populate a metric.

All metric names below have the `karpenter_voluntary_disruption_` prefix.

## Bounded labels

Labels contain no node, pod, namespace, instance type, zone, object ID, raw error, or raw topology key. The allowed values are compile-time constants.

- `method`: `empty`, `drift`, `static_drift`, `single`, `multi`.
- candidate `stage`: `possible`, `eligible`, `budget_eligible`.
- simulation `stage`: `evaluation`, `validation`.
- simulation-session `stage`: `build`, `fork`.
- simulation-session `outcome`: `created`, `stale`, `fallback`.
- validation `stage`: `delay`, `candidate_refresh_before`, `simulation`, `candidate_refresh_after`, `total`.
- pass `outcome`: `no_candidates`, `no_command`, `unchanged`, `budget_blocked`, `validation_failed`, `selected`, `partial_selected`, `queue_rejected`, `timeout`, `error`.
- simulation `outcome`: `schedulable`, `unschedulable`, `candidate_deleting`, `timeout`, `error`.
- validation `outcome`: `success`, `candidate_deleting`, `invalidated`, `timeout`, `error`.
- opportunity `disposition`: `selected`, `superseded`, `policy_rejected`, `validation_failed`, `queue_rejected`, `error`.
- unevaluated `reason`: `budget`, `timeout`, `search_limit`, `selected_early`, `policy`, `error`.
- simulation `phase`: `topology_build`, `scheduler_build`, `pod_data_preparation`, `scheduling`, `result_finalization`, `solve`, `volume_topology_lookup`, `dynamic_resource_setup`.
- pod `source`: `candidate`, `pending`, `deleting`, `excluded`, `total`.
- placement `destination`: `existing_node`, `new_nodeclaim`.
- timeout `kind`: `eligible`, `evaluated`, `unevaluated`.
- topology `kind`: `spread`, `affinity`, `anti_affinity`, `inverse_anti_affinity`.
- work `operation`: `pod_attempt`, `preference_relaxation`, `existing_node_check`, `inflight_nodeclaim_check`, `nodepool_template_check`, `topology_match_check`, `topology_api_read`.
- input `kind`: `pod`, `topology_state_node`, `accounting_state_node`, `nodepool`, `nodepool_template`, `instance_type`, `daemonset_pod`, `additional_excluded_pod`, `prior_removed_candidate`, `topology_key`.
- result `kind`: `new_nodeclaim`, `existing_node`, `pod_on_new_nodeclaim`, `pod_on_existing_node`, `pod_error`.

NodePool is allowed only on counters and current-state gauges where the observation has meaningful ownership. A shared multi-pool observation uses `<multiple>` and an observation with no NodePool uses `<none>`. Histograms never have a NodePool label.

## Metric contracts

### Pass progress

`passes_total{method,outcome}` is incremented once for every completed, non-canceled method pass. Errors and timeouts are terminal outcomes. A canceled controller context does not create a completed pass.

`last_evaluated_timestamp_seconds{method}` is the Unix timestamp of the latest completed pass. It is cluster-wide, not per NodePool. Dashboards use it to detect stale lower-priority methods.

### Simulations

`simulation_duration_seconds{method,stage,outcome}` records monotonic wall-clock duration for each scheduling simulation. It includes scheduler construction and solving for that invocation.

`simulations_total{method,nodepool,stage,outcome}` counts the same simulation invocations. NodePool uses `<multiple>` or `<none>` when ownership is shared or absent.

`candidate_batch_size{method,stage}` records candidate nodes supplied to each simulation.

`candidates_evaluated_per_pass{method,outcome}` records the number of distinct candidates included in evaluation-stage simulations during one completed pass. Repeated validation and overlapping multi-node windows do not double-count a candidate.

`simulation_pod_count{method,stage,source}` records deduplicated pod input counts by bounded source. `source="total"` is the deduplicated union, not the sum of source observations.

`simulation_pod_placements_total{method,nodepool,destination}` counts candidate-pod placements only for selected commands accepted by the disruption queue. It distinguishes existing nodes from new NodeClaims. It does not count placements from rejected, superseded, or validation-only simulations.

`validation_duration_seconds{method,stage,outcome}` records monotonic validation timings. `candidate_refresh_before` is the candidate refresh before validation simulation; `candidate_refresh_after` is the race-closing refresh after simulation. `delay`, `simulation`, and `total` remain separate stages. Each stage carries the terminal validation outcome.

### Candidate snapshots

`candidates{method,nodepool,stage}` is a latest-completed-pass gauge:

- `possible` passed common candidate construction, including queue exclusion, disruptability, NodePool and instance-type lookup, and pod and PDB checks;
- `eligible` also passed the method-specific predicate; and
- `budget_eligible` also had disruption budget in the pass snapshot.

`selected_candidates_total{method,nodepool,decision}` counts candidates only after validation succeeds and the queue accepts the command. `decision` uses the existing bounded `delete` and `replace` values.

`oldest_eligible_timestamp_seconds{method,nodepool}` is the durable eligibility-condition timestamp of the oldest eligible candidate in the latest completed pass. It is a timestamp rather than a frozen age. Query `time() - oldest_eligible_timestamp_seconds` only when `last_evaluated_timestamp_seconds` is fresh.

`timeout_candidate_count{method,kind}` records, for each timed-out pass, eligible candidates, distinct evaluated candidates, and the difference as unevaluated candidates.

### Scheduler detail

`simulation_topology_constraint_count{method,kind}` records active, deduplicated topology groups immediately after scheduler construction. The scheduler's constructed topology state is authoritative, including inverse anti-affinity; PodSpecs are not rescanned.

`simulation_topology_key_count{method}` records distinct topology keys referenced by those active groups. Raw keys never become labels.

`simulation_phase_duration_seconds{method,phase}` records monotonic topology build, scheduler build, pod-data preparation, scheduling, result finalization, solve, volume-topology lookup, and dynamic-resource setup duration. Phase measurements describe work actually performed by each simulation report; reused state is not re-observed on every simulation.

`simulation_input_count{method,kind}` records immutable input and constructed-state sizes from the scheduler report. `topology_state_node` and `accounting_state_node` distinguish placement-visible topology state from state retained only for limits, devices, reservations, and other accounting.

`simulation_work_count{method,operation}` records exact local work counters once after a simulation. Collection may use local atomics in parallel loops, but Prometheus is updated only after the simulation.

`simulation_result_count{method,kind}` records scheduling result sizes, including existing-node results, new NodeClaims, pods placed on each destination type, and pod errors.

`simulation_session_duration_seconds{method,stage}` records pass-scoped immutable session build duration and the mutable scheduler fork duration paid by each simulation.

`simulation_session_total{method,outcome}` counts sessions successfully created, rejected as stale, or abandoned in favor of the legacy construction path.

`simulation_session_shared_input_count{method,kind}` records the immutable nodes, NodePools, templates, instance types, DaemonSet pods, and topology keys prepared once for a method pass.

### Consolidation opportunity

`consolidation_opportunity_optimistic_estimated_savings_price{method,disposition}` records the existing optimistic `Command.EstimatedSavings()` value only when source and replacement pricing is complete. Values use the cloud provider's `Offering.Price` units. The core interface does not define currency or billing period, so the metric does not claim USD or hourly units. These values are estimates, not realized billing savings.

`consolidation_opportunity_latest_max_blocked_optimistic_estimated_savings_price{method,nodepool,disposition}` is the largest completely priced, positive optimistic estimate blocked in the latest completed pass. Only `policy_rejected`, `validation_failed`, `queue_rejected`, and `error` are blocked dispositions. Overlapping opportunities replaced by a selected command are `superseded` and are excluded from this maximum.

`consolidation_opportunity_unevaluated_source_cost_price{method,nodepool,reason}` is completely priced eligible source cost omitted from simulations because of budget, timeout, search limit, early selection, or error. It is not achievable savings.

`consolidation_opportunity_evaluation_coverage_ratio{method,nodepool}` is a latest-pass gauge in the closed interval `[0,1]`. Its numerator is completely priced eligible source cost included in at least one simulation; its denominator is all completely priced eligible source cost.

`consolidation_opportunity_pricing_coverage_ratio{method,nodepool}` is a latest-pass gauge in `[0,1]` describing the fraction of eligible candidates with a known provider source price.

`consolidation_opportunity_unpriced_candidate_count{method,nodepool}` is the latest number of eligible candidates excluded from source-cost observations because their provider source price is unknown. A feasible command with incomplete replacement pricing is also excluded from savings observations, but does not change this source-candidate count.

Feasible overlapping commands must never be summed. Opportunity accounting reuses only commands and prices already produced by disruption. It never runs an extra simulation.

## Buckets

Dedicated duration buckets cover 1 ms through 10 minutes:

`0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600`

Dedicated count buckets cover zero and powers of two through 16,384:

`0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384`

The bounded ratio bucket contract, for any ratio distributions added during alpha, is:

`0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1`

Current coverage ratios are gauges, not histograms, and must be clamped to `[0,1]`.

Provider-price buckets retain negative and zero estimates and cover magnitudes through 1,000 provider price units:

`-1000, -500, -250, -100, -50, -25, -10, -5, -1, 0, 1, 5, 10, 25, 50, 100, 250, 500, 1000`

Prometheus supplies the final `+Inf` bucket. Price observations outside the explicit range remain visible there.

## Gauge lifecycle

All current-state gauges are snapshots from the latest completed pass. On every successful refresh, emitters must:

1. write zeroes for bounded stages or reasons that are absent but still in the current NodePool scope;
2. delete series for NodePools no longer returned by the managed NodePool listing;
3. delete obsolete `<multiple>` and `<none>` series when the scope changes; and
4. retain the previous complete snapshot if input listing fails, rather than publishing a partial snapshot.

This lifecycle applies to `candidates`, `oldest_eligible_timestamp_seconds`, the latest max-blocked estimate, unevaluated source cost, both coverage ratios, and unpriced candidate count. Counters and histograms are historical and are never deleted.

## Cardinality and interpretation

Histogram labels are bounded and exclude NodePool. NodePool counters and gauges grow only with currently managed NodePools plus the two sentinel scopes.

Snapshot gauges must be joined with `last_evaluated_timestamp_seconds` before alerting. A stale snapshot describes the latest completed pass; it is not current cluster truth. Counters and histograms remain cumulative across later state changes.

These alpha metrics supplement, and do not replace, existing decision, timeout, disruption, and termination metrics.
