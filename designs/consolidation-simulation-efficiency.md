# Consolidation Simulation Efficiency

Reuse immutable scheduling inputs within one disruption method pass so consolidation does not
rebuild cluster-wide scheduler state for every candidate window.

## Motivation

Consolidation repeatedly calls `SimulateScheduling` while searching for a command. Every call
currently pays cluster-sized setup costs:

1. copy the cluster node snapshot;
2. list or resolve scheduler catalog inputs;
3. rebuild topology state;
4. rebuild NodeClaim templates and daemon overhead groups;
5. rebuild existing-node scheduling representations; and
6. create fresh mutable scheduler state before calling `Solve`.

Most of this work is independent of the candidate or candidate window being evaluated.

[High CPU usage during node consolidation at scale
(#2972)](https://github.com/kubernetes-sigs/karpenter/issues/2972) measured the effect on a
500-node cluster. `NewScheduler` accounted for 78.85% of `SimulateScheduling` CPU,
`DeepCopyNodes` accounted for 18.95%, and `Scheduler.Solve` accounted for 1.12%. Allocation
churn from construction also drove GC.

The first rollout of disruption performance metrics in `awscmhqa5` points in the same
direction. The initial 55-minute window is too short for a rollout decision, and Datadog
represents these Prometheus histograms as approximate distributions, but it is sufficient to
rank the work:

- scheduler construction averaged about 2.37 seconds;
- topology construction averaged about 1.42 seconds;
- scheduling and `Solve` each averaged about 0.18 seconds;
- 388 evaluation simulations ran across 46 multi-node passes that reached candidates, about
  8.4 simulations per pass;
- each simulation considered about 164 nodes and 41 NodePools/templates on average;
- NodePool-instance-type entries and some feasibility-check counts exceeded the current
  16,384-count histogram bucket;
- multi-node candidate windows averaged only about 2.5 nodes and reached a maximum histogram
  bucket near eight nodes; and
- no consolidation timeout was observed.

The candidate window is small compared with the state rebuilt around it. Reusing immutable
prepared state is therefore a higher-leverage change than optimizing `Solve`.

[PR #3240](https://github.com/kubernetes-sigs/karpenter/pull/3240) explored removing repeated
deep copies through copy-on-write cluster state. Its reported end-to-end improvement was
approximately 12-29%, consistent with deep copy being material but secondary to scheduler
construction. It also made the high-churn pod update path about 1.6 times slower and required
copy-on-write correctness across every cluster-state mutation. This RFC keeps deep-copy
removal as a complementary optimization rather than making it a prerequisite.

### Use Cases

1. **Multi-node binary search.** A method pass evaluates several overlapping windows from the
   same candidate and cluster snapshots. Each window should pay for candidate-specific state,
   not reconstruct identical NodePool, instance-type, daemon, existing-node, and topology
   inputs.
2. **Single-node consolidation at scale.** A cluster with hundreds of candidates may run one
   simulation per candidate. Prepared cluster-wide state should be reused while preserving a
   fresh mutable solve for every candidate.
3. **Long-running method passes.** A method may spend tens of seconds searching. It needs a
   coherent pass snapshot and explicit stale-state behavior rather than reading shared mutable
   state through a long sequence of scheduler builds.
4. **Validation after selection.** A command selected from reusable evaluation state must
   still be revalidated against fresh cluster, pod, PDB, provider, topology, and DRA state
   after the validation delay.

### Non-Goals

- Changing candidate ordering, binary search, scoring, consolidation policy, or selected
  commands.
- Parallelizing candidate simulations.
- Sharing a mutable `Scheduler`, `Topology`, `ExistingNode`, allocator, or reservation manager
  between simulations.
- Caching scheduler state across disruption passes, controller replicas, or process restarts.
- Removing the validation delay or replacing validation with snapshot generation checks.
- Redesigning `Cluster` around copy-on-write state or removing `DeepCopyNodes` globally.
- Changing CRDs, disruption budgets, PDB handling, or `terminationGracePeriod`.
- Optimizing scheduling algorithms inside `Solve`.
- Reusing evaluation state for provisioning.

## Proposal

Introduce a pass-scoped `SimulationSession` that owns one coherent snapshot and prepared,
immutable scheduler inputs. Every candidate simulation forks fresh mutable state from the
session. The session is discarded when the method pass completes.

The first version targets consolidation evaluation. Validation always creates a new session
after the validation delay. Drift replacement may adopt the same abstraction later; its
current `driftReplacementSimulator` already demonstrates pass-scoped catalog reuse and fresh
solves.

No user-facing API changes. Ship behind the alpha `DisruptionSimulationReuse` feature gate,
disabled by default.

### Proposed Spec

There are no CRD, annotation, or CLI additions other than the standard feature gate:

```text
--feature-gates DisruptionSimulationReuse=true
```

The internal interface is intentionally narrow:

```go
// Provisioner constructs the session so scheduling internals do not leak into disruption.
func (p *Provisioner) NewSimulationSession(
	ctx context.Context,
	nodes state.StateNodes,
	opts ...scheduling.Options,
) (*scheduling.SimulationSession, error)

type SimulationRequest struct {
	Pods                   []*corev1.Pod
	CandidateNames         sets.Set[string]
	DeletingPodUIDs        sets.Set[types.UID]
	AdditionalExcludedPods []*corev1.Pod
	AccountingNodeNames    sets.Set[string]
}

func (s *SimulationSession) Simulate(
	ctx context.Context,
	request SimulationRequest,
) (Results, error)
```

The exact ownership of these types may change during implementation. The invariant is stable:
disruption owns session lifetime and supplies candidate-specific inputs; provisioning and
scheduling own construction and forking.

### How It Works

#### Session lifetime

`Controller.disrupt` continues to discover candidates and build one disruption budget mapping.
When a method needs its first scheduling simulation, it lazily creates a session from the same
pass-level node snapshot.

```mermaid
flowchart TD
    pass["Disruption method pass"]
    candidates["Candidate + budget snapshots"]
    session["Build immutable simulation session once"]
    request["Candidate-specific request"]
    fork["Fork mutable scheduling state"]
    solve["Scheduler.Solve"]
    selected{"Command selected?"}
    validateDelay["Validation delay"]
    fresh["Build fresh validation session"]
    validate["Validate command"]
    queue["Queue.StartCommand"]

    pass --> candidates --> session
    session --> request --> fork --> solve
    solve -->|"next window"| request
    solve --> selected
    selected -->|"No"| request
    selected -->|"Yes"| validateDelay --> fresh --> validate
    validate -->|"valid"| queue
    validate -->|"invalid"| pass
```

The session is not created for methods or passes that emit commands without simulation.

#### Immutable prepared state

The session owns immutable state that can be safely shared by every fork:

- a deep-copied `StateNode` snapshot taken once per pass;
- managed NodePools ordered by weight;
- provider instance types filtered by pass-level availability and launch back-off;
- immutable NodeClaim template bases and initial requirement filtering;
- DaemonSet pod compatibility and daemon overhead groups;
- NodePool-to-instance-type indexes;
- topology domain groups derived from NodePools and instance types;
- an inverse anti-affinity index over the pass snapshot;
- immutable existing-node template data: requirements, capacity, taints, labels, daemon
  overhead, initialization state, and instance type; and
- pass-level caches for namespace selectors and topology selector counts.

The session never exposes these objects for mutation. Types that currently contain mutable
and immutable fields together must be split into an immutable base and a per-fork overlay.

Taking one deep copy per pass preserves isolation from live cluster state without requiring
copy-on-write correctness across every cluster mutation. It also bounds the initial
implementation: eliminating the remaining one-per-pass copy is a separate optimization.

#### Mutable per-simulation fork

Every `Simulate` call creates fresh mutable state:

- `newNodeClaims`;
- existing-node host-port and volume usage overlays;
- `remainingResources`, adjusted for candidate and accounting exclusions;
- reservation manager state;
- topology owner sets, mutable domain counts, and preference-relaxation state;
- pod data and resolved volume requirements;
- DRA allocation state and resource-claim memoization;
- scheduling preferences;
- pod errors and result accumulators; and
- batch-ledger deltas where applicable.

Fork cost must scale with changed candidate and pod state where possible. It must not rerun
NodePool-instance-type filtering, daemon compatibility, or cluster-wide topology discovery.

#### Topology baseline and deltas

Topology is the most correctness-sensitive reusable state. The session caches only immutable
facts:

- domain groups;
- namespace-selector resolution;
- inverse anti-affinity selectors;
- immutable baseline counts from the pass snapshot; and
- lazily computed selector-count baselines keyed by deduplicated topology-group hash.

A fork clones the small mutable count maps for groups active in that simulation, subtracts
pods on candidate and deleting nodes, registers topology groups owned by pods being scheduled,
and applies later scheduling records to its private overlay. Preference relaxation may replace
owners or groups only in that overlay.

Raw `Topology` instances are never shared between solves.

#### Existing-node templates

`calculateExistingNodeClaims` currently repeats daemon compatibility and immutable requirement
construction for all nodes. The session prepares one immutable template per node.

A fork:

1. excludes candidate nodes;
2. selects accounting nodes;
3. creates lightweight mutable usage overlays for remaining destination nodes; and
4. reuses immutable requirements, capacity, taints, daemon overhead, and instance-type data.

`ExistingNode.Add` continues to mutate only fork-local host-port, volume, resource, topology,
and DRA state.

#### Dynamic inputs

The first implementation preserves current read timing for inputs that can change during a
long pass:

- pending pods;
- current PDB limits;
- volume topology;
- deleting-node pods;
- DRA claims, slices, and allocated devices; and
- live candidate activity checks.

These are read for each simulation unless differential testing proves a pass-level snapshot is
behaviorally equivalent. The session may cache immutable transformations keyed by resource
version, but it must not silently freeze their source data.

This boundary deliberately leaves some work in each simulation. Correctness and behavioral
equivalence take priority over maximum cache coverage.

#### Validation

Evaluation-session reuse does not weaken validation:

1. wait for the existing validation delay;
2. refresh candidates and budgets;
3. take a fresh node snapshot;
4. rebuild a fresh session, including catalog and topology baseline;
5. run the validation simulation; and
6. perform the final candidate refresh.

Evaluation state is never reused after the delay. Validation remains the authority for
changes that occurred during search.

#### Stale and live-state checks

The existing checks remain:

- skip a candidate already deleting before scheduler construction;
- check candidate activity after `Solve`;
- reject commands whose refreshed candidates, budgets, or scheduling results changed; and
- let `Queue.StartCommand` atomically reject candidates already in another command.

An optional cluster scheduling generation may short-circuit a session when relevant state
changes during a pass, but it is not sufficient to replace validation. If introduced, it must
increment for node, NodeClaim, pod-binding, nomination, deletion, and scheduler-relevant
configuration changes. A generation mismatch returns a typed retryable error rather than
continuing with partially refreshed state.

### Interaction with Existing Features

- **Multi-node consolidation.** One session is shared across binary-search windows and
  NodePool/architecture attempts in a pass. Each window still gets a fresh mutable fork.
- **Single-node consolidation.** All candidate evaluations in one pass share immutable
  prepared state. This is expected to have the largest scale benefit because simulation count
  can approach candidate count.
- **Emptiness.** Delete-only commands do not build a session. Emptiness validation behavior is
  unchanged.
- **Dynamic drift.** Its current replacement simulator and `SchedulerCatalog` remain unchanged
  initially. A later refactor may implement them through `SimulationSession`.
- **Static drift.** It does not schedule and is unaffected.
- **Disruption budgets.** The budget mapping remains a pass snapshot. Session reuse does not
  change admission or decrement behavior.
- **PDBs and `do-not-disrupt`.** Candidate construction and validation are unchanged. Current
  PDB state remains a dynamic simulation input in the first version.
- **Topology.** All mutable topology state is fork-local. Inverse anti-affinity remains
  included in the immutable baseline and per-fork deltas.
- **DRA.** Device allocation is fork-local. A shared immutable device catalog may be added only
  if resource-version invalidation is explicit.
- **Reserved capacity.** `ReservationManager` is forked per simulation; reservations from one
  speculative window cannot leak into another.
- **Launch back-off.** Provider availability is captured when the session catalog is built.
  Real replacement admission still rechecks and reserves offerings in `Queue.StartCommand`.
- **Capacity buffers.** Pending virtual pods remain part of each simulation's current pod
  input and keep existing nomination behavior.

### Observability

The disruption performance metrics remain the primary before/after comparison:

- `karpenter_voluntary_disruption_simulation_duration_seconds`;
- `karpenter_voluntary_disruption_simulation_phase_duration_seconds`;
- `karpenter_voluntary_disruption_simulation_input_count`;
- `karpenter_voluntary_disruption_simulation_work_count`;
- `karpenter_voluntary_disruption_simulations_total`;
- `karpenter_voluntary_disruption_candidates_evaluated_per_pass`;
- `karpenter_voluntary_disruption_validation_duration_seconds`;
- `karpenter_voluntary_disruption_passes_total`; and
- validation, queue-rejection, selected-candidate, and opportunity coverage metrics.

Add:

| Signal | Type | Purpose |
| ------ | ---- | ------- |
| `karpenter_voluntary_disruption_simulation_session_duration_seconds` | histogram by `method`, `stage=build\|fork` | Separates one-time session preparation from per-simulation fork cost. |
| `karpenter_voluntary_disruption_simulation_session_total` | counter by `method`, `result=created\|stale\|fallback` | Tracks reuse, stale-session retries, and legacy fallback without object labels. |
| `karpenter_voluntary_disruption_simulation_session_shared_input_count` | histogram by bounded `kind` | Measures nodes, templates, topology groups, and other state prepared once per session. |

Do not add session IDs, candidate IDs, topology keys, pod identities, or raw error labels.

The performance goal is not merely a lower total duration. A successful rollout should move
work from repeated `scheduler_build` and `topology_build` observations into one session build,
reduce allocation and GC profiles, and leave scheduling results and failure rates unchanged.

### Edge Cases

- **Cluster changes during a long search.** The session remains internally coherent. Live
  activity checks skip deleting candidates, and fresh validation catches changes before a
  command enters the queue.
- **NodePool or instance types change.** Evaluation may finish against the pass snapshot.
  Validation rebuilds the catalog and rejects an obsolete command.
- **A PDB changes between windows.** PDB state is refreshed per simulation in the first
  version. Validation still performs the final check.
- **Pending pods arrive during search.** Pending pods remain dynamic inputs. Prepared
  cluster-wide state can be reused without freezing the pending set.
- **A candidate window overlaps a previous window.** Each fork is independent. No placements,
  reservations, topology counts, or resource usage leak between windows.
- **Context deadline expires while forking or solving.** All preparation and fork loops check
  the supplied context. Deadline behavior and last-valid-command handling remain unchanged.
- **One simulation relaxes preferences.** Relaxation mutates only its pod copies and topology
  overlay.
- **A selector baseline is first requested concurrently.** Session-local lazy caches use
  single-flight or immutable publish-after-build semantics. Failed builds are not cached.
- **Session memory is large.** Only one session per active singleton disruption pass is held.
  It is released at pass completion. Graduation includes an explicit memory guardrail.
- **Legacy and reusable paths disagree.** Alpha returns the legacy result in shadow tests and
  fails differential CI. Feature-gated production falls back to legacy on typed preparation
  failures, not on result mismatch.

## Alternatives Considered

### Alternative 1: Remove deep copies with copy-on-write cluster state

[PR #3240](https://github.com/kubernetes-sigs/karpenter/pull/3240) replaces repeated deep copies
with copy-on-write mutation and generation-cached snapshots. It directly reduces copying,
allocation, GC, and read-lock contention.

It is complementary but rejected as the primary solution:

- scheduler and topology construction remain the dominant measured phases;
- reported end-to-end gains were approximately 12-29%, not enough to resolve the repeated
  construction problem;
- every cluster-state write path becomes part of snapshot correctness; and
- the benchmarked pod update path became about 1.6 times slower.

The session design safely reduces deep copies from one per simulation to one per pass without
requiring global copy-on-write state.

### Alternative 2: Cache a complete `Scheduler`

Rejected because `Solve` mutates NodeClaims, existing-node usage, remaining resources,
topology counts, preferences, reservations, DRA allocation, and pod caches. Correctly resetting
all fields is equivalent to implementing a fork, but with a larger risk that new mutable state
will be missed.

### Alternative 3: Global scheduler cache

Cache prepared state across reconciles and invalidate it from cluster events. Rejected for the
first version because NodePools, provider offerings, DaemonSets, pods, PDBs, volumes, DRA,
reservations, nomination, and deletion all contribute invalidation rules. Pass-scoped reuse
captures most repeated work while making lifetime and ownership explicit.

### Alternative 4: Parallelize candidate simulations

Rejected because it multiplies CPU, memory, API reads, and speculative reservation work; makes
ordering nondeterministic; and does not remove repeated construction. It may reduce one pass's
wall time while worsening the scale problem described by #2972.

### Alternative 5: Optimize candidate ordering or reduce search depth

Useful and compatible, particularly for multi-node. Rejected as the primary solution because
it trades evaluation coverage for performance and leaves every remaining simulation
cluster-sized. Current QA coverage is already low, so reducing search depth first would hide
rather than remove the cost.

### Alternative 6: Reuse evaluation state for validation

Rejected. Validation exists to observe state changes during search and the validation delay.
Reusing the evaluation session would turn the primary safety check into a deterministic replay
of stale state.

## Backward Compatibility

- No NodePool, NodeClaim, or Pod configuration changes.
- The feature gate defaults off during alpha; disabled behavior is byte-for-byte the existing
  simulation path.
- Candidate ordering, scoring, timeout, command shape, and queue behavior remain unchanged.
- Validation always uses fresh state under both paths.
- A typed session-preparation failure falls back to or retries through the legacy path during
  alpha.
- The new metrics are alpha and additive.
- Removing the gate restores legacy construction without data migration.

## Testing and Rollout

### Differential correctness

Build a reusable test harness that runs legacy and session simulations from the same snapshot
and compares:

- schedulable result;
- normalized pod errors;
- existing-node pod placements;
- number of new NodeClaims;
- NodePool, requirements, resources, taints, labels, and instance-type options on each new
  NodeClaim;
- DRA allocations;
- reservation behavior; and
- final consolidation decision after filtering and scoring.

Comparison ignores nondeterministic identifiers and ordering that is not part of the scheduler
contract. Any semantic difference must be explained and approved as a separate behavior
change.

Run the harness over:

- single- and multi-node consolidation;
- delete and replace commands;
- overlapping binary-search windows;
- initialized and in-flight nodes;
- daemon overhead, host ports, and volumes;
- required and preferred affinity and anti-affinity;
- topology spread and inverse anti-affinity;
- pending and deleting pods;
- PDB changes;
- NodePool limits, weights, `minValues`, and reserved offerings;
- DRA devices and claims;
- context cancellation; and
- randomized cluster mutation sequences under the race detector.

### Performance

Add benchmarks at 100, 250, 500, and 1,000 nodes with:

- one, ten, and all nodes eligible;
- one, ten, and fifty candidate windows per pass;
- narrow and broad NodePool-instance-type catalogs;
- low and high DaemonSet counts;
- no topology, spread, anti-affinity, and inverse anti-affinity workloads; and
- schedulable and unschedulable candidate pods.

Report CPU, wall time, allocations, retained memory, and GC. Separate session build, fork, and
solve.

### Rollout

1. Land immutable preparation and fork APIs with legacy behavior.
2. Run differential tests in CI and benchmark both paths.
3. Enable the gate in a QA cluster for multi-node consolidation.
4. Compare at least 24 hours covering normal workload churn and several selected commands.
5. Expand to a larger cluster and single-node consolidation.
6. Enable by default only after beta criteria are met.

Do not run both full simulations in production shadow mode by default; doubling consolidation
work would distort the performance measurement. A debug-only sampled comparator may run in
small clusters.

## Graduation Criteria

### Alpha (`DisruptionSimulationReuse=false`)

Ship:

- pass-scoped immutable session ownership;
- fresh mutable scheduler forks;
- prepared NodeClaim templates, daemon groups, and existing-node bases;
- topology baseline and per-fork deltas;
- fresh validation sessions;
- typed preparation, stale, and fallback outcomes;
- differential tests; and
- session build/fork observability.

Required evidence:

- legacy and reusable paths produce identical normalized results across the differential suite;
- race tests find no shared-state mutation;
- no simulation fork leaks state into a later fork;
- cancellation interrupts session build, fork, and solve;
- the disabled gate does not change current behavior; and
- benchmarks show no regression for a one-simulation pass.

### Beta (default on)

Requires evidence across QA and a scale environment that:

- scheduler plus topology construction time per pass falls by at least 50%;
- overall evaluation simulation duration falls by at least 30% at 250 or more nodes;
- allocation bytes and GC CPU during consolidation fall materially;
- selected command count and shape remain equivalent;
- validation-failure and queue-rejection rates do not regress by more than five percent
  relative;
- stale-session or fallback results remain below 0.1% of simulations;
- peak controller memory does not regress by more than ten percent; and
- no correctness incident is attributed to shared simulation state.

### GA

Remove the feature gate after:

- at least two releases enabled by default;
- scale tests at 500 or more nodes;
- no open correctness issue in session forking, topology deltas, DRA, reservations, or
  validation;
- performance improvements remain visible across multiple workload shapes; and
- legacy construction has no remaining rollback use.

## Open Questions

1. **Which inputs remain dynamic in the first implementation?** Pending pods and PDBs should
   initially remain per-simulation. Determine whether volume topology, deleting pods, and DRA
   inputs also need that boundary for behavioral equivalence.
2. **Should the pass snapshot expose a scheduling generation?** A generation can fail fast
   during long searches, but validation already protects command execution. Measure stale
   frequency before adding another invalidation contract.
3. **What is the smallest safe topology baseline?** Domain groups are clearly reusable.
   Selector-count and inverse-affinity baselines offer more benefit but require careful pod
   subtraction and resource-version handling.
4. **Should prepared existing-node bases be eager or lazy?** Eager preparation maximizes reuse;
   lazy preparation may avoid work for nodes never considered as destinations.
5. **Can the current drift replacement simulator implement the same session interface?**
   Generalization would reduce duplicate cache and fork mechanisms but must preserve
   replacement-only semantics and the batch ledger.
6. **Are current count histogram buckets large enough?** QA observations reached the 16,384
   ceiling for instance-type and feasibility-check counts. Increase buckets or add exact
   summaries before using those metrics for graduation thresholds.
7. **What fallback is safe after partial session use?** A preparation error can use the legacy
   path. A fork error after speculative mutable work should retry from a fresh fork or fail the
   pass, never reuse the failed fork.
8. **How should provider catalog changes invalidate a session?** A pass-scoped catalog matches
   existing drift behavior, while consolidation currently resolves it per simulation. Confirm
   equivalence under launch back-off and provider availability churn.

## References

- [High CPU usage during node consolidation at scale
  (#2972)](https://github.com/kubernetes-sigs/karpenter/issues/2972)
- [Deep-copy reduction prototype
  (#3240)](https://github.com/kubernetes-sigs/karpenter/pull/3240)
- [Disruption performance metrics
  (#3288)](https://github.com/kubernetes-sigs/karpenter/issues/3288)
- [Drift replacement throughput](drift-replacement-throughput.md)
- [Launch backoff for insufficient capacity](launch-backoff-insufficient-capacity.md)
- [Capacity reservations](capacity-reservations.md)
- [DRA scheduling](dra-scheduling.md)
