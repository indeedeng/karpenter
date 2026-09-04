# Drift Replacement Throughput

Raise drift replacement throughput on large clusters so a fleet-wide rollout completes in bounded time, by generating many independent single-node commands per disruption pass inside a time cap.

## Motivation

In large clusters, drift replacement throughput does not scale with cluster size — it degrades. A fleet-wide rollout (AMI / NodeClass / NodePool change touching most nodes) takes **days**. Tracked in [#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197).

This is a **command-generation** bottleneck, not an execution or budget bottleneck:

1. The disruption controller is a **singleton** (`singleton.AsReconciler`). All disruption decisions for the cluster are produced by one worker.
2. **Drift emits at most one command per pass.** `Drift.ComputeCommands` returns as soon as it finds the first eligible candidate, and a drift command is limited to a single candidate.
3. **Each pass does O(cluster) work before dispatching that one node.** `disrupt()` runs `GetCandidatesWithTotals` (`DeepCopyNodes` + per-node `NewCandidate`) over the entire cluster, then `SimulateScheduling` builds a scheduler over every remaining node and tries to pack the candidate's pods onto existing capacity.
4. Drift has **no timeout**. A pass that skips many unschedulable candidates simulates them serially with no bound. (MultiNode and SingleNode consolidation do have caps, but MultiNode's is a `context.WithTimeout` around a binary search rather than a candidate-walk cap, so only SingleNode is a true precedent.)

The executor is not the constraint. `Controller.disrupt` already starts every returned command in parallel via `workqueue.ParallelizeUntil`, the `Queue` runs `MaxConcurrentReconciles` between 100 and 1000 scaling with CPU, and `Queue.ProviderIDToCommand` is unbounded — commands requeue every `queueBaseDelay` (1s), so a single worker services many commands over time. The queue's real ceiling is its rate limiter, `rate.NewLimiter(rate.Limit(100), 1000)`. Raising NodePool disruption budgets does not help either, because the generator never produces enough in-flight commands to approach them.

### Production evidence

Seven-day averages from a multi-cluster Karpenter fleet. The largest cluster is the one that motivated [#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197).

| Cluster size (nodes) | Drift eval (s) | Disruption `SimulateScheduling` (s) |
| -------------------- | -------------- | ----------------------------------- |
| ~20                  | 0.2–0.4        | ~0.01                               |
| ~400–900             | 0.7–2.0        | 0.05–0.3                            |
| ~1,700               | 3.8            | 1.9                                 |
| ~5,100               | **26.8**       | **14.8**                            |

On the ~5,100-node cluster, over the same window:

| Signal | Value | What it implies |
| ------ | ----- | --------------- |
| Eligible drifted nodes | ~990 standing | A persistent backlog, not a brief wave |
| Drift decisions | 2,934 / 7d (~17/hour) | Generator-limited, not bursty execution |
| Nodes consuming disruption budgets | **~4.5** of ~2,100 allowed | Budgets are idle |
| Underutilized-eligible nodes | ~2,000 | Consolidation has work |
| Underutilized decisions | **25 / 7d** | Consolidation is starved (drift requeues to the top of the method list) |
| Drift eval **max** | **604 s** | Unbounded candidate walk; one pass can run for ten minutes |
| Disruption `SimulateScheduling` **max** | **150 s** | A single existing-node packing sim can exceed a 1-minute cap by itself |
| Disruption reconcile avg / max | 43 s / 60 s | Singleton is busy the entire poll interval producing one decision |

Two conclusions follow:

1. **A time cap is necessary.** Drift's candidate walk is unbounded. Worst-case 10-minute evaluations monopolize the singleton and still emit one command.
2. **A time cap is not sufficient.** Candidate discovery is paid once per `disrupt()`, and a full existing-node simulation costs ~15 s typical / 150 s worst. Inside any reasonable cap that is a handful of commands. Throughput only moves if each accepted candidate is cheap — the per-candidate cost has to stop scaling with cluster size.

Two caveats on the numbers. The cost of `GetCandidatesWithTotals` alone is **not yet measured** and cannot be inferred from the emptiness evaluation duration, because `Emptiness.ComputeCommands` includes a hardcoded 15 s `commandValidationDelay` sleep plus a second `GetCandidates` and `BuildDisruptionBudgetMapping` inside `EmptinessValidator.validateCandidates`; instrument it directly before finalizing. And this fork disables `SingleNodeConsolidation` unless `INDEED_ENABLE_SINGLE_NODE_CONSOLIDATION=true`, so the "Underutilized decisions" row covers MultiNode only and is not comparable to upstream defaults.

### Use Cases

A **fleet AMI / NodeClass rollout** flips thousands of nodes to `Drifted=True` at once and should complete in hours at budget-limited parallelism, not days. **Continuous low-rate drift** (one NodePool at a time) should replace nodes promptly without a per-node generation tax and without starving consolidation. And in an **emergency rollout**, an operator raising budgets to 100% to cycle a bad AMI off the fleet should actually get faster replacement — today the budget is not the ceiling, so raising it does nothing.

### Non-Goals

- CFS / time-slicing **across** disruption methods. That is [#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927); see Alternative 1.
- Per-NodePool replacement back-off, already specified in [`drift-per-nodepool-backoff.md`](drift-per-nodepool-backoff.md).
- Making the disruption reconciler itself parallel, or sharding it by NodePool.
- Changing disruption budget semantics, PDBs, or `TerminationGracePeriod`.
- Speeding up `GetCandidates` / `DeepCopyNodes`; see Open Questions.

## Proposal

When Drift runs, `ComputeCommands` walks candidates for up to a **time cap** and returns **every independent single-node command** it can prove in that window, bounded by existing disruption budgets and per-NodePool back-off.

Independence comes from restricting what each simulation considers. A drift candidate is simulated against **only its own reschedulable pods, packed onto new NodeClaim(s)** — no existing nodes as packing targets, no cluster-wide pending pods, no pods from other deleting nodes. Two consequences:

- **Commands cannot conflict.** They share no spare capacity and no pending demand, so they can be dispatched in parallel without violating the "simulation B must not depend on simulation A" invariant.
- **Per-candidate cost stops scaling with the cluster.** The solve is over the pods on one node, not over every pending pod in the cluster.

Excluding pending pods is not just an optimization: including them would be incorrect here. `Command.Replacements` is built from *all* new NodeClaims a solve produces (`replacementsFromNodeClaims(results.NewNodeClaims...)`), and `Queue.createReplacementNodeClaims` launches all of them. If N drift commands each simulated the same cluster-wide pending set, each would carry replacements covering that same demand and we would over-provision N times over. Pending demand is the provisioning controller's job.

Empty drifted candidates skip simulation entirely and are deleted without a replacement. "Empty" here means `len(candidate.reschedulablePods) == 0`, the predicate Drift already uses — not `Candidate.IsEmpty()`, which is a different (disruption-cost-based) test used by the Emptiness method.

No CRD or API changes. Behavior change for operators: drift rollouts become faster and more parallel, up to the budgets they already configured; drifted pods are replaced onto new capacity rather than squeezed onto remaining (often also-drifted) nodes.

### Proposed Spec

One new package constant, analogous to consolidation:

```go
// DriftTimeoutDuration caps the drift candidate walk. Zero restores the previous
// behavior of emitting at most one command per pass.
const DriftTimeoutDuration = 1 * time.Minute
```

Setting this to `0` is the rollback lever: the walk emits its first command and returns, which is exactly today's contract. That is cheaper than a feature gate and needs no plumbing. Promoting it to a controller flag is an optional follow-on.

### How It Works

`Controller.disrupt` is unchanged: it still calls `GetCandidatesWithTotals` once, `ComputeCommands` once, and `Queue.StartCommand` in parallel for every returned command. The change is entirely inside `Drift.ComputeCommands`.

```
ComputeCommands(candidates, budgets):
    sort by Drifted LastTransitionTime (oldest first)
    split empty / non-empty (empty first, existing invariant)

    sim := newDriftSimulator()      // shared setup, built ONCE per pass
    deadline := now + DriftTimeoutDuration
    cmds := []

    for c in empty ++ nonEmpty:
        if DriftTimeoutDuration > 0 and now > deadline: break
        if budgets[c.NodePool] == 0: continue
        if backoff.IsBackedOff(c.NodePool): continue

        if c is empty:
            cmd = delete(c)                  // no simulation, no replacement
        else:
            results = sim.Replace(c)         // c's pods -> new NodeClaim(s) only
            if !results.AllNonPendingPodsScheduled(): continue
            cmd = replace(c, results)

        cmds.append(cmd)
        budgets[c.NodePool]--
        if DriftTimeoutDuration == 0: break  // rollback: one command per pass
    return cmds
```

**Shared setup is hoisted out of the loop**, which is what actually converts the time cap into throughput and is the "reuse scheduling-simulation state across candidates" direction from [#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197). `SimulateScheduling` today rebuilds `pdb.NewLimits`, the node snapshot, and — inside `provisioner.NewScheduler` — `nodepoolutils.ListManaged`, `GetInstanceTypes` per NodePool, and `getDaemonSetPods` on *every* call. The drift simulator builds these once per `ComputeCommands`. Dropping existing nodes and pending pods removes the rest of the per-candidate O(cluster) work: `provisioner.GetPendingPods` (which lists every provisionable pod and runs `Validate` on each), `calculateExistingNodeClaims`, `addToExistingNode`, and a `NewTopology` / `Solve` over the full pod set. What remains per candidate is a solve over the pods on one node.

The drift simulator is **new code**: there is no existing scheduler option that skips `addToExistingNode`, so one has to be added or the scheduler constructed with an empty `stateNodes` slice. It still filters the candidate's pods through `pdbs.IsCurrentlyReschedulable` and still returns `scheduling.Results`, so `AllNonPendingPodsScheduled()` and `NonPendingPodSchedulingErrors()` keep their meaning.

After `ComputeCommands` returns, the existing parallel `StartCommand` path cordons, launches replacements, nominates, and enqueues. The queue already documents that nominations are "essential in correctly modeling multiple disruption commands in parallel."

### Interaction with Existing Features

- **Disruption budgets.** Unchanged formula, but the bookkeeping is now load-bearing. Budget is consumed **optimistically at emit time**, so a single walk cannot exceed the mapping computed at the start of `disrupt()`. Because `StartCommand` runs in parallel and can fail per-command (mark-disrupted conflict, launch failure), some consumed budget may belong to commands that never started. That is **not** compensated within the pass; the next pass re-derives truth from `BuildDisruptionBudgetMapping`, which counts `MarkedForDeletion` and NotReady nodes. The existing comment in `Drift.ComputeCommands` ("We don't need to decrement any budget counter since drift commands can only have one candidate") must be removed.
- **Per-NodePool back-off.** The `IsBackedOff` skip remains, and back-off's assumption that a drift command maps to one NodePool (`cmd.Candidates[0]`, via `driftNodePool`) still holds — commands stay single-candidate, there are just more of them per pass.
- **PDBs, eventual disruption class, StaticDrift.** Unchanged. Drift remains `EventualDisruptionClass`; dynamic Drift simply becomes the analogue of the already-multi-command StaticDrift.
- **Empty vs non-empty ordering.** Preserved. Now that we no longer pack onto existing nodes this is belt-and-suspenders, but it keeps churn-reduction behavior if a later change reintroduces packing.
- **Consolidation.** Not modified. Drift still requeues immediately on success and can starve consolidation; that is [#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927). More work per Drift turn makes method fairness *more* important, not less — but a 1-minute cap at least bounds how long consolidation waits for Drift's turn to end (today: up to 10 minutes).
- **`waitOrTerminate`.** Unchanged. Replacements must still reach `Initialized=True` before candidates are deleted. More in-flight commands push `GetMaxRetryDuration` (10m–1h, scaling with queue depth) toward the high end; that is existing behavior.

### Observability

No new metrics. The existing ones answer the questions:

- `karpenter_voluntary_disruption_decision_evaluation_duration_seconds{reason="drifted"}` now measures a time-capped walk; p99 should fall under the cap.
- `karpenter_voluntary_disruption_decisions_total{reason="drifted"}` should rise toward the budget-limited rate during a rollout. This is the success metric.
- `karpenter_nodepools_nodes_consuming_budgets{reason="Drifted"}` should move off ~0 toward `allowed_disruptions`. Note the capitalization: `BuildDisruptionBudgetMapping` sets this label from `string(reason)` without lowercasing, unlike the two metrics above.

One new `V(1)` log line when the walk hits the cap, with `candidates_evaluated`, `commands`, and `candidates_remaining`. A `commands` *label* on the evaluation histogram was considered and rejected — it would change an existing metric's label set, and the value varies per observation, so it is unbounded cardinality.

### Edge Cases

- **First candidates unschedulable, later ones fine.** We already `continue`, but under a time cap we may spend the whole window on doomed candidates and emit nothing. Back-off skips pools whose *replacements* fail, not pools whose *simulation* fails (blocked pods, missing instance types), so those still burn the window. Acceptable for v1 — the cap at least prevents a 10-minute no-op. Follow-on: treat repeated simulation failure like replacement failure for back-off.
- **Pods that only fit because of spare capacity on other live nodes.** Today drift emits a *delete* when `SimulateScheduling` packs everything onto remaining nodes; replacement-only simulation emits a *replace*. This is the one deliberate semantic change: a rollout of a new AMI should not drain pods onto nodes that are about to drift or already have. Capacity during a rollout is higher (1:1 replace rather than shrink), and consolidation still removes the slack on its own turn.
- **Partial progress.** On timeout mid-pass or a restart mid-batch, we keep the commands already proven. In-flight commands live in the queue and cluster state (`MarkedForDeletion`, disruption taint); unstarted candidates are reconsidered next pass, oldest-first, with in-flight nodes excluded by `queue.HasAny` inside `NewCandidate`.

## Alternatives Considered

### Alternative 1: Time-cap wrapping `disrupt()` ([#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927)'s `disruptWithTimeCap`)

Loop `disrupt()` until a wall-clock budget expires, leaving `ComputeCommands` as "return the first candidate."

Rejected as the primary mechanism: each iteration re-runs candidate discovery over the whole cluster, so most of the outer budget is spent rediscovering candidates we already had. The cap belongs *inside* `ComputeCommands`, after candidates are in hand. #2927's method-fairness scheduler remains useful and complementary; its drift time-cap should call into this batched `ComputeCommands` rather than re-invoke `disrupt()`.

### Alternative 2: Copy StaticDrift exactly — 1:1 replacement with no simulation

`StaticDrift.ComputeCommands` already emits many single-candidate commands per pass by building a bare `scheduling.NewNodeClaimTemplate(np)` per candidate and skipping simulation entirely. It is the closest working precedent and far less code than this RFC.

Rejected because a template-only replacement carries the NodePool's full instance type options and lets the cloud provider pick the cheapest, which can be too small for the candidate's pods. Static NodePools are homogeneous with pinned requirements, so this is safe there; dynamic NodePools are not. A simulation restricted to the candidate's own pods is what buys correct instance-type selection while staying O(pods on one node).

### Alternative 3: Independent existing-node simulations without accounting

Emit many commands, each simulated against the original cluster snapshot, packing onto existing nodes. Rejected: this is exactly the invariant DerekFrank called out on [#3197](https://github.com/kubernetes-sigs/karpenter/issues/3197). Two candidates can both claim the same spare slot, and both drains then fail to reschedule. Knowing when two simulations are independent is the hard problem this RFC avoids by construction.

### Rejected variants

- **Keep existing-node packing, add sequential state accounting.** After accepting command A, mark A deleting and add A's replacements to the simulated cluster before simulating B. Correct, and it preserves today's "delete if it all packs" semantics, but each sim is still ~15 s typical / 150 s worst, so the cap still yields only a handful of commands. Worth revisiting if we later need packing during drift.
- **Only improve the ordering heuristic** so the first candidate is usually valid. Cheap, but it does nothing for the per-command cost or for the one-command-per-pass ceiling.
- **Parallelize / shard `SimulateScheduling` and `GetCandidates`.** A real performance win and compatible with this RFC, but it does not by itself feed the queue and does not bound worst-case work. See Open Questions.

## Backward Compatibility

- **No API / CRD / YAML changes.** Existing NodePools, budgets, and `terminationGracePeriod` keep their meaning. Budgets that were previously ineffective during a drift wave become effective.
- **More parallel disruption.** Clusters that sized PDBs, kube-apiserver, or cloud `CreateFleet` rate limits around "Karpenter drifts one node at a time" will start to see those limits. That is the budget contract working as documented; operators who want the old ceiling can set `nodes: 1` on the drifted reason, or set `DriftTimeoutDuration = 0`.
- **Replace instead of shrink**, as described under Edge Cases. This is the one semantic change.
- **HA replicas.** Still one active disruption reconciler. The timeout clock is in-memory and per-process; no new state.

## Testing Plan

Ships enabled by default; `DriftTimeoutDuration = 0` is the rollback lever instead of a feature gate.

- **Timeout:** fake clock, N schedulable candidates; `ComputeCommands` returns more than one command and stops after `DriftTimeoutDuration` with candidates unevaluated.
- **Rollback lever:** `DriftTimeoutDuration = 0` returns at most one command, matching today.
- **Empty batch:** empty drifted nodes emit delete commands with no simulation (spy/fake provisioner) up to budget.
- **Independence:** two candidates whose pods would both fit the same leftover hole on a third node; both get their own replacement and neither claims the hole.
- **No duplicated pending capacity:** with pending pods present, N drift commands in one pass produce N replacements total, not N × (1 + pending).
- **Back-off + budget:** backed-off pool skipped; a second command in the same pool respects the decremented mapping.
- **Suite:** existing drift replacement tests still pass, plus a pass that `StartCommand`s two drift commands concurrently and initializes both replacements.

Before rollout, instrument `GetCandidatesWithTotals` and the per-candidate replacement simulation directly so the unmeasured numbers above can be replaced with real ones.

## Open Questions

1. **1 minute or 3 minutes?** 1 minute matches MultiNode's constant and should be plenty if per-candidate sims are milliseconds. If they are not (DRA, huge pod lists), 3 minutes matching SingleNode is the obvious bump. Prefer 1 minute until measured.
2. **Is `GetCandidates` in scope?** It is paid by *every* method before `ComputeCommands` and becomes the next ceiling once generation is no longer one-command-per-pass. Out of scope here; prefer a dedicated change around `DeepCopyNodes` / candidate construction, which would speed up all disruption methods.
3. **Should this wait for or land with [#2927](https://github.com/kubernetes-sigs/karpenter/pull/2927)?** No. This RFC is useful on today's method loop, and #2927 should call batched Drift rather than looping single-command `disrupt()`.

## References

- Issue: [Karpenter drift reconciliation is a bottleneck in larger clusters (#3197)](https://github.com/kubernetes-sigs/karpenter/issues/3197), including DerekFrank's note on simulation independence
- [RFC for time slicing disruption (#2927)](https://github.com/kubernetes-sigs/karpenter/pull/2927)
- [Per-NodePool exponential back-off for drift](drift-per-nodepool-backoff.md)