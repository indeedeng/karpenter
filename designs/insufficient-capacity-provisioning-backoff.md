# Per-NodePool Rate Limiting for Insufficient Capacity Provisioning

## Summary

When a cloud provider cannot satisfy a launch (`InsufficientCapacity` / ICE), Karpenter
creates and destroys NodeClaims in a tight loop for as long as the pods remain pending.
The launch path deletes the NodeClaim immediately and relies entirely on the cloud
provider's unavailable-offerings cache for suppression — but that cache is a *scheduling
filter*, not a *provisioning throttle*: it narrows which offerings the scheduler considers
and adds no delay to the create path. The result is a create → ICE → delete → recreate loop
whose rate is bounded only by controller throughput.

We propose three changes, in increasing order of blast radius:

1. **A per-NodePool creation rate limit on ICE**, modeled directly on the per-NodePool
   back-off tracker introduced for drift disruption in
   [#3128](https://github.com/kubernetes-sigs/karpenter/pull/3128) /
   [#3178](https://github.com/kubernetes-sigs/karpenter/pull/3178). A pool that ICEs is
   limited to a single **probe** NodeClaim per exponentially-growing window instead of
   being removed from scheduling entirely, so pods can still be served the moment capacity
   returns.
2. **A fix to the static provisioning sync gate**, which currently disables itself
   permanently after the first cluster sync.
3. **Scoping cluster-state sync by NodePool**, so one NodePool's in-flight NodeClaims cannot
   halt provisioning and disruption for every other NodePool.

## Background

Two independent incidents, reported in
[#3198](https://github.com/kubernetes-sigs/karpenter/issues/3198), share a signature: a
partially-unsatisfiable scale-out drives sustained NodeClaim churn that degrades
reconciliation for the entire cluster, not just the affected workload.

In the more heavily instrumented of the two, over a 90-minute window one cluster created
~2,700 NodeClaims and disrupted ~1,560 with `reason=insufficient_capacity`, peaking at 498
created and 281 ICE'd in a single five-minute bucket. Provisioner scheduling latency rose
from a 10–15s baseline to a sustained ~60s with 120s peaks — for NodePools that recorded
zero ICE events.

### Why the offerings cache does not throttle

Breaking down 1,381 launch failures from that window by the error `CreateFleet` returned:

| count | error | distinct offerings named |
| ----- | ----- | ------------------------ |
| 1,213 (88%) | single `InsufficientInstanceCapacity`, one instance type in one AZ | 1 |
| 74 (5%) | generic `UnfulfillableCapacity`, no instance type or AZ in the message | unclear |
| 52 (4%) | `all requested instance types were unavailable during launch` | 0 — already saturated |
| 34 (2%) | three `InsufficientInstanceCapacity` errors, three sibling types in one AZ | 3 |
| 8 (<1%) | generic spot-capacity error plus one instance type/AZ | 1 |

88% of failures invalidate exactly one `instanceType:zone:capacityType`, and the widest
invalidated three. This follows from the provider marking offerings unavailable per *fleet
error* rather than per *compatible offering*: `CreateFleet` only reports errors for the
offerings it actually attempted. A broad NodePool therefore walks its offering space one
full NodeClaim lifecycle at a time, paying a create-and-delete cycle to learn each entry.

The 52 `all requested instance types were unavailable during launch` failures are the
decisive evidence. That error is returned *before* any `CreateFleet` call, because every
compatible offering was already cached — the exact state the cache TTL is meant to cover.
Karpenter still created a NodeClaim, called `Create`, took the error, and deleted it. All 52
land in two clusters of roughly two seconds each. Churn continued at full rate with the
cache fully saturated, which is what distinguishes a filter from a throttle.

### Why one NodePool degrades the whole cluster

`Cluster.Synced` returns false if **any** NodeClaim in the cluster lacks a provider ID
(`pkg/controllers/state/cluster.go`). A NodeClaim that is mid-launch legitimately has no
provider ID, so sustained churn keeps at least one such NodeClaim resident essentially
always. That single global boolean gates the dynamic provisioner
(`provisioning/provisioner.go`), the disruption controller
(`disruption/controller.go`), and the NodePool counter (`nodepool/counter/controller.go`).

This also explains why the two reported incidents differed in severity. The dynamic
provisioner checks `Synced` before creating, so it self-gates: churn throttles itself at the
cost of cluster-wide provisioning latency. The static provisioner's gate is

```go
if !c.cluster.HasSynced() && !c.cluster.Synced(ctx) {
```

`HasSynced()` latches true permanently after the first successful sync, so the left operand
is always false thereafter and `Synced(ctx)` is never evaluated. Combined with
`MaxConcurrentReconciles: 10` and a watch on NodeClaim deletion events, each ICE deletion
immediately re-triggers a create with no sync gate at all. The reporter of the second
incident observed exactly this: multiple provider-ID-less NodeClaims resident
simultaneously, pinning `Synced` false and halting provisioning cluster-wide.

## Goals

- Bound ICE-driven NodeClaim churn to a rate the controllers can absorb, without stalling
  provisioning for pools that can still be satisfied.
- Recover automatically and promptly once capacity returns — within one back-off window.
- Prevent one NodePool's capacity shortage from degrading provisioning and disruption for
  unrelated NodePools.
- No API/CRD changes; no persisted state.

## Non-Goals

- Changing how cloud providers populate their unavailable-offerings caches. This RFC treats
  provider-side offering tracking as orthogonal; the evidence above shows that even perfect
  cache coverage does not stop the churn.
- Retaining failed NodeClaims in a backed-off state rather than deleting them. That was
  raised as a possible direction in #3198 but changes NodeClaim lifecycle semantics and the
  meaning of the `Launched` condition; rate limiting achieves the same throughput reduction
  without touching the object model.
- Fairness or ordering across NodePools contending for scarce capacity.

## Proposal

### Reuse the drift back-off tracker

[#3178](https://github.com/kubernetes-sigs/karpenter/pull/3178) introduces
`NodePoolBackoff` in `pkg/controllers/disruption/`, with exactly the surface needed here:

```go
func (b *NodePoolBackoff) Fail(nodePool string)
func (b *NodePoolBackoff) Reset(nodePool string)
func (b *NodePoolBackoff) IsBackedOff(nodePool string) bool
func (b *NodePoolBackoff) Snapshot(nodePool string) (level int, until time.Time)
```

It carries an injectable clock and `*rand.Rand`, and configurable base/max delays. We
propose promoting it to a shared package — `pkg/state/nodepoolbackoff`, alongside the
existing `pkg/state/nodepoolhealth` — and instantiating it separately for drift and for
provisioning. Two independent instances, one shared implementation: a pool that is backing
off drift replacements is not necessarily one that should stop provisioning, and vice versa.

### Probe semantics, not exclusion

The drift design skips a backed-off pool's candidates entirely, which is free: nothing waits
on a drifted node. Provisioning is different — every second a pool is throttled, pending pods
stay pending. Removing a NodePool from scheduling would also deny pods that a *different*
offering in that pool could still satisfy, and the data above shows a single ICE invalidates
only one offering out of dozens.

So while a NodePool is backed off it is **not** removed from scheduling. Instead it is
limited to **one create attempt per window**:

- The pool remains a scheduling candidate, so pods continue to be assigned to it and the
  scheduler's decisions are unchanged.
- At most one NodeClaim per window is actually created for that pool. The remainder of the
  scheduler's NodeClaims for that pool in that pass are dropped, and their pods remain
  pending, exactly as they would if the launches had ICE'd.
- If the probe launches successfully, `Reset` clears back-off and the pool immediately
  returns to unthrottled provisioning.
- If the probe ICEs, the window escalates.

This bounds churn for a fully-unsatisfiable pool from ~100 NodeClaims/minute to one per
window, while capping the recovery delay at a single window.

### Back-off formula and defaults

Identical in shape to the drift tracker — exponential with equal jitter, clamped to a
ceiling:

```
level  := level + 1
window := min(baseDelay * 2^(level-1), maxDelay)
window := window/2 + rand[0, window/2)   // equal jitter: floor of window/2
until  := now + window
```

The defaults differ substantially from drift's, because the cost of delay is different:

| Parameter | Drift default | Proposed here | Rationale |
| --------- | ------------- | ------------- | --------- |
| `baseDelay` | `1m` | `5s` | Pods are waiting. Five seconds is already a ~500x reduction from the observed churn rate while being imperceptible for a workload that will wait minutes for a node to become ready. |
| `maxDelay` | `10m` | `2m` | Reached after ~6 consecutive failures (`5s → 10s → 20s → 40s → 80s → 2m`). Bounds worst-case recovery latency at roughly one provider cache TTL. |

### Partial availability

A pool where some launches succeed and others ICE should not be throttled. This falls out of
the design without special handling: `Reset` is called on every successful launch, so any
success returns the pool to unthrottled provisioning. Only a pool that cannot launch
*anything* stays backed off. This is the same reset-on-success property the drift tracker
relies on, and it means no separate success-ratio tracking (as in `nodepoolhealth`'s ring
buffer) is required.

### Where outcomes are observed

In the ICE branch of `launchNodeClaim`, before the existing delete:

```go
case cloudprovider.IsInsufficientCapacityError(err):
	l.recorder.Publish(InsufficientCapacityErrorEvent(nodeClaim, err))
	log.FromContext(ctx).Error(err, "failed launching nodeclaim")
	l.backoff.Fail(nodeClaim.Labels[v1.NodePoolLabelKey])   // NEW
	if err = l.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	// ... existing metrics, unchanged ...
```

`Fail` is a no-op if the pool is already backed off and the window has not elapsed, so a
burst of concurrent ICEs from one pass escalates `level` exactly once. `level` therefore
counts failed *windows*, not individual attempts.

`Reset` is called on the success path of `launchNodeClaim`, where the launch is logged.

Only ICE arms this back-off. `NodeClassNotReady` already has its own handling, and generic
launch errors set `Launched=Unknown` and are retried with the controller-runtime rate
limiter rather than deleting the NodeClaim.

### Where the probe limit is enforced

Both creation paths funnel through `Provisioner.CreateNodeClaims`, which is the natural
choke point:

- **Dynamic path.** `CreateNodeClaims` filters its input: for each NodePool that
  `IsBackedOff`, retain at most one NodeClaim and drop the rest. The retained NodeClaim is
  recorded against the current window under the tracker's lock, so concurrent passes cannot
  both spend it.
- **Static path.** The static controller's `Reconcile` consults `Snapshot` before reserving
  node count and returns `reconcile.Result{RequeueAfter: until.Sub(now)}` when backed off,
  having reserved at most one. Returning a `RequeueAfter` rather than filtering keeps the
  controller from spinning on NodeClaim deletion events.

Recording the attempt at the choke point rather than on the launch outcome is what keeps this
free of lifecycle bookkeeping. `Provisioner.Create` can abandon before a NodeClaim object
exists — the NodePool `Get` can fail, `Limits.ExceededBy` can reject the launch, or the API
create itself can fail — and the ICE outcome, when there is one, lands asynchronously in
`nodeclaim.lifecycle`. Because the attempt is spent at creation time and reclaimed only by the
window elapsing, none of those paths need to unwind anything.

Dropped NodeClaims are not an error. The pods they would have served stay pending and are
reconsidered on the next pass, which is the same outcome as the launch having ICE'd — just
without the create/delete cycle.

### Static provisioning sync gate

Independently of back-off, correct the latching gate in the static provisioning controller:

```go
// before
if !c.cluster.HasSynced() && !c.cluster.Synced(ctx) {
// after
if !c.cluster.Synced(ctx) {
```

`Synced` already returns true trivially once `hasSynced` has latched and no NodeClaim is
missing a provider ID, so this preserves the original startup intent — wait for the first
full sync — while restoring the steady-state check the dynamic provisioner performs. 

### Scoping cluster-state sync by NodePool

The changes above bound how much churn a NodePool generates, but they do not address why one
pool's churn degrades others: `Synced` is a single global boolean, and a mid-launch NodeClaim
legitimately has no provider ID.

The intent of the provider-ID scan is to avoid acting on incomplete state — specifically, to
avoid making a scheduling decision that omits capacity Karpenter has already requested. That
concern is inherently NodePool-scoped: an unlaunched NodeClaim in pool A tells us nothing
about whether pool B's state is complete. We propose scoping the steady-state check
accordingly.

This is not the first time this predicate has needed bounding.
[#3176](https://github.com/kubernetes-sigs/karpenter/issues/3176) reported that a
create/delete race could leave a stale unlaunched entry in cluster state and hold `Synced`
false indefinitely; the fix was the dedicated `state.nodeclaimgc` controller. That handles
entries whose NodeClaim no longer exists. The case here is different — the NodeClaims are
real and genuinely unlaunched — but it establishes that unlaunched entries holding `Synced`
false is a recognized failure mode with an accepted precedent for narrowing the predicate.

#### Proposed API

```go
// Synced reports whether cluster state is complete for the given NodePools.
// Passing no NodePools checks every NodePool, preserving the existing behavior.
func (c *Cluster) Synced(ctx context.Context, nodePools ...string) bool
```

The **initial hydration** branch (`hasSynced == false`) is unchanged and remains global: until
the first full list of NodeClaims and Nodes has been reconciled against cluster state, no
NodePool can be considered synced, and `Synced` returns false regardless of arguments. Only
the post-latch provider-ID scan becomes scoped.

#### Tracking

The NodeClaim-to-NodePool mapping already exists. `NodePoolState` maintains
`nodeClaimNameToNodePoolName`, and `Cluster.updateNodeClaim` already calls
`NodePoolState.UpdateNodeClaim` immediately before writing `nodeClaimNameToProviderID` — so
both facts are recorded at the same site under the same lock. The proposal is to maintain a
`map[string]sets.Set[string]` of unlaunched NodeClaim names per NodePool, updated at the three
sites that already mutate `nodeClaimNameToProviderID`: `updateNodeClaim`, `cleanupNodeClaim`,
and `Reset`. The scoped check is then a map lookup rather than a scan of every NodeClaim in
the cluster, which also removes an O(NodeClaims) walk from a hot path.

**NodeClaims with no NodePool label** are the one case that cannot be attributed.
`NodePoolState.UpdateNodeClaim` already returns early when the label is empty. Such a
NodeClaim must therefore count against *every* NodePool, so the scoped check degrades to
current behavior rather than silently ignoring state it cannot attribute.

#### Consumers

Two of the four callers are already NodePool-scoped reconcilers and become correct with a
one-line change:

- **Static provisioning** (`static/provisioning/controller.go`) reconciles a single NodePool.
  Combined with the sync gate fix above, it checks `Synced(ctx, np.Name)`. This is the
  incident from #3198 where a single ODCR-pinned pool halted the cluster.
- **NodePool counter** (`nodepool/counter/controller.go`) likewise reconciles one NodePool.

The other two operate across pools:

- **Dynamic provisioner** (`provisioning/provisioner.go`) currently skips the entire pass when
  unsynced. Instead, it proceeds and excludes unsynced NodePools from the scheduling pass,
  filtered in `NewScheduler` alongside the existing `ConditionReady` check. Excluding a
  NodePool mid-pass is established behavior there: pools are already dropped for being
  not-ready, for failing instance type resolution, and for awaiting NodeOverlay evaluation.
- **Disruption** (`disruption/controller.go`) skips candidates whose NodePool is unsynced,
  the same per-candidate skip shape the drift back-off uses in #3178, rather than abandoning
  the whole pass.

#### Metrics

`ClusterStateSynced` and `ClusterStateUnsyncedTimeSeconds` keep their current global,
unlabeled semantics, computed from the all-NodePools evaluation. Operators alert on these
today — both incidents in #3198 were diagnosed partly from `cluster_state_synced` — so their
meaning must not change. A NodePool-labeled variant is added alongside them so an operator can
tell *which* pool is unsynced, which is not answerable today.

## Observability

- **Metric (counter):** `karpenter_nodepool_provisioning_backoffs_total`, labeled by
  NodePool — incremented each time a pool enters or escalates back-off (an effective `Fail`,
  excluding within-window no-ops).
- **Metric (gauge):** `karpenter_nodepool_provisioning_backoff_seconds` — seconds remaining
  in the current window per NodePool, `0` when healthy.
- **Metric (counter):** NodeClaims suppressed by the probe limit, labeled by NodePool. This
  is the quantity that would have appeared as `nodeclaims_created` churn before this change,
  and makes the impact directly measurable against the incident graphs above.
- **Event:** a rate-limited event on the NodePool when it enters back-off, so
  `kubectl describe nodepool` surfaces "insufficient capacity back-off until T (level N)"
  rather than pods silently staying pending.
- **Logs:** `V(1)` when a pool enters back-off and when it resets.

## Edge Cases

- **Probe succeeds but the node never registers.** `Reset` fires on successful launch, so the
  pool returns to full rate. Registration failures are already handled by `liveness.go` and
  the `NodeRegistrationHealthy` condition.
- **NodePool deleted while backed off.** Entries are keyed by name and are inert; they can be
  lazily garbage-collected the same way the drift tracker does.
- **Controller restart.** In-memory state clears. Worst case the loop re-attempts a failing
  pool once before backing off again.
- **A pool needing hundreds of nodes where capacity is available.** Never enters
  back-off, so it is unaffected. A pool that ICEs partway through a large scale-out backs off
  once, probes, succeeds, and resets — costing one window of delay.
- **Static pool pinned to a single exhausted ODCR.** This is the second reported incident.
  The pool backs off to one probe per window and, with the sync gate fixed, no longer keeps
  provider-ID-less NodeClaims resident continuously.
- **NodeClaim with no NodePool label.** Cannot be attributed to a pool, so it counts against
  every pool and the scoped check degrades to today's global behavior. Failing closed here is
  deliberate: state that cannot be attributed must not be silently dropped.
- **NodePool with no NodeClaims.** Trivially synced, so a brand-new or scaled-to-zero pool is
  never blocked by unrelated churn.
- **Cluster state reset or controller restart.** `hasSynced` returns to false and the global
  hydration path runs before any pool is considered synced, unchanged from today.

## Alternatives Considered

### Filter backed-off NodePools out of scheduling entirely

Mirrors the drift design exactly, and is marginally simpler in that it needs no per-window
attempt tracking at all. Rejected because a single ICE invalidates roughly one offering out of
dozens, so removing the pool denies pods that other offerings in the same pool could satisfy,
converting a capacity shortage into a scheduling outage for that workload. Given that the
attempt tracking amounts to one timestamp on the back-off entry, that simplicity is not worth
the cost.

### Rely on the cloud provider's unavailable-offerings cache

The status quo. The 52 saturated-cache failures show churn continuing at full rate with every
compatible offering already cached, because the cache filters the scheduler's offering set
without delaying the create path.

### Retain the NodeClaim in a backed-off state instead of deleting it

Raised as a possible direction in #3198. Avoids the delete/recreate cycle, but requires a new
NodeClaim state, changes the meaning of `Launched`, and leaves objects whose provider ID is
permanently empty — which makes the `Synced` problem worse rather than better.

### Global NodeClaim creation rate limit

A simple cluster-wide cap would bound total churn, but it penalizes healthy NodePools for
one pool's capacity shortage — the same coupling we are trying to remove.

### Exclude not-yet-launched NodeClaims from the sync check instead of scoping by NodePool

Rather than scoping `Synced` by NodePool, the provider-ID scan could ignore any NodeClaim
whose `Launched` condition is still `Unknown` and which is younger than `LaunchTimeout`, on
the grounds that such a NodeClaim is in a normal transient state. This is a smaller diff and
needs no per-NodePool tracking.

Rejected because it weakens the predicate for every NodePool rather than narrowing it. The
scan exists to stop Karpenter scheduling against capacity it has already requested but not yet
recorded, and a NodeClaim mid-launch is exactly that capacity — ignoring it globally invites
over-provisioning in the general case to solve a problem that is specific to one pool.
Scoping by NodePool keeps the guarantee intact wherever it is load-bearing and removes it only
where it was never meaningful.

## Backward Compatibility

No API or CRD changes. Clusters that never hit ICE are unaffected: `level` stays `0`,
`IsBackedOff` always returns false, and `CreateNodeClaims` filters nothing. The static
provisioning sync gate fix restores intended behavior and only takes effect when cluster
state is genuinely unsynced.

## Graduation Criteria

The back-off tracker and the sync gate fix are low-risk, in-memory, and backwards compatible,
and are proposed to ship enabled by default without a feature gate — consistent with how the
drift back-off is proposed in #3128. If maintainers prefer, the probe limit could ship behind
a gate for one minor release to collect the suppression-counter metric before defaulting on.

The `Synced` scoping has the widest reach of the three changes and is the most reasonable
candidate for a feature gate, since it is the one that could mask a genuine state-completeness
bug in a way the current global check would not. A gate would also allow the global and scoped
predicates to be compared against the NodePool-labeled metric in a real cluster before
defaulting on.

## Open Questions

1. Should the provisioning tracker be a separate instance of the shared `NodePoolBackoff`, or
   should drift and provisioning share one back-off state per NodePool? Proposed: separate
   instances, since the failure modes and appropriate delays differ by an order of magnitude.
2. Are `5s`/`2m` the right bounds? They are chosen to be visibly effective against the
   measured churn rate while staying below the latency a pending pod already tolerates, but
   the only real-world data available is from the two incidents in #3198.
3. Should the probe limit be one NodeClaim per window, or a small constant (e.g. 3) to
   discover multi-offering availability faster? Proposed: one, with the counter metric to
   validate.
4. Should the dynamic provisioner exclude unsynced NodePools from a scheduling pass, or
   continue to skip the pass entirely when *any* pool is unsynced? Excluding is proposed, on
   the grounds that pools are already dropped mid-pass for readiness and instance-type
   resolution, but it is the one place where scoping changes a scheduling decision rather
   than just letting work proceed.
5. Should `Synced` scoping ship behind a feature gate? Proposed: yes for one minor release,
   with the NodePool-labeled metric used to validate that scoped and global evaluations agree
   outside of incidents.
6. Should `Fail` distinguish the saturated-cache case (`all requested instance types were
   unavailable`) from a single-offering ICE? The former is strictly stronger evidence that
   the pool cannot be satisfied. Proposed: uniform handling for v1.

## References

- Issue: [Karpenter NodeClaim churn for Insufficient Capacity degrades reconciliation
  (#3198)](https://github.com/kubernetes-sigs/karpenter/issues/3198)
- Merged RFC: [drift-per-nodepool-backoff](https://github.com/kubernetes-sigs/karpenter/pull/3128),
  whose `NodePoolBackoff` tracker this RFC proposes promoting to a shared package.
- Implementation: [#3178](https://github.com/kubernetes-sigs/karpenter/pull/3178)
- Related: [#3080](https://github.com/kubernetes-sigs/karpenter/issues/3080), the drift-side
  instance of one NodePool starving others.
- Related: [#3159](https://github.com/kubernetes-sigs/karpenter/pull/3159), a different
  trigger for cluster-sync paralysis.
- Precedent: [#3176](https://github.com/kubernetes-sigs/karpenter/issues/3176) and the
  `state.nodeclaimgc` controller added to keep stale unlaunched NodeClaim entries from holding
  `Synced` false indefinitely.
- Precedent: `pkg/state/nodepoolhealth` and the `NodeRegistrationHealthy` condition, the
  existing per-NodePool health state feeding a NodePool status condition.
- Precedent: `Cluster.NodePoolState` (`pkg/controllers/state/statenodepool.go`), the existing
  per-NodePool NodeClaim tracking this proposal extends.
