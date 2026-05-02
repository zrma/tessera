# Dynamic Split/Merge Design Note

Last reviewed: 2026-05-02

## Scope

This note fixes the V2 direction for quadtree-style cell split/merge. The
Orchestrator now has an inactive split/merge planner skeleton with deterministic
ranking, hysteresis/cooldown, churn-budget, complete sibling validation, overlap
tests, an assignment-safe dry-run preview endpoint, a fixture-backed runtime
smoke that proves the preview can emit a non-empty split plan, and a
default-off manual split activation replay/publish RPC. Automatic rebalancing,
runtime merge activation, real metrics ingestion, and multi-depth assignment
changes are not implemented yet. A local two-Worker activation smoke now proves
successful manual split publication, Gateway child route convergence,
source/target Worker owned-cell refresh, stable-session post-split Move, and
remote child AOI resync. A companion failure smoke injects a post-publish target
Worker outage, records failed child convergence without automatic rollback, and
verifies target Worker restart recovery. A local activation soak smoke runs
sustained child Ping/Move traffic after publish and records route convergence,
remote AOI frames, Gateway latency histogram growth, and Gateway client close
counters. A planner-to-operator helper converts dry-run preview output plus
Orchestrator health/listing into mutation-free operator evidence and a manual
submission command template. An internal MicroK8s helper now automates the
port-forwarded plan, optional publish, and guarded target Worker scale-down/up
failure recovery smoke. The 2026-05-02 `v2026.05.2` controlled GitOps smoke
window verified live internal split publish, target-only failure detection, and
target Worker recovery, then a cleanup GitOps revision removed the manual
activation flag and preview fixture from the live Orchestrator.

P4.3's first activation shape is now fixed as a manual, feature-flagged,
split-only runtime slice. The implementation milestone should follow this
contract instead of widening into automatic rebalancing or merge activation.
The local post-publish failure smoke verifies failure evidence and target Worker
restart recovery; it does not implement automatic rollback or runtime cooldown
enforcement. The P5 recovery policy is explicit operator recovery plus GitOps
backout for controlled smoke rollback; runtime merge activation stays outside
this completion boundary.

The design assumes the existing V0/V1 foundations stay intact:

- `CellId` is the routing key and every cell has one authoritative Worker.
- Gateway routing follows Orchestrator `ListAssignments`/`WatchAssignments`.
- Cell movement uses the existing handover path before ownership changes.
- Worker AOI can be bounded by radius, visibility radius, and max-cell cap.

## Goals

- Split hot cells before one Worker's tick loop or relay fanout becomes the
  bottleneck.
- Merge cold sibling cells to reduce scheduling, routing, and AOI overhead.
- Keep route churn bounded so Gateway, Worker, and Orchestrator convergence stay
  observable.
- Prefer deterministic plans over reactive per-tick reshuffling.

## Non-goals

- No automatic multi-region migration.
- No hard real-time guarantee during split/merge windows.
- No direct mutation of assignments without the handover state machine.
- No merge activation in the first runtime slice.
- No automatic planner submission in the first runtime slice.
- No deep quadtree encoding change in the first runtime slice.

## Split Inputs

Split decisions should use rolling windows rather than instantaneous samples:

- Actor count per cell.
- Per-cell move/broadcast queue pressure.
- Tick stage duration once per-cell tick metrics exist.
- Relay fanout and remote AOI subscriber count.
- Handover retry/error rate around the candidate cell.

Initial suggested thresholds:

- Candidate only after the cell exceeds at least two pressure signals for three
  consecutive windows.
- Minimum cell age before split: 60 seconds.
- Minimum cooldown after any split or merge touching the cell family: 120 seconds.

## Merge Inputs

Merge is only valid for a complete sibling set owned by compatible assignments.
For the first implementation, "compatible" should mean all siblings are assigned
to the same Worker or all can be moved through handover without violating the
global churn budget.

Initial suggested thresholds:

- All siblings remain below low-water actor, tick, and fanout thresholds for five
  consecutive windows.
- No active handover in the parent or sibling set.
- No recent split/merge cooldown on the parent.

## Hysteresis

Use separate high-water and low-water thresholds:

- Split at high pressure.
- Merge only after sustained low pressure.
- Never split and merge the same parent in the same planning epoch.
- A failed split/merge plan should place the cell family into cooldown before the
  planner retries.

This prevents load oscillation when actors hover around cell borders or when AOI
fanout briefly spikes.

## Assignment Churn Limits

The Orchestrator planner should enforce a hard budget before emitting commands:

- Max active split/merge plans per world.
- Max handover operations started per planning interval.
- Max cells moved per planning interval.
- Max sibling families in cooldown.
- No overlapping plans that touch the same parent, child, source Worker, and
  target Worker pair unless explicitly allowed by policy.

The first safe default should be one active split/merge plan per world and one
handover commit at a time per cell family.

## P4.3 First Activation Spec

Chosen shape:

1. Split-only activation. Runtime merge stays disabled; the merge planner remains
   dry-run/design evidence until a later milestone chooses a merge rollback and
   sibling coalescing policy.
2. Manual activation only. The planner may continue to rank candidates and expose
   preview output, but it must not submit mutating plans from observed metrics.
3. Default-off feature flag. A mutating command surface must reject activation
   unless an explicit operator flag such as
   `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual` is enabled. The exact env var or
   config field can be adjusted during implementation, but the default behavior
   must remain preview-only.
4. One active split operation per world and per cell family. Do not run sibling
   or nested splits concurrently with a handover touching the same parent,
   child, source Worker, or target Worker set.
5. The first mutating implementation remains manual and split-only:
   `SubmitSplitActivation` requires the manual feature flag, validates the
   parent and child target map, prepares target Workers for child replay, asks
   the source Worker to partition the parent snapshot and buffered moves, and
   publishes child assignments only after all child replay paths ack success.

### Target Worker Selection

The mutating split command must require an explicit target map for all four
children. The operator can use planner output as a recommendation, but the
assignment-changing request must name the final target Worker for each child
`sub` value.

Validation rules for the first runtime slice:

- The source parent cell must be assigned to exactly one configured Worker.
- Every target Worker id must be configured and currently registered before the
  split can commit.
- The target map must cover child `sub` values `0`, `1`, `2`, and `3` exactly
  once.
- At least one child should target a non-source Worker; otherwise the plan does
  not relieve load and should stay a dry-run/no-op recommendation.
- A target Worker may receive more than one child, but the command must keep the
  global churn budget at one split family and one handover commit at a time.
- If implementation adds a target recommendation helper, use a deterministic
  order: registered non-source Workers with no active handover first, then lower
  assigned-cell count, then lexicographic Worker id. The helper is advisory only
  and must not auto-submit activation.

### `CellId.depth/sub` Semantics

The first activation slice supports only a single split level:

- Parent: `CellId { world, cx, cy, depth: 0, sub: 0 }`.
- Children: the same `world/cx/cy`, `depth: 1`, and `sub` values `0..=3`.
- Quadrant convention for child routing and replay partitioning:
  - `sub=0`: lower-left quadrant of the parent cell.
  - `sub=1`: lower-right quadrant.
  - `sub=2`: upper-left quadrant.
  - `sub=3`: upper-right quadrant.
- Parent and child cells must not be published as simultaneously writable
  assignments. Child assignments may exist only in a private staged plan until
  the split commits atomically.
- Runtime activation must reject parent cells with `depth > 0` and any request
  that would create `depth > 1` cells.

This keeps the current shallow `CellId.depth/sub` encoding honest. The existing
shape is not sufficient for arbitrary nested quadtree paths because `sub` stores
only one local quadrant, not the full path from the root. A later multi-depth
milestone must first choose an encoding such as path bits, leaf-resolution
`cx/cy`, or another explicit quadtree id before enabling nested splits.

### Rollback And Error Handling

The first activation flow should be all-or-nothing at the assignment publication
boundary:

1. Validate the feature flag, operation id, source parent, full child target map,
   target registration, no active family handover, no published child overlap,
   and churn budget.
2. Reserve the split operation and staged child assignments without publishing
   them through `ListAssignments` or `WatchAssignments`.
3. Freeze the parent through the existing handover policy so source-side moves
   are bounded-buffered.
4. Partition the source snapshot and buffered moves into the four child cells
   using the same quadrant convention used for routing.
5. Replay each child payload to its target Worker and require idempotent success
   before publication.
6. Atomically publish the child assignments and remove the parent assignment.
7. Verify Gateway route convergence, Worker owned-cell refresh, and AOI resync;
   then clear the operation. A later automated planner policy may also clear a
   cooldown marker here.

Rollback rules:

- Any validation failure leaves assignments unchanged and returns a rejected
  operation.
- Any target registration, replay prepare, replay ack, buffer overflow, TTL, or
  route-prepublish failure aborts before child assignments are published. The
  parent remains the only writable assignment, prepared child payloads are
  aborted, and source buffered moves stay on the source-side split replay path.
- If publication succeeds but post-publish convergence checks fail, do not
  automatically merge back in the first slice. Surface the failed convergence
  evidence and require explicit operator target restoration. For a controlled
  smoke rollback, revert the GitOps image/topology/fixture/flag slice and wait
  for ArgoCD `Synced / Healthy`. Cooldown marking remains policy-only until a
  later planner/runtime milestone implements it.
- Automatic planner retries must stay disabled. Manual retry is allowed only
  after the failed operation is cleared; if a later cooldown policy is enabled,
  it must also permit another attempt.

## Runtime Sequence

1. Observe rolling metrics and build a deterministic candidate list.
2. Reserve a split/merge plan with an operation id and, for automated planner
   policy, a cooldown marker.
3. Materialize target assignments in Orchestrator memory but do not publish them
   as active ownership yet.
4. Use `PreCopy -> Freeze -> Diff -> Commit` handover for each ownership move.
5. Publish updated assignments through existing listing/watch paths.
6. Monitor replay, route switch, AOI subscription counts, and retry/error rates.
7. Clear the plan or mark it failed with policy-selected cooldown.

## Required Invariants

- A client-visible route change must be backed by a committed assignment change.
- A source Worker must not drop actors before target replay and owner transfer are
  attempted.
- A target Worker must reject replay for cells it neither owns nor has been
  explicitly staged to own by the active split operation.
- AOI subscriptions must be recalculated after a split/merge assignment update.
- Planner output must be reproducible from the same metrics snapshot and config.

## Verification Plan

Before runtime implementation, keep or extend tests for:

- Split candidate ranking and hysteresis. (initial inactive skeleton covered)
- Merge candidate validation for complete sibling sets. (inactive skeleton
  covered)
- Churn budget rejection. (initial inactive skeleton covered)
- No overlapping active plans for the same cell family. (initial inactive
  skeleton covered for exact CellId families)
- Assignment listing shape before and after a simulated split/merge.
- Dry-run preview response without assignment mutation, including a smoke
  fixture that emits a non-empty split plan.

Runtime implementation should additionally run the existing full gate:

- `cargo xt`
- `cargo test`
- `cargo xt dev up --with-orch`
- `cargo run -p tessera-client -- ping --ts 123`
- `cargo xt dev down --with-orch`

The current P4.3 replay/publish slice adds focused checks for:

- Feature flag disabled: mutating activation is rejected and preview remains
  assignment-safe.
- Manual target map validation: missing children, duplicate `sub` values,
  unknown targets, unregistered targets, and source-only no-op plans are
  rejected.
- `CellId` depth validation: only `depth=0/sub=0` parents can split into
  `depth=1/sub=0..3` children.
- Assignment atomicity before publication: disabled activation, validation
  failures, unregistered targets, and staged-family conflicts leave the parent
  assignment published and publish no child assignment.
- Successful replay/publish: the response returns four child assignments with
  `assignments_changed=true`; `ListAssignments` and `WatchAssignments` publish
  the four children and no longer publish the parent.
- Replay failure rollback: failed source replay keeps `assignments_changed=false`,
  leaves the parent assignment published, and aborts prepared target payloads.
- Local activation convergence smoke: `cargo xt dev activation-smoke` starts a
  two-Worker dev stack, enables manual activation, publishes a split, waits for
  Gateway `/ready` to report four routes, pings all children through the
  source/target Workers, moves a replayed actor through a stable Gateway
  session after the route switch, and verifies a remote child AOI resync
  snapshot. The smoke writes the latest local evidence to
  `.dev/reports/activation-smoke-latest.json`.
- Local post-publish failure smoke: `cargo xt dev activation-failure-smoke`
  publishes the same split, stops the target Worker after publication, verifies
  only target-owned children fail convergence while assignments remain
  published, restarts the target Worker, and verifies all child routes recover.
  The smoke writes the latest local evidence to
  `.dev/reports/activation-failure-smoke-latest.json`.
- Local activation soak smoke: `cargo xt dev activation-soak [--iterations 32]
  [--sleep-ms 10]` publishes the same split, runs repeated child Ping/Move
  traffic, verifies route convergence remains at four child routes, observes
  remote AOI frames, checks Gateway latency histogram growth and zero Gateway
  client close counters, and writes evidence to
  `.dev/reports/activation-soak-latest.json`.
- Local activation report check: `cargo xt dev activation-report-check`
  validates the latest local plan, success, failure/recovery, and soak reports
  against the P5 local evidence contract and rollback policy.
- Planner-to-operator smoke: `cargo xt dev activation-plan-smoke` starts a
  two-Worker dev stack with a fixture-backed split preview, runs
  `cargo xt split-activation-plan`, verifies the report is ready, verifies the
  deterministic `worker-a`/`worker-b` target map, and confirms assignments stay
  unchanged. The helper writes evidence to
  `.dev/reports/split-activation-plan-latest.json`.
- Operator activation helper: `cargo xt split-activation` submits the same
  default-off manual RPC using an explicit `--target sub=worker-id` map and exits
  non-zero unless the operation publishes four child assignments.
- Internal MicroK8s activation helper: `cargo xt k8s activation-smoke` starts
  Orchestrator/Gateway service port-forwards, writes a mutation-free plan by
  default, requires `--allow-activation` before `SubmitSplitActivation`, and
  requires `--with-failure --allow-scale` before scaling the target Worker down
  and back up. `--require-target-worker` extends the read-only preflight so the
  target Worker deployment and image must exist before the plan is trusted.
  Successful cluster runs write
  `.dev/reports/internal-microk8s-activation-smoke-latest.json`.

### P5 rollback and recovery policy

The P5 split-activation completion boundary uses
`operator_recovery_no_automatic_merge_rollback_v1`.

- Automatic rollback is disabled for this slice. A post-publish target outage
  must be surfaced as operator evidence instead of silently merging back.
- Runtime merge activation remains deferred outside the P5 split-activation
  completion boundary. The merge planner can stay dry-run/design evidence until
  a later milestone chooses sibling coalescing and rollback semantics.
- Operator recovery is target restoration: restart or restore the failed target
  Worker and rerun convergence checks until all child routes answer.
- GitOps backout for a controlled smoke window can revert the smoke slice:
  image tag, second Worker topology, preview fixture, and manual activation
  flag. The 2026-05-02 post-smoke cleanup kept the `v2026.05.2` two-Worker
  topology and removed only the preview fixture plus manual activation flag.
  Wait for ArgoCD `Synced / Healthy` before retrying activation.
- `cargo xt k8s activation-report-check --require-published --require-failure`
  requires the final cluster report to include this policy and to have an empty
  `remaining_uncovered` list.

Future runtime slices must add focused checks for:

- Automatic planner submission from live metrics, runtime merge activation, and
  multi-depth split activation if those decision gates are opened.
