# Dynamic Split/Merge Design Note

Last reviewed: 2026-05-09

## Scope

This note fixes the V2 direction for quadtree-style cell split/merge. The
completed P6 through P9 work now covers the manual/default-off dynamic cell
control plane: split, merge, canonical multi-depth activation, durable
assignment state, policy-gated operation execution, cadence, recommend-only
history, replay audit, guarded Kubernetes evidence, and post-smoke default-off
cleanup.

The remaining boundary is not "finish split/merge activation"; that control
plane is complete for the current scope. Future work should focus on the next
design boundary selected in `docs/todo-next.md`, such as long-running
observability, request latency tracing, ghost relay hardening, or broader
runtime soak. Unguarded automatic mutation remains out of scope unless a later
milestone adds a separate policy and evidence contract.

The Orchestrator has deterministic split/merge planning, hysteresis/cooldown,
churn-budget, complete sibling validation, overlap tests, assignment-safe
preview, default-off manual split and merge activation RPCs, policy-gated
planner activation, durable assignment state, and operation ledger integration.
Same-Worker merge coalesces child actor/owner/root/pending state into the
parent, while mixed-owner merge pre-stages the owner Worker parent and replays
remote child actor/owner state before parent assignment publish.

The internal rollout history is preserved in the milestone docs:
`docs/p6-completion-audit.md`, `docs/p7-operation-loop.md`,
`docs/p8-closed-loop-operation-cadence.md`, and
`docs/p9-operation-control-plane-readiness.md`.
`cargo xt dev merge-activation-smoke` verifies same-Worker Gateway parent route
convergence plus stable-session parent Move,
`cargo xt dev canonical-merge-activation-smoke` verifies the same path for a
canonical parent, `cargo xt dev canonical-merge-activation-failure-smoke`,
`cargo xt dev canonical-merge-activation-restart-smoke`, and
`cargo xt dev canonical-merge-activation-soak` verify canonical owner outage,
restart, and load/soak paths, and
`cargo xt dev merge-activation-cross-worker-smoke` verifies mixed-owner remote
child replay plus local/remote stable-session parent Moves. `cargo xt dev
merge-activation-soak` adds sustained parent Ping/Move traffic, Gateway latency
histogram, and close-counter evidence for the same manual merge path. `cargo xt
dev planner-mutation-smoke` verifies that planner-selected merge mutation is
blocked/no-op by default and publishes only with the explicit policy id.

P4.3's first activation shape was fixed as a manual, feature-flagged,
split-only runtime slice. P6 extends that with same-Worker merge coalescing and
cross-Worker merge replay, but automatic rebalancing remains outside this
boundary.
The local post-publish failure smoke verifies failure evidence and target Worker
restart recovery; it does not implement automatic rollback or runtime cooldown
enforcement. The P5 recovery policy is explicit operator recovery plus deployment
backout for controlled smoke rollback; automatic merge rollback stays outside
this completion boundary. The P6 persistent state slice stores only published
assignment maps. In-flight `staged_splits` remain memory-only and are cleared by
process restart rather than resumed.

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
- No direct mutation of assignments outside explicit default-off activation
  commands or the handover state machine.
- No automatic cross-Worker merge submission in the first merge runtime slice.
- No planner submission without an explicit policy id in the first runtime
  slice.
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

The first safe default should be one active split/merge plan per world, one
handover commit at a time per cell family, and up to four cells moved per
interval so a complete sibling merge can appear as a dry-run operator plan.

## P4.3 First Activation Spec

Chosen shape:

1. Split-first activation. P4.3/P5 are split-only; P6 permits only the later
   same-Worker merge slice described below.
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

## P6 Same-Worker Merge Activation

The first merge runtime slice is intentionally narrower than the planner model:

- `SubmitMergeActivation` requires `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`.
- The request names a `depth=0/sub=0` parent and `owner_worker_id`.
- The parent must be unpublished.
- All four `depth=1/sub=0..3` siblings must be assigned to configured and
  currently registered Workers; `owner_worker_id` selects the parent owner.
- Active handover and staged split overlap with the parent/sibling family are
  rejected.
- Publication replaces the four child assignments with the parent assignment in
  one persisted assignment-map update.
- Worker assignment refresh treats local child -> parent transitions as
  coalesce events, moving child actors, owners, client root actors, buffered
  moves, pending broadcasts, and replay claims to the parent before local child
  cells are dropped. For mixed-owner families, remote source Workers replay
  child actor/owner state to a source-specific staged parent operation on the
  owner Worker before publication.

This slice does not implement unapproved planner submission, cooldown
persistence, guarded Kubernetes merge evidence, or internal planner mutation
evidence yet.

### Target Worker Selection

The mutating split command must require an explicit target map for all four
children. The operator can use planner output as a recommendation, but the
assignment-changing request must name the final target Worker for each child
`sub` value. P6 also adds an optional explicit child `cell` field to the
`SplitChildTarget` wire shape so nested canonical families can be validated
before runtime support is enabled.

Validation rules for the first runtime slice:

- The source parent cell must be assigned to exactly one configured Worker.
- Every target Worker id must be configured and currently registered before the
  split can commit.
- The target map must cover child `sub` values `0`, `1`, `2`, and `3` exactly
  once.
- Explicit child-cell targets must form exactly one complete legacy shallow
  family or one complete canonical leaf-coordinate family, and must not be
  mixed with legacy `sub` targets in the same request.
- Canonical leaf-coordinate target families can publish only through the
  default-off/manual gRPC path with explicit child cells. Worker replay batch
  construction partitions canonical child families by exact child `CellId`,
  Gateway assignment listing updates keep exact canonical child route keys, and
  Worker assignment refresh/AOI helpers have focused canonical coverage.
  `cargo xt dev multi-depth-activation-smoke` covers the local success path for
  canonical split publish, Gateway convergence, child traffic, stable-session
  post-split Move, remote AOI resync, and route-change/relay metrics. The
  `cargo xt dev multi-depth-activation-failure-smoke` path covers local target
  Worker outage detection, no automatic rollback, retained child assignments,
  and target Worker restart recovery for exact canonical child routes. The
  `cargo xt dev multi-depth-activation-restart-smoke` path covers persisted
  canonical child assignment recovery after Orchestrator restart with manual
  activation disabled, Worker refresh, Gateway route convergence, child Ping,
  and remote AOI interest resync metrics. The
  `cargo xt dev multi-depth-activation-soak` path covers sustained Ping/Move
  traffic over exact canonical child cells, route convergence retention, remote
  AOI frame observation, Gateway latency histogram growth, and zero Gateway
  client close counters. The multi-depth completion gate remains closed until
  guarded Kubernetes evidence exists.
- The operator helper accepts canonical requests with `cargo xt
  split-activation --depth <d> --target-cell
  world,cx,cy,depth,sub=worker-id` repeated exactly four times. Do not mix
  `--target` and `--target-cell` in one command.
- At least one child should target a non-source Worker; otherwise the plan does
  not relieve load and should stay a dry-run/no-op recommendation.
- A target Worker may receive more than one child, but the command must keep the
  global churn budget at one split family and one handover commit at a time.
- If implementation adds a target recommendation helper, use a deterministic
  order: registered non-source Workers with no active handover first, then lower
  assigned-cell count, then lexicographic Worker id. The helper is advisory only
  and must not auto-submit activation.

### `CellId.depth/sub` Semantics

The legacy activation smoke path supports only the shallow split alias:

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
- The legacy `--target sub=worker-id` request shape must reject parent cells
  with `depth > 0` and any request that would create `depth > 1` cells.

This keeps the current shallow `CellId.depth/sub` encoding honest. The existing
shape is not sufficient for arbitrary nested quadtree paths because `sub` stores
only one local quadrant, not the full path from the root. The canonical
multi-depth request shape uses the leaf-resolution coordinate decision in
`docs/multi-depth-cellid-decision.md`: explicit child-cell targets use
leaf-resolution `cx/cy`, `depth=parent.depth+1`, and `sub=0`. That path now has
local success, recovery, restart, and soak smoke/report evidence; internal
evidence remains a future gate.

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

If `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` is set, step 6 first persists the
complete assignment map to the state file with an atomic temp-file replace. A
persistence failure restores the pre-publish in-memory assignment map and leaves
the split unpublished.

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
  smoke rollback, revert the deployment image/topology/fixture/flag slice and wait
  for deployment controller `Synced / Healthy`. Cooldown marking remains policy-only until a
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

For the original runtime implementation, keep or extend tests for:

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

The P4.3 replay/publish slice added focused checks for:

- Feature flag disabled: mutating activation is rejected and preview remains
  assignment-safe.
- Manual target map validation: missing children, duplicate `sub` values,
  unknown targets, unregistered targets, and source-only no-op plans are
  rejected.
- `CellId` depth validation: the initial legacy `depth=0/sub=0` parent publish
  path split into `depth=1/sub=0..3` children; later P6 work added canonical
  explicit-child split/merge coverage.
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
- Local Orchestrator restart recovery smoke:
  `cargo xt dev activation-restart-smoke` publishes the same split with
  `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` enabled, verifies the state file is
  written, restarts the Orchestrator without manual activation enabled, waits
  for Worker re-registration, restarts the Gateway against the recovered
  listing, verifies four child routes, pings all children, and verifies
  post-restart AOI resync. The smoke writes evidence to
  `.dev/reports/activation-restart-smoke-latest.json`.
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
- Live metrics planner-to-operator smoke:
  `cargo xt dev activation-live-plan-smoke` starts the same two-Worker dev
  stack without preview fixture pressure, creates real Gateway traffic, scrapes
  Worker per-cell actor metrics through `--live-worker-metrics`, verifies a
  ready operator plan, and confirms assignments stay unchanged. This keeps live
  metric usage in the operator evidence lane rather than auto-submitting
  mutations.
- Live metrics activation smoke:
  `cargo xt dev activation-live-metrics-smoke` builds the operator plan from
  live Worker metrics, writes the plan report, submits the recommended target
  map through the same default-off manual RPC, and verifies route convergence,
  stable-session post-split Move, and remote AOI resync. The activation report
  records the linked `operator_plan.source` and plan report path.
- Merge planner-to-operator smoke:
  `cargo xt dev merge-plan-smoke` starts a two-Worker dev stack with four
  depth-1 siblings assigned to one Worker and a cold merge preview fixture, runs
  `cargo xt merge-activation-plan`, verifies the report is ready, and confirms
  assignments stay unchanged. The report records `activation_mutated=false` and
  a ready same-owner merge precondition.
- Merge activation smoke:
  `cargo xt dev merge-activation-smoke` starts the same sibling topology,
  records the merge operator plan, submits `SubmitMergeActivation`, verifies the
  Orchestrator listing converges to the parent assignment, waits for Gateway
  route count 1, checks parent Ping, verifies Worker parent actor metrics after
  coalescing, and proves a pre-merge child session can issue a parent Move. The
  smoke writes `.dev/reports/merge-activation-smoke-latest.json`.
- Canonical merge activation smoke:
  `cargo xt dev canonical-merge-activation-smoke` starts a canonical
  `depth>0/sub=0` sibling topology, records the merge operator plan, submits
  `SubmitMergeActivation`, verifies the Orchestrator listing converges to the
  canonical parent assignment, waits for Gateway route count 1, checks parent
  Ping, verifies Worker parent actor metrics after canonical child coalescing,
  and proves a pre-merge child session can issue a parent Move. The smoke writes
  `.dev/reports/canonical-merge-activation-smoke-latest.json`.
- Canonical merge activation failure/restart/soak smokes:
  `cargo xt dev canonical-merge-activation-failure-smoke` injects owner Worker
  outage after canonical parent publish and verifies no automatic rollback plus
  owner restart recovery.
  `cargo xt dev canonical-merge-activation-restart-smoke` persists the canonical
  parent assignment and verifies Orchestrator restart recovery without the
  manual activation flag.
  `cargo xt dev canonical-merge-activation-soak` runs sustained parent
  Ping/Move traffic with Gateway latency histogram and close-counter checks.
- Merge activation cross-Worker smoke:
  `cargo xt dev merge-activation-cross-worker-smoke` starts a mixed-owner
  sibling topology, submits the same default-off manual merge RPC with
  `owner_worker_id=worker-a`, prepares the owner Worker parent with source-specific
  replay operation ids, has the remote child Worker replay actor/owner state to
  the parent, publishes the parent assignment, and proves both local-child and
  remote-child stable Gateway sessions can issue parent Moves. The smoke writes
  `.dev/reports/merge-activation-cross-worker-smoke-latest.json`.
- Merge activation failure smoke:
  `cargo xt dev merge-activation-failure-smoke` injects a post-publish owner
  Worker outage, verifies parent route traffic fails while the parent assignment
  remains published and no automatic rollback is observed, then restarts the
  owner Worker and verifies parent route plus fresh Ping recovery. The smoke
  writes `.dev/reports/merge-activation-failure-smoke-latest.json`. The report
  includes `actor_state_recovery_policy.policy_id=volatile_worker_actor_state_rejoin_required_v1`:
  same-Worker merge assignment state is durable, but Worker actor runtime state
  is still volatile in this slice, so owner Worker restart recovery requires
  parent route convergence plus affected client rejoin/reseed.
- Merge activation restart smoke:
  `cargo xt dev merge-activation-restart-smoke` runs the same manual merge with
  persisted assignment state enabled, restarts Orchestrator with manual
  activation disabled, and verifies the parent assignment, Gateway parent route,
  parent Ping/Move, and Worker coalesced actor metrics survive restart. The
  smoke writes `.dev/reports/merge-activation-restart-smoke-latest.json`.
- Merge activation soak smoke:
  `cargo xt dev merge-activation-soak [--iterations 32] [--sleep-ms 10]` runs
  the same manual merge, then exercises the published parent route with four
  parent actors and sustained Ping/Move traffic. It verifies parent route count
  retention, Gateway Ping/Join/Move histogram growth, zero Gateway close
  counters, and writes `.dev/reports/merge-activation-soak-latest.json`.
  `cargo xt dev activation-report-check --merge-plan-report ... --merge-activation-report ... --merge-cross-worker-report ... --merge-failure-report ... --merge-restart-report ... --merge-soak-report ... --planner-mutation-report ...`
  validates the local merge evidence contract.
- Policy-gated planner mutation smoke:
  `cargo xt dev planner-mutation-smoke` runs a selected merge plan twice. The
  first pass omits policy approval and writes a `blocked_by_policy` no-mutation
  report; the second pass includes
  `--allow-mutation --policy-id operator_approved_planner_mutation_v1`, submits
  the selected plan, verifies parent route convergence, and writes
  `.dev/reports/planner-activation-latest.json`.
  `cargo xt dev activation-live-planner-mutation-smoke` runs a selected split
  plan from live Worker metrics twice. The first pass writes
  `.dev/reports/planner-activation-live-blocked-latest.json` without mutation;
  the second pass uses the same explicit policy id, publishes child
  assignments, verifies Gateway convergence, and leaves
  `.dev/reports/planner-activation-latest.json` with a `live_worker_metrics:`
  source. The `--planner-mutation-report ... --require-planner-live-metrics`
  report-check flags validate that report shape.
- Operator activation helper: `cargo xt split-activation` submits the same
  default-off manual RPC using an explicit `--target sub=worker-id` map and exits
  non-zero unless the operation publishes four child assignments.
  `cargo xt merge-activation` submits the same default-off manual merge RPC with
  an explicit `--owner-worker-id` and exits non-zero unless four children are
  replaced by the parent assignment. Canonical merge parents are submitted with
  `--depth <n>` and `sub=0`; root `depth=0` remains the legacy shallow alias.
  `cargo xt planner-activation` reads the planner-selected split or merge plan
  and only submits it when `--allow-mutation --policy-id
  operator_approved_planner_mutation_v1` are both present. For split, it can use
  `--live-worker-metrics worker-id=addr` to select the plan from Worker metrics
  instead of the Orchestrator preview fixture.
- Guarded Kubernetes activation helper: `cargo xt k8s activation-smoke` starts
  Orchestrator/Gateway service port-forwards, writes a mutation-free plan by
  default, requires `--allow-activation` before `SubmitSplitActivation`, and
  requires `--with-failure --allow-scale` before scaling the target Worker down
  and back up. `--require-target-worker` extends the read-only preflight so the
  target Worker deployment and image must exist before the plan is trusted.
  `--use-live-worker-metrics` also port-forwards the source/target Worker
  metrics services and uses their per-cell actor/pending-move gauges as the
  plan source instead of Orchestrator preview fixture data; the report verifier
  uses `--require-live-metrics-plan` to assert this source remained
  mutation-free.
  `--require-assignment-state-storage` extends the read-only preflight so the
  Orchestrator Deployment must already expose a PVC-backed assignment-state
  path before a P6 restart smoke is attempted.
  `--with-restart --allow-rollout-restart` extends the controlled run by
  requiring `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` to be backed by a writable PVC,
  restarting the Orchestrator Deployment after publish, and verifying recovered
  child assignments, Worker registration, Gateway convergence, child traffic,
  and AOI resync. Successful cluster runs write
  `.dev/reports/guarded-kubernetes-activation-smoke-latest.json`.
- Guarded Kubernetes merge readiness helper:
  `cargo xt k8s merge-activation-smoke` runs deployment controller/image/deployment preflight,
  opens the Orchestrator service port-forward, builds the merge operator plan,
  writes `.dev/reports/guarded-kubernetes-merge-activation-smoke-latest.json`,
  and stops before activation mutation.
  `cargo xt k8s merge-activation-report-check --require-ready-plan` validates
  that report's no-mutation ready-plan contract. Internal merge publish,
  failure/recovery, restart, and load/soak evidence remain separate approval
  gates.

### P5 rollback and recovery policy

The P5 split-activation completion boundary uses
`operator_recovery_no_automatic_merge_rollback_v1`.

- Automatic rollback is disabled for this slice. A post-publish target outage
  must be surfaced as operator evidence instead of silently merging back.
- Same-Worker merge activation exists only as an operator-controlled P6 slice.
  Cross-Worker sibling replay is also operator-controlled/manual and is not
  automatic rollback for split failure.
- Same-Worker merge actor state is intentionally volatile until a future durable
  Worker state slice. After owner Worker restart, the recovery contract is
  parent route convergence plus client rejoin/reseed, recorded as
  `volatile_worker_actor_state_rejoin_required_v1`.
- Operator recovery is target restoration: restart or restore the failed target
  Worker and rerun convergence checks until all child routes answer.
- deployment backout for a controlled smoke window can revert the smoke slice:
  image tag, second Worker topology, preview fixture, and manual activation
  flag. The 2026-05-02 post-smoke cleanup kept the `v2026.05.2` two-Worker
  topology and removed only the preview fixture plus manual activation flag.
  Wait for deployment controller `Synced / Healthy` before retrying activation.
- `cargo xt k8s activation-report-check --require-published --require-failure`
  requires the final cluster report to include this policy and to have an empty
  `remaining_uncovered` list.

Completed P6/P7 runtime evidence now includes:

- Guarded Kubernetes restart recovery for
  `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` with PVC-backed state and report
  verifier coverage.
- Guarded Kubernetes live metrics planner-to-operator evidence using
  `--use-live-worker-metrics` and `--require-live-metrics-plan`, still without
  automatic mutation by default.
- Cross-Worker merge replay, merge failure/recovery smoke, and internal
  Kubernetes merge execution/failure/restart/soak evidence.
- Canonical merge end-to-end smoke, internal planner mutation evidence, and
  multi-depth publish/failure/restart/soak evidence through the P7 completion
  audit.

Future P8 runtime slices should add focused checks for repeated cadence
candidate batches, proposal idempotency, cooldown/budget/concurrency blocking,
bounded approved execution windows, and post-smoke default-off cleanup.
