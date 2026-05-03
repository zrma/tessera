# P6+ Completion Audit

Last reviewed: 2026-05-03

This is a prompt-to-artifact audit for the current P6+ objective. It is not a
completion declaration. The objective is broader than the current local slice:
Tessera should become a durable, policy-gated, observable dynamic cell control
plane with local/dev and internal MicroK8s evidence for split, merge, failure,
restart recovery, load/soak, rollback/backout, and GitOps rollout state.

## Success Criteria

The P6+ goal is complete only when all of these are true:

1. Published split assignment state survives Orchestrator restart.
2. Restart recovery is verified locally and in internal MicroK8s.
3. Live Worker metrics can produce an operator-reviewed split plan, and any
   mutation remains default-off or policy-gated.
4. Runtime merge activation is implemented with safe policy gates.
5. Split and merge have success, failure/recovery, restart, load/soak, and
   rollback/backout evidence.
6. Gateway route convergence, Worker assignment refresh, AOI resync, and report
   verifiers cover both local/dev and internal evidence.
7. GitOps rollout state is captured for the internal gates.
8. Multi-depth split activation has a chosen `CellId`/quadtree encoding and
   verified runtime behavior.

## Current Gate Matrix

This matrix is based on the current code, report JSON, and verifier output. A
prepared helper or a passing local test is not counted as completion unless it
covers the explicit internal MicroK8s gate.

`cargo xt p6-completion-audit --json` is the machine-readable completion gate
for this matrix. It intentionally returns nonzero until all required internal
P6+ evidence reports are present and passing. The current report set passes the
historical P5 split baseline but still fails P6+ completion with missing
restart, live-metrics, GitOps rollout, merge, multi-depth, and planner mutation
gates. The planner mutation gate is validated with the same internal report
schema as `cargo xt k8s planner-activation-report-check`: one default-off
blocked planner report, one policy-approved published planner report,
ArgoCD/image evidence, and no automatic mutation observation.
`cargo xt k8s planner-activation-report` composes that internal evidence file
from the local blocked/published planner reports and read-only cluster state.
The merge and canonical multi-depth gates use the same report checkers as the
operator commands: `--require-ready-plan` for non-mutating readiness and
`--require-published --require-failure --require-restart --require-soak` for
approved full internal evidence.
The GitOps rollout gate is recorded by `cargo xt p6-rollout-report`, which
reads ArgoCD status and runtime deployment images without mutation, and is
validated by `cargo xt p6-rollout-report-check`. The checker expects
`.dev/reports/p6-gitops-rollout-latest.json` to record a non-P5 P6 image,
matching runtime deployment images, approved GitOps rollout revision, ArgoCD
`Synced / Healthy`, and post-smoke default-off cleanup.

| Gate | Current evidence | Status | Missing before completion |
| --- | --- | --- | --- |
| P5 internal split publish/failure/recovery | `.dev/reports/internal-microk8s-activation-smoke-latest.json` has `stage=published`, `activation_mutated=true`, `split_published=true`, `post_publish_failure_smoke_ran=true`, `target_worker_restart_recovered_convergence=true`, and `remaining_uncovered=[]` for `v2026.05.2` | Complete for P5 split only | Does not cover P6 restart/live-metrics/multi-depth/merge gates |
| Persistent split restart recovery | Local `.dev/reports/activation-restart-smoke-latest.json` has `assignment_state_file_written=true`, `orchestrator_restarted=true`, `restarted_orchestrator_loaded_child_routes=true`, `worker_assignment_refresh_after_restart=true`, `gateway_ready_routes_after_restart=4`, and AOI evidence | Local complete, internal prepared | Approved image/GitOps rollout with PVC-backed state plus `cargo xt k8s activation-report-check --require-published --require-restart --expected-image <new-tag>` |
| Live metrics planner-to-operator | Local live metrics plan/submission and policy-gated mutation smokes pass; internal read-only run on `v2026.05.2` recorded negative readiness rather than a ready candidate | Local complete, internal blocked/readiness only | New image exposing per-cell Worker metrics, ready internal live-metrics plan, and approved internal planner mutation evidence if mutation is exercised |
| Runtime merge activation | Local same-Worker, cross-Worker replay, canonical success/failure/restart/soak smokes and report checks pass | Local complete for current merge slice | Internal ready merge plan, publish, failure/recovery, restart, load/soak, and planner mutation evidence |
| Internal merge readiness | `.dev/reports/internal-microk8s-merge-activation-smoke-latest.json` has `stage=blocked_before_activation`, `activation_mutated=false`, `plan.status=no_merge_candidate`, `plan.preview.source=assignment_listing_zero_metrics`, `merge_plan_ready=false`, and `remaining_uncovered` includes ready-plan/publish/failure/restart/soak | Helper exercised, not complete | A controlled topology or live metrics state that yields a ready merge plan, then approved publish/failure/restart/soak gate |
| Canonical multi-depth split | Local success/failure/restart/soak smokes and report checks pass for canonical explicit child cells | Local complete | Internal ready plan, publish, failure/recovery, restart, and load/soak |
| Internal multi-depth readiness | `.dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json` has `stage=blocked_before_activation`, `activation_mutated=false`, `plan.status=blocked`, `reason=canonical multi-depth parent is not currently assigned`, `multi_depth_plan_ready=false`, and `remaining_uncovered` includes ready-plan/publish/failure/restart/soak | Helper exercised, not complete | A controlled topology with the canonical parent assigned, then approved publish/failure/restart/soak gate |
| Rollback/backout policy | P5 split reports use `operator_recovery_no_automatic_merge_rollback_v1`; merge reports use `operator_controlled_manual_merge_v1`; owner Worker actor recovery is scoped by `volatile_worker_actor_state_rejoin_required_v1` | Local/P5 covered | Internal P6 merge and multi-depth reports must carry the same no-automatic-rollback/backout policy evidence |
| GitOps rollout evidence | P5 `v2026.05.2` publish, controlled smoke GitOps revision, ArgoCD `Synced / Healthy`, and cleanup revision are documented; no `.dev/reports/p6-gitops-rollout-latest.json` report exists yet | Complete for P5 only | P6 image publish, approved GitOps rollout, ArgoCD state, runtime deployment image match, smoke report, `cargo xt p6-rollout-report --image <new-tag> ...`, `cargo xt p6-rollout-report-check --expected-image <new-tag>`, and default-off cleanup for each mutating internal gate |

## Prompt-to-Artifact Checklist

- Persistent split assignment state after Orchestrator restart: covered
  locally. Evidence:
  `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` implementation in Orchestrator, unit
  tests `persistent_assignment_state_recovers_published_split_after_restart` and
  `load_assignment_state_rejects_unknown_worker`, `cargo xt dev
  activation-restart-smoke`, and
  `.dev/reports/activation-restart-smoke-latest.json` with
  `restarted_orchestrator_loaded_child_routes=true`,
  `worker_assignment_refresh_after_restart=true`,
  `gateway_ready_routes_after_restart=4`, child Ping/Move, and AOI resync.
  Missing: internal MicroK8s restart report still listed as
  `internal_microk8s_restart_recovery_smoke` in the local report.

- Internal MicroK8s restart recovery: prepared, not complete. Evidence:
  `cargo xt k8s activation-smoke --with-restart --allow-rollout-restart`,
  `cargo xt k8s activation-report-check --require-restart`, and the companion
  GitOps draft PVC `tessera-orch-state` plus
  `TESSERA_ORCH_ASSIGNMENT_STATE_PATH=/var/lib/tessera/assignment-state.json`.
  Missing: a new image containing P6 code, approved GitOps rollout, and a
  published internal report passing `--require-published --require-restart`
  with the expected image.

- Live metrics planner-to-operator submission: covered locally for split,
  prepared internally. Evidence: Worker per-cell actor/pending-move gauges,
  `cargo xt split-activation-plan --live-worker-metrics ...`, `cargo xt dev
  activation-live-plan-smoke`, `cargo xt dev activation-live-metrics-smoke`, and
  `cargo xt dev activation-report-check --require-live-metrics-plan`. Policy
  gating is also covered locally by `cargo xt planner-activation --kind split
  --live-worker-metrics ...`, `cargo xt dev
  activation-live-planner-mutation-smoke`, and `cargo xt dev
  activation-report-check --planner-mutation-report ... --require-planner-live-metrics`:
  the default run records `blocked_by_policy` with no assignment mutation, and
  only `--allow-mutation --policy-id operator_approved_planner_mutation_v1`
  publishes child assignments. Missing: internal MicroK8s live-metrics and
  planner mutation evidence after a new image rollout. The
  2026-05-03 internal read-only run is negative readiness evidence
  (`plan.status=no_split_candidate` on live `v2026.05.2`), not completion
  evidence.

- Runtime split activation: covered for split-only depth-1 publish/replay.
  Evidence: `SubmitSplitActivation`, split replay prepare/request/ack, atomic
  child assignment publication, pre-publication rollback on replay/persist
  failure, `cargo xt dev activation-smoke`,
  `cargo xt dev activation-failure-smoke`, `cargo xt dev activation-soak`, and
  P5 internal MicroK8s `v2026.05.2` report passing
  `cargo xt k8s activation-report-check --require-published --require-failure`.
  Canonical explicit-child local success/failure/restart/soak is covered by
  `cargo xt dev multi-depth-activation-smoke`,
  `cargo xt dev multi-depth-activation-failure-smoke`,
  `cargo xt dev multi-depth-activation-restart-smoke`, and
  `cargo xt dev multi-depth-activation-soak` with dedicated report checkers.
  Missing: P6 internal restart/live-metrics evidence and multi-depth internal
  evidence.

- Runtime merge activation: partially covered. Evidence:
  `SubmitMergeActivation` is default-off/manual for same-Worker coalescing and
  mixed-owner cross-Worker replay;
  `cargo xt dev merge-plan-smoke`, `cargo xt dev merge-activation-smoke`,
  `cargo xt dev canonical-merge-activation-smoke`,
  `cargo xt dev canonical-merge-activation-report-check`,
  `cargo xt dev canonical-merge-activation-failure-smoke`,
  `cargo xt dev canonical-merge-activation-failure-report-check`,
  `cargo xt dev canonical-merge-activation-restart-smoke`,
  `cargo xt dev canonical-merge-activation-restart-report-check`,
  `cargo xt dev canonical-merge-activation-soak`,
  `cargo xt dev canonical-merge-activation-soak-report-check`,
  `cargo xt dev merge-activation-cross-worker-smoke`,
  `cargo xt dev merge-activation-failure-smoke`,
  `cargo xt dev merge-activation-restart-smoke`,
  `cargo xt dev merge-activation-soak`, and
  `cargo xt dev activation-report-check --merge-plan-report ... --merge-activation-report ... --merge-cross-worker-report ... --merge-failure-report ... --merge-restart-report ... --merge-soak-report ...`.
  The actor-state recovery boundary is explicit:
  `actor_state_recovery_policy.policy_id=volatile_worker_actor_state_rejoin_required_v1`
  states that Worker actor state is not durable in this slice and owner Worker
  restart recovery requires parent route convergence plus client rejoin/reseed.
  `cargo xt dev canonical-merge-activation-smoke` covers canonical
  `depth>0/sub=0` same-Worker merge through the actual dev stack: preview
  plan, manual `SubmitMergeActivation`, parent listing, Gateway parent route
  convergence, Worker canonical child coalescing, and stable-session parent
  Move. The canonical failure/restart/soak smokes cover the same canonical
  parent through owner outage recovery, persisted Orchestrator restart recovery,
  and sustained parent traffic with Gateway close-counter checks. `cargo xt dev
  merge-activation-cross-worker-smoke` covers mixed-owner
  sibling replay by pre-staging the owner Worker parent, replaying remote child
  actor/owner state from the source Worker, publishing the parent assignment,
  and proving local/remote stable-session parent Moves through Gateway.
  `cargo xt planner-activation` now keeps planner-selected mutation
  default-off unless `--allow-mutation --policy-id
  operator_approved_planner_mutation_v1` are both present, and `cargo xt dev
  planner-mutation-smoke` records both blocked/no-op and policy-approved merge
  publish evidence in `.dev/reports/planner-activation-{blocked-,}latest.json`.
  `cargo xt k8s merge-activation-smoke` now provides an internal MicroK8s
  helper for merge: by default it checks ArgoCD/image/deployment preflight,
  builds the port-forwarded merge plan, writes
  `.dev/reports/internal-microk8s-merge-activation-smoke-latest.json`, and
  stops before mutation. During an approved smoke window,
  `--allow-activation --with-failure --allow-scale --with-restart
  --allow-rollout-restart --with-soak` publishes the merge and records
  failure/restart/load-soak checks in the same internal report schema.
  `cargo xt k8s merge-activation-report-check --require-ready-plan` verifies
  the no-mutation ready-plan contract. Missing: approved internal MicroK8s
  merge publish, failure/recovery, restart, load/soak evidence, and internal
  planner mutation evidence. The 2026-05-03 read-only internal merge readiness
  run against live `v2026.05.2` verified the helper/report-check path but
  stopped with `plan.status=no_merge_candidate`,
  `plan.preview.source=assignment_listing_zero_metrics`,
  `activation_mutated=false`, and `merge_plan_ready=false`; this is negative
  readiness evidence, not completion evidence.

- Failure/recovery and rollback/backout policy: covered locally for split and
  same-Worker merge route recovery. Evidence:
  `operator_recovery_no_automatic_merge_rollback_v1` for split,
  `operator_controlled_manual_merge_v1` for same-Worker merge,
  `volatile_worker_actor_state_rejoin_required_v1` for same-Worker merge actor
  state,
  `activation-failure-smoke-latest.json`, and
  `merge-activation-failure-smoke-latest.json`. Missing: internal P6 failure
  evidence.

- Gateway convergence, Worker refresh, AOI resync, and load/soak: covered
  locally for split and canonical multi-depth split; partly covered locally for
  same-Worker merge. Evidence: split activation/restart/soak reports record
  Gateway route counts, Worker assignment refresh, child traffic, and remote AOI
  frames. Merge soak reports record parent route count, sustained parent
  Ping/Move traffic, Gateway latency histogram growth, and zero close counters.
  Canonical multi-depth success/failure/restart/soak reports record
  exact child route convergence, child traffic, restart recovery, and sustained
  Ping/Move traffic over canonical child cells. Merge reports record parent
  route convergence, Worker coalescing, parent Ping/Move, failure route
  recovery, and Orchestrator restart recovery. Missing: internal MicroK8s
  evidence.

- GitOps rollout evidence: covered for P5 only. Evidence:
  `docs/p5-completion-audit.md` and
  `.dev/reports/internal-microk8s-activation-smoke-latest.json` for
  `v2026.05.2`. Missing: P6 image publish, PVC-backed state rollout, ArgoCD
  Synced/Healthy evidence, and P6 report verifier success with `<new-tag>`.

- Multi-depth split activation: encoding decision, core validation, and local
  runtime evidence exist; internal evidence is not complete. Evidence:
  `docs/multi-depth-cellid-decision.md` chooses leaf-resolution `cx/cy`
  coordinates with `sub=0` for canonical multi-depth cells and treats current
  `depth=1/sub=0..3` cells as a legacy shallow alias.
  `tessera-core::CellId` now has canonical child, parent, quadrant, sibling,
  canonical children, legacy shallow alias helpers, and mixed-family
  classification with unit coverage. Orchestrator split/merge activation also
  rejects nested legacy `sub`-only parents without assignment mutation.
  `SplitChildTarget` can carry explicit child cells, and Orchestrator validation
  accepts only complete legacy or canonical child families while rejecting mixed
  target modes and mixed/duplicate/incomplete families. Worker split replay
  batch construction now partitions canonical child families by exact child
  `CellId` instead of legacy `sub`, with unit coverage. Gateway assignment listing
  updates use exact `CellId` route keys and have regression coverage for
  replacing a canonical parent route with four canonical child routes. Worker
  assignment refresh has coverage for replacing an owned canonical parent with
  canonical children, and AOI helper coverage verifies canonical depth leaf
  neighbors beyond the legacy shallow alias. Canonical explicit-child
  `SubmitSplitActivation` can now publish through the default-off/manual
  surface, and persisted Orchestrator assignment state recovers the published
  canonical child listing after restart in unit coverage. `cargo xt
  split-activation` can submit canonical explicit child-cell requests via
  repeated `--target-cell world,cx,cy,depth,sub=worker-id` values. The first
  local end-to-end success path is covered by
  `cargo xt dev multi-depth-activation-smoke`, with a dedicated report checker
  for Gateway convergence, exact child traffic, stable-session post-split Move,
  remote AOI resync, and route-change/relay metrics. Local target outage and
  recovery is covered by `cargo xt dev multi-depth-activation-failure-smoke`
  and `cargo xt dev multi-depth-activation-failure-report-check`. Local restart
  recovery is covered by `cargo xt dev multi-depth-activation-restart-smoke`
  and `cargo xt dev multi-depth-activation-restart-report-check`. Local
  canonical load/soak is covered by
  `cargo xt dev multi-depth-activation-soak` and
  `cargo xt dev multi-depth-activation-soak-report-check`. Canonical merge
  sibling detection is locally covered by Orchestrator planner/runtime tests,
  Worker same-Worker coalescing detection tests, xtask merge plan tests, and
  the canonical merge success/failure/restart/soak smoke/report-check set for
  a `depth>0/sub=0` parent. `cargo xt k8s
  multi-depth-activation-smoke` now provides a read-only internal MicroK8s
  readiness helper for canonical multi-depth split: it checks
  ArgoCD/image/source-target Worker preflight, validates the canonical parent
  and explicit child target map against live Orchestrator listing, writes
  `.dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json`,
  and stops before mutation. `cargo xt k8s
  multi-depth-activation-report-check --require-ready-plan` verifies the
  no-mutation ready-plan contract. The 2026-05-03 read-only run against live
  `v2026.05.2` verified the helper/report-check path but stopped with
  `plan.status=blocked`,
  `reason=canonical multi-depth parent is not currently assigned`, and
  `activation_mutated=false`; this is negative readiness evidence, not
  completion evidence. Missing: planner policy for canonical merge and
  internal multi-depth publish/failure/restart/soak evidence.

## Latest Local Verification

The latest local verification run in this rollout passed:

```sh
cargo fmt --all
cargo test -p xtask planner
cargo test -p xtask merge_activation
cargo test -p xtask multi_depth
cargo test -p tessera-orch merge
cargo test -p tessera-worker merge
cargo test -p xtask merge_activation_plan
cargo xt dev planner-mutation-smoke
cargo xt dev activation-live-planner-mutation-smoke
cargo xt dev merge-activation-cross-worker-smoke
cargo xt dev merge-activation-failure-smoke
cargo xt dev merge-activation-soak --iterations 8 --sleep-ms 5
cargo xt dev merge-activation-soak-report-check --min-iterations 8
cargo xt dev multi-depth-activation-smoke
cargo xt dev multi-depth-activation-report-check
cargo xt dev multi-depth-activation-failure-smoke
cargo xt dev multi-depth-activation-failure-report-check
cargo xt dev multi-depth-activation-restart-smoke
cargo xt dev multi-depth-activation-restart-report-check
cargo xt dev multi-depth-activation-soak --iterations 8 --sleep-ms 5
cargo xt dev multi-depth-activation-soak-report-check --min-iterations 8
cargo xt dev canonical-merge-activation-smoke
cargo xt dev canonical-merge-activation-report-check
cargo xt dev canonical-merge-activation-failure-smoke
cargo xt dev canonical-merge-activation-failure-report-check
cargo xt dev canonical-merge-activation-restart-smoke
cargo xt dev canonical-merge-activation-restart-report-check
cargo xt dev canonical-merge-activation-soak --iterations 8 --sleep-ms 5
cargo xt dev canonical-merge-activation-soak-report-check --min-iterations 8
cargo fmt --all --check
cargo test -p xtask internal_k8s_merge
cargo test -p xtask internal_k8s_multi_depth
cargo test -p xtask merge_activation
cargo xt dev activation-report-check \
  --restart-report .dev/reports/activation-restart-smoke-latest.json \
  --merge-plan-report .dev/reports/merge-activation-plan-latest.json \
  --merge-activation-report .dev/reports/merge-activation-smoke-latest.json \
  --merge-cross-worker-report .dev/reports/merge-activation-cross-worker-smoke-latest.json \
  --merge-failure-report .dev/reports/merge-activation-failure-smoke-latest.json \
  --merge-restart-report .dev/reports/merge-activation-restart-smoke-latest.json \
  --merge-soak-report .dev/reports/merge-activation-soak-latest.json \
  --planner-mutation-report .dev/reports/planner-activation-latest.json \
  --require-planner-live-metrics \
  --min-soak-iterations 8
cargo xt
cargo test
```

The companion k8s manifest draft was also checked with server-side dry-run:

```sh
kubectl --context microk8s-ts apply --dry-run=server \
  -f ../k8s/k8s/apps/tessera/manifests/tessera-runtime.yaml
```

The latest read-only internal merge readiness probe also ran:

```sh
bash ~/.codex/skills/microk8s-cluster-ops/scripts/env_guard.sh
cargo xt k8s merge-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --operation-id internal-merge-readiness-2026-05-03
cargo xt k8s merge-activation-report-check \
  --report .dev/reports/internal-microk8s-merge-activation-smoke-latest.json \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2
```

The smoke intentionally stopped before mutation with `status=no_merge_candidate`
and wrote `.dev/reports/internal-microk8s-merge-activation-smoke-latest.json`;
the report checker accepted that blocked/no-mutation report with the expected
image assertion. It observed ArgoCD `Synced / Healthy`, live image
`harbor.1day1coding.com/1day1coding/tessera:v2026.05.2` for Orchestrator,
Gateway, and owner Worker, `activation_mutated=false`, and
`remaining_uncovered` still containing merge ready-plan, publish,
failure/recovery, restart, and load/soak gates.

The latest read-only internal multi-depth readiness probe also ran:

```sh
cargo xt k8s multi-depth-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --operation-id internal-multi-depth-readiness-2026-05-03
cargo xt k8s multi-depth-activation-report-check \
  --report .dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2
```

The smoke intentionally stopped before mutation because the canonical parent
`world=0,cx=-2,cy=3,depth=2,sub=0` is not assigned in the current live
topology. The report checker accepted that blocked/no-mutation report with the
expected image assertion. It observed ArgoCD `Synced / Healthy`, live image
`harbor.1day1coding.com/1day1coding/tessera:v2026.05.2` for Orchestrator,
Gateway, source Worker, and target Worker, `activation_mutated=false`, and
`remaining_uncovered` still containing multi-depth ready-plan, publish,
failure/recovery, restart, and load/soak gates.

The current P6+ completion gate was also exercised:

```sh
cargo test -p xtask p6_completion
cargo test -p xtask p6_gitops_rollout
cargo test -p xtask internal_planner_activation
cargo xt p6-completion-audit --json
```

The targeted `xtask` tests passed. `cargo xt p6-completion-audit --json`
returned nonzero as expected with `complete=false` and 15 missing P6+ gates:
internal restart recovery, internal live-metrics split plan, P6 GitOps rollout
image, P6 GitOps rollout evidence report, internal merge
ready-plan/publish/failure/restart/load-soak, internal multi-depth
ready-plan/publish/failure/restart/load-soak, and internal planner mutation
policy evidence. The planner mutation finding is based on the absence or
failure of `.dev/reports/internal-microk8s-planner-activation-latest.json`
  against the internal planner report schema, not a raw existence check. The
  report can be composed after approved planner mutation evidence with
  `cargo xt k8s planner-activation-report --expected-image <new-tag>`. The
  GitOps rollout evidence finding is likewise based on the absence or failure of
`.dev/reports/p6-gitops-rollout-latest.json` against the rollout report schema.
This is the correct current state; it is not a completion declaration.

## Audit Result

P6+ is not complete. The current artifact set closes the local durable split
state, live metrics planner-to-operator, same-Worker merge activation,
cross-Worker merge replay, same-Worker merge failure, same-Worker merge restart,
same-Worker merge soak, and canonical explicit-child local split
success/failure/restart/soak lanes, plus local policy-gated planner mutation
for preview-backed merge and live-metrics-backed split. It also closes the
canonical merge sibling detection and local success/failure/restart/soak smoke
gap. The read-only internal merge and canonical multi-depth
helper/report-check paths have now been exercised against live `v2026.05.2`,
but only as blocked negative readiness evidence.
Completion still requires approved internal MicroK8s P6 evidence, planner
mutation internal evidence, canonical merge internal evidence, merge
publish/failure/restart/soak internal evidence, and multi-depth
publish/failure/restart/soak internal evidence.
