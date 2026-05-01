# P5 Operational Dynamic Scaling Runtime Completion Audit

Last reviewed: 2026-05-02

This document maps the P5 objective to concrete Tessera artifacts. It is not a
completion record yet. Treat unchecked or blocked rows as open work.

## Objective Restatement

P5 upgrades the default-off/manual split replay and publish surface into an
operational dynamic scaling runtime that can be observed, activated, verified,
and recovered in the internal cluster.

Completion requires all of the following:

1. Local/dev split success, failure, recovery, and soak paths are automatically
   verified.
2. Gateway route convergence, Worker assignment refresh, and AOI resync are
   covered by smoke evidence.
3. Post-publish failure handling records operator-visible failure and recovery
   evidence.
4. Planner output can be turned into a reviewed operator activation plan without
   mutating assignments.
5. Internal MicroK8s split success, failure, and recovery paths are
   automatically verified against the live cluster or a controlled internal
   smoke topology.
6. Operators have documented commands, safety gates, evidence files, and
   recovery expectations.
7. The P5 rollback policy is explicit: automatic rollback is disabled, target
   Worker restoration is the recovery path, GitOps backout reverts the
   controlled smoke slice, and runtime merge activation stays outside the P5
   split-activation completion boundary.

## Prompt-To-Artifact Checklist

| Requirement | Evidence | Status |
| --- | --- | --- |
| Split activation evidence harness | `cargo xt dev activation-smoke`; `.dev/reports/activation-smoke-latest.json` | Covered locally |
| Gateway route convergence | `activation-smoke` and `activation-soak` assert Gateway `/ready` has four child routes after publish | Covered locally |
| Worker assignment refresh | `activation-smoke` pings source/target-owned child cells and Worker tests cover split-publish owned-cell refresh | Covered locally |
| AOI resync | `activation-smoke` verifies live remote AOI resync snapshot; `activation-soak` observes remote AOI frames | Covered locally |
| Post-publish failure handling | `cargo xt dev activation-failure-smoke`; `.dev/reports/activation-failure-smoke-latest.json` | Covered locally |
| Recovery after target outage | `activation-failure-smoke` stops target Worker, verifies target children fail, restarts target Worker, verifies all child routes recover | Covered locally |
| Planner-to-operator flow | `cargo xt split-activation-plan`; `cargo xt dev activation-plan-smoke`; `.dev/reports/split-activation-plan-latest.json` | Covered locally |
| Load/soak observation loop | `cargo xt dev activation-soak`; `.dev/reports/activation-soak-latest.json` | Covered locally |
| Local/dev report validation | `cargo xt dev activation-report-check` validates latest plan/success/failure/soak reports, rollback policy, and local-only remaining uncovered state | Covered by helper |
| Internal MicroK8s read-only preflight | `cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera ...`; `.dev/reports/internal-microk8s-activation-smoke-latest.json` | Covered as blocked preflight |
| Internal MicroK8s split publish | `cargo xt k8s activation-smoke --allow-activation` against approved two-Worker topology | Blocked |
| Internal MicroK8s failure/recovery | `cargo xt k8s activation-smoke --allow-activation --with-failure --allow-scale` | Blocked |
| ArgoCD readiness gate before cluster smoke | `cargo xt k8s activation-smoke` checks Application `Synced / Healthy` unless `--skip-argocd-check` is passed | Covered by helper |
| Runtime image freshness gate before cluster smoke | `cargo xt k8s activation-smoke --expected-image <tag>` checks live deployment images and records them in `cluster.deployment_images` | Covered by helper |
| Final cluster evidence report validation | `cargo xt k8s activation-report-check --require-published --require-failure --expected-image <tag>` validates the final internal smoke report, including ArgoCD Synced/Healthy, clean preflight, ready plan, deployment image evidence for orchestrator/gateway/source Worker/target Worker, all-child success/recovery probes, and a failure probe that matches the target Worker plan map | Covered by helper |
| Safe operator activation and recovery docs | `docs/internal-microk8s-activation-smoke.md` and `docs/dynamic-split-merge.md` | Covered by runbook; cluster execution blocked |
| Merge/rollback policy | `docs/dynamic-split-merge.md` records `operator_recovery_no_automatic_merge_rollback_v1`; final report checker requires this policy and empty `remaining_uncovered` with `--require-failure` | Decided for P5 split activation |

## Current Evidence Snapshot

Latest local verification in this working tree:

```sh
cargo test -p xtask
cargo xt
cargo test
cargo xt dev activation-plan-smoke
cargo xt dev activation-smoke
cargo xt dev activation-failure-smoke
cargo xt dev activation-soak
cargo xt dev activation-report-check
cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera \
  --require-target-worker \
  --local-orch-port 6600 \
  --local-orch-metrics-port 6601 \
  --local-gateway-port 4600 \
  --local-gateway-metrics-port 4601
cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --require-target-worker \
  --local-orch-port 6602 \
  --local-orch-metrics-port 6603 \
  --local-gateway-port 4602 \
  --local-gateway-metrics-port 4603 \
  --out .dev/reports/internal-microk8s-expected-image-negative.json
cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-expected-image-negative.json
cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-readiness-negative.json \
  --expect-preflight-error tessera-worker-b
```

The local/dev activation reports were regenerated after the rollback policy
gate was fixed:

- `.dev/reports/split-activation-plan-latest.json`: `status=ready`,
  `activation_mutated=false`, deterministic `worker-a`/`worker-b` target map,
  and `remaining_uncovered=["internal_microk8s_activation_smoke"]`.
- `.dev/reports/activation-smoke-latest.json`: split publish succeeded,
  Gateway reported four child routes, child pings passed, stable-session
  post-split Move passed, remote AOI resync snapshot passed, and
  `remaining_uncovered=["internal_microk8s_activation_smoke"]`.
- `.dev/reports/activation-failure-smoke-latest.json`: target-owned children
  failed during target outage, source-owned children continued, no automatic
  rollback was observed, target Worker restart recovered all child routes, and
  `remaining_uncovered=["internal_microk8s_activation_smoke"]`.
- `.dev/reports/activation-soak-latest.json`: 32 per-child iterations produced
  128 successful pings, 128 successful moves, 183 remote AOI frames, four
  Gateway routes after soak, zero Gateway close counters, and
  `remaining_uncovered=["internal_microk8s_activation_smoke"]`.
- All local/dev reports include
  `rollback_policy.policy_id=operator_recovery_no_automatic_merge_rollback_v1`.
- `cargo xt dev activation-report-check` validates those latest local/dev
  reports without relying on manual `jq` inspection.

The internal MicroK8s helper wrote
`.dev/reports/internal-microk8s-activation-smoke-latest.json` with:

- `stage=blocked_before_activation`
- `activation_mutated=false`
- `reason=preview returned no split candidate`
- `preview.source=assignment_listing_zero_metrics`
- `preview.plan_count=0`
- `configured_workers=1`
- `registered_workers=0`
- `remaining_uncovered` lists internal MicroK8s publish and failure/recovery
  evidence only. The current live one-Worker topology fails the stricter
  `--require-target-worker` readiness gate until the two-Worker GitOps slice is
  synced.
- A stricter read-only readiness run with `--require-target-worker
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`
  currently writes
  `.dev/reports/internal-microk8s-readiness-negative.json` with
  `stage=blocked_before_plan`, ArgoCD `Synced / Healthy`, the three existing
  `v2026.05.1` deployment images, and the explicit failure that deployment
  `tessera-worker-b` does not exist in the live namespace. The same failure is
  also recorded as structured `preflight_errors[]` entries for machine checks.
  `activation-report-check --expect-preflight-error tessera-worker-b` validates
  that blocked readiness evidence without relying on the human-readable
  `reason`.

The expected-image negative preflight wrote
`.dev/reports/internal-microk8s-expected-image-negative.json` with:

- `stage=blocked_before_plan`
- `activation_mutated=false`
- `plan.status=not_run`
- `cluster.argocd.sync=Synced`
- `cluster.argocd.health=Healthy`
- live deployment images still on
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.1`
- `rollback_policy.policy_id=operator_recovery_no_automatic_merge_rollback_v1`

## Remaining Blockers

Internal cluster activation is still blocked by the live topology and image:

1. The live GitOps manifest still has one Orchestrator, one Worker, and one
   Gateway.
2. The live Orchestrator has no manual activation flag.
3. The live preview emits no split candidate because it uses
   assignment-listing zero metrics.
4. The promoted image is still `v2026.05.1`, which predates this working-tree
   helper and may not contain the latest activation evidence fixes.

A local, unpushed GitOps draft exists in `../k8s` that changes
`k8s/apps/tessera/manifests/tessera-runtime.yaml` to the expected
`v2026.05.2` smoke image tag, adds `tessera-worker-b`, enables the manual
activation flag, and adds the controlled preview fixture. It passed:

```sh
make validate-files FILES='k8s/apps/tessera/manifests/tessera-runtime.yaml'
make validate
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

This draft must not be pushed until the matching Tessera image tag exists and
the controlled smoke window is approved.

Before P5 can be marked complete, an approved GitOps slice must:

1. Publish and promote a Tessera image containing this P5 working tree.
2. Run a controlled two-Worker topology with `worker-a` and `worker-b`.
3. Configure `TESSERA_WORKER_ADVERTISE_ADDR` for both Worker services.
4. Enable `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual` only for the smoke
   window.
5. Provide either a controlled `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON` fixture
   or real metrics that emit a split candidate.
6. Run `cargo xt k8s activation-smoke --allow-activation --with-failure
   --allow-scale`.
7. Record the exact image tag, ArgoCD health, command output, report paths, and
   final `activation-report-check --require-published --require-failure`
   success. The final report must include
   `rollback_policy.policy_id=operator_recovery_no_automatic_merge_rollback_v1`
   and an empty `remaining_uncovered` list. Its failure probe must fail exactly
   the child subs assigned to `cluster.target_worker_id` in `plan.targets[]`
   while the remaining child subs continue through the Gateway.
