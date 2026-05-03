# P5 Operational Dynamic Scaling Runtime Completion Audit

Last reviewed: 2026-05-02

This document maps the P5 objective to concrete Tessera artifacts and records
the 2026-05-02 completion evidence. Treat the residual scope section as future
work outside the P5 split-activation completion boundary.

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
| Internal MicroK8s read-only preflight | `cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera ...`; `.dev/reports/internal-microk8s-activation-smoke-latest.json` | Covered |
| Internal MicroK8s split publish | `cargo xt k8s activation-smoke --allow-activation` against approved two-Worker topology | Covered in internal cluster |
| Internal MicroK8s failure/recovery | `cargo xt k8s activation-smoke --allow-activation --with-failure --allow-scale` | Covered in internal cluster |
| ArgoCD readiness gate before cluster smoke | `cargo xt k8s activation-smoke` checks Application `Synced / Healthy` unless `--skip-argocd-check` is passed | Covered by helper |
| Runtime image freshness gate before cluster smoke | `cargo xt k8s activation-smoke --expected-image <tag>` checks live deployment images and records them in `cluster.deployment_images` | Covered by helper |
| Final cluster evidence report validation | `cargo xt k8s activation-report-check --require-published --require-failure --expected-image <tag>` validates the final internal smoke report, including ArgoCD Synced/Healthy, clean preflight, ready plan, deployment image evidence for orchestrator/gateway/source Worker/target Worker, all-child success/recovery probes, and a failure probe that matches the target Worker plan map | Covered by helper |
| Safe operator activation and recovery docs | `docs/internal-microk8s-activation-smoke.md` and `docs/dynamic-split-merge.md` | Covered |
| Merge/rollback policy | `docs/dynamic-split-merge.md` records `operator_recovery_no_automatic_merge_rollback_v1`; final report checker requires this policy and empty `remaining_uncovered` with `--require-failure` | Verified for P5 split activation |

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
cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --allow-activation \
  --with-failure \
  --allow-scale
cargo xt k8s activation-report-check \
  --require-published \
  --require-failure \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2
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

The internal MicroK8s completion run wrote
`.dev/reports/internal-microk8s-activation-smoke-latest.json` with:

- `schema=tessera.internal_microk8s_activation_smoke.v1`
- `operation_id=internal-smoke-1777687783`
- `stage=published`
- `activation_mutated=true`
- `cluster.argocd.sync=Synced`
- `cluster.argocd.health=Healthy`
- all runtime deployments on
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`
- `plan.status=ready`, `source_worker_id=worker-a`, and target map
  `0=worker-a, 1=worker-b, 2=worker-a, 3=worker-b`
- `checks.split_published=true`
- `checks.gateway_ready_routes=4`
- `checks.child_ping_all_routes=true`
- `checks.gateway_close_counters_success_delta_zero=true`
- `checks.post_publish_failure_smoke_ran=true`
- `checks.target_worker_restart_recovered_convergence=true`
- `checks.automatic_rollback_observed=false`
- `success_probe.succeeded=[0,1,2,3]`
- `failure_probe.succeeded=[0,2]` and `failure_probe.failures[].sub=[1,3]`,
  matching the target Worker plan map for `worker-b`
- `recovery_probe.succeeded=[0,1,2,3]`
- `rollback_policy.policy_id=operator_recovery_no_automatic_merge_rollback_v1`
- `remaining_uncovered=[]`

`cargo xt k8s activation-report-check --require-published --require-failure
--expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`
validated that report.

## Internal Cluster Rollout Record

The controlled smoke image and topology were applied through GitOps:

1. Tessera `main` was pushed at commit `8dd5c48205d65b5aa592de64f8588dfbe7f84029`.
2. GitHub Actions run
   `https://github.com/zrma/tessera/actions/runs/25241047849` published
   `harbor.1day1coding.com/1day1coding/tessera:v2026.05.2` for `linux/amd64`
   with digest
   `sha256:08a3fe1a5f560d1a487a99f2b036d842d47abd408ee05d9c6f80c8778e423235`.
3. k8s GitOps commit `8196f720596f066437b06a1011bc1cd0f488489d` applied the
   controlled smoke topology: two Workers, manual activation flag, preview
   fixture, and `v2026.05.2`.
4. ArgoCD Application `tessera` reached `Synced / Healthy` on that revision.
5. The internal smoke command above published the split, verified failure and
   recovery, and wrote the final report.
6. k8s GitOps commit `bf363f87f9710ec7ad49be44572948f2cb259d64` closed the
   smoke window by removing the manual activation flag and preview fixture
   while keeping the `v2026.05.2` two-Worker topology.
7. ArgoCD Application `tessera` reached `Synced / Healthy` on the post-smoke
   revision. Live Orchestrator env now omits
   `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION` and
   `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON`.

GitOps validation used before applying the controlled smoke topology:

```sh
make validate-files FILES='k8s/apps/tessera/manifests/tessera-runtime.yaml'
make validate
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

Post-smoke cleanup validation used:

```sh
make validate-files FILES='k8s/apps/tessera/manifests/tessera-runtime.yaml'
make validate-repo-policies
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

The full repo-wide `make validate` was not reused for the post-smoke cleanup
commit because its Helm template hook attempted to rewrite unrelated Tailscale
generated files. Those hook side effects were excluded from the Tessera cleanup
logical unit.

## Residual Scope

P5 split activation is complete for the objective in this document. Remaining
work is intentionally outside this completion boundary:

1. Automatic planner mutation from live metrics.
2. Runtime merge activation and sibling coalescing policy.
3. Multi-depth split activation beyond `depth=0/sub=0 -> depth=1/sub=0..3`.
4. Persistent split state internal MicroK8s restart evidence remains P6
   follow-up scope. The local durable assignment-state slice, prepared
   PVC-backed storage draft, and restart verifier are tracked in
   `docs/p6-durable-split-state.md`.
