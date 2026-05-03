# Internal MicroK8s Activation Smoke Evidence

Last reviewed: 2026-05-03

This is the controlled internal-cluster gate for turning the local
default-off/manual split activation harness into cluster evidence. The
2026-05-02 run completed the controlled smoke window, verified split
success/failure/recovery in MicroK8s, and then closed the mutating activation
flag and preview fixture through GitOps while keeping the `v2026.05.2`
two-Worker topology live.

## Preflight Evidence

Read-only checks already performed:

```sh
bash ~/.codex/skills/microk8s-cluster-ops/scripts/env_guard.sh
kubectl config current-context
kubectl -n tessera get deploy,po,svc,cm -o wide
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --local-orch-port 6600 \
  --local-orch-metrics-port 6601 \
  --local-gateway-port 4600 \
  --local-gateway-metrics-port 4601
```

Observed state:

- Kubernetes context: `microk8s-ts`.
- `tessera-orch`, `tessera-worker`, and `tessera-gateway` are `1/1`
  available.
- Live image: `harbor.1day1coding.com/1day1coding/tessera:v2026.05.1`.
- GitOps manifest path: `../k8s/k8s/apps/tessera/manifests/`.
- Current `tessera-orch-config` has only `worker-a`.
- Current deployment does not set
  `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`.
- The plan-only helper stopped before mutation with `status=no_split_candidate`,
  `preview.source=assignment_listing_zero_metrics`, `plan_count=0`,
  `configured_workers=1`, and no registered Worker in the Orchestrator health
  snapshot. The helper writes this blocked preflight to
  `.dev/reports/internal-microk8s-activation-smoke-latest.json` with
  `stage=blocked_before_activation` and `activation_mutated=false`.
- The two-Worker GitOps draft passed Kubernetes server-side dry-run:

```sh
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

  The API server accepted updates for `tessera-orch`, `tessera-worker`, and
  `tessera-gateway`, plus create operations for `tessera-worker-b` Deployment
  and Service.

## Controlled GitOps Slice

This slice required explicit approval because it changed runtime topology and
enabled a mutating activation flag in a controlled environment.

Applied changes in the k8s GitOps repo:

1. Promote a Tessera image tag that contains the current activation evidence
   harness and runtime fixes.
2. Add a second Worker deployment and service. The prepared GitOps draft keeps
   the existing `tessera-worker` deployment/service as `worker-a` and adds:
   - `tessera-worker-b` service/deployment using `TESSERA_WORKER_ID=worker-b`
     and `TESSERA_WORKER_ADVERTISE_ADDR=tessera-worker-b:5001`.
3. Update `tessera-orch-config` so the parent starts on `worker-a` and
   `worker-b` is configured with no initial cells:

```json
{
  "workers": [
    {
      "id": "worker-a",
      "addr": "tessera-worker:5001",
      "cells": [{"world": 0, "cx": 0, "cy": 0}]
    },
    {
      "id": "worker-b",
      "addr": "tessera-worker-b:5001",
      "cells": []
    }
  ]
}
```

4. Enable `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual` only for the controlled
   smoke window.
5. Until real metrics ingestion exists, provide a controlled preview fixture so
   `GET /split-merge/preview` emits one split candidate:

```json
{
  "cells": [
    {
      "cell": {"world": 0, "cx": 0, "cy": 0},
      "actor_count": 140,
      "move_queue_pressure": 70,
      "high_pressure_windows": 3,
      "cell_age_secs": 120,
      "owner_worker_id": "worker-a"
    }
  ]
}
```

   This can be passed as `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON` for the
   controlled smoke window.
6. Keep Gateway internal-only. The first activation smoke should use
   port-forwarding from the operator machine, or a separately approved
   in-cluster smoke Job.

NetworkPolicy already allows same-namespace ingress/egress, so the split replay
and Worker relay paths should work without additional namespace policy changes.

## Completed Smoke Evidence

Completed run:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --allow-activation \
  --with-failure \
  --allow-scale
cargo xt k8s activation-report-check \
  --require-published \
  --require-failure \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2
```

Evidence:

- GitHub Actions run
  `https://github.com/zrma/tessera/actions/runs/25241047849` published
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.2` for `linux/amd64`
  with digest
  `sha256:08a3fe1a5f560d1a487a99f2b036d842d47abd408ee05d9c6f80c8778e423235`.
- k8s GitOps revision `8196f720596f066437b06a1011bc1cd0f488489d` applied the
  controlled smoke topology and reached ArgoCD `Synced / Healthy`.
- `.dev/reports/split-activation-plan-latest.json` recorded
  `operation_id=internal-smoke-1777687783`, `status=ready`,
  `activation_mutated=false`, `configured_workers=2`, `registered_workers=2`,
  and target map `0=worker-a, 1=worker-b, 2=worker-a, 3=worker-b`.
- `.dev/reports/internal-microk8s-activation-smoke-latest.json` recorded
  `stage=published`, `activation_mutated=true`, `split_published=true`,
  `gateway_ready_routes=4`, all-child success and recovery probes, target-only
  failure for child subs `1` and `3`, `automatic_rollback_observed=false`, and
  `remaining_uncovered=[]`.
- `cargo xt k8s activation-report-check --require-published --require-failure
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`
  accepted the final report.
- k8s GitOps revision `bf363f87f9710ec7ad49be44572948f2cb259d64` closed the
  smoke window by removing `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION` and
  `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON`; ArgoCD reached `Synced / Healthy`
  on that post-smoke revision.

## Read-Only Merge Readiness Probe

The P6 merge internal path has a separate helper that must stay read-only until
a rollout candidate and controlled merge smoke window are approved:

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

Observed 2026-05-03 state:

- ArgoCD `tessera` was `Synced / Healthy`.
- `tessera-orch`, `tessera-gateway`, and `tessera-worker` were all running
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`.
- The helper opened the Orchestrator port-forward, built the merge plan, then
  stopped before mutation with `status=no_merge_candidate` and
  `reason=preview returned no merge candidate`.
- `.dev/reports/merge-activation-plan-latest.json` recorded
  `preview.source=assignment_listing_zero_metrics`,
  `activation_mutated=false`, empty `recommendation.siblings`, and no
  submission command.
- `.dev/reports/internal-microk8s-merge-activation-smoke-latest.json` recorded
  `stage=blocked_before_activation`, `activation_mutated=false`,
  `checks.merge_plan_ready=false`, `checks.merge_published=false`, and
  `remaining_uncovered` entries for merge ready-plan, publish,
  failure/recovery, restart, and load/soak.
- `cargo xt k8s merge-activation-report-check --report
  .dev/reports/internal-microk8s-merge-activation-smoke-latest.json` accepted
  the blocked/no-mutation report.

This is helper-path readiness evidence only. It does not close internal merge
activation because no ready plan, publish, failure/recovery, restart, or
load/soak evidence exists yet.

## Default-Off Multi-Depth Readiness Probe

The P6 canonical multi-depth internal path has a separate helper. Its default
mode is read-only: it builds the canonical parent/child plan, writes the
internal report, and stops before submitting activation unless the operator
passes explicit mutation flags:

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

Observed 2026-05-03 state:

- ArgoCD `tessera` was `Synced / Healthy`.
- `tessera-orch`, `tessera-gateway`, `tessera-worker`, and
  `tessera-worker-b` were all running
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`.
- The helper opened the Orchestrator port-forward, read health/listing, then
  stopped before mutation with `status=blocked` and
  `reason=canonical multi-depth parent is not currently assigned`.
- `.dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json`
  recorded `stage=blocked_before_activation`, `activation_mutated=false`,
  canonical parent `world=0,cx=-2,cy=3,depth=2,sub=0`, explicit canonical
  child target map split across `worker-a`/`worker-b`,
  `checks.multi_depth_plan_ready=false`, `checks.multi_depth_published=false`,
  and `remaining_uncovered` entries for multi-depth ready-plan, publish,
  failure/recovery, restart, and load/soak.
- `cargo xt k8s multi-depth-activation-report-check --report
  .dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2`
  accepted the blocked/no-mutation report.

This is helper-path readiness evidence only. It does not close internal
multi-depth activation because no ready canonical parent plan, publish,
failure/recovery, restart, or load/soak evidence exists yet.

After a P6 image and controlled multi-depth topology are approved, the same
helper can exercise the mutation path. Each mutating branch has an explicit
guard:

```sh
cargo xt k8s multi-depth-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-activation \
  --with-failure \
  --allow-scale \
  --with-restart \
  --allow-rollout-restart \
  --with-soak
cargo xt k8s multi-depth-activation-report-check \
  --require-ready-plan \
  --require-published \
  --require-failure \
  --require-restart \
  --require-soak \
  --expected-image <new-tag>
```

`--with-failure` is blocked unless `--allow-scale` is also present,
`--with-restart` is blocked unless `--allow-rollout-restart` is present, and
failure/restart/soak all require `--allow-activation`. A successful controlled
run publishes canonical child assignments, verifies Gateway route convergence,
detects the target Worker outage, restores the target Worker, verifies
Orchestrator restart recovery, runs child-route soak traffic, and records those
checks in
`.dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json`.

## Port-Forwarded Operator Smoke

After ArgoCD reports `tessera` as `Synced / Healthy`, run from the Tessera
repo. The first command is intentionally plan-only and does not mutate
assignments:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --require-target-worker
```

The helper checks the ArgoCD Application first and refuses to proceed unless it
is `Synced / Healthy`. Use `--skip-argocd-check` only for a deliberately
non-ArgoCD test namespace. The report records the checked Application,
sync status, health status, and live deployment images under `cluster.argocd`
and `cluster.deployment_images`. If `--expected-image` does not match the live
deployments, the helper stops before port-forwarding and writes
`stage=blocked_before_plan` with `plan.status=not_run`. `--require-target-worker`
keeps this first command read-only while also requiring the `tessera-worker-b`
deployment and image to be present before the plan is trusted. If the target
deployment is missing, the helper still writes a blocked report that records
ArgoCD status, the existing deployment images, and the missing target Worker
reason. The report also includes structured `preflight_errors[]` entries so a
later readiness script can distinguish image mismatch, missing service, and
missing target Worker cases without parsing the human-readable `reason`. To
assert the expected blocked readiness state, run:

```sh
cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-readiness-negative.json \
  --expect-preflight-error tessera-worker-b
```

To exercise the P6 live metrics planner-to-operator path without mutation, use
the Worker service metrics port-forwards instead of the Orchestrator preview
fixture:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --require-target-worker \
  --use-live-worker-metrics \
  --live-min-pressure-signals 1
cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-activation-smoke-latest.json \
  --require-live-metrics-plan \
  --expected-image <new-tag>
```

This path remains read-only unless `--allow-activation` is also present. It
port-forwards `tessera-worker` and `tessera-worker-b` metrics services, scrapes
their per-cell actor/pending-move gauges, writes
`plan.preview.source=live_worker_metrics:...`, and requires a ready four-target
operator map. It needs a Tessera image that exposes the per-cell Worker metrics;
the post-P5 `v2026.05.2` live image is therefore only a baseline for the older
preview-backed smoke, not completion evidence for this P6 live metrics gate.

Read-only evidence captured on 2026-05-03:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --require-target-worker \
  --use-live-worker-metrics \
  --live-min-pressure-signals 1 \
  --local-orch-port 6600 \
  --local-orch-metrics-port 6601 \
  --local-gateway-port 4600 \
  --local-gateway-metrics-port 4601 \
  --local-source-worker-metrics-port 5603 \
  --local-target-worker-metrics-port 5604 \
  --out .dev/reports/internal-microk8s-live-metrics-readiness-negative.json
```

The command stopped before mutation with
`stage=blocked_before_activation`, `activation_mutated=false`,
`plan.status=no_split_candidate`, `plan.preview.source=live_worker_metrics:...`,
`plan.preview.plan_count=0`, and all recorded deployment images still matching
`v2026.05.2`. This verifies the read-only helper path and records that live
metrics internal completion still needs a new image/evidence run that produces a
ready plan.

Policy-gated planner mutation from live metrics is currently covered by local
evidence only (`cargo xt dev activation-live-planner-mutation-smoke`). The
internal variant is a separate approved mutation gate: after a P6 image rollout
and explicit smoke window approval, an operator may port-forward Orchestrator
gRPC plus the Worker metrics services and run `cargo xt planner-activation
--kind split --live-worker-metrics worker-a=<addr> --live-worker-metrics
worker-b=<addr> --allow-mutation --policy-id
operator_approved_planner_mutation_v1`. Without both `--allow-mutation` and the
policy id, the helper must only write a `blocked_by_policy` report and leave
assignments unchanged.

During the approved controlled smoke window, run the mutating success path:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --allow-activation
```

For the complete success/failure/recovery gate, run the guarded scale-down/up
variant:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2 \
  --allow-activation \
  --with-failure \
  --allow-scale
cargo xt k8s activation-report-check \
  --require-published \
  --require-failure \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.2
```

Minimum success evidence:

- `internal-microk8s-activation-smoke-latest.json` uses
  `stage=planned_without_activation` when plan-only checks pass without
  mutation, or `stage=published` after an approved activation run.
- `split-activation-plan-latest.json` has `status=ready` and
  `activation_mutated=false`.
- `cluster.deployment_images[]` all match the expected smoke image tag.
  The final report checker requires deployment image evidence for
  `orchestrator`, `gateway`, `source_worker`, and `target_worker`.
- `cluster.argocd` records `checked=true`, `sync=Synced`, and
  `health=Healthy`.
- `preflight_errors` is absent or empty, and `plan.status=ready` with four
  target mappings.
- If `--require-live-metrics-plan` is used, `plan.preview.source` starts with
  `live_worker_metrics:` and `plan.preview.plan_count >= 1`.
- The final report checker compares `failure_probe.failures[].sub` and
  `failure_probe.succeeded[]` against `cluster.target_worker_id` and
  `plan.targets[]`, so target-owned children must fail during the guarded
  target outage while non-target children continue.
- `internal-microk8s-activation-smoke-latest.json` records
  `split_activation.accepted=true`, `state=published`,
  `assignments_changed=true`, and four child assignments.
- `cargo xt k8s activation-report-check --require-published --require-failure`
  passes against the final report.
- Gateway `/ready` reports `routes 4`.
- Child pings succeed through the port-forwarded Gateway after publication.
- Gateway metrics show route and request counters with no client close counter
  increase caused by the success smoke.

## Failure And Recovery Smoke

Post-publish failure handling requires an explicit, reversible target outage.
Use only during the controlled smoke window:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --allow-activation \
  --with-failure \
  --allow-scale
```

The current first slice intentionally does not implement automatic rollback.
Expected evidence is:

- Target-owned children fail while source-owned children continue.
- The failure probe maps those failures to the children assigned to
  `cluster.target_worker_id`; a generic or partial failure list is not accepted
  as completion evidence.
- Orchestrator listing still shows four child assignments.
- `automatic_rollback_observed=false`.
- Worker B restart restores all child pings.
- `rollback_policy.policy_id=operator_recovery_no_automatic_merge_rollback_v1`.
- `remaining_uncovered=[]` after the full `--allow-activation --with-failure
  --allow-scale` run.

The helper restores `tessera-worker-b` to its original replica count if the
failure smoke exits early after scaling down.

## P6 Restart Recovery Draft

The P6 durable assignment-state implementation adds an internal restart helper,
but the internal evidence is not complete until a new Tessera image is published
and the GitOps storage patch is approved and rolled out.

GitOps storage draft:

- `tessera-orch` sets
  `TESSERA_ORCH_ASSIGNMENT_STATE_PATH=/var/lib/tessera/assignment-state.json`.
- `tessera-orch` mounts a writable PVC-backed volume at `/var/lib/tessera`.
- The PVC is `tessera-orch-state` with `ReadWriteOnce` access.
- The helper treats `emptyDir` as insufficient for this internal restart gate.

Prepared controlled command:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --require-target-worker \
  --require-assignment-state-storage \
  --out .dev/reports/internal-microk8s-restart-readiness-negative.json
cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-restart-readiness-negative.json \
  --expect-preflight-error TESSERA_ORCH_ASSIGNMENT_STATE_PATH
```

The readiness command is read-only and is expected to block before the P6 image
and PVC-backed Orchestrator state storage are rolled out.

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-activation \
  --with-failure \
  --allow-scale \
  --with-restart \
  --allow-rollout-restart
cargo xt k8s activation-report-check \
  --require-published \
  --require-failure \
  --require-restart \
  --expected-image <new-tag>
```

`--with-restart` is blocked unless `--allow-rollout-restart` is also present.
Before mutation, the helper checks ArgoCD readiness, deployment image freshness,
the two-Worker target topology, and the live Orchestrator Deployment's
PVC-backed state path. After publish it restarts the Orchestrator Deployment,
recreates port-forwards, waits for both Workers to re-register, verifies the
four child assignments survived the rollout, checks Gateway route convergence,
and runs child traffic plus AOI resync observation. The report checker accepts
this gate only with `--require-restart`.

The restart command may be combined with `--use-live-worker-metrics` after the
new image and GitOps rollout expose Worker per-cell metrics. In that case the
final verifier should include `--require-live-metrics-plan` in addition to
`--require-restart`.

## Exit Criteria

The internal cluster slice is complete because these artifacts exist:

- GitOps commit and ArgoCD `Synced / Healthy` evidence for the two-Worker
  controlled-smoke topology.
- Kubernetes server-side dry-run evidence before push/apply.
- ArgoCD Application `Synced / Healthy` evidence before activation.
- Orchestrator health evidence with `configured_workers=2` and
  `registered_workers=2`.
- Successful `split-activation-plan` evidence.
- Successful `internal-microk8s-activation-smoke-latest.json` evidence from the
  `--allow-activation --with-failure --allow-scale` run.
- `cargo xt k8s activation-report-check --require-published
  --require-failure --expected-image <tag>` passes, proving the report includes
  the P5 rollback policy and no remaining uncovered requirements.
- Documentation update records exact image tag, commands, report paths, ArgoCD
  revisions, and the post-smoke cleanup caveat.
