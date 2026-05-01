# Internal MicroK8s Activation Smoke Plan

Last reviewed: 2026-05-02

This is the controlled internal-cluster gate for turning the local
default-off/manual split activation harness into cluster evidence. It is a
plan, not completed evidence. The 2026-05-02 preflight found the live GitOps
deployment still has one Orchestrator, one Worker, one Gateway, and no
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual` flag, so runtime activation smoke
is blocked until a dedicated GitOps slice changes topology and exposes a split
preview candidate.

## Current Preflight Evidence

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
- The unpushed two-Worker GitOps draft passed Kubernetes server-side dry-run:

```sh
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

  The API server accepted updates for `tessera-orch`, `tessera-worker`, and
  `tessera-gateway`, plus create operations for `tessera-worker-b` Deployment
  and Service.

## Required GitOps Slice

This slice must be approved before applying because it changes runtime topology
and enables a mutating activation flag in a controlled environment.

Required changes in the k8s GitOps repo:

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

## Exit Criteria

The internal cluster slice is complete only when these artifacts exist:

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
- Documentation update that records exact image tag, commands, timestamps, and
  any newly observed operational caveats.

Until then, the P5 goal remains open.
