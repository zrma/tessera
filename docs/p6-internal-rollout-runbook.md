# P6 Internal Rollout Runbook

Last reviewed: 2026-05-03

This runbook is the approval packet for the remaining P6+ internal MicroK8s
evidence. Do not run the mutating commands here until the operator explicitly
approves the smoke window, image tag, GitOps rollout revision, and cleanup
revision.

The current machine gate is:

```sh
cargo xt p6-completion-audit --json
```

It currently fails with 14 missing gates:

- `p6_internal_restart_recovery`
- `p6_internal_live_metrics_plan`
- `p6_gitops_rollout_image`
- `p6_gitops_rollout_evidence`
- `p6_internal_merge_ready_plan`
- `p6_internal_merge_publish`
- `p6_internal_merge_failure_recovery`
- `p6_internal_merge_restart_recovery`
- `p6_internal_merge_load_soak`
- `p6_internal_multi_depth_ready_plan`
- `p6_internal_multi_depth_publish`
- `p6_internal_multi_depth_failure_recovery`
- `p6_internal_multi_depth_restart_recovery`
- `p6_internal_multi_depth_load_soak`

## Required Inputs

Set these values before the approved rollout:

```sh
export TESSERA_P6_IMAGE="harbor.1day1coding.com/1day1coding/tessera:<new-tag>"
export TESSERA_P6_ROLLOUT_REV="<gitops-rollout-revision>"
export TESSERA_P6_CLEANUP_REV="<gitops-cleanup-revision>"
```

The image must not be `v2026.05.2`; that tag is the P5 internal split baseline.
The rollout revision must apply the P6 image and runtime settings needed for
the controlled smoke. The cleanup revision must return mutating flags and
fixtures to default-off after the smoke.

## Image Publish

The Tessera image is published by the manual GitHub Actions workflow
`.github/workflows/tessera.build-push.yml` (`build and push image`). The
workflow has no custom dispatch inputs: it checks the selected commit, creates
or reuses the latest `vYYYY.MM.N` tag for that commit, builds `linux/amd64`,
and pushes:

```text
harbor.1day1coding.com/1day1coding/tessera:<tag>
```

After the workflow finishes, record the final Docker tag printed by the
`Set final Docker tag` step and set `TESSERA_P6_IMAGE` to that tag. The tag
must then be promoted in the k8s GitOps repo before the internal smoke commands
below can count as P6 evidence.

Recommended post-publish checks:

```sh
docker buildx imagetools inspect "$TESSERA_P6_IMAGE"
cargo xt p6-completion-audit --json
```

The audit should still fail after image publish alone; it must not pass until
GitOps rollout, controlled smoke, cleanup, and rollout report evidence all
exist.

## Approval Boundary

Read-only commands may be run before approval:

- `cargo xt p6-completion-audit --json`
- `cargo xt k8s planner-activation-report`
- `cargo xt k8s planner-activation-report-check`
- `cargo xt k8s activation-smoke` without `--allow-activation`
- `cargo xt k8s merge-activation-smoke` without `--allow-activation`
- `cargo xt k8s multi-depth-activation-smoke` without `--allow-activation`
- `cargo xt p6-rollout-report` without operator assertion flags

These commands require explicit smoke-window approval:

- any `jj git push` or remote bookmark movement
- k8s GitOps rollout or cleanup push
- `cargo xt k8s activation-smoke --allow-activation`
- `cargo xt k8s activation-smoke --with-failure --allow-scale`
- `cargo xt k8s activation-smoke --with-restart --allow-rollout-restart`
- `cargo xt k8s merge-activation-smoke --allow-activation`
- `cargo xt k8s merge-activation-smoke --with-failure --allow-scale`
- `cargo xt k8s merge-activation-smoke --with-restart --allow-rollout-restart`
- `cargo xt k8s multi-depth-activation-smoke --allow-activation`
- `cargo xt k8s multi-depth-activation-smoke --with-failure --allow-scale`
- `cargo xt k8s multi-depth-activation-smoke --with-restart --allow-rollout-restart`
- `cargo xt p6-rollout-report --image-published --gitops-rollout-approved --post-smoke-default-off-cleanup --manual-activation-default-off --preview-fixture-removed`

## Read-Only Preflight

Run these before opening the smoke window:

```sh
cargo xt
cargo test
cargo xt p6-completion-audit --json

cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --require-target-worker \
  --require-assignment-state-storage \
  --out .dev/reports/internal-microk8s-restart-readiness-preflight.json

cargo xt k8s activation-report-check \
  --report .dev/reports/internal-microk8s-restart-readiness-preflight.json \
  --expect-preflight-error TESSERA_ORCH_ASSIGNMENT_STATE_PATH
```

After the GitOps rollout is synced to the new image and PVC-backed state path,
the restart preflight should stop producing the
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH` error.

The companion k8s GitOps repo currently has a local P6 storage draft change
that adds `TESSERA_ORCH_ASSIGNMENT_STATE_PATH`, mounts `/var/lib/tessera`, and
creates PVC `tessera-orch-state`. It has been checked without applying it:

```sh
kubectl apply --dry-run=client -f k8s/apps/tessera/manifests
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

Both dry-runs accepted the runtime manifest, including the new
`persistentvolumeclaim/tessera-orch-state` object.

The planner policy report is read-only and should remain green:

```sh
cargo xt k8s planner-activation-report \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE"

cargo xt k8s planner-activation-report-check \
  --expected-image "$TESSERA_P6_IMAGE"
```

## Controlled Smoke Commands

### Split Restart And Live Metrics

Run the split restart gate after the P6 image and PVC-backed state storage are
live:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE" \
  --require-target-worker \
  --require-assignment-state-storage \
  --use-live-worker-metrics \
  --live-min-pressure-signals 1 \
  --allow-activation \
  --with-failure \
  --allow-scale \
  --with-restart \
  --allow-rollout-restart

cargo xt k8s activation-report-check \
  --require-published \
  --require-failure \
  --require-restart \
  --require-live-metrics-plan \
  --expected-image "$TESSERA_P6_IMAGE"
```

This should close:

- `p6_internal_restart_recovery`
- `p6_internal_live_metrics_plan`

### Merge

First confirm a ready merge plan:

```sh
cargo xt k8s merge-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE"

cargo xt k8s merge-activation-report-check \
  --require-ready-plan \
  --expected-image "$TESSERA_P6_IMAGE"
```

During the approved smoke window, publish and verify recovery:

```sh
cargo xt k8s merge-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE" \
  --allow-activation \
  --with-failure \
  --allow-scale \
  --with-restart \
  --allow-rollout-restart \
  --with-soak

cargo xt k8s merge-activation-report-check \
  --require-ready-plan \
  --require-published \
  --require-failure \
  --require-restart \
  --require-soak \
  --expected-image "$TESSERA_P6_IMAGE"
```

This should close:

- `p6_internal_merge_ready_plan`
- `p6_internal_merge_publish`
- `p6_internal_merge_failure_recovery`
- `p6_internal_merge_restart_recovery`
- `p6_internal_merge_load_soak`

### Canonical Multi-Depth Split

First confirm a ready canonical parent and child target map:

```sh
cargo xt k8s multi-depth-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE"

cargo xt k8s multi-depth-activation-report-check \
  --require-ready-plan \
  --expected-image "$TESSERA_P6_IMAGE"
```

During the approved smoke window, publish and verify recovery:

```sh
cargo xt k8s multi-depth-activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image "$TESSERA_P6_IMAGE" \
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
  --expected-image "$TESSERA_P6_IMAGE"
```

This should close:

- `p6_internal_multi_depth_ready_plan`
- `p6_internal_multi_depth_publish`
- `p6_internal_multi_depth_failure_recovery`
- `p6_internal_multi_depth_restart_recovery`
- `p6_internal_multi_depth_load_soak`

## GitOps Rollout Evidence

After the smoke and default-off cleanup revision are both applied and ArgoCD is
`Synced / Healthy`, write the rollout evidence:

```sh
cargo xt p6-rollout-report \
  --context microk8s-ts \
  --namespace tessera \
  --image "$TESSERA_P6_IMAGE" \
  --rollout-revision "$TESSERA_P6_ROLLOUT_REV" \
  --cleanup-revision "$TESSERA_P6_CLEANUP_REV" \
  --image-published \
  --gitops-rollout-approved \
  --post-smoke-default-off-cleanup \
  --manual-activation-default-off \
  --preview-fixture-removed

cargo xt p6-rollout-report-check \
  --expected-image "$TESSERA_P6_IMAGE"
```

This should close:

- `p6_gitops_rollout_image`
- `p6_gitops_rollout_evidence`

## Final Audit

The rollout is complete only when this passes:

```sh
cargo xt p6-completion-audit --json
```

The final report must have `complete=true` and no missing gates. If it fails,
the failure output is the next work queue; do not treat partial smoke success as
P6 completion.
