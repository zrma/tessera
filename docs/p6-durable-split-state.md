# P6 Durable Split State

Last reviewed: 2026-05-03

This document records the first P6 slice after P5 internal activation
completion. It is intentionally narrower than the full P6+ objective: it makes
published split assignments survive Orchestrator restart, while keeping
unapproved planner submission, internal MicroK8s restart evidence, and later
runtime expansion as follow-up gates. Later same-Worker/cross-Worker merge,
policy-gated planner mutation, and multi-depth local split slices now exist, but
they are not part of the durable split-state proof.

## Objective Slice

P6 first requires that a manual split activation published in runtime does not
disappear when the Orchestrator process restarts. The durable source of truth for
this slice is the published assignment map, not an in-flight replay operation.

Implemented behavior:

1. `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` is opt-in and default disabled.
2. When enabled, the Orchestrator loads that JSON state file on boot. If the file
   is missing, it falls back to `TESSERA_ORCH_CONFIG` or
   `TESSERA_ORCH_CONFIG_JSON` initial assignments.
3. Handover commit and split activation publish write the complete assignment
   map to the state file with an atomic temp-file replace before publishing the
   listing update.
4. If persistence fails, the in-memory assignment mutation is rolled back before
   `ListAssignments` or `WatchAssignments` can expose it.
5. The state file validates schema, configured Worker ids, duplicate Worker ids,
   and duplicate cell ownership before it is trusted.
6. In-flight `staged_splits` remain memory-only. A process restart clears
   incomplete staged operations instead of trying to resume partially replayed
   Worker state.

## State File

Example shape:

```json
{
  "schema": "tessera.orch.assignment_state.v1",
  "workers": [
    {
      "worker_id": "worker-a",
      "cells": [
        {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 0}
      ]
    }
  ]
}
```

The state file may contain dynamically published child cells that were not in
the static config. Worker ids must still exist in the current Orchestrator
config so runtime addresses and health/listing shape remain policy-controlled.

## Local Evidence

Unit coverage:

```sh
cargo test -p tessera-orch
```

Relevant checks:

- `persistent_assignment_state_recovers_published_split_after_restart` publishes
  a split, verifies the state file, creates a fresh Orchestrator with manual
  activation disabled, and verifies the restarted listing and Worker
  registration snapshot contain the four child assignments.
- `load_assignment_state_rejects_unknown_worker` verifies stale state cannot add
  an unconfigured Worker id.

Dev smoke:

```sh
cargo xt dev activation-restart-smoke
cargo xt dev activation-report-check \
  --restart-report .dev/reports/activation-restart-smoke-latest.json
```

The smoke publishes a manual split with
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH=.dev/reports/activation-restart-assignment-state.json`,
restarts the Orchestrator without
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, waits for both Workers to
re-register, restarts the Gateway against the recovered listing, and verifies:

- persisted child assignment listing loaded after restart
- Worker assignment refresh after restart
- Gateway `/ready` reports four child routes
- child Ping succeeds for all four routes
- post-restart child Move traffic observes remote AOI frames

The latest report is `.dev/reports/activation-restart-smoke-latest.json` and
uses `remaining_uncovered=["internal_microk8s_restart_recovery_smoke"]` until the
cluster evidence gate is run.

Live metrics planner-to-operator local evidence:

```sh
cargo xt dev activation-live-plan-smoke
cargo xt dev activation-live-metrics-smoke
cargo xt dev activation-report-check \
  --require-live-metrics-plan \
  --restart-report .dev/reports/activation-restart-smoke-latest.json
```

The first command proves live Worker metrics can create a mutation-free operator
plan. The second uses that plan's target map as the manual
`SubmitSplitActivation` input, then verifies Gateway route convergence,
stable-session post-split Move, and remote AOI resync. The report checker flag
asserts the latest plan report came from `live_worker_metrics:`.

Merge planner-to-operator local evidence:

```sh
cargo xt dev merge-plan-smoke
cargo xt dev merge-activation-smoke
cargo xt dev merge-activation-failure-smoke
cargo xt dev merge-activation-restart-smoke
cargo xt dev activation-report-check \
  --merge-plan-report .dev/reports/merge-activation-plan-latest.json \
  --merge-activation-report .dev/reports/merge-activation-smoke-latest.json \
  --merge-failure-report .dev/reports/merge-activation-failure-smoke-latest.json \
  --merge-restart-report .dev/reports/merge-activation-restart-smoke-latest.json
```

The plan command records `.dev/reports/merge-activation-plan-latest.json` with
a ready dry-run merge recommendation for a complete cold sibling family. It does
not mutate assignments; the report marks
`runtime_activation.state=manual_available` for the same-Worker merge slice.
`cargo xt dev merge-activation-smoke` is the local mutating proof: it submits
`SubmitMergeActivation`, verifies Worker child-to-parent coalescing, waits for
Gateway parent route convergence, and checks a stable-session parent Move. The
failure smoke injects a post-publish owner Worker outage, verifies parent route
failure is detected without automatic rollback, then restarts the owner Worker
and verifies parent route plus fresh Ping recovery. It records
`actor_state_recovery_policy.policy_id=volatile_worker_actor_state_rejoin_required_v1`:
Worker actor runtime state remains volatile in this slice, and owner Worker
restart recovery requires parent route convergence plus affected client
rejoin/reseed. The restart smoke repeats that merge with
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH`, restarts Orchestrator with manual
activation disabled, and verifies the persisted parent assignment, Gateway
parent route, parent Ping/Move, and Worker coalesced actor metrics still hold.
The report checker validates all four merge report shapes and the manual merge
backout policy. Cross-Worker merge replay and internal MicroK8s merge evidence
remain later gates.

## Internal MicroK8s Restart Gate Draft

The next gate is prepared but not yet completed. It requires a new image that
contains this P6 code and an approved GitOps rollout of the Orchestrator state
storage before any live mutation.

Storage policy draft:

- Policy id: `orchestrator_assignment_state_pvc_rwo_v1`
- Env: `TESSERA_ORCH_ASSIGNMENT_STATE_PATH`
- Path: `/var/lib/tessera/assignment-state.json`
- Volume: writable `persistentVolumeClaim`, not `emptyDir`
- GitOps claim name: `tessera-orch-state`
- Mount path: `/var/lib/tessera`

The companion k8s GitOps draft for this storage policy has passed both
client-side and server-side dry-run without applying it:

```sh
kubectl apply --dry-run=client -f k8s/apps/tessera/manifests
kubectl --context microk8s-ts apply --dry-run=server \
  -f k8s/apps/tessera/manifests/tessera-runtime.yaml
```

Prepared helper:

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

The read-only command above can be run before approval. It records whether the
live Orchestrator Deployment already has the PVC-backed state path. The current
post-P5 live state is expected to block because it still runs the previous image
and does not set the assignment-state env/volume.

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-activation \
  --with-restart \
  --allow-rollout-restart
cargo xt k8s activation-report-check \
  --require-published \
  --require-restart \
  --expected-image <new-tag>
```

For the full internal gate, combine the existing target outage recovery check
with restart recovery:

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

The helper refuses `--with-restart` unless `--allow-rollout-restart` is present
and preflights the live Deployment for the PVC-backed state path before it
publishes a split. The restart path then rolls the Orchestrator Deployment,
recreates service port-forwards, waits for Worker re-registration and the four
child listing, checks Gateway routes, pings all child routes, and runs a small
post-restart AOI resync loop.

## Internal Live Metrics Plan Gate

The live metrics planner-to-operator path is also prepared for internal
MicroK8s, but remains mutation-free by default:

```sh
cargo xt k8s activation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --require-target-worker \
  --use-live-worker-metrics \
  --live-min-pressure-signals 1
cargo xt k8s activation-report-check \
  --require-live-metrics-plan \
  --expected-image <new-tag>
```

This helper opens source/target Worker metrics port-forwards, scrapes per-cell
actor and pending-move gauges, and records
`plan.preview.source=live_worker_metrics:...` in the internal report. It should
be run only after the new image exposing those Worker metrics is rolled out.
For a combined restart/live-metrics evidence run, add `--use-live-worker-metrics`
to the restart command and add `--require-live-metrics-plan` to the verifier.

## Remaining P6 Gates

1. Publish a new image, roll out the PVC-backed Orchestrator state storage, and
   run the internal MicroK8s restart recovery evidence gate above.
2. Run the internal MicroK8s live Worker metrics planner-to-operator evidence
   gate above without enabling automatic mutation by default.
3. Run internal MicroK8s merge evidence for the same-Worker and cross-Worker
   local replay paths. Owner Worker restart actor state is currently excluded by
   `volatile_worker_actor_state_rejoin_required_v1`.
4. After a controlled topology assigns the canonical parent, run the guarded
   internal multi-depth publish, failure/recovery, restart, and load/soak
   helper with `--allow-activation`, `--allow-scale`, and
   `--allow-rollout-restart`.
5. Extend report verifiers so final P6 evidence covers local/dev and internal
   MicroK8s success, failure, restart recovery, load/soak, rollback/backout, and
   GitOps rollout state. Keep `docs/p6-completion-audit.md` updated as the
   checklist source of truth.
