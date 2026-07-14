# P6 Durable Split State

Last reviewed: 2026-07-14

This document records the first P6 slice after P5 internal activation
completion. It is intentionally narrower than the full P6+ objective: it makes
published split assignments survive Orchestrator restart. Later P6/P7 work added
planner submission, guarded Kubernetes restart evidence, same-Worker/cross-Worker
merge, policy-gated planner mutation, and multi-depth split/operation evidence,
but those are not part of this durable split-state proof.

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
5. The state file validates schema, Worker topology compatibility, duplicate
   Worker ids, and duplicate cell ownership before it is trusted.
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
the static config. Every Worker id that owns cells must still exist in the
current Orchestrator config so runtime addresses and health/listing shape remain
policy-controlled.

## Worker Topology Compatibility

Persisted assignment state is the authoritative assignment map after it exists;
static config is not merged into it implicitly. Restart compatibility is:

- A newly configured Worker is accepted only when its static `cells` list is
  empty. It enters the listing with no assignments and can receive later
  policy-gated assignment changes.
- A configured Worker missing from persisted state is rejected when its static
  `cells` list is non-empty. This avoids silently dropping or adopting a new
  static assignment while durable state is authoritative.
- A removed Worker entry is ignored only when its persisted `cells` list is
  empty. The Orchestrator emits a warning and does not retain that identity.
- A removed or otherwise unknown Worker that still owns persisted cells is a
  startup error. Cells are never silently reassigned to another identity.
- Existing schema, duplicate-Worker, and duplicate-cell checks remain
  fail-closed.

Therefore an environment may safely add empty Worker capacity or remove an
already drained Worker across restart. Adding preassigned capacity or removing
a Worker that still owns cells requires an explicit assignment-state migration
or a policy-gated runtime assignment change first.

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
- `load_assignment_state_rejects_unknown_worker` verifies stale state cannot
  retain cells under an unconfigured Worker id.
- `assignment_state_accepts_configured_empty_worker_addition` and
  `assignment_state_accepts_removed_empty_worker` cover compatible topology
  changes.
- `assignment_state_rejects_removed_worker_with_owned_cells` and
  `assignment_state_rejects_missing_configured_worker_with_static_cells` cover
  fail-closed incompatibilities.
- `persistent_assignment_state_applies_compatible_worker_changes_on_restart`
  verifies both compatible changes through fresh Orchestrator construction.

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
uses `remaining_uncovered=["guarded_kubernetes_restart_recovery_smoke"]` until the
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
backout policy. Cross-Worker merge replay and guarded Kubernetes merge evidence
remain later gates.

## Guarded Kubernetes Restart Gate Draft

The next gate is prepared but not yet completed. It requires a new image that
contains this P6 code and an approved deployment rollout of the Orchestrator state
storage before any live mutation.

Storage policy draft:

- Policy id: `orchestrator_assignment_state_pvc_rwo_v1`
- Env: `TESSERA_ORCH_ASSIGNMENT_STATE_PATH`
- Path: `/var/lib/tessera/assignment-state.json`
- Volume: writable `persistentVolumeClaim`, not `emptyDir`
- deployment claim name: `tessera-orch-state`
- Mount path: `/var/lib/tessera`

The private deployment storage policy has passed both
client-side and server-side dry-run without applying it:

```sh
kubectl apply --dry-run=client -f <private-gitops-application-manifests>
kubectl --context example-cluster apply --dry-run=server \
  -f <private-gitops-runtime-manifest>
```

Prepared helper:

```sh
cargo xt k8s activation-smoke \
  --context example-cluster \
  --namespace tessera \
  --require-target-worker \
  --require-assignment-state-storage \
  --out .dev/reports/guarded-kubernetes-restart-readiness-negative.json
cargo xt k8s activation-report-check \
  --report .dev/reports/guarded-kubernetes-restart-readiness-negative.json \
  --expect-preflight-error TESSERA_ORCH_ASSIGNMENT_STATE_PATH
```

The read-only command above can be run before approval. It records whether the
live Orchestrator Deployment already has the PVC-backed state path. The current
post-P5 live state is expected to block because it still runs the previous image
and does not set the assignment-state env/volume.

Latest read-only preflight against live `v2026.05.2` wrote
`.dev/reports/guarded-kubernetes-restart-readiness-preflight.json` with
`stage=blocked_before_plan`, `activation_mutated=false`,
`checks.assignment_state_storage_configured=false`, and the expected
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH` preflight error. The verifier accepted this
as negative pre-rollout evidence:

```sh
cargo xt k8s activation-report-check \
  --report .dev/reports/guarded-kubernetes-restart-readiness-preflight.json \
  --expect-preflight-error TESSERA_ORCH_ASSIGNMENT_STATE_PATH
```

```sh
cargo xt k8s activation-smoke \
  --context example-cluster \
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
  --context example-cluster \
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
Kubernetes, but remains mutation-free by default:

```sh
cargo xt k8s activation-smoke \
  --context example-cluster \
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

## Closure Status

The original P6 gates in this design note are closed as of the `v2026.05.3`
guarded Kubernetes rollout and post-smoke default-off cleanup. The current source
of truth for P6 completion is `docs/p6-completion-audit.md`, whose final gate is:

```sh
cargo xt p6-completion-audit --json
```

This document remains useful as the implementation note for durable assignment
state, restart recovery, live-metrics operator planning, and the PVC-backed
internal rollout shape. It is not the active TODO list; use `docs/todo-next.md`
for current open work.
