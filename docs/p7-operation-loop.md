# P7 Operation Loop

Last reviewed: 2026-05-04

## Objective

P7 turns the P6 durable/manual/default-off split, merge, and multi-depth control
plane into an operator-facing closed loop:

```text
planner proposal -> approval -> execution -> observation -> recovery/backout -> audit report
```

The goal is not unguarded autoscaling. The goal is a durable, policy-gated,
observable operation system where planner output can be reviewed, approved,
executed, recovered, and audited repeatedly.

## Completion Criteria

P7 is complete only when all of these are true:

1. Live Worker metrics and current assignment state can produce split and merge
   operation proposals without mutating assignments.
2. Every proposal is written to a durable operation record with stable operation
   id, proposal source, target cells, worker roles, preconditions, and
   submission template.
3. Operator approval is explicit and durable. Approval records include policy
   id, approver, timestamp, TTL, expected proposal hash, cooldown budget, and
   allowed mutation kind.
4. The executor is default-off unless a policy gate is enabled. It executes only
   approved, unexpired, idempotent operations that pass cooldown and budget
   checks.
5. Split, merge, and canonical multi-depth operations record execution phases:
   preflight, replay/prepare, publish, observation, failure detection,
   recovery/backout, and final audit.
6. Failure handling remains operator-visible. P7 does not introduce automatic
   rollback unless a later policy explicitly adds it with separate evidence.
7. Gateway route convergence, Worker assignment refresh, AOI resync,
   latency/close counters, and operation state transitions are recorded in
   reports and validated by machine checkers.
8. Local/dev smoke covers proposal, blocked execution, approved execution,
   failure/recovery, restart recovery, and soak.
9. Internal MicroK8s smoke covers the same runtime-affecting paths after image
   publish and GitOps rollout.
10. Post-smoke cleanup restores mutating flags to default-off and leaves ArgoCD
    `Synced / Healthy`.
11. A P7 completion audit maps every requirement above to concrete artifacts
    and fails until all required evidence exists.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Planner proposal records | operation record schema and writer | report checker proves `activation_mutated=false` proposal evidence from live metrics and assignment listing |
| Durable operation state | file-backed operation ledger or equivalent repo-local persistence | restart test proves proposals and approvals survive Orchestrator restart |
| Explicit approval | CLI/API for approval records | tests reject missing, expired, mismatched, or wrong-policy approvals |
| Policy/cooldown/budget gate | executor preflight validator | local smoke records blocked-by-policy and approved execution reports |
| Idempotent execution | stable operation id and replay-safe phase transitions | duplicate execution test/report proves no double publish |
| Failure and recovery | operation phase updates for target/owner outage | smoke report records failure detection, no automatic rollback, operator recovery, and final convergence |
| Observation | metrics/report fields for route count, AOI frames, latency, close counters | verifier rejects missing route convergence, Worker refresh, AOI, or counter evidence |
| Internal rollout | k8s GitOps smoke window runbook | image publish, GitOps rollout rev, cleanup rev, ArgoCD `Synced / Healthy`, default-off cleanup report |
| Completion audit | `cargo xt p7-completion-audit --json` | audit returns `complete=true` only after every P7 operation-loop gate is backed by real evidence |

Current completion audit:

```sh
cargo xt p7-completion-audit --json
```

As of the `v2026.05.5` internal operation-loop evidence set, this returns
`complete=true` with an empty `findings` list. It aggregates the local
default-off/execution/observation/recovery/restart/soak reports, the GitOps
rollout/default-off cleanup report, and the three internal MicroK8s operation
reports for execution+soak, failure/recovery, and restart recovery.

Current ledger verifier:

```sh
cargo xt p7-operation-ledger-check \
  --ledger .dev/operation-ledger.json \
  --require-approval \
  --require-blocked-execution
```

This validates the first proposal -> approval -> default-off blocked execution
artifact. It is not a replacement for runtime execution, observation,
recovery, internal rollout, or final P7 completion audit evidence.
Approved execution ledgers use the same checker with
`--require-published-execution`; completed observation ledgers add
`--require-completed-observation`; recovery-required ledgers use
`--require-recovery-required`.

Current local closed-loop smoke:

```sh
cargo xt dev p7-operation-loop-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-loop-ledger-latest.json \
  --require-approval \
  --require-blocked-execution
```

This starts Orchestrator-only dev stacks with registered two-Worker listings and
split, merge, and canonical multi-depth preview candidates, records proposal ->
approval -> default-off execution block for each path, and writes
`.dev/reports/p7-operation-loop-smoke-latest.json` plus
`.dev/reports/p7-operation-loop-ledger-latest.json`. It still covers only
default-off execution.

Current approved execution smoke:

```sh
cargo xt dev p7-operation-execution-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-execution-ledger-latest.json \
  --require-approval \
  --require-published-execution
```

This starts an Orchestrator-only dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, records proposal -> approval ->
published execution for an approved same-Worker merge operation, verifies that a
repeat execution returns `already_published` without another mutation, and
writes `.dev/reports/p7-operation-execution-smoke-latest.json` plus
`.dev/reports/p7-operation-execution-ledger-latest.json`. Legacy split and
canonical multi-depth execution expansion are covered by the follow-up smokes
below.

Current split execution expansion smoke:

```sh
cargo xt dev p7-operation-split-execution-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-split-execution-ledger-latest.json \
  --require-approval \
  --require-published-execution
```

This starts a full local dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, records proposal -> approval ->
published execution for an approved legacy split operation, verifies that the
parent assignment is removed and four child assignments are published, then
verifies repeat execution returns `already_published` without another mutation.
It writes `.dev/reports/p7-operation-split-execution-smoke-latest.json` plus
`.dev/reports/p7-operation-split-execution-ledger-latest.json`. Split
failure/recovery, restart, soak, and internal MicroK8s evidence remain explicit
P7+ follow-up gates.

Current canonical multi-depth execution expansion smoke:

```sh
cargo xt dev p7-operation-multi-depth-execution-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-multi-depth-execution-ledger-latest.json \
  --require-approval \
  --require-published-execution
```

This starts a full local dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, records proposal -> approval ->
published execution for an approved canonical multi-depth split operation, and
verifies that the canonical parent assignment is removed while the four
explicit canonical child assignments are published. It also proves Gateway
child routes converge, a stable parent session can move through a post-split
child route, remote AOI resync is observed, and repeat execution returns
`already_published` without another mutation. It writes
`.dev/reports/p7-operation-multi-depth-execution-smoke-latest.json` plus
`.dev/reports/p7-operation-multi-depth-execution-ledger-latest.json`.
Canonical multi-depth observation, recovery, restart, and soak are covered
below; internal MicroK8s evidence remains an explicit P7+ follow-up gate.

Current canonical multi-depth observation expansion smoke:

```sh
cargo xt dev p7-operation-multi-depth-observation-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-multi-depth-observation-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, publishes an approved canonical
multi-depth split operation through the P7 executor, verifies Gateway canonical
child route convergence, Worker child actor refresh, stable-session child Move,
remote AOI resync, traffic metrics, and clean close counters, then records
`POST /operations/observations` as `status=completed`. It writes
`.dev/reports/p7-operation-multi-depth-observation-smoke-latest.json` plus
`.dev/reports/p7-operation-multi-depth-observation-ledger-latest.json`.
Canonical multi-depth recovery and restart are covered below; soak and internal
MicroK8s evidence remain explicit P7+ follow-up gates.

Current canonical multi-depth recovery expansion smoke:

```sh
cargo xt dev p7-operation-multi-depth-recovery-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-multi-depth-recovery-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-recovery-required
```

This starts a full local dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, publishes an approved canonical
multi-depth split operation, terminates the target Worker, verifies only the
target-owned canonical child routes fail, records `POST /operations/observations`
as `status=recovery_required`, then restarts the target Worker and verifies
canonical child route traffic recovers without automatic rollback. It writes
`.dev/reports/p7-operation-multi-depth-recovery-smoke-latest.json` plus
`.dev/reports/p7-operation-multi-depth-recovery-ledger-latest.json`.
Canonical multi-depth restart is covered below; soak and internal MicroK8s
evidence remain explicit P7+ follow-up gates.

Current canonical multi-depth restart expansion smoke:

```sh
cargo xt dev p7-operation-multi-depth-restart-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-multi-depth-restart-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack with both the operation ledger and assignment
state path enabled, publishes an approved canonical multi-depth split operation,
restarts the Orchestrator, verifies the persisted canonical child assignment map
and operation ledger survive, confirms Gateway canonical child route traffic,
stable-session child Move, Worker child refresh, and remote AOI resync after
restart, then records `POST /operations/observations` as `status=completed`. It
writes `.dev/reports/p7-operation-multi-depth-restart-smoke-latest.json` plus
`.dev/reports/p7-operation-multi-depth-restart-ledger-latest.json`.
Canonical multi-depth soak is covered below; internal MicroK8s evidence remains
an explicit P7+ follow-up gate.

Current canonical multi-depth soak expansion smoke:

```sh
cargo xt dev p7-operation-multi-depth-soak-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-multi-depth-soak-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack, publishes an approved canonical multi-depth
split operation through the P7 executor, verifies all canonical child routes
converge, then runs sustained Ping/Move traffic against all four canonical child
routes. The smoke checks Gateway route retention, latency histogram growth,
clean close counters, Worker child actor metrics, and remote AOI frames before
`POST /operations/observations` closes the operation to `completed`. It writes
`.dev/reports/p7-operation-multi-depth-soak-smoke-latest.json` plus
`.dev/reports/p7-operation-multi-depth-soak-ledger-latest.json`. Internal
MicroK8s evidence remains an explicit P7+ follow-up gate.

Current split observation expansion smoke:

```sh
cargo xt dev p7-operation-split-observation-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-split-observation-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, joins actors on the parent cell,
publishes an approved legacy split operation through the P7 executor, proves
Gateway child routes converge, Worker child actors refresh, child Ping traffic
and stable-session child Move succeed, and close counters stay clean. It then
calls `POST /operations/observations` and verifies the ledger reaches
`status=completed`. The smoke writes
`.dev/reports/p7-operation-split-observation-smoke-latest.json` plus
`.dev/reports/p7-operation-split-observation-ledger-latest.json`. Split
recovery, restart, and soak are covered below; internal MicroK8s evidence
remains an explicit P7+ follow-up gate.

Current split recovery expansion smoke:

```sh
cargo xt dev p7-operation-split-recovery-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-split-recovery-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-recovery-required
```

This starts a full local dev stack, publishes an approved legacy split
operation through the P7 executor, verifies all child routes succeed, then
terminates the target Worker that owns child subs 1 and 3. The smoke records
target child Ping failures while the published child assignment map remains in
place, calls `POST /operations/observations` with failed traffic/counter
evidence, and verifies the ledger reaches `status=recovery_required`. It then
restarts the target Worker and proves all child routes recover without automatic
rollback. The smoke writes
`.dev/reports/p7-operation-split-recovery-smoke-latest.json` plus
`.dev/reports/p7-operation-split-recovery-ledger-latest.json`. Split restart
and soak are covered below; internal MicroK8s evidence remains an explicit P7+
follow-up gate.

Current split restart expansion smoke:

```sh
cargo xt dev p7-operation-split-restart-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-split-restart-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack with both
`TESSERA_ORCH_OPERATION_LEDGER_PATH` and
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH`, publishes an approved legacy split
operation through the P7 executor, verifies all child routes succeed, restarts
the Orchestrator, then proves the operation record remains in the published
execution state and the child assignment map is restored from durable assignment
state. Gateway child routes, Worker child actor metrics, stable-session child
traffic, and clean close counters are verified after restart before
`POST /operations/observations` closes the operation to `completed`. The smoke
writes `.dev/reports/p7-operation-split-restart-smoke-latest.json` plus
`.dev/reports/p7-operation-split-restart-ledger-latest.json`. The follow-up
split soak smoke covers sustained child-route traffic; internal MicroK8s
evidence remains an explicit P7+ follow-up gate.

Current split soak expansion smoke:

```sh
cargo xt dev p7-operation-split-soak-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-split-soak-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack, publishes an approved legacy split operation
through the P7 executor, verifies all child routes converge, then runs sustained
Ping/Move traffic against all four child routes. The smoke checks Gateway route
retention, latency histogram growth, clean close counters, Worker child actor
metrics, and remote AOI frames before `POST /operations/observations` closes the
operation to `completed`. It writes
`.dev/reports/p7-operation-split-soak-smoke-latest.json` plus
`.dev/reports/p7-operation-split-soak-ledger-latest.json`. Canonical
multi-depth operation execution, observation, recovery, restart, and soak are
covered above; internal MicroK8s evidence remains an explicit P7+ follow-up
gate.

Current observation smoke:

```sh
cargo xt dev p7-operation-observation-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-observation-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack, records proposal -> approval -> published
execution through the P7 HTTP endpoints, then proves Gateway route convergence,
Worker parent actor refresh, stable-session parent traffic, latency metrics, and
clean close counters before calling `POST /operations/observations`. The ledger
ends with `status=completed` and a succeeded `observation_completed` phase in
`.dev/reports/p7-operation-observation-ledger-latest.json`; the smoke report is
`.dev/reports/p7-operation-observation-smoke-latest.json`. Recovery, restart,
and soak are covered below; internal MicroK8s observation evidence remains an
explicit follow-up gate.

Current recovery smoke:

```sh
cargo xt dev p7-operation-recovery-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-recovery-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-recovery-required
```

This starts a full local dev stack, publishes an approved same-Worker merge
operation through the P7 HTTP endpoints, then terminates the owner Worker after
publish. Gateway parent Ping failure is recorded as the failure signal, the
operation observation is written with missing traffic/counter evidence, and the
ledger ends with `status=recovery_required` plus a failed
`observation_failed` phase. The smoke then restarts the owner Worker and proves
the parent route and fresh parent Ping recover without automatic rollback.
Restart and soak are covered below; internal MicroK8s recovery evidence remains
an explicit follow-up gate.

Current restart smoke:

```sh
cargo xt dev p7-operation-restart-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-restart-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack with both
`TESSERA_ORCH_OPERATION_LEDGER_PATH` and
`TESSERA_ORCH_ASSIGNMENT_STATE_PATH`, publishes an approved same-Worker merge
operation through the P7 HTTP endpoints, restarts the Orchestrator, and then
verifies that the operation record remains in the published execution state,
the parent assignment is restored from durable assignment state, Gateway routes
converge to the parent, Worker parent actor metrics refresh, stable-session
parent traffic succeeds, and `POST /operations/observations` can close the
operation to `completed` after restart. The smoke writes
`.dev/reports/p7-operation-restart-smoke-latest.json` plus
`.dev/reports/p7-operation-restart-ledger-latest.json`. Internal MicroK8s
restart evidence remains an explicit follow-up gate.

Current soak smoke:

```sh
cargo xt dev p7-operation-soak-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-soak-ledger-latest.json \
  --require-approval \
  --require-published-execution \
  --require-completed-observation
```

This starts a full local dev stack, publishes an approved same-Worker merge
operation through the P7 HTTP endpoints, runs sustained parent-route Ping/Move
traffic with `--iterations` and `--sleep-ms`, verifies Gateway route count,
latency histograms, clean close counters, and Worker parent actor metrics, then
closes the operation with `POST /operations/observations`. The smoke writes
`.dev/reports/p7-operation-soak-smoke-latest.json` plus
`.dev/reports/p7-operation-soak-ledger-latest.json`. Internal MicroK8s soak
evidence remains an explicit follow-up gate.

Current internal rollout baseline:

```sh
cargo xt p6-rollout-report \
  --context microk8s-ts \
  --namespace tessera \
  --image harbor.1day1coding.com/1day1coding/tessera:v2026.05.5 \
  --rollout-revision 2c01847475ecbc65f43e9a5979449422f3ed2b4f \
  --cleanup-revision 2c01847475ecbc65f43e9a5979449422f3ed2b4f \
  --image-published \
  --gitops-rollout-approved \
  --post-smoke-default-off-cleanup \
  --manual-activation-default-off \
  --preview-fixture-removed
cargo xt p6-rollout-report-check \
  --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.5
```

The `v2026.05.5` rollout carries the P7 ledger/executor/observation code. The
initial default-off rollout enabled operation ledger persistence on the live
Orchestrator while leaving `TESSERA_ORCH_OPERATION_EXECUTION` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION` unset. ArgoCD `tessera` reached
`Synced / Healthy` at GitOps revision
`2c01847475ecbc65f43e9a5979449422f3ed2b4f`, all four Tessera deployments ran
`harbor.1day1coding.com/1day1coding/tessera:v2026.05.5`, Gateway port-forward
Ping returned `Pong { ts: 123 }`, `GET /operations` reported
`persistence_enabled=true`, and `POST /operations/proposals` returned zero plans
without mutation for the default topology.

On 2026-05-04 KST, the internal controlled window opened via k8s GitOps
revisions `0d2cc86aa29be5220d14f49936c4e8e95c5b19dd` and
`2a172c6ba6c7acb3ed15563b077c7531cd2301e1`: worker-a owned the four shallow
merge siblings, the Orchestrator used fresh assignment-state and operation-ledger
paths, and manual operation execution plus split/merge activation were enabled
with a cold-sibling preview fixture. `cargo xt k8s operation-smoke
--allow-execution --with-soak` completed operation
`p7-merge-w0-cx0-cy0-d0-s0-1586936fe8a4`, and
`cargo xt k8s operation-report-check --require-published-execution
--require-completed-observation --require-soak` validated the report at
`.dev/reports/internal-microk8s-p7-operation-smoke-latest.json`. The report
records approved merge execution, parent-route convergence, Worker parent actor
refresh, clean Gateway close counters, 64 successful soak pings, 64 successful
soak moves, and completed observation.

Cleanup revision `b5ae307da1309633f9b5f15df0ebb807ec364c2d` removed the manual
execution flag, split/merge activation flag, and preview fixture while keeping
the fresh state/ledger paths. After cleanup, ArgoCD returned to
`Synced / Healthy`, Gateway parent Ping still returned `Pong { ts: 123 }`,
`GET /operations` retained the completed operation record, and
`GET /split-merge/preview` reported `assignment_listing_zero_metrics` with zero
plans.

The same day, failure/recovery was recorded in a second controlled window. k8s
revision `4db4487efda3a50c0eef1739a5bdcaccc85d7d85` reopened the four-sibling
topology with fresh failure state/ledger paths and manual mutation flags.
`cargo xt k8s operation-smoke --allow-execution --with-failure --allow-scale`
completed operation `p7-merge-w0-cx0-cy0-d0-s0-1586936fe8a4`, and
`cargo xt k8s operation-report-check --require-published-execution
--require-recovery-required` validated
`.dev/reports/internal-microk8s-p7-operation-failure-smoke-latest.json`. The
report records approved execution, owner Worker scale-down detection,
`status=recovery_required`, one recovery-required ledger record, operator
scale-up recovery, and no automatic rollback observation.

Cleanup revision `b0307901cb037492a8dbb57084170a1c56c8bd6e` removed the manual
execution flag, split/merge activation flag, and preview fixture again while
keeping the failure state/ledger paths. After cleanup, ArgoCD returned to
`Synced / Healthy`, live Orchestrator env contained only the non-mutating
config/state/ledger paths, Gateway parent Ping returned `Pong { ts: 123 }`,
`GET /operations` retained one `recovery_required` record, and
`GET /split-merge/preview` again reported `assignment_listing_zero_metrics` with
zero plans.

The restart gate then ran in a third controlled window. k8s revision
`085cd7715eeaae01e898225e2a07ded85e31ce7f` reopened the four-sibling topology
with restart-specific PVC-backed assignment state and operation ledger paths,
manual mutation flags, and the cold-sibling preview fixture. `cargo xt k8s
operation-smoke --allow-execution --with-restart --allow-rollout-restart
--expected-assignment-state-path
/var/lib/tessera/assignment-state-p7-operation-restart-20260504.json` completed
operation `p7-merge-w0-cx0-cy0-d0-s0-1586936fe8a4`, rollout-restarted
`deploy/tessera-orch`, and wrote
`.dev/reports/internal-microk8s-p7-operation-restart-smoke-latest.json`.
`cargo xt k8s operation-report-check --require-published-execution
--require-restart` validated that report against
`harbor.1day1coding.com/1day1coding/tessera:v2026.05.5`. The report records
PVC-backed assignment-state storage, approved execution, persisted parent route
and operation ledger recovery after Orchestrator restart, parent traffic, and a
completed post-restart observation.

Cleanup revision `50100c54363b22d759e61bfb1935af5bbe10327c` removed the manual
execution flag, split/merge activation flag, and preview fixture while keeping
the restart state/ledger paths. After cleanup, ArgoCD returned to `Synced /
Healthy`, all Tessera deployments stayed on
`harbor.1day1coding.com/1day1coding/tessera:v2026.05.5`, the live Orchestrator
env contained only non-mutating config/state/ledger paths, the ConfigMap
returned to the root-cell default topology, Gateway parent Ping returned
`Pong { ts: 123 }`, `GET /operations` retained one `completed` restart record,
and `GET /split-merge/preview` again reported
`assignment_listing_zero_metrics` with zero plans. Internal completion-audit
evidence remains the follow-up gate.

Current internal operation helper:

```sh
cargo xt k8s operation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag>

cargo xt k8s operation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-execution \
  --with-soak

cargo xt k8s operation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-execution \
  --with-failure \
  --allow-scale

cargo xt k8s operation-smoke \
  --context microk8s-ts \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-execution \
  --with-restart \
  --allow-rollout-restart \
  --expected-assignment-state-path /var/lib/tessera/assignment-state-p7-operation-restart-20260504.json

cargo xt k8s operation-report-check \
  --expected-image <new-tag> \
  --require-published-execution \
  --require-completed-observation \
  --require-soak

cargo xt k8s operation-report-check \
  --expected-image <new-tag> \
  --require-published-execution \
  --require-recovery-required

cargo xt k8s operation-report-check \
  --expected-image <new-tag> \
  --require-published-execution \
  --require-restart
```

The default helper is read-only: it port-forwards the Orchestrator operation
endpoint, records a P7 merge operation proposal when the live controlled-smoke
topology exposes one, and writes
`.dev/reports/internal-microk8s-p7-operation-smoke-latest.json` with
ArgoCD/image/ledger evidence. The approved helper additionally port-forwards
Gateway and owner Worker metrics, writes approval and execution records,
verifies parent-route convergence, Worker parent refresh, traffic and close
counters, optionally runs parent-route soak, and closes the operation via
`POST /operations/observations`. The failure helper is a separate evidence gate:
after approved publish and pre-failure parent traffic, it scales the owner
Worker deployment to zero, records parent Ping failure and a
`recovery_required` observation, scales the deployment back to its original
replica count, and requires fresh parent Ping recovery without automatic
rollback. The restart helper is also separate: it preflights the configured
assignment-state PVC path, publishes the operation, rollout-restarts the
Orchestrator deployment, verifies that persisted parent assignment and operation
ledger state reload, then closes the operation with a completed observation.

Because P7 operation ids are deterministic from proposal content, the approved
internal smoke window should use a fresh operation ledger path in the GitOps
manifest, alongside the manual execution and split/merge activation flags.
After the smoke, cleanup must remove mutating flags and preview fixtures again.

## Slice Cadence

Each slice should be self-contained:

1. Inspect current docs/code/reports before editing.
2. Implement the smallest useful artifact.
3. Run `cargo xt` and relevant tests.
4. Run local/dev smoke when runtime behavior changes.
5. Commit with `<type>: <summary>` and the Codex co-author trailer.
6. Push the relevant bookmark.
7. For runtime-affecting slices, publish an image, promote through GitOps, wait
   for ArgoCD `Synced / Healthy`, run internal smoke, write verifier reports,
   and clean up default-off state.

## Initial Implementation Order

1. **Operation record model**: define durable proposal and execution records
   without changing runtime behavior. The initial opt-in surface is
   `TESSERA_ORCH_OPERATION_LEDGER_PATH` plus the read-only Orchestrator
   `GET /operations` endpoint.
2. **Ledger append/update path**: let Orchestrator-owned proposal and phase
   writes persist durable records across restart, using the repo's existing
   conservative persistence style before introducing heavier storage. The
   first write surface is `POST /operations/proposals`, which appends planner
   proposals only and leaves assignment mutation disabled.
3. **Proposal writer**: convert live metrics split candidates and assignment
   listing merge candidates into durable proposals with stable operation ids,
   proposal hashes, target cells, worker roles, preconditions, and submission
   commands.
4. **Approval gate**: add explicit approval records with TTL, policy id,
   expected proposal hash, cooldown, budget constraints, and allowed mutation
   kind. The first write surface is `POST /operations/approvals?...`; it
   approves an existing proposal only when the supplied proposal hash still
   matches and still leaves execution mutation disabled.
5. **Executor dry run and first manual execution**: report blocked execution by
   default and approved execution only under explicit policy. The first surface is
   `POST /operations/executions?...`, which evaluates proposal hash,
   approval, policy, TTL, cooldown, and budget metadata. Default-off execution
   records a durable `blocked_by_policy` phase/status. A controlled execution
   window additionally requires `TESSERA_ORCH_OPERATION_EXECUTION=manual` and
   `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`; the first runtime mutation path
   is approved same-Worker merge publish with idempotent repeat execution. The
   P7+ expansion now also covers approved legacy split and canonical
   multi-depth split publish/idempotent repeat execution locally.
6. **Closed-loop smoke**: verify proposal-to-approval-to-execution locally for
   split, merge, and canonical multi-depth paths. The first repo-native smoke is
   `cargo xt dev p7-operation-loop-smoke`, which covers split/merge/canonical
   multi-depth proposal -> approval -> default-off execution block without
   assignment mutation.
7. **Approved merge execution smoke**: verify that an approved same-Worker merge
   operation can publish once through the P7 executor and that duplicate execute
   calls are idempotent. The first repo-native smoke is
   `cargo xt dev p7-operation-execution-smoke`.
8. **Approved split execution smoke**: verify that an approved legacy split
   operation can publish child assignments once through the P7 executor and that
   duplicate execute calls are idempotent. The first repo-native smoke is
   `cargo xt dev p7-operation-split-execution-smoke`.
9. **Approved canonical multi-depth execution smoke**: verify that an approved
   canonical multi-depth split operation can publish explicit child assignments
   once through the P7 executor, remove the canonical parent assignment, converge
   Gateway child routes, preserve stable-session traffic, and keep duplicate
   execute calls idempotent. The first repo-native smoke is
   `cargo xt dev p7-operation-multi-depth-execution-smoke`.
10. **Approved canonical multi-depth observation smoke**: verify that an
   approved canonical multi-depth split operation closes only after child route
   convergence, Worker child refresh, stable-session child traffic, remote AOI
   resync, latency metrics, and clean close counters are recorded. The first
   repo-native smoke is
   `cargo xt dev p7-operation-multi-depth-observation-smoke`.
11. **Approved canonical multi-depth recovery smoke**: verify that post-publish
   target outage records `recovery_required`, avoids automatic rollback, and
   recovers only after operator-visible Worker restart. The first repo-native
   smoke is `cargo xt dev p7-operation-multi-depth-recovery-smoke`.
12. **Approved canonical multi-depth restart smoke**: verify that canonical
   multi-depth operation ledger state and persisted child assignments survive
   Orchestrator restart and can still be closed with completed observation
   evidence. The first repo-native smoke is
   `cargo xt dev p7-operation-multi-depth-restart-smoke`.
13. **Approved canonical multi-depth soak smoke**: verify that approved
   canonical child routes stay converged under sustained Ping/Move traffic,
   Worker child actors remain refreshed, remote AOI frames are observed, and the
   operation closes with completed observation evidence. The first repo-native
   smoke is `cargo xt dev p7-operation-multi-depth-soak-smoke`.
14. **Approved split observation smoke**: verify that an approved legacy split
   operation closes only after child route convergence, Worker child refresh,
   stable-session child traffic, latency metrics, and clean close counters are
   recorded. The first repo-native smoke is
   `cargo xt dev p7-operation-split-observation-smoke`.
15. **Approved split recovery smoke**: verify that post-publish target outage
   records `recovery_required`, avoids automatic rollback, and recovers only
   after operator-visible Worker restart. The first repo-native smoke is
   `cargo xt dev p7-operation-split-recovery-smoke`.
16. **Approved split restart smoke**: verify that split operation ledger state
   and persisted child assignments survive Orchestrator restart and can still be
   closed with completed observation evidence. The first repo-native smoke is
   `cargo xt dev p7-operation-split-restart-smoke`.
17. **Approved split soak smoke**: verify that approved split child routes stay
   converged under sustained Ping/Move traffic, Worker child actors remain
   refreshed, remote AOI frames are observed, and the operation closes with
   completed observation evidence. The first repo-native smoke is
   `cargo xt dev p7-operation-split-soak-smoke`.
18. **Approved merge observation smoke**: verify that a published operation is
   closed only after route convergence, Worker refresh, stable-session traffic,
   latency metrics, and clean close counters are recorded. The first repo-native
   smoke is `cargo xt dev p7-operation-observation-smoke`.
19. **Approved merge recovery smoke**: verify that a post-publish owner outage
   records `recovery_required`, avoids automatic rollback, and recovers only
   after operator-visible Worker restart. The first repo-native smoke is
   `cargo xt dev p7-operation-recovery-smoke`.
20. **Internal operation helper**: add `cargo xt k8s operation-smoke` and
   `cargo xt k8s operation-report-check` so internal MicroK8s can record P7
   proposal evidence by default and approved execution/observation/soak
   evidence during a controlled smoke window.
21. **Internal rollout**: repeat the controlled image/GitOps/smoke/cleanup flow
   and add the P7 audit gate.

## Guardrails

- Do not make planner-selected mutation automatic by default.
- Do not weaken P6 rollback policy by silently merging back after a failure.
- Do not count read-only proposal evidence as execution evidence.
- Do not count local smoke as internal MicroK8s evidence.
- Do not count ArgoCD `Synced / Healthy` as runtime success without the
  operation-specific smoke report.
- Keep docs, verifier contracts, and GitOps cleanup state synchronized with the
  actual code and reports.
