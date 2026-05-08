# Tessera Next Todo

Last reviewed: 2026-05-08

## Baseline

- V0 through P5 are complete through handover replay ownership, stable Gateway
  sessions, AOI precision, observability, packaging samples, request latency
  correlation, default-off manual split activation, planner-to-operator
  evidence, controlled internal MicroK8s split smoke, and P5 rollback policy.
- P6+ is complete as of the `v2026.05.3` internal rollout. The final gate is
  `cargo xt p6-completion-audit --json`, which reports `complete=true` against
  the current `.dev/reports` evidence set.
- P7 operation loop is complete as of the `v2026.05.6` evidence set.
  `cargo xt p7-completion-audit --json` reports `complete=true` with local,
  internal, GitOps, cleanup, observation, recovery, restart, and soak evidence.
- P8 policy-governed closed-loop cadence is complete as of the `v2026.05.7`
  evidence set. `cargo xt p8-completion-audit --json` reports `complete=true`
  with local read-only/proposal/approval/gate/execution/recovery/restart/soak
  evidence, split/merge/canonical multi-depth candidate coverage, P8 GitOps
  rollout/default-off evidence, and internal controlled cadence smoke/cleanup.
- The live Tessera GitOps cleanup revision keeps Orchestrator execution and
  split/merge activation default-off outside controlled smoke windows, removes
  preview fixtures after smoke, and leaves ArgoCD `tessera` `Synced / Healthy`.
- The next active design boundary is
  `docs/p9-operation-control-plane-readiness.md`.

## Next

The next milestone is P9: Operation Control-Plane Readiness.

P9 should not enable automatic runtime mutation. The implementation track turns
the completed P8 policy-governed cadence into a durable recommend/replay/control
plane:

1. Run long-lived recommend-only loops from live Worker metrics and assignment
   snapshots, recording candidate decisions, skip/deny reasons, policy inputs,
   cooldown/budget/concurrency state, and stable batch keys without assignment
   mutation or execution.
2. Persist replayable operation history so proposal hashes, operation ids,
   approvals/denials, skipped execution, observation, recovery, and restart
   records can be reconstructed after process restart.
3. Keep split, merge, and canonical multi-depth candidate evidence under one
   report/history contract.
4. Verify default-off behavior and explicit approval/deny/policy gates with a
   local regression report before any internal runtime work.
5. Publish the P9 runtime image, promote it through the k8s GitOps repo, verify
   ArgoCD `Synced / Healthy`, run internal recommend-mode soak, then run only a
   short operator-approved controlled mutation spot-check before default-off
   cleanup.
6. Close with `cargo xt p9-completion-audit --json`, logical commits/pushes, CI
   verification, live health checks, and final default-off state.

Recommended P9 slices:

1. Done: `test: add p9 completion audit` - mark P8 complete in docs, add the P9
   goal contract, and add a fail-closed `cargo xt p9-completion-audit --json`
   skeleton requiring local recommend-loop, replay, policy, GitOps, internal
   recommend soak, and controlled spot-check evidence.
2. Done: `feat: add p9 recommend loop history` - add `cargo xt dev
   p9-recommend-loop-soak` to collect repeated live Worker metrics and
   assignment snapshots, write durable recommend-only history, prove stable
   split/merge/canonical multi-depth candidate keys, and report
   `no_assignment_mutation=true` plus `no_execution_attempted=true`.
3. `test: add p9 replay audit` - add `cargo xt dev p9-replay-audit` to replay
   durable history after restart and verify stable proposal hashes, stable
   operation ids, and no mutation.
4. `test: add p9 policy regression smoke` - add a local verifier for
   default-off execution, explicit approval, deny evidence, cooldown, budget,
   and concurrency gates.
5. `build: publish p9 control-plane runtime image` - publish the P9 image,
   promote it through the k8s GitOps repo, verify ArgoCD `Synced / Healthy`, and
   record `.dev/reports/p9-gitops-rollout-latest.json`.
6. `test: add internal p9 recommend soak` - run internal MicroK8s recommend-only
   soak against live Worker metrics and durable storage, then replay the history
   while keeping assignment state unchanged.
7. `test: add internal p9 controlled spot-check` - open a short approved window,
   execute one bounded operation, observe/replay it, clean mutating flags back
   to default-off, and close the P9 audit.

## Guardrails

- P9 recommend-mode code must remain mutation-free by default.
- Runtime-affecting paths require local evidence first, then image publish,
  GitOps rollout, ArgoCD health, internal smoke, cleanup, and completion audit.
- Controlled windows must be short-lived, explicit, and followed by default-off
  cleanup before the next slice begins.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
