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
- P9 operation control-plane readiness is complete as of the `v2026.05.8`
  evidence set. `cargo xt p9-completion-audit --json` reports `complete=true`
  with durable recommend-only history, replay audit, policy regression, P9
  GitOps rollout/default-off evidence, internal recommend soak, controlled
  operation restart spot-check, and final cleanup evidence.
- The live Tessera GitOps cleanup revision keeps Orchestrator execution and
  split/merge activation default-off outside controlled smoke windows, removes
  preview fixtures after smoke, and leaves ArgoCD `tessera` `Synced / Healthy`.
- The next active design boundary is not selected yet.

## Next

P9 is closed. The next milestone should be selected from current runtime
constraints rather than continuing P9 by default.

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
3. Done: `test: add p9 replay audit` - add `cargo xt dev p9-replay-audit` to replay
   durable history after restart and verify stable proposal hashes, stable
   operation ids, and no mutation.
4. Done: `test: add p9 policy regression smoke` - add a local verifier for
   default-off execution, explicit approval, deny evidence, cooldown, budget,
   and concurrency gates.
5. Done: `build: publish p9 control-plane runtime image` - publish the P9 image,
   promote it through the k8s GitOps repo, verify ArgoCD `Synced / Healthy`, and
   record `.dev/reports/p9-gitops-rollout-latest.json`.
6. Done: `test: add internal p9 recommend soak` - run internal MicroK8s
   recommend-only soak against live Worker metrics and durable storage, then
   replay the history while keeping assignment state unchanged.
7. Done: `test: add internal p9 controlled spot-check` - open a short approved
   window, execute one bounded operation, observe/replay it, clean mutating
   flags back to default-off, and close the P9 audit.

## Guardrails

- P9 recommend-mode code must remain mutation-free by default.
- Runtime-affecting paths require local evidence first, then image publish,
  GitOps rollout, ArgoCD health, internal smoke, cleanup, and completion audit.
- Controlled windows must be short-lived, explicit, and followed by default-off
  cleanup before the next slice begins.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
