# Tessera Next Todo

Last reviewed: 2026-05-11

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
- P10 runtime observability and soak hardening is complete as of the
  `v2026.05.9` evidence set. `cargo xt p10-completion-audit --json` reports
  `complete=true` with local observability soak, ghost relay soak, replay audit,
  P10 GitOps rollout/default-off evidence, and internal MicroK8s observability
  soak.
- The live Tessera GitOps cleanup revision keeps Orchestrator execution and
  split/merge activation default-off outside controlled smoke windows, removes
  preview fixtures after smoke, and leaves ArgoCD `tessera` `Synced / Healthy`.
- P11 Operational Endurance and Failure Recovery is the active design boundary.

## Next

P10 is closed. P11 should prove that the P10 observability loop remains useful
under longer load, reconnects, restarts, and controlled failure windows. The
active contract is `docs/p11-operational-endurance-failure-recovery.md`.

Recommended P11 slices:

1. Done: `test: add p11 completion audit` - mark P11 active in docs, add the
   P11 goal contract, and add a fail-closed
   `cargo xt p11-completion-audit --json` skeleton requiring local endurance,
   restart recovery, transient failure/reconnect recovery, GitOps rollout,
   internal MicroK8s endurance/recovery, and default-off cleanup evidence.
2. Done: `feat: add local p11 endurance soak` - compose the P10 observability
   and ghost-relay loops into repeated load/reconnect evidence and write
   `.dev/reports/p11-endurance-soak-latest.json`.
3. Done: `test: add p11 restart recovery smokes` - cover Gateway, Worker,
   and Orchestrator restart recovery with persisted assignment and operation
   state in `.dev/reports/p11-restart-recovery-latest.json`.
4. Pending: `test: add p11 transient failure recovery` - cover target Worker
   unavailability, controlled component failure, port-forward reconnect, and
   post-recovery convergence in
   `.dev/reports/p11-transient-failure-recovery-latest.json`.
5. Pending: `build: publish p11 endurance runtime image` - publish only after
   local evidence is green, promote it through GitOps, verify ArgoCD
   `Synced / Healthy`, and record `.dev/reports/p11-gitops-rollout-latest.json`.
6. Pending: `test: add internal p11 endurance recovery smoke` - validate the
   promoted image in internal MicroK8s with pod restarts, controlled failures,
   Gateway smoke, durable report capture, and final default-off cleanup.

## Guardrails

- P11 endurance/recovery code must remain mutation-free by default unless an
  explicit controlled window requires a bounded runtime action.
- Runtime-affecting paths require local evidence first, then image publish,
  GitOps rollout, ArgoCD health, internal smoke, cleanup, and completion audit.
- Controlled windows must be short-lived, explicit, and followed by default-off
  cleanup before the next slice begins.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
