# Tessera Next Todo

Last reviewed: 2026-05-09

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
- P10 Runtime Observability and Soak Hardening is the active design boundary.

## Next

P9 is closed. P10 should harden the runtime evidence surface rather than add
automatic mutation. The active contract is
`docs/p10-runtime-observability-soak-hardening.md`.

Recommended P10 slices:

1. Done: `test: add p10 completion audit` - mark P10 active in docs, add
   the P10 goal contract, and add a fail-closed
   `cargo xt p10-completion-audit --json` skeleton requiring local
   observability soak, ghost relay soak, replay audit, GitOps rollout/default-off
   cleanup, and internal MicroK8s observability soak evidence.
2. Done: `feat: add p10 observability soak` - add `cargo xt dev
   p10-observability-soak` to sample Gateway, Worker, and Orchestrator metrics,
   request latency histograms, ghost relay counters, assignment snapshots, and
   operation/recommend histories into a durable replayable report.
3. Done: `test: add p10 ghost relay soak` - add focused local/dev coverage
   for relay fanout, backpressure, reconnect counters, route convergence,
   close-counter cleanliness, assignment stability, and default-off state.
4. Pending: `test: add p10 replay audit` - replay durable P10 reports and prove
   stable report hashes without touching runtime state.
5. Pending: `build: publish p10 observability runtime image` - publish the P10
   image only after local evidence is green, promote it through the k8s GitOps
   repo, verify ArgoCD `Synced / Healthy`, and record
   `.dev/reports/p10-gitops-rollout-latest.json`.
6. Pending: `test: add internal p10 observability soak` - validate the promoted
   image in internal MicroK8s with Gateway smoke, live metrics, durable report
   capture, and final default-off cleanup.

## Guardrails

- P10 observability code must remain mutation-free by default.
- Runtime-affecting paths require local evidence first, then image publish,
  GitOps rollout, ArgoCD health, internal smoke, cleanup, and completion audit.
- Controlled windows must be short-lived, explicit, and followed by default-off
  cleanup before the next slice begins.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
