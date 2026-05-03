# Tessera Next Todo

Last reviewed: 2026-05-04

## Baseline

- V0 through P5 are complete through handover replay ownership, stable Gateway
  sessions, AOI precision, observability, packaging samples, request latency
  correlation, default-off manual split activation, planner-to-operator
  evidence, controlled internal MicroK8s split smoke, and P5 rollback policy.
- P6+ is complete as of the `v2026.05.3` internal rollout. The final gate is
  `cargo xt p6-completion-audit --json`, which currently reports
  `complete=true` with no findings against `.dev/reports`.
- P6 internal evidence covers PVC-backed persistent assignment state,
  live-metrics planner-to-operator evidence, split publish/failure/restart
  recovery, runtime merge publish/failure/restart/soak, canonical multi-depth
  publish/failure/restart/soak, P6 GitOps rollout evidence, and post-smoke
  default-off cleanup.
- The live Tessera GitOps cleanup revision returned Orchestrator activation to
  default-off, restored the standard assignment state path, removed smoke
  preview fixtures, and left ArgoCD `tessera` `Synced / Healthy`.
- P6 completion audit details remain in `docs/p6-completion-audit.md`. The next
  active design boundary is `docs/p7-operation-loop.md`.
- P7 initial local/dev slices are complete through durable proposal records,
  explicit approvals, default-off execution blocks, and approved same-Worker
  merge execution idempotency. `v2026.05.4` has been published and promoted
  through the k8s GitOps repo with the P7 operation ledger path enabled on the
  live Orchestrator, while executor and split/merge activation flags remain
  default-off. The internal baseline is ArgoCD `Synced / Healthy`, all Tessera
  deployments on `harbor.1day1coding.com/1day1coding/tessera:v2026.05.4`,
  Gateway Ping smoke green, and `GET /operations` reporting
  `persistence_enabled=true`.

## Next

The next milestone is P7: Closed-Loop, Policy-Gated Dynamic Cell Operations.

P7 should not start by enabling autonomous mutations. The first implementation
track is a durable, auditable operation loop:

1. Record planner proposals from live Worker metrics and current assignment
   state without mutation.
2. Persist operation records with proposal, approval, execution, observation,
   recovery/backout, and audit states.
3. Require explicit policy, approval, cooldown, and budget gates before any
   executor submits split or merge activation.
4. Keep all mutating automation default-off or policy-gated, with no automatic
   rollback unless a future policy explicitly changes that contract.
5. Verify each runtime-affecting slice with local/dev smoke first, then image
   publish, k8s GitOps rollout, ArgoCD `Synced / Healthy`, internal MicroK8s
   success/failure/restart/soak smoke, default-off cleanup, and verifier
   reports.

Recommended next slices:

1. Done: `test: add p7 execution observation smoke` - local full-stack smoke
   records route convergence, Worker refresh, stable-session traffic,
   latency/close-counter evidence, and transitions a published same-Worker merge
   operation from `observing` to `completed`.
2. `test: add p7 execution recovery smoke` - inject owner/target outage after an
   approved execution, record `recovery_required`, verify no automatic rollback,
   and prove operator-visible recovery.
3. `test: add p7 execution restart smoke` - restart the Orchestrator with the
   operation ledger and assignment state mounted, then verify operation state and
   route convergence survive.
4. `test: add internal p7 operation smoke` - add a repo-native `cargo xt k8s ...`
   helper for approved P7 operation execution against a controlled GitOps smoke
   window, with default-off cleanup and report checks.
5. `feat: extend p7 executor beyond same-worker merge` - only after the
   observation/recovery/restart evidence exists, add split and canonical
   multi-depth execution gates.
6. `test: add p7 completion audit` - aggregate local, internal, rollout, cleanup,
   observation, recovery, restart, and soak evidence and fail until all gates are
   backed by real artifacts.
