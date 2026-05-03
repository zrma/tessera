# Tessera Next Todo

Last reviewed: 2026-05-03

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

Recommended first slices:

1. `docs: open p7 operation loop` - establish the P7 checklist, completion
   criteria, and commit/push/smoke cadence.
2. `feat: add dynamic operation ledger` - add durable operation record types and
   persistence without planner or activation mutation.
3. `feat: record planner proposals` - turn live metrics and assignment listing
   candidates into mutation-free operation proposals.
4. `feat: gate approved operation execution` - require policy id, operator
   approval, cooldown, and budget checks before executing a proposal.
5. `test: verify closed-loop recovery evidence` - add local/dev smoke and
   report checks for proposal-to-execution-to-recovery flow.
6. `chore: roll out p7 operation loop internally` - publish image, promote via
   GitOps, run controlled smoke, clean up default-off state, and record rollout
   evidence.

