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
  explicit approvals, default-off execution blocks, approved same-Worker merge
  execution idempotency, approved legacy split execution success/idempotency,
  approved legacy split observation completion, approved legacy split
  target-outage recovery-required handling, approved legacy split Orchestrator
  restart recovery, approved legacy split load/soak observation completion,
  approved canonical multi-depth split execution success/idempotency, approved
  canonical multi-depth observation completion, local merge observation
  completion, and local recovery-required owner-outage handling, plus
  Orchestrator restart recovery for the published operation ledger and
  assignment state, and local load/soak observation completion.
  `v2026.05.5` has been published and promoted
  through the k8s GitOps repo with the P7 operation ledger path enabled on the
  live Orchestrator, while executor and split/merge activation flags remain
  default-off outside controlled smoke windows. Internal `v2026.05.5` controlled
  windows have now covered approved execution/observation/soak, owner Worker
  failure/recovery, Orchestrator restart recovery, ArgoCD `Synced / Healthy`
  cleanup, Gateway parent Ping, and durable operation ledger persistence after
  each cleanup.

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
2. Done: `test: add p7 execution recovery smoke` - after approved same-Worker
   merge publish, owner Worker outage records `recovery_required`, leaves parent
   assignment published without automatic rollback, and proves operator-visible
   Worker restart recovery.
3. Done: `test: add p7 execution restart smoke` - restart the Orchestrator with the
   operation ledger and assignment state mounted, then verify operation state and
   route convergence survive, and complete observation after restart.
4. Done: `test: add p7 operation soak smoke` - run sustained post-execution parent
   Ping/Move traffic and verify route count, latency histograms, and close
   counters stay clean while the operation closes to `completed`.
5. Done: `test: add internal p7 operation smoke` - add a repo-native
   `cargo xt k8s ...` helper for approved P7 operation execution against a
   controlled GitOps smoke window, with default-off cleanup and report checks.
6. Done: `test: add internal p7 operation recovery smoke` - record owner Worker
   failure, recovery-required ledger state, operator scale-up recovery, and
   post-smoke default-off cleanup.
7. Done: `test: add internal p7 operation restart smoke` - record PVC-backed
   assignment state, Orchestrator rollout restart recovery, completed
   post-restart observation, and post-smoke default-off cleanup.
8. Done: `test: add p7 completion audit` - aggregate local, internal, rollout,
   cleanup, observation, recovery, restart, and soak evidence and fail until all
   P7 gates are backed by real artifacts.
9. Done: `test: add p7 split operation execution smoke` - approved legacy split
   operation execution now publishes child assignments once, removes the parent
   assignment, and proves repeat execution is an idempotent no-op.
10. Done: `test: add p7 split operation observation smoke` - close the split
   execution path with Gateway child route convergence, Worker child refresh,
   child traffic, metrics, and `completed` observation evidence.
11. Done: `test: add p7 split operation recovery smoke` - target Worker outage
   records `recovery_required`, leaves child assignments published without
   automatic rollback, and proves operator-visible Worker restart recovery.
12. Done: `test: add p7 split operation restart smoke` - restart the
   Orchestrator after approved legacy split publish, verify operation ledger and
   persisted child assignments survive, then close observation after restart.
13. Done: `test: add p7 split operation soak smoke` - extend the split path
   through sustained child Ping/Move traffic before internal rollout.
14. Done: `feat: extend p7 executor to canonical multi-depth operations` -
   approved canonical explicit-child operation execution now publishes child
   assignments once, removes the canonical parent assignment, proves Gateway
   child route convergence, stable-session child Move, remote AOI resync, and
   repeat execution idempotency.
15. Done: `test: add p7 multi-depth operation observation smoke` - close the
   canonical multi-depth operation path with Gateway/Worker child route
   convergence, stable-session child Move, remote AOI resync, clean counters,
   and completed observation evidence.
16. `test: add p7 multi-depth operation recovery smoke` - target Worker outage
   should record `recovery_required`, preserve canonical child assignments
   without automatic rollback, and verify operator-visible Worker restart
   recovery.
