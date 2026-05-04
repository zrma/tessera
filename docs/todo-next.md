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
- P6 completion audit details remain in `docs/p6-completion-audit.md`.
- P7 initial local/dev slices are complete through durable proposal records,
  explicit approvals, default-off execution blocks, approved same-Worker merge
  execution idempotency, approved legacy split execution success/idempotency,
  approved legacy split observation completion, approved legacy split
  target-outage recovery-required handling, approved legacy split Orchestrator
  restart recovery, approved legacy split load/soak observation completion,
  approved canonical multi-depth split execution success/idempotency, approved
  canonical multi-depth observation completion, approved canonical multi-depth
  target-outage recovery-required handling, approved canonical multi-depth
  Orchestrator restart recovery, approved canonical multi-depth load/soak
  observation completion, local merge observation completion, and local
  recovery-required owner-outage handling, plus Orchestrator restart recovery for
  the published operation ledger and assignment state, and local load/soak
  observation completion.
  `v2026.05.6` has been published and promoted
  through the k8s GitOps repo with the P7 operation ledger path enabled on the
  live Orchestrator, while executor and split/merge activation flags remain
  default-off outside controlled smoke windows. Internal `v2026.05.6` controlled
  windows have now covered approved execution/observation/soak, owner Worker
  failure/recovery, Orchestrator restart recovery, canonical multi-depth
  operation execution/observation/child-route soak, ArgoCD `Synced / Healthy`
  cleanup, Gateway parent Ping, durable operation ledger persistence after each
  cleanup, and `cargo xt p7-completion-audit --json` returning `complete=true`.
- The next active design boundary is
  `docs/p8-closed-loop-operation-cadence.md`.

## Next

The next milestone is P8: Policy-Governed Closed-Loop Operation Cadence.

P8 should not start by enabling autonomous mutations. The implementation track
turns the completed P7 operation loop into a bounded operator cadence:

1. Collect live Worker metrics and current assignment state on repeated planner
   ticks, then produce candidate batches without assignment mutation.
2. Persist candidate/proposal records with stable hashes so cadence reruns are
   idempotent and auditable.
3. Require explicit policy, approval, cooldown, budget, and concurrency gates
   before any bounded executor window submits split, merge, or canonical
   multi-depth activation.
4. Keep all mutating automation default-off or policy-gated, with no automatic
   rollback unless a future policy explicitly changes that contract.
5. Verify each runtime-affecting slice with local/dev smoke first, then image
   publish, k8s GitOps rollout, ArgoCD `Synced / Healthy`, internal MicroK8s
   success/failure/restart/soak smoke, default-off cleanup, and verifier
   reports.

Recently completed P7 slices:

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
16. Done: `test: add p7 multi-depth operation recovery smoke` - target Worker
   outage records `recovery_required`, preserves canonical child assignments
   without automatic rollback, and verifies operator-visible Worker restart
   recovery.
17. Done: `test: add p7 multi-depth operation restart smoke` - restart the
   Orchestrator after approved canonical multi-depth publish, verify operation
   ledger and persisted canonical child assignments survive, then close
   observation after restart.
18. Done: `test: add p7 multi-depth operation soak smoke` - extend the canonical
   multi-depth operation path through sustained child Ping/Move traffic before
   internal rollout.
19. Done: `build: publish p7 multi-depth operation runtime image` - published
   `harbor.1day1coding.com/1day1coding/tessera:v2026.05.6`, promoted it
   through the k8s GitOps repo, verified ArgoCD `Synced / Healthy`, ran
   canonical multi-depth internal operation smoke, cleaned mutating flags back
   to default-off, and closed `cargo xt p7-completion-audit --json` with
   `complete=true`.

Recommended P8 slices:

1. `docs: refresh p8 cadence goal` - close stale P7 status references, add the
   P8 prompt-to-artifact contract, and keep the next milestone explicitly
   default-off/no automatic mutation.
2. Done: `feat: add p8 read-only cadence planner` - `cargo xt dev
   p8-cadence-plan-smoke` runs repeated live-metrics and assignment-state
   planner ticks, emits stable candidate batches, and proves
   `assignments_changed=false` with no execution attempt.
3. Done: `test: add p8 cadence proposal idempotency smoke` - `cargo xt dev
   p8-cadence-proposal-smoke` materializes live Worker actor metrics into the
   Orchestrator proposal preview path, writes one durable proposal record, and
   proves repeat ticks reuse it without assignment mutation or execution.
4. Done: `test: add p8 cadence approval preflight smoke` - `cargo xt dev
   p8-cadence-approval-smoke` records a live-metrics proposal, persists an
   operator approval with policy/cooldown/budget keys, proves repeat approval is
   idempotent, and keeps unapproved/missing-policy/wrong-policy/default-off
   execution preflight mutation-free.
5. Done: `feat: add p8 cooldown budget concurrency gates` - Orchestrator
   operation execution preflight now supports active cooldown keys,
   per-budget-key limits, and max in-flight operations per budget key, with
   `cargo xt dev p8-cadence-gate-smoke` proving each gate blocks before
   default-off execution and without mutation.
6. Done: `test: add p8 bounded execution cadence smoke` - `cargo xt dev
   p8-cadence-execution-smoke` executes one approved bounded local operation
   set from live Worker metrics, observes it to completion, and proves duplicate
   execution remains idempotent.
7. Done: `test: add p8 cadence recovery smoke` - `cargo xt dev
   p8-cadence-recovery-smoke` extends the bounded cadence through
   recovery-required target Worker outage evidence and operator-visible Worker
   restart recovery without automatic rollback.
8. Done: `test: add p8 cadence restart smoke` - `cargo xt dev
   p8-cadence-restart-smoke` proves the approved bounded cadence ledger and
   published child assignments survive Orchestrator restart, then closes
   post-restart observation to `completed`.
9. `test: add p8 cadence soak smoke` - extend the bounded cadence through
   sustained post-execution child traffic and clean counters.
10. `build: publish p8 cadence runtime image` - publish a new image only after
   local P8 gates are green.
11. `test: add internal p8 cadence smoke` - promote through k8s GitOps, wait for
   ArgoCD `Synced / Healthy`, run controlled internal cadence smoke, restore
   default-off state, and verify a P8 completion audit.
