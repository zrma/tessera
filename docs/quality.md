# Tessera Quality Harness

Last verified: 2026-07-14

This document is the repo-local quality map for agents. It keeps the expected autonomy, feedback loops, and crate boundaries visible without requiring external chat history.

## Autonomy contract

- Default mode is autonomous execution: read `README.md` and `AGENTS.md`, inspect the local pattern, implement, verify, document, and report assumptions.
- Escalate to the user only for judgment-heavy requirement gaps, destructive operations, published history rewrite, unspecified remote bookmark movement, external cost or operations risk, and secrets or credentials.
- Small uncertainty should not stop the task. Capture the assumption in code, docs, tests, or the final report.

## GPT-5.6 project overlay

The common model, prompt-budget, permission, persistence, verification, output, Pro/PTC, and evaluation contract is owned by `AGENTS.md` under `Agent Harness Baseline (GPT-5.6)`.

- Tessera changes must preserve crate boundaries and verify the affected gateway/worker/orchestrator path through `cargo xt harness` or the narrower documented smoke.
- Do not add runtime model, reasoning, Responses API, Pro mode, PTC, or tool wiring unless an active OpenAI integration point exists in the repository.
- Leave historical examples, fixtures, eval baselines, provider comparisons, and fallback paths unchanged unless the task explicitly includes them.

## Feedback loops

- `cargo xt` is the default local gate and runs fmt, clippy with `-D warnings`, workspace check, and `cargo xt harness`.
- `cargo test` is the default behavioral gate.
- `docs/README.md` is the document index, `docs/todo-next.md` is the active
  open-work source, and `docs/smoke-runbook.md` is the command catalog.
- `cargo xt p6-completion-audit` is the machine gate for P6+ completion evidence. In the completed P6+ state it returns `complete=true` only when the guarded Kubernetes reports cover restart recovery, live metrics, deployment rollout, merge, canonical multi-depth gates, and post-smoke default-off cleanup.
- `cargo xt p10-completion-audit --json` is the machine gate for P10 runtime
  observability and soak hardening. In the completed `v2026.05.9` state it
  returns `complete=true` only when local observability, ghost relay, replay,
  deployment rollout, guarded Kubernetes soak, and default-off cleanup evidence are
  present.
- `cargo xt p11-completion-audit --json` is the completed P11 gate for
  operational endurance and failure recovery. In the completed `v2026.05.10`
  state it returns `complete=true` only when local endurance, restart recovery,
  transient failure/reconnect recovery, deployment rollout, guarded Kubernetes
  endurance/recovery, and default-off cleanup evidence are present.
- `cargo xt p12-readiness-audit --json` is the completed P12 machine gate for
  read-only operator evidence. It returns `complete=true`
  when read-only operator readiness, metric candidates, runbook drill, source
  replay, and explicit decision packet reports are present.
- `scripts/check-k8s-packaging.py` is the P13 render gate. It runs Helm lint,
  proves deterministic default and scale-out renders, validates object,
  namespace, topology, persistence, credential-reference, probe, and
  default-off mutation policy, and exercises fail-closed negative cases.
  `cargo xt harness` and CI run it automatically; code changes still require
  `cargo xt` and `cargo test`.
- Runtime or networking changes also need the local smoke loop: `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`, and `cargo xt dev down --with-orch`.
- GitHub Actions runs the same verification and smoke loop on push and pull requests.

## Crate boundary policy

- `tessera-core` and `tessera-proto` must stay free of internal Tessera crate dependencies.
- Runtime crates (`tessera-gateway`, `tessera-worker`, `tessera-orch`) may depend on `tessera-core` and `tessera-proto`, but not on each other.
- `tessera-client` may depend on `tessera-core`, but not on runtime crates.
- `tessera-sim` may depend on `tessera-core` or `tessera-client`, but not on runtime crates unless the design overview and harness rule are updated together.

## Mechanical guardrails

- `scripts/check-agent-harness-interface.sh` verifies this repository's canonical `agent-harness-v1` structure and GPT-5.6 baseline.
- `scripts/check-publication-boundary.py` rejects concrete private registry endpoints, machine-specific Kubernetes contexts, hardcoded private registry projects, and cross-repository state from public trees.
- `scripts/check-k8s-packaging.py` validates the portable Helm chart without a
  live cluster.
- `cargo xt harness` verifies this document, README/AGENTS discoverability, CI
  smoke coverage, Kubernetes packaging, publication policy, and crate
  dependency boundaries.
- If an intentional architecture edge fails the harness, update `README.md` Design Overview, this document, and the `xtask` rule in the same change.
- Keep implemented, historical, and planned work separate: `README.md` is the
  front door, `docs/todo-next.md` is the current status/open-work source, and
  closed milestone evidence stays in the matching `docs/p*-*.md` file.

## Known gaps

- Orchestrator/Gateway/Worker have opt-in Prometheus text endpoints, Gateway has a `/ready` endpoint, Ping/Pong round-trip latency is covered by a histogram, and Join/Move request latency is covered by request-id correlation histograms. Long-running scrape/tracing assertions are not covered yet.
- Docker/Compose/Kubernetes sample packaging and the portable Helm chart exist.
  The remaining packaging gap is the operator-facing example runbook;
  cluster-specific live
  operations policy remains outside this repository.
- Orchestrator has an inactive split/merge planner skeleton, dry-run preview endpoint, planner-to-operator `cargo xt split-activation-plan` and `cargo xt merge-activation-plan` helpers, policy-gated `cargo xt planner-activation` with live Worker metrics support for split, default-off manual split activation, same-Worker merge activation, cross-Worker merge replay activation, canonical merge sibling detection for `depth>0/sub=0` parents, and an opt-in persistent assignment state path for P6 restart recovery.
- Local split evidence includes `cargo xt dev activation-plan-smoke`, `cargo xt dev activation-live-plan-smoke`, `cargo xt dev activation-live-metrics-smoke`, `cargo xt dev activation-live-planner-mutation-smoke`, `cargo xt dev activation-smoke`, `cargo xt dev activation-failure-smoke`, `cargo xt dev activation-restart-smoke`, and `cargo xt dev activation-soak`; the reports cover mutation-free planning, live Worker metrics planning, manual submission, policy-gated live metrics planner mutation with default no-op, Gateway route convergence, Worker assignment refresh, target relay replay, stable-session Move, AOI resync, failure/recovery, restart recovery, load/soak, and Gateway close-counter checks. `cargo xt dev activation-report-check --planner-mutation-report ... --require-planner-live-metrics` verifies the live metrics planner activation evidence source.
- Local merge evidence includes `cargo xt dev merge-plan-smoke`, `cargo xt dev planner-mutation-smoke`, `cargo xt dev merge-activation-smoke`, `cargo xt dev canonical-merge-activation-smoke`, `cargo xt dev canonical-merge-activation-report-check`, `cargo xt dev canonical-merge-activation-failure-smoke`, `cargo xt dev canonical-merge-activation-failure-report-check`, `cargo xt dev canonical-merge-activation-restart-smoke`, `cargo xt dev canonical-merge-activation-restart-report-check`, `cargo xt dev canonical-merge-activation-soak`, `cargo xt dev canonical-merge-activation-soak-report-check`, `cargo xt dev merge-activation-cross-worker-smoke`, `cargo xt dev merge-activation-failure-smoke`, `cargo xt dev merge-activation-restart-smoke`, `cargo xt dev merge-activation-soak`, and `cargo xt dev activation-report-check --merge-plan-report ... --merge-activation-report ... --merge-cross-worker-report ... --merge-failure-report ... --merge-restart-report ... --merge-soak-report ... --planner-mutation-report ...`; the reports cover policy-gated merge planner mutation, same-Worker coalescing, canonical `depth>0/sub=0` same-Worker coalescing, mixed-owner remote child replay into the owner parent, Gateway parent route convergence, stable-session parent Moves, owner outage detection/recovery, Orchestrator restart recovery, load/soak, manual rollback policy, and the explicit volatile actor-state recovery boundary. Internal merge publish/recovery/restart/soak evidence is covered by the P6+ completion audit.
- Local multi-depth split evidence includes `cargo xt dev multi-depth-activation-smoke`, `cargo xt dev multi-depth-activation-report-check`, `cargo xt dev multi-depth-activation-failure-smoke`, `cargo xt dev multi-depth-activation-failure-report-check`, `cargo xt dev multi-depth-activation-restart-smoke`, `cargo xt dev multi-depth-activation-restart-report-check`, `cargo xt dev multi-depth-activation-soak`, and `cargo xt dev multi-depth-activation-soak-report-check`; the reports cover canonical explicit child cells, exact Gateway child routes, child traffic, route-change/relay metrics, failure/recovery, restart recovery, and load/soak.
- `cargo xt k8s activation-smoke` provides a guarded guarded Kubernetes operator helper for service port-forward, plan-only evidence, optional live Worker metrics plan source, explicit split publish, target Worker scale-down/up recovery, and a P6 `--with-restart --allow-rollout-restart` path that preflights PVC-backed Orchestrator assignment state before restarting the deployment. `cargo xt k8s merge-activation-smoke`, `cargo xt k8s multi-depth-activation-smoke`, and their report checkers cover read-only readiness plus approved publish/failure/restart/soak evidence when the corresponding `--allow-*` gates are present. `cargo xt k8s planner-activation-report` and `cargo xt k8s planner-activation-report-check` validate the internal planner mutation policy evidence with default-off blocked and policy-approved published reports. `cargo xt p6-rollout-report` and `cargo xt p6-rollout-report-check` validate image publish, runtime deployment image match, approved deployment rollout, deployment controller `Synced / Healthy`, and post-smoke default-off cleanup. `cargo xt p6-completion-audit --json` aggregates these internal report JSON files and is expected to be green only after the concrete P6+ report artifacts exist. The P5 split-activation rollback policy is `operator_recovery_no_automatic_merge_rollback_v1`: no automatic merge rollback, target Worker restoration as recovery, and deployment backout for controlled smoke rollback. Owner Worker restart actor state is explicitly excluded by `volatile_worker_actor_state_rejoin_required_v1`.
- `cargo xt dev merge-activation-soak` covers local same-Worker merge load/soak with sustained parent Ping/Move traffic, parent route retention, Gateway latency histogram growth, and zero Gateway close counters; `cargo xt dev merge-activation-soak-report-check` validates that report shape.
- P7 local operation coverage includes default-off split/merge/canonical
  multi-depth proposal and approval records, approved same-Worker merge
  execution/observation/recovery/restart/soak, and approved legacy split
  execution, completed observation, target-outage recovery-required handling,
  and Orchestrator restart recovery via
  `cargo xt dev p7-operation-split-execution-smoke`,
  `cargo xt dev p7-operation-multi-depth-execution-smoke`,
  `cargo xt dev p7-operation-multi-depth-observation-smoke`,
  `cargo xt dev p7-operation-multi-depth-recovery-smoke`,
  `cargo xt dev p7-operation-multi-depth-restart-smoke`,
  `cargo xt dev p7-operation-multi-depth-soak-smoke`,
  `cargo xt dev p7-operation-split-observation-smoke`,
  `cargo xt dev p7-operation-split-recovery-smoke`, and
  `cargo xt dev p7-operation-split-restart-smoke`, plus sustained child-route
  soak via `cargo xt dev p7-operation-split-soak-smoke`. Canonical multi-depth
  execution, completed observation, recovery-required target outage handling,
  Orchestrator restart recovery, and load/soak observation completion now have
  local full-stack evidence; guarded Kubernetes merge success/failure/restart
  windows and canonical multi-depth child-route soak are covered by the P7
  completion audit.
- P7 internal operation coverage now has a repo-native helper/verifier surface:
  `cargo xt k8s operation-smoke` records guarded Kubernetes proposal evidence by
  default and can run approved execution/observation/soak when
  `--allow-execution --with-soak` is used during a controlled smoke window.
  It also supports a separate guarded failure/recovery gate with
  `--allow-execution --with-failure --allow-scale`, which scales the owner
  Worker down, records `recovery_required`, scales it back up, and verifies
  parent-route recovery. `--allow-execution --with-restart
  --allow-rollout-restart` separately preflights PVC-backed assignment state,
  rollout-restarts the Orchestrator deployment, verifies parent-route recovery,
  and completes observation after restart.
  `cargo xt k8s operation-report-check` validates the resulting operation
  ledger, parent route convergence, Worker refresh, traffic, close counters,
  soak evidence, recovery-required evidence, restart evidence, and canonical
  multi-depth operation evidence. The `v2026.05.6` evidence set has passed
  approved merge execution/observation/soak, owner Worker failure/recovery,
  Orchestrator restart recovery, canonical multi-depth operation
  execution/observation/child-route soak, post-smoke default-off cleanup, and
  `cargo xt p7-completion-audit --json`.
- P8 read-only cadence coverage starts with `cargo xt dev p8-cadence-plan-smoke`,
  which collects repeated live Worker metrics plus Orchestrator assignment
  listing ticks, emits `.dev/reports/p8-cadence-plan-smoke-latest.json`, and
  verifies stable candidate keys with no assignment mutation or execution
  attempt. Proposal ledger idempotency is covered by `cargo xt dev
  p8-cadence-proposal-smoke`, which materializes live Worker actor metrics into
  the Orchestrator proposal preview path and proves repeated proposal ticks
  reuse one durable record without mutation or execution. Approval/default-off
  preflight is covered by `cargo xt dev p8-cadence-approval-smoke`, which
  records policy/cooldown/budget approval evidence and blocks
  unapproved/missing-policy/wrong-policy/default-off execution attempts without
  mutation. Cooldown/budget/concurrency enforcement is covered by `cargo xt dev
  p8-cadence-gate-smoke`, which validates each gate before the default-off
  executor block and keeps assignment mutation disabled. Bounded execution is
  covered by `cargo xt dev p8-cadence-execution-smoke`, which opens a local
  manual execution window only after durable approval/gate metadata, publishes
  one live-metrics split operation, proves repeat execution idempotency, and
  closes observation with Gateway route, Worker refresh, traffic, and close
  counter evidence. Failure/recovery evidence is covered by `cargo xt dev
  p8-cadence-recovery-smoke`, which publishes the same bounded cadence path,
  injects a target Worker outage, records `recovery_required` without automatic
  rollback, and verifies operator Worker restart recovery. Orchestrator restart
  recovery is covered by `cargo xt dev p8-cadence-restart-smoke`, which mounts a
  P8 assignment state path, restarts the Orchestrator after approved bounded
  publish, verifies ledger/assignment recovery, and completes post-restart
  observation. Child-route soak is covered by `cargo xt dev
  p8-cadence-soak-smoke`, which keeps the bounded cadence path under sustained
  post-execution Ping/Move traffic and records route convergence, Worker child
  refresh, remote AOI frames, clean close counters, and completed observation.
  `cargo xt k8s p8-cadence-smoke --allow-execution` is the guarded internal
  Kubernetes helper for the P8 runtime-affecting path: it requires the deployment
  smoke window to expose manual operation execution, manual split/merge
  activation, a P8 preview path, a P8 operation ledger/state path, and
  budget/concurrency gates; it then writes a live Worker metrics preview
  snapshot into the Orchestrator PVC, publishes one approved bounded split
  operation, proves repeat execution idempotency, runs child-route soak, and
  records `.dev/reports/guarded-kubernetes-p8-cadence-smoke-latest.json`.
  `cargo xt k8s p8-cadence-cleanup-check` must run after the deployment cleanup
  revision; it verifies deployment controller `Synced / Healthy`, image match, default-off
  execution/activation env, preview fixture removal, and finalizes that report.
  `cargo xt p8-completion-audit --json` aggregates the P8 local cadence
  evidence, including stable split/merge/canonical multi-depth candidate
  coverage, P8 deployment rollout/default-off evidence, and finalized internal
  controlled cadence smoke; the `v2026.05.7` evidence set returns
  `complete=true`.
- P9 operation control-plane readiness is complete as of the `v2026.05.8`
  evidence set. `cargo xt p9-completion-audit --json` should return
  `complete=true` with local recommend-only soak, durable history replay audit,
  policy regression, P9 deployment rollout/default-off cleanup, internal recommend
  soak, and a controlled mutation spot-check all machine-checkable. P9 keeps
  live runtime mutation default-off; recommend-mode evidence proves no
  assignment mutation or execution attempt, while the runtime-affecting
  spot-check happened only inside an operator-approved controlled window
  followed by default-off cleanup.
  `cargo xt dev p9-recommend-loop-soak` is the first local gate: it runs the
  two-Worker dev stack, records repeated live Worker metrics and assignment
  snapshots, writes `.dev/reports/p9-recommend-history-latest.json`, and
  validates `.dev/reports/p9-recommend-loop-soak-latest.json` with
  `no_assignment_mutation=true` and `no_execution_attempted=true`.
  `cargo xt dev p9-replay-audit` then reloads that durable history from disk,
  recomputes stable proposal hashes, verifies stable operation ids, and records
  `.dev/reports/p9-replay-audit-latest.json` without touching runtime state.
  `cargo xt dev p9-policy-regression-smoke` reruns the default-off/cooldown/
  budget/concurrency deny cases and folds explicit approval plus blocked
  execution evidence into `.dev/reports/p9-policy-regression-latest.json`.
  `cargo xt k8s p9-recommend-soak` is the internal read-only gate: it requires
  deployment controller `Synced / Healthy`, matching deployment images, and default-off
  execution env, then port-forwards Orchestrator/Gateway/Worker services to
  write durable recommend history plus replay evidence without assignment
  mutation. The controlled mutation gate is intentionally separate:
  `cargo xt k8s p9-controlled-spot-check-report` only folds a finalized
  internal P7 operation restart smoke report after post-smoke default-off
  cleanup is visible on the live deployment.
- P10 runtime observability and soak hardening is complete as of the
  `v2026.05.9` evidence set. `cargo xt p10-completion-audit --json` should
  return `complete=true` with local Gateway/Worker/Orchestrator observability,
  request latency, ghost relay, replay, deployment rollout/default-off, and
  guarded Kubernetes observability soak reports all machine-checkable. `cargo xt
  k8s p10-observability-soak` is read-only against assignment state: it requires
  deployment controller `Synced / Healthy`, matching deployment images, Gateway smoke, sampled
  Gateway/Worker/Orchestrator metrics, assignment stability, and live
  default-off cleanup.
- P11 operational endurance and failure recovery is complete as of the
  `v2026.05.10` evidence set. It keeps the P10 observability loop as the
  baseline and adds repeated load, reconnect, Gateway restart, Worker restart,
  Orchestrator restart with persisted assignment/operation state, transient
  target Worker unavailability, port-forward reconnect, and post-recovery
  route/assignment convergence evidence. The guarded Kubernetes gate is
  `cargo xt k8s p11-endurance-recovery-smoke` and requires explicit
  `--allow-pod-restart` plus `--allow-controlled-failure` for the bounded,
  deployment self-heal-safe restart/failure window.
- P12 read-only operator evidence is complete as support material. It turns the
  P11 report set into operator-facing metrics, runbook drill, replay, and
  out-of-scope decision evidence. The machine gate is
  `cargo xt p12-readiness-audit --json`.
- P13 Kubernetes packaging template is the active planning boundary. It should
  finish portable chart/template documentation for the
  containerized Gateway/Worker/Orchestrator architecture without owning a
  specific live service's alerting, ingress, registry, secret, certificate,
  incident, or production rollout policy.
- `docs/completed-milestones.md` records completed P0 through P4 work and
  `docs/todo-next.md` is the current execution-plan index. Keep README's
  implemented/planned sections and detailed `docs/` notes in sync when a task
  spans multiple changes.
