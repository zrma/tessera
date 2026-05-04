# Tessera Quality Harness

Last verified: 2026-05-03

This document is the repo-local quality map for agents. It keeps the expected autonomy, feedback loops, and crate boundaries visible without requiring external chat history.

## Autonomy contract

- Default mode is autonomous execution: read `README.md` and `AGENTS.md`, inspect the local pattern, implement, verify, document, and report assumptions.
- Escalate to the user only for judgment-heavy requirement gaps, destructive operations, published history rewrite, unspecified remote bookmark movement, external cost or operations risk, and secrets or credentials.
- Small uncertainty should not stop the task. Capture the assumption in code, docs, tests, or the final report.

## OpenAI GPT-5.5 guidance

- Use the `openai-docs` skill and official OpenAI developer docs before changing OpenAI model, API, prompt, or agent guidance.
- If the task asks for the latest OpenAI baseline, treat `gpt-5.5` as the target and keep the change narrow: active model strings and directly related prompts or harness instructions only.
- Prefer outcome-first GPT-5.5 instructions: goal, success criteria, allowed side effects, evidence or validation rules, stop/escalation conditions, and output shape.
- Preserve existing reasoning effort, verbosity, tool definitions, structured output contracts, and Responses API state handling unless this repo exposes a safe configuration point. Record broader API or tool rewiring as a blocker.
- Leave historical examples, fixtures, eval baselines, provider comparisons, and fallback paths unchanged unless the task explicitly includes them.

## Feedback loops

- `cargo xt` is the default local gate and runs fmt, clippy with `-D warnings`, workspace check, and `cargo xt harness`.
- `cargo test` is the default behavioral gate.
- `cargo xt p6-completion-audit` is the machine gate for P6+ completion evidence. In the completed P6+ state it returns `complete=true` only when the internal MicroK8s reports cover restart recovery, live metrics, GitOps rollout, merge, canonical multi-depth gates, and post-smoke default-off cleanup.
- Runtime or networking changes also need the local smoke loop: `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`, and `cargo xt dev down --with-orch`.
- GitHub Actions runs the same verification and smoke loop on push and pull requests.

## Crate boundary policy

- `tessera-core` and `tessera-proto` must stay free of internal Tessera crate dependencies.
- Runtime crates (`tessera-gateway`, `tessera-worker`, `tessera-orch`) may depend on `tessera-core` and `tessera-proto`, but not on each other.
- `tessera-client` may depend on `tessera-core`, but not on runtime crates.
- `tessera-sim` may depend on `tessera-core` or `tessera-client`, but not on runtime crates unless the design overview and harness rule are updated together.

## Mechanical guardrails

- `cargo xt harness` verifies this document, README/AGENTS discoverability, CI smoke coverage, and crate dependency boundaries.
- If an intentional architecture edge fails the harness, update `README.md` Design Overview, this document, and the `xtask` rule in the same change.
- Keep implemented and planned work separate: README's implemented/planned sections are treated as the user-facing status source.

## Known gaps

- Orchestrator/Gateway/Worker have opt-in Prometheus text endpoints, Gateway has a `/ready` endpoint, Ping/Pong round-trip latency is covered by a histogram, and Join/Move request latency is covered by request-id correlation histograms. Long-running scrape/tracing assertions are not covered yet.
- Docker/Compose/Kubernetes sample packaging exists, but production manifests are intentionally deferred until target cluster conventions are known.
- Orchestrator has an inactive split/merge planner skeleton, dry-run preview endpoint, planner-to-operator `cargo xt split-activation-plan` and `cargo xt merge-activation-plan` helpers, policy-gated `cargo xt planner-activation` with live Worker metrics support for split, default-off manual split activation, same-Worker merge activation, cross-Worker merge replay activation, canonical merge sibling detection for `depth>0/sub=0` parents, and an opt-in persistent assignment state path for P6 restart recovery.
- Local split evidence includes `cargo xt dev activation-plan-smoke`, `cargo xt dev activation-live-plan-smoke`, `cargo xt dev activation-live-metrics-smoke`, `cargo xt dev activation-live-planner-mutation-smoke`, `cargo xt dev activation-smoke`, `cargo xt dev activation-failure-smoke`, `cargo xt dev activation-restart-smoke`, and `cargo xt dev activation-soak`; the reports cover mutation-free planning, live Worker metrics planning, manual submission, policy-gated live metrics planner mutation with default no-op, Gateway route convergence, Worker assignment refresh, target relay replay, stable-session Move, AOI resync, failure/recovery, restart recovery, load/soak, and Gateway close-counter checks. `cargo xt dev activation-report-check --planner-mutation-report ... --require-planner-live-metrics` verifies the live metrics planner activation evidence source.
- Local merge evidence includes `cargo xt dev merge-plan-smoke`, `cargo xt dev planner-mutation-smoke`, `cargo xt dev merge-activation-smoke`, `cargo xt dev canonical-merge-activation-smoke`, `cargo xt dev canonical-merge-activation-report-check`, `cargo xt dev canonical-merge-activation-failure-smoke`, `cargo xt dev canonical-merge-activation-failure-report-check`, `cargo xt dev canonical-merge-activation-restart-smoke`, `cargo xt dev canonical-merge-activation-restart-report-check`, `cargo xt dev canonical-merge-activation-soak`, `cargo xt dev canonical-merge-activation-soak-report-check`, `cargo xt dev merge-activation-cross-worker-smoke`, `cargo xt dev merge-activation-failure-smoke`, `cargo xt dev merge-activation-restart-smoke`, `cargo xt dev merge-activation-soak`, and `cargo xt dev activation-report-check --merge-plan-report ... --merge-activation-report ... --merge-cross-worker-report ... --merge-failure-report ... --merge-restart-report ... --merge-soak-report ... --planner-mutation-report ...`; the reports cover policy-gated merge planner mutation, same-Worker coalescing, canonical `depth>0/sub=0` same-Worker coalescing, mixed-owner remote child replay into the owner parent, Gateway parent route convergence, stable-session parent Moves, owner outage detection/recovery, Orchestrator restart recovery, load/soak, manual rollback policy, and the explicit volatile actor-state recovery boundary. Internal merge publish/recovery/restart/soak evidence is covered by the P6+ completion audit.
- Local multi-depth split evidence includes `cargo xt dev multi-depth-activation-smoke`, `cargo xt dev multi-depth-activation-report-check`, `cargo xt dev multi-depth-activation-failure-smoke`, `cargo xt dev multi-depth-activation-failure-report-check`, `cargo xt dev multi-depth-activation-restart-smoke`, `cargo xt dev multi-depth-activation-restart-report-check`, `cargo xt dev multi-depth-activation-soak`, and `cargo xt dev multi-depth-activation-soak-report-check`; the reports cover canonical explicit child cells, exact Gateway child routes, child traffic, route-change/relay metrics, failure/recovery, restart recovery, and load/soak.
- `cargo xt k8s activation-smoke` provides a guarded internal MicroK8s operator helper for service port-forward, plan-only evidence, optional live Worker metrics plan source, explicit split publish, target Worker scale-down/up recovery, and a P6 `--with-restart --allow-rollout-restart` path that preflights PVC-backed Orchestrator assignment state before restarting the deployment. `cargo xt k8s merge-activation-smoke`, `cargo xt k8s multi-depth-activation-smoke`, and their report checkers cover read-only readiness plus approved publish/failure/restart/soak evidence when the corresponding `--allow-*` gates are present. `cargo xt k8s planner-activation-report` and `cargo xt k8s planner-activation-report-check` validate the internal planner mutation policy evidence with default-off blocked and policy-approved published reports. `cargo xt p6-rollout-report` and `cargo xt p6-rollout-report-check` validate image publish, runtime deployment image match, approved GitOps rollout, ArgoCD `Synced / Healthy`, and post-smoke default-off cleanup. `cargo xt p6-completion-audit --json` aggregates these internal report JSON files and is expected to be green only after the concrete P6+ report artifacts exist. The P5 split-activation rollback policy is `operator_recovery_no_automatic_merge_rollback_v1`: no automatic merge rollback, target Worker restoration as recovery, and GitOps backout for controlled smoke rollback. Owner Worker restart actor state is explicitly excluded by `volatile_worker_actor_state_rejoin_required_v1`.
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
  local full-stack evidence; internal MicroK8s merge success/failure/restart
  windows and canonical multi-depth child-route soak are covered by the P7
  completion audit.
- P7 internal operation coverage now has a repo-native helper/verifier surface:
  `cargo xt k8s operation-smoke` records internal MicroK8s proposal evidence by
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
  rollback, and verifies operator Worker restart recovery. Restart/soak,
  internal rollout, and P8 completion audit remain future P8 slices.
- `docs/completed-milestones.md` records completed P0/P1/P2/P3/P4.1 work; `docs/todo-next.md` is the current execution-plan index; `docs/todo-p4-next-milestones.md` records the current decision gates. Keep README's implemented/planned sections and detailed `docs/` notes in sync when a task spans multiple changes.
