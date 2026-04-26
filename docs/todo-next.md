# Tessera Next Todo

Last reviewed: 2026-06-04

## Baseline

- V0 through P5 are complete through handover replay ownership, stable Gateway
  sessions, AOI precision, observability, packaging samples, request latency
  correlation, default-off manual split activation, planner-to-operator
  evidence, controlled guarded Kubernetes split smoke, and P5 rollback policy.
- P6+ is complete as of the `v2026.05.3` internal rollout. The final gate is
  `cargo xt p6-completion-audit --json`, which reports `complete=true` against
  the current `.dev/reports` evidence set.
- P7 operation loop is complete as of the `v2026.05.6` evidence set.
  `cargo xt p7-completion-audit --json` reports `complete=true` with local,
  internal, deployment, cleanup, observation, recovery, restart, and soak evidence.
- P8 policy-governed closed-loop cadence is complete as of the `v2026.05.7`
  evidence set. `cargo xt p8-completion-audit --json` reports `complete=true`
  with local read-only/proposal/approval/gate/execution/recovery/restart/soak
  evidence, split/merge/canonical multi-depth candidate coverage, P8 deployment
  rollout/default-off evidence, and internal controlled cadence smoke/cleanup.
- P9 operation control-plane readiness is complete as of the `v2026.05.8`
  evidence set. `cargo xt p9-completion-audit --json` reports `complete=true`
  with durable recommend-only history, replay audit, policy regression, P9
  deployment rollout/default-off evidence, internal recommend soak, controlled
  operation restart spot-check, and final cleanup evidence.
- P10 runtime observability and soak hardening is complete as of the
  `v2026.05.9` evidence set. `cargo xt p10-completion-audit --json` reports
  `complete=true` with local observability soak, ghost relay soak, replay audit,
  P10 deployment rollout/default-off evidence, and guarded Kubernetes observability
  soak.
- P11 operational endurance and failure recovery is complete as of the
  `v2026.05.10` evidence set. `cargo xt p11-completion-audit --json` reports
  `complete=true` with local endurance, restart recovery, transient
  failure/reconnect recovery, P11 deployment rollout/default-off evidence, and
  guarded Kubernetes endurance/recovery.
- The live Tessera deployment cleanup revision keeps Orchestrator execution and
  split/merge activation default-off outside controlled smoke windows, removes
  preview fixtures after smoke, and leaves deployment controller `tessera` `Synced / Healthy`.
- P12 read-only Operator Readiness and Alert Handoff is complete. The open
  boundary is optional read-only Kubernetes snapshot evidence or explicit
  operator decisions for external observability/live alert wiring.

## Next

P12 closes the P11 runtime evidence loop as an operator-ready, read-only
handoff while keeping mutation default-off and avoiding external observability
or production-manifest assumptions until explicitly approved. The planning and
decision source is `docs/todo-p12-ops-readiness.md`.

Recommended P12 slices:

1. Done: `docs: plan p12 operator readiness` - add the P12 goal,
   evidence map, guardrails, and escalation points.
2. Done: `test: add p12 readiness audit skeleton` - add a fail-closed
   `cargo xt p12-readiness-audit --json` gate that requires a read-only
   operator readiness report, SLO/alert candidate report, runbook drill report,
   source-of-truth replay evidence, and explicit unresolved decision list.
3. Done: `test: add p12 local report replay` - replay P11 local, deployment,
   and internal reports into a compact operator packet without touching
   runtime state.
4. Done: `test: add p12 slo alert candidates` - derive Gateway latency,
   Gateway close-counter, Worker relay, Orchestrator assignment, and
   default-off alert candidates from existing metrics and P11/P12 report
   evidence without provisioning external alerts.
5. Done: `test: add p12 runbook drill` - map operator symptoms to exact
   read-only checks for Gateway routing, Worker relay state, Orchestrator
   assignment state, operation ledger durability, and default-off
   verification.
6. Pending: `test: add p12 read-only k8s snapshot` - collect deployment controller,
   deployment image, readiness, metrics, assignment, and default-off state from
   guarded Kubernetes without pod restarts, scale changes, or activation.
7. Done: `test: add p12 decision packet` - record unresolved choices for alert
   backend, notification target, SLO thresholds, report retention, production
   manifest ownership, and live alert wiring without external wiring.
8. Escalated: external observability decisions - choose the
   decision gates for alert backend, SLO thresholds, notification route,
   production manifest ownership, and retention policy before any external
   wiring is implemented.

## Guardrails

- P12 defaults to read-only evidence. Runtime mutation, pod restarts, scale
  changes, alert backend creation, and production manifest changes require a
  separate explicit approval.
- P12 should use the completed P11 report set as source evidence instead of
  rerunning controlled failure windows unless a regression requires it.
- External observability choices must be captured as decisions before code,
  manifests, or credentials are introduced.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
