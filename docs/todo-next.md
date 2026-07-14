# Tessera Next Todo

Last reviewed: 2026-07-14

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
- Prior guarded Kubernetes evidence proves the runtime can be exercised in a
  containerized environment, but cluster-specific inventory and live operations
  policy remain outside this repository.
- P12 read-only operator evidence is complete as a historical support packet.
  P13 portable Kubernetes packaging is also complete with deterministic
  default/scale-out render validation. The active boundary is P14 runtime
  hardening.

## Next

Tessera's main direction is still seamless-cell orchestration: stable packet
handling, cell ownership, handover, split/merge, generalized routing/state data
structures, and a horizontally deployable container architecture. Kubernetes
work in this repository should stop at a reusable chart/template boundary, not
expand into owning a specific live service's operations stack.

The active planning source is `docs/todo-p14-runtime-hardening.md`.

P13 closure:

1. Complete: `docs: define p13 packaging contract` - Helm v3 chart shape,
   caller-owned namespace, existing-secret references, values validation,
   cluster-free render checks, and non-goals are explicit.
2. Complete: `build: add k8s packaging template` - the Helm chart renders
   Gateway, Worker, and Orchestrator workloads and Services with probes,
   metrics, ConfigMap wiring, existing-secret references, and optional state
   mounts.
3. Complete: `test: add k8s render policy check` - the repo-native gate proves
   deterministic default and scale-out renders, validates portable object and
   safety policy, and is part of `cargo xt harness` and CI.
4. Complete: `docs: update packaging runbook` - render, caller-owned
   install/readiness/ping, retained-state, and cleanup boundaries are explicit.
5. Complete: `refactor: generalize topology values` - the Worker list drives
   identity, deterministic advertised Services, Orchestrator assignment seeds,
   and the committed three-Worker render case.

P14 runtime hardening queue:

1. Complete: packet pipeline backpressure and partial-frame stress coverage,
   including observable bounded correlation tracking.
2. Complete: route convergence under worker scale-out/identity changes through
   both watch and refresh paths while a Gateway session remains active.
3. Complete: fail-closed assignment-state compatibility for adding empty
   Workers, removing drained Workers, and rejecting implicit cell adoption.
4. Active: load-based split/merge planner quality beyond current guarded
   operator evidence.

## Guardrails

- Repository-owned Kubernetes work stops at portable chart/template artifacts,
  render validation, and example smoke instructions.
- Cluster-specific live operations, alerting, paging, credentials, ingress,
  certificate, registry, and incident policy are outside this repository unless
  explicitly introduced as placeholders or documented non-goals.
- Runtime mutation, pod restarts, scale changes, and controlled failure windows
  remain guarded helper paths, not default packaging behavior.
- P12 metric/readiness evidence is historical support material, not the next
  product direction.
- Each logical slice should be committed and pushed separately before moving to
  the next gate.
