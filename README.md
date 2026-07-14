# Tessera

Cell-based world orchestration for real-time servers in Rust.

**Tessera** divides a seamless open world into cells and provides the control
plane for ownership handover, AOI/ghost synchronization, dynamic split/merge,
and operator-governed runtime operations. The target is the basic runtime
skeleton for MMO-scale simulation, not a finished game server or a live-service
operations stack.

- Runtime: Rust + Tokio
- Deployment target: container-first and Kubernetes-template friendly
- Current state: P0 through P16 runtime/evidence/packaging, hardening,
  deterministic simulation, and distributed topology-convergence gates are
  complete. P17 runtime trace correlation is active in `docs/todo-next.md`.

## Workspace

- `crates/tessera-gateway`: stateless client ingress and cell route gateway
- `crates/tessera-worker`: stateful cell owner, tick loop, AOI/ghost relay
- `crates/tessera-orch`: control plane, assignments, operations, audits
- `crates/tessera-core`: shared types, framing, `CellId`
- `crates/tessera-proto`: gRPC/IDL generated surface
- `crates/tessera-sim`: load and player simulation helpers
- `crates/tessera-client`: test CLI client
- `xtask`: local verification, smoke, audit, and report helpers

## Quick Start

- Build: `cargo build`
- Full local quality gate: `cargo xt`
- Harness only: `cargo xt harness`
- Tests: `cargo test`
- Last completed audit: `cargo xt p12-readiness-audit --json`
- Historical P11 audit: `cargo xt p11-completion-audit --json`
- Active plan: `docs/todo-next.md`
- Kubernetes packaging runbook: `docs/packaging.md`
- Latest completed milestone: `docs/todo-p16-distributed-simulation.md`
- Active milestone: `docs/todo-p17-runtime-trace-correlation.md`
- Documentation index: `docs/README.md`
- Smoke/runbook commands: `docs/smoke-runbook.md`

## Run Locally

Start the local Worker + Gateway + Orchestrator stack:

```sh
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

Build a deterministic simulator plan without starting the runtime:

```sh
cargo run -p tessera-sim -- plan --seed 7 --clients 4 --cells 2
```

With the local stack running, execute independent bounded client sessions:

```sh
cargo run -p tessera-sim -- run --seed 7 --clients 4 --cells 1 --moves-per-client 2 --max-concurrency 2
```

Add `--json` for the `tessera.sim.result.v1` aggregate and use caller-owned
gates such as `--max-failed-clients 0 --max-p95-latency-ms 100` when a concrete
development check needs them. These thresholds are not production SLOs.

Useful variants:

- Logs: `cargo xt dev logs --target all --follow`
- Structured local logs: `TESSERA_LOG_FORMAT=json cargo xt dev up --with-orch`
- Gateway only smoke: `cargo xt dev up`, client ping, then `cargo xt dev down`
- Metrics smoke: `cargo xt dev metrics-smoke`
- Two-Worker simulator smoke: `cargo xt dev distributed-simulation-smoke`
- P9 regression lane:
  - `cargo xt dev p9-recommend-loop-soak`
  - `cargo xt dev p9-replay-audit`
  - `cargo xt dev p9-policy-regression-smoke`
  - `cargo xt p9-completion-audit --json`
- P10 completion lane:
  - `cargo xt dev p10-observability-soak --iterations 2 --sleep-ms 1`
  - `cargo xt dev p10-ghost-relay-soak --iterations 2 --sleep-ms 1`
  - `cargo xt dev p10-replay-audit`
  - `cargo xt k8s p10-observability-soak --context example-cluster --namespace tessera --expected-image registry.example.com/example/tessera:v2026.05.9`
  - `cargo xt p10-completion-audit --json`
- P11 completed gate:
  - `cargo xt dev p11-endurance-soak`
  - `cargo xt dev p11-restart-recovery-smoke`
  - `cargo xt dev p11-transient-failure-recovery-smoke`
  - `cargo xt k8s p11-endurance-recovery-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-pod-restart --allow-controlled-failure`
  - `cargo xt p11-completion-audit --json`
- P12 read-only handoff reports:
  - `cargo xt dev p12-local-report-replay`
  - `cargo xt dev p12-slo-alert-candidates`
  - `cargo xt dev p12-runbook-drill`
  - `cargo xt dev p12-decision-packet`
  - `cargo xt p12-readiness-audit --json`
  - `docs/todo-p12-ops-readiness.md`

The longer smoke command catalog, guarded Kubernetes commands, historical
P6/P7/P8 lanes, and packaging-template boundary are kept in
`docs/smoke-runbook.md` and `docs/packaging.md`.

## Status Snapshot

### Implemented

- Core envelope/framing and `CellId{world,cx,cy,depth,sub}` model.
- Gateway to Worker TCP pipeline for Join/Move/Ping.
- Gateway routing from Orchestrator `WatchAssignments` plus periodic
  `ListAssignments` refresh.
- Worker-owned cell processing, per-cell tick flush, same-worker AOI ghosting,
  remote worker ghost relay, remote actor cache, and opt-in metrics.
- Handover control plane with `PreCopy -> Freeze -> Diff -> Commit/Abort`,
  bounded move buffering, idempotent target replay, and assignment switching.
- Manual/default-off split and merge activation with replay, validation,
  all-or-nothing publish, failure/recovery evidence, restart recovery, and
  local plus guarded deployment smoke coverage.
- Durable assignment state, operation ledger, policy-gated execution,
  observation/recovery records, cooldown/budget/concurrency gates, runtime
  metrics, soak evidence, failure recovery, and completion audits through P12.
- Container and guarded Kubernetes verification evidence through P12, with
  environment identity, image coordinates, rollout revisions, and cluster
  topology retained outside this public repository.
- Portable P13 Helm packaging for Gateway, Worker, and Orchestrator with
  schema-checked values, deterministic default/scale-out renders, explicit
  namespace and Secret-reference boundaries, probes, and optional state mounts.
- P14 packet/route/state/planner hardening, the P15 deterministic simulator,
  and P16 distributed simulation across two cell owners with cell-level result
  evidence, stable-identity address convergence, and bounded CI smoke gates.

### Latest Closed Boundary

P16 extends the deterministic simulator result with canonical per-cell
coverage and adds `cargo xt dev distributed-simulation-smoke`. The bounded
full-stack gate routes the same two-cell profile through distinct Workers,
replaces one Worker's advertised address without changing identity or
ownership, waits for Orchestrator/Gateway convergence, and reruns the profile.
Its completed evidence map is in `docs/todo-p16-distributed-simulation.md`; the
next boundary is tracked in `docs/todo-next.md`.

## Protocol Snapshot

- Envelope: `cell: CellId`, `seq: u64`, `epoch: u32`,
  `payload: ClientMsg|ServerMsg`
- Gateway injects stable client `session` ids and optional request ids for
  Join/Move latency correlation.
- Opt-in JSON logging emits privacy-bounded `gateway.request.forwarded`,
  `worker.request.received`, `worker.response.sent`, and
  `gateway.response.forwarded` lifecycle events for those direct requests.
- Broadcast AOI traffic does not reuse request ids, so Gateway latency metrics
  only count direct correlated responses.
- Handover, split, merge, and operation-loop mutations are all default-off
  unless the explicit manual/operator policy gates are present.
- Operation ledger endpoints are exposed only when
  `TESSERA_ORCH_OPERATION_LEDGER_PATH` is configured.

## Troubleshooting

- Port conflict: adjust `TESSERA_GW_ADDR`, `TESSERA_WORKER_ADDR`, or
  `TESSERA_ORCH_ADDR`.
- Logs: `cargo xt dev logs --target all --follow`
- Gateway readiness/metrics:
  `TESSERA_GW_METRICS_ADDR=127.0.0.1:4100 cargo run -p tessera-gateway`
- Worker metrics:
  `TESSERA_WORKER_METRICS_ADDR=127.0.0.1:5100 cargo run -p tessera-worker`
- Orchestrator metrics and preview:
  `TESSERA_ORCH_METRICS_ADDR=127.0.0.1:6100 cargo run -p tessera-orch`
- Runtime log format: leave `TESSERA_LOG_FORMAT` unset for compact output or set
  it to `json`; unsupported values fail startup.
- Detailed smoke and audit commands: `docs/smoke-runbook.md`

## Automation Harness

- 자동화 에이전트는 `README.md`, `AGENTS.md`, and `docs/quality.md`를 우선
  읽고, 기존 패턴에 맞춰 구현, 검증, 문서화, VCS 정리까지 진행한다.
- `cargo xt harness` checks README/AGENTS/docs/CI discoverability and internal
  crate dependency boundaries.
- `cargo xt` runs fmt, clippy, check, and harness.
- `cargo test` remains the broad Rust test gate.
- CI runs `cargo xt`, `cargo test`, the local dev ping smoke, and both bounded
  simulator smokes with
  `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`,
  `cargo xt dev down --with-orch`, `cargo xt dev simulation-smoke`, and
  `cargo xt dev distributed-simulation-smoke`.

## Design Overview

- Problem: a single process or shard bottlenecks seamless worlds and causes
  discontinuities during load shifts.
- Goal: absorb load with cell-level ownership, routing, handover, split, merge,
  packet/replay robustness, generalized topology data structures, and explicit
  evidence while keeping the client on a stable gateway session.
- Non-goals for the initial scope: finished game-server gameplay features,
  complete zero-downtime migration, multi-region consistency, and complete
  live-service operations.
- Key model: `CellId` is the ownership unit; Orchestrator owns assignment truth;
  Gateway routes by assignment watch/snapshot; Worker owns tick/runtime state
  for assigned cells and relays AOI/ghost state for nearby cells.
- Deployment boundary: this repository should provide portable container and
  Kubernetes chart/template artifacts that a hosting environment can adopt. It
  should not encode private cluster inventory, credentials, alert routes, or
  site-specific operations policy.
- Operational defaults: runtime mutation is disabled by default, controlled
  windows must be explicit, and deployment cleanup must restore default-off state.

## Contributing & Workflow

- Primary bookmark: `main`
- Local VCS workflow: use `jj status`, `jj diff`, `jj describe`, and
  `jj git push`.
- Commit message format: `<type>: <summary>`
- Before publishing code changes, run `cargo xt` and `cargo test`; for runtime
  changes also run the relevant local smoke from `docs/smoke-runbook.md`.
- Additional agent instructions: `AGENTS.md`
