# Tessera

Cell-based world orchestration for real-time servers in Rust.

**Tessera** divides a seamless open world into cells and provides the control
plane for ownership handover, AOI/ghost synchronization, dynamic split/merge,
and operator-governed runtime operations. The target is the basic runtime
skeleton for MMO-scale simulation, not a finished game server.

- Runtime: Rust + Tokio
- Deployment target: self-hosted and Kubernetes-friendly
- Current state: P0 through P9 are complete; P10 Runtime Observability and Soak
  Hardening is the active design boundary.

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
- Last completed audit: `cargo xt p9-completion-audit --json`
- Active P10 audit: `cargo xt p10-completion-audit --json` (expected to fail
  until P10 evidence exists)
- Documentation index: `docs/README.md`
- Smoke/runbook commands: `docs/smoke-runbook.md`

## Run Locally

Start the local Worker + Gateway + Orchestrator stack:

```sh
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

Useful variants:

- Logs: `cargo xt dev logs --target all --follow`
- Gateway only smoke: `cargo xt dev up`, client ping, then `cargo xt dev down`
- Metrics smoke: `cargo xt dev metrics-smoke`
- Last completed P9 local audit lane:
  - `cargo xt dev p9-recommend-loop-soak`
  - `cargo xt dev p9-replay-audit`
  - `cargo xt dev p9-policy-regression-smoke`
  - `cargo xt p9-completion-audit --json`
- Active P10 completion gate:
  - `cargo xt dev p10-observability-soak --iterations 2 --sleep-ms 1`
  - `cargo xt dev p10-ghost-relay-soak --iterations 2 --sleep-ms 1`
  - `cargo xt p10-completion-audit --json`

The longer smoke command catalog, internal MicroK8s commands, and historical
P6/P7/P8 lanes are kept in `docs/smoke-runbook.md`.

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
  local plus internal smoke coverage.
- Durable assignment state, operation ledger, policy-gated execution,
  observation/recovery records, cooldown/budget/concurrency gates, and
  completion audits through P9.
- Kubernetes/GitOps rollout evidence through image
  `harbor.1day1coding.com/1day1coding/tessera:v2026.05.8`, with final live
  state restored to default-off after controlled smoke windows.

### Current Open Boundary

P9 is closed. `docs/todo-next.md` is the active open-work index and now points
to P10 Runtime Observability and Soak Hardening. P10 keeps runtime mutation
default-off and focuses on durable, replayable evidence for long-running
Gateway/Worker/Orchestrator metrics, request latency, ghost relay behavior,
route convergence, close-counter cleanliness, and assignment stability.

## Protocol Snapshot

- Envelope: `cell: CellId`, `seq: u64`, `epoch: u32`,
  `payload: ClientMsg|ServerMsg`
- Gateway injects stable client `session` ids and optional request ids for
  Join/Move latency correlation.
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
- Detailed smoke and audit commands: `docs/smoke-runbook.md`

## Automation Harness

- 자동화 에이전트는 `README.md`, `AGENTS.md`, and `docs/quality.md`를 우선
  읽고, 기존 패턴에 맞춰 구현, 검증, 문서화, VCS 정리까지 진행한다.
- `cargo xt harness` checks README/AGENTS/docs/CI discoverability and internal
  crate dependency boundaries.
- `cargo xt` runs fmt, clippy, check, and harness.
- `cargo test` remains the broad Rust test gate.
- CI runs `cargo xt`, `cargo test`, and the local dev ping smoke with
  `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`,
  and `cargo xt dev down --with-orch`.

## Design Overview

- Problem: a single process or shard bottlenecks seamless worlds and causes
  discontinuities during load shifts.
- Goal: absorb load with cell-level ownership, routing, handover, split, merge,
  operation review, and explicit audit evidence while keeping the client on a
  stable gateway session.
- Non-goals for the initial scope: finished game-server gameplay features,
  complete zero-downtime migration, and multi-region consistency.
- Key model: `CellId` is the ownership unit; Orchestrator owns assignment truth;
  Gateway routes by assignment watch/snapshot; Worker owns tick/runtime state
  for assigned cells and relays AOI/ghost state for nearby cells.
- Operational defaults: runtime mutation is disabled by default, controlled
  windows must be explicit, and GitOps cleanup must restore default-off state.

## Contributing & Workflow

- Primary bookmark: `main`
- Local VCS workflow: use `jj status`, `jj diff`, `jj describe`, and
  `jj git push`.
- Commit message format: `<type>: <summary>`
- Before publishing code changes, run `cargo xt` and `cargo test`; for runtime
  changes also run the relevant local smoke from `docs/smoke-runbook.md`.
- Additional agent instructions: `AGENTS.md`
