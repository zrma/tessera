# P15 Deterministic Simulation Harness

Last reviewed: 2026-07-14

Status: complete

## Objective

Turn `tessera-sim` from a placeholder into a bounded, deterministic player and
load simulation harness for the repository-owned Gateway protocol. The harness
must make a scenario reproducible from explicit inputs, exercise independent
client sessions without depending on runtime crates, and emit a compact result
that can be checked locally and in a CI-safe smoke profile.

P15 is a development and regression tool. It does not define production SLOs,
generate traffic against an external service by default, or own cluster load,
capacity, alerting, or incident policy.

## Architecture Boundary

- `tessera-sim` may depend on `tessera-core` and general-purpose workspace
  libraries, but not on Gateway, Worker, or Orchestrator crates.
- The simulator speaks the same length-prefixed `Envelope<ClientMsg>` protocol
  as `tessera-client`; runtime behavior remains owned by the runtime crates.
- Every run is bounded by explicit client, operation, timeout, and concurrency
  limits. Defaults must remain safe for a developer workstation.
- Deterministic scenario planning is pure and testable without a listening
  Gateway. Network execution is an explicit command.
- Human output may be concise, while machine output uses a versioned JSON
  report with aggregate counts and latency summaries. Reports must not include
  local absolute paths or environment inventory.

## Milestone Queue

1. **Deterministic scenario contract (complete)**
   - `tessera-sim plan` validates client, cell, move, actor-range, cell-range,
     and total-operation bounds before allocating a plan.
   - Seeded actor/cell placement and Join/Move/Ping steps serialize as
     `tessera.sim.plan.v1`; the default output is stable compact JSON and
     `--pretty` is presentation-only.
   - Unit tests cover CLI defaults/overrides, byte-stable reproducibility, seed
     variation, complete bounded mappings, and fail-closed invalid boundaries.
2. **Bounded multi-client execution (complete)**
   - `tessera-sim run` gives each player an independent Gateway connection and
     actor identity, while a rolling task set enforces the concurrency bound.
   - Join, Move, and Ping steps use per-connect/per-operation timeouts. Direct
     replies are distinguished from unsolicited AOI pushes before payload,
     cell, and actor validation.
   - Connect, protocol, timeout, and server-close failures have stable classes.
     Focused fake-network tests cover every class and prove the active session
     cap; a four-client local full-stack run completed all operations cleanly.
3. **Result and threshold contract (complete)**
   - `--json` emits `tessera.sim.result.v1` with aggregate planned/completed
     counts, failure classes, monotonic elapsed time, throughput, and nearest-rank
     p50/p95/p99/max operation latency in microseconds.
   - The result omits target addresses, actor-level records, and raw errors.
     Focused tests cover JSON round-trip, empty/boundary percentiles, failure
     counts, and privacy shape.
   - `--max-failed-clients` and optional `--max-p95-latency-ms` are
     caller-owned gates. Violations are stable typed records and produce a
     non-zero exit after the JSON is emitted; they are not production SLOs.
4. **Repository smoke integration (complete)**
   - `cargo xt dev simulation-smoke` builds the simulator, starts the local
     Worker/Gateway/Orchestrator stack, executes a fixed four-client and
     sixteen-operation profile, validates `tessera.sim.result.v1`, and tears
     the stack down on both success and failure.
   - The validator checks counts, failure classes, latency sample count,
     positive elapsed/throughput values, threshold success, and absence of
     target-address fields. Focused fixtures cover success and rejection.
   - CI runs the same small local profile. Broader capacity or cluster tests
     remain explicit external work.

## Verification

Every slice requires focused simulator tests plus the repository defaults:

```text
cargo test -p tessera-sim
cargo xt
cargo test
```

Network execution slices also require the narrowest matching local full-stack
smoke. Before each push, run both repository and authorized machine-local
publication gates and wait for remote CI success.

The first slice also has a runtime-free command check:

```text
cargo run -p tessera-sim -- plan --seed 7 --clients 4 --cells 2
```

The second slice full-stack check is:

```text
cargo xt dev up --with-orch
cargo run -p tessera-sim -- run --seed 7 --clients 4 --cells 1 --moves-per-client 2 --operation-timeout-ms 2000 --max-concurrency 2
cargo xt dev down --with-orch
```

The third slice checks both successful JSON parsing and a deliberately strict
latency gate that must emit a failed result and return non-zero.

The final repository-owned smoke is:

```text
cargo xt dev simulation-smoke
```

## Completion Boundary

All four slices are separate verified changes. Scenario planning is
reproducible without a runtime, bounded multi-client execution and failure
classification are covered, the versioned result contract is checked, and the
local simulator smoke runs in CI. Production capacity decisions and
live-service load execution remain outside this milestone.
