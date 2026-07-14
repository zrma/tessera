# P15 Deterministic Simulation Harness

Last reviewed: 2026-07-14

Status: active

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

1. **Deterministic scenario contract (active)**
   - Add a CLI that validates bounded scenario inputs and supports a
     network-free `plan` command.
   - Deterministically map seed, actors, cells, and operation order into a
     versioned JSON plan.
   - Cover default, override, reproducibility, and invalid-boundary behavior
     with unit tests.
2. **Bounded multi-client execution**
   - Give each simulated player an independent Gateway connection and actor
     identity.
   - Execute Join, Move, and Ping steps with explicit per-operation timeouts and
     a bounded concurrency limit.
   - Classify connect, protocol, timeout, and server-close failures without
     leaking raw environment details into reports.
3. **Result and threshold contract**
   - Emit versioned JSON counts, failure classes, elapsed time, throughput, and
     latency summaries.
   - Add caller-owned failure thresholds and a deterministic non-zero exit
     contract; do not label development defaults as production SLOs.
4. **Repository smoke integration**
   - Add a small full-stack simulator smoke that starts the local stack,
     executes a bounded scenario, checks the report, and tears the stack down.
   - Keep CI traffic local and small. Broader capacity or cluster tests remain
     explicit external work.

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

## Completion Boundary

P15 is complete when all four slices are separate verified changes, scenario
planning is reproducible without a runtime, bounded multi-client execution and
failure classification are covered, the versioned result contract is checked,
and the local simulator smoke runs in CI. Production capacity decisions and
live-service load execution remain outside this milestone.
