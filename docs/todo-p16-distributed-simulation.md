# P16 Distributed Simulation And Topology Convergence

Last reviewed: 2026-07-14

Status: active

## Objective

Extend the completed P15 simulator evidence from one root cell on the default
local Worker to a deterministic multi-cell topology routed across independent
Workers. P16 must prove that aggregate simulator success represents every
planned cell, that one Gateway can route the bounded workload across distinct
owners, and that the same workload succeeds again after a Worker identity keeps
its ownership while its advertised address changes.

P16 is a local development and CI regression milestone. It does not introduce
automatic assignment mutation, production capacity targets, cluster load
execution, or live-service operations policy.

## Architecture Boundary

- `tessera-sim` remains independent of Gateway, Worker, and Orchestrator crates.
  Cell-level result evidence is derived from the deterministic plan and client
  outcomes, not runtime internals.
- The distributed smoke owns an isolated local Orchestrator, Gateway, and two
  Workers with explicit loopback addresses and static root-cell assignments.
- Topology convergence keeps the Worker identity and cell ownership stable
  while replacing only its local advertised address. It does not publish a
  split/merge operation or alter durable assignment policy.
- Machine output remains versioned, aggregate, and free of target addresses,
  process inventory, raw errors, and actor-level records.
- Every scenario, retry, timeout, and concurrency limit remains bounded for CI.

## Milestone Queue

1. **Cell coverage result contract (complete)**
   - `tessera.sim.result.v1` includes deterministic per-cell planned,
     completed, and failed client counts plus planned/completed operation
     counts.
   - Coverage uses canonical cell ordering and rejects duplicate client
     indexes, actor mismatches, incomplete successful clients, and inconsistent
     plan/summary totals before emitting a result.
   - Runtime-free tests cover complete and partial-failure multi-cell results,
     ambiguous inputs, round-trip serialization, and privacy shape.
2. **Two-Worker distributed simulation smoke**
   - Add `cargo xt dev distributed-simulation-smoke` with two root cells owned
     by distinct Workers and routed through one Gateway.
   - Run a fixed bounded simulator plan across both cells and verify complete
     cell coverage, clean failure classes, Gateway route count, and evidence
     that both Workers accepted the planned sessions.
3. **Advertised-address convergence under workload**
   - Replace the second Worker's local address while retaining its identity and
     assigned cell, wait for Orchestrator/Gateway convergence, and rerun the
     exact deterministic plan.
   - Prove the second run has the same plan identity and complete cell coverage
     without using split/merge mutation.
4. **Repository gate and closeout**
   - Add the bounded distributed profile to CI and `cargo xt harness`, document
     the command and evidence boundary, and close P16 only after remote CI is
     green.

## Verification

Every slice requires the narrow focused tests plus repository defaults:

```text
cargo test -p tessera-sim
cargo test -p xtask
cargo xt
cargo test
```

Network slices also require:

```text
cargo xt dev distributed-simulation-smoke
```

Before each push, run the repository publication gate and the authorized
machine-local private-inventory gate, then wait for remote CI success.

## Completion Boundary

P16 is complete when cell-level result evidence is deterministic, a bounded
two-Worker/two-cell workload is exercised through one Gateway, the same plan
succeeds after a stable Worker identity changes address, and CI runs the full
local convergence smoke. Production load, live clusters, automatic ownership
mutation, and operator SLO policy remain outside the milestone.
