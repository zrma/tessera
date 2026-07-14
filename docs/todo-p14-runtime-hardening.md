# P14 Runtime Hardening

Last reviewed: 2026-07-14

Status: active

## Objective

P14 hardens the seamless-cell runtime paths that become more important when the
P13 chart runs multiple Workers: fragmented and bursty packet ingress, bounded
pressure behavior, route convergence across Worker identity changes,
assignment-state compatibility, and load-based planner quality.

P14 does not add production operations policy. It preserves the existing crate
ownership boundaries and default-off mutation gates.

## Observed Baseline

- `tessera-core` rejects declared frame payloads above `MAX_FRAME_LEN` and keeps
  incomplete frames unconsumed.
- Gateway client/upstream buffers start at 8 KiB and parse length-prefixed
  frames incrementally. Pending Ping and Join/Move correlation queues are
  capped at 32 and 128 entries respectively.
- Worker frame readers allocate only after validating the declared length, and
  per-subscriber outbound channels are capped at 256 entries.
- Worker overload paths use `try_send`; subscriber saturation requests an
  explicit disconnect instead of awaiting an unbounded queue.
- Existing coverage includes core incomplete/oversized frames, Gateway partial
  upstream replies, Worker mid-frame disconnect, and a saturated Worker
  subscriber. It does not yet stress successful fragmented Gateway client
  ingress, pipelined bursts, or the Gateway pending-queue eviction contract.

## Invariants

1. Frame chunking must not change decoded message order or routing semantics.
2. An oversized declared frame must fail before an upstream connection or a
   payload-sized allocation is made.
3. Pending/correlation and subscriber queues must remain bounded under bursty
   traffic; overload must have an observable drop/close policy.
4. Slow client or upstream peers must apply transport/channel backpressure
   without creating a second unbounded application queue.
5. Route and assignment changes must preserve canonical `CellId`, Worker
   identity, and advertised-address compatibility.

## Planned Slices

1. **Packet ingress stress (active)**
   - Add Gateway tests that send a valid client frame in many header/payload
     fragments and verify one ordered Ping/Pong result.
   - Add an oversized-prefix case that proves the client closes before any
     upstream connection.
   - Add a pipelined burst case and focused pending Ping/Join/Move queue tests
     that prove fixed capacity and deterministic oldest-entry eviction.
   - Reuse existing Worker saturation and mid-frame coverage; add Worker code
     only if the focused stress exposes a missing invariant.
2. **Route convergence under topology changes**
   - Exercise Worker identity/address replacement and scale-out assignment
     listings while a Gateway session remains active.
   - Prove convergence from both watch updates and periodic snapshot refresh.
3. **Assignment-state compatibility**
   - Define compatibility for Worker additions/removals and persisted
     assignment state without silently adopting unknown identities.
   - Cover restart and rejected-incompatible-state behavior.
4. **Planner quality under load**
   - Add representative skew, hysteresis, and churn-budget datasets.
   - Improve split/merge ranking only where a failing quality case justifies a
     runtime change.

## First Slice Verification

```sh
cargo test -p tessera-core
cargo test -p tessera-gateway
cargo test -p tessera-worker
cargo xt
cargo test
```

If the slice changes runtime networking behavior, also run the local
Worker/Gateway/Orchestrator ping smoke from `docs/smoke-runbook.md`.

## Completion Boundary

P14 is complete only when all four slices have focused regression evidence,
the runtime docs describe the resulting compatibility/overload contracts, and
`cargo xt`, `cargo test`, the task-relevant smoke, publication gates, and remote
CI are green. Each slice remains a separate change and push.
