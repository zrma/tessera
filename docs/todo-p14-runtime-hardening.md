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

1. **Packet ingress stress (complete)**
   - A byte-fragmented Gateway client frame produces exactly one Ping/Pong
     result through one upstream connection.
   - An oversized prefix closes the client before any upstream connection.
   - A pipelined burst preserves response order while exercising the fixed
     pending-Ping tracking window.
   - Focused Ping and Join/Move queue tests prove fixed capacity and
     deterministic oldest-entry eviction. New Prometheus counters expose
     correlation-tracking eviction without reporting it as packet loss.
   - Existing Worker saturation and mid-frame disconnect coverage remained
     green, so no Worker behavior change was needed.
2. **Route convergence under topology changes (complete)**
   - An active Gateway session follows a watch-delivered scale-out listing to a
     newly assigned cell without reconnecting the client.
   - A periodic refresh replaces a Worker's address while retaining its stable
     identity, and the active session reconnects to the replacement endpoint.
   - Existing identity replacement and stale-refresh guards remain green, so
     watch updates cannot be overwritten by an older snapshot.
3. **Assignment-state compatibility (active)**
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

## Completed Slice Evidence

Packet ingress stress is covered by:

- `fragmented_client_frame_reaches_upstream_once`
- `pipelined_ping_burst_preserves_order_and_observes_tracking_pressure`
- `oversized_client_prefix_closes_before_upstream_connect`
- `pending_correlation_queues_evict_oldest_at_fixed_capacity`
- `tessera_gateway_pending_ping_tracking_evictions_total`
- `tessera_gateway_pending_request_tracking_evictions_total`

Route convergence is covered by:

- `scale_out_watch_routes_new_cell_on_active_session`
- `refresh_replaces_worker_address_on_active_session`
- `route_change_reconnects_to_new_worker`
- `route_change_after_non_ping_reconnects_with_stable_session`
- `refresh_skips_stale_snapshot_after_watch_update`

## Completion Boundary

P14 is complete only when all four slices have focused regression evidence,
the runtime docs describe the resulting compatibility/overload contracts, and
`cargo xt`, `cargo test`, the task-relevant smoke, publication gates, and remote
CI are green. Each slice remains a separate change and push.
