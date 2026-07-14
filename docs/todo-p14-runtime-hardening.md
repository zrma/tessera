# P14 Runtime Hardening

Last reviewed: 2026-07-14

Status: complete

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
   - Split child and merge parent replay targets remain unpublished during
     staging, while Workers validate their identities and addresses against the
     current listing directory rather than requiring an impossible active cell
     route.
3. **Assignment-state compatibility (complete)**
   - Configured empty Worker additions and removal of drained persisted Workers
     are compatible across Orchestrator restart.
   - A new configured Worker with static cells, or a removed Worker that still
     owns persisted cells, is rejected instead of being silently adopted or
     reassigned.
   - The durable-state guide and packaging contract now define the required
     drain/migration boundary.
4. **Planner quality under load (complete)**
   - Representative datasets cover a single-signal spike versus sustained
     multi-signal pressure, exact hysteresis/age boundaries, and global churn
     selection across multiple worlds.
   - Planner overlap now uses the parent plus direct-child touched set. An
     active or newly selected parent operation blocks child operations in the
     same interval without blocking unrelated cell families.
   - Existing score ordering remained valid; only the failing parent/child
     overlap invariant required a planner behavior change.

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
- `worker_directory_from_listing_keeps_empty_and_self_workers`
- `replay_target_uses_worker_directory_before_cell_publish`
- `cargo xt dev activation-restart-smoke`
- `cargo xt dev merge-activation-cross-worker-smoke`

Assignment-state compatibility is covered by:

- `assignment_state_accepts_configured_empty_worker_addition`
- `assignment_state_accepts_removed_empty_worker`
- `assignment_state_rejects_removed_worker_with_owned_cells`
- `assignment_state_rejects_missing_configured_worker_with_static_cells`
- `persistent_assignment_state_applies_compatible_worker_changes_on_restart`

Planner quality is covered by:

- `split_merge_planner_quality_dataset_handles_skew_and_threshold_boundaries`
- `split_merge_planner_applies_global_churn_budget_across_worlds`
- `split_merge_planner_blocks_parent_child_overlap_from_active_plan`
- `split_merge_planner_does_not_plan_parent_and_child_operations_together`

## Completion Boundary

All four P14 slices have focused regression evidence. The runtime docs describe
the resulting overload, route/replay, assignment compatibility, and planner
overlap contracts. Completion requires `cargo xt`, `cargo test`, the
task-relevant smoke, publication gates, and remote CI to remain green for the
closing change.
