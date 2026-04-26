# Handover Replay Todo

Last reviewed: 2026-04-26

## Scope

This plan records the completed P0/P1 handover replay milestones: target-side
replay, bounded commit retry, stable Gateway sessions, and explicit owner
transfer.

## Assumptions

- Target-side replay is a state-transfer path, not a guarantee that the current
  client socket stays open across every route switch. The Gateway reconnects
  upstream with its stable session id when a route changes after non-ping
  traffic.
- Replayed actors on the target should receive explicit owner sessions from the
  source replay manifest. Actors missing owner entries keep the legacy
  claim-on-first-use compatibility path.
- Replay must be idempotent per operation/cell so a source retry cannot apply
  buffered moves twice.
- Commit retry budget is enforced before assignment transfer. Once assignment
  transfer succeeds, replay delivery failures are handled as target replay
  errors rather than rolling the assignment back in this slice.

## Work Items

1. [done 2026-04-26] Orchestrator commit retry budget
   - Add a bounded retry counter to active handovers.
   - Keep the handover in `Diffing` while the target Worker is not registered
     and retry budget remains.
   - Abort the handover before assignment transfer once the retry budget is
     exhausted.
   - Verify registered-target success, retryable unregistered target, and
     exhausted-budget abort.

2. [done 2026-04-26] Worker replay payload and source send path
   - Add a worker-to-worker relay payload for `HandoverReplay`.
   - When a source Worker sees a freeze/diff policy released and no longer owns
     the cell, drain its buffered moves, capture the cell actor snapshot, and
     send replay to the target route with bounded send retry.
   - Preserve FIFO order and TTL/overflow behavior for source buffered moves.

3. [done 2026-04-26] Target replay apply path
   - Accept `HandoverReplay` only when the target Worker owns the cell.
   - Apply base actor state plus buffered deltas in order, mark replayed actors
     as claimable by the next client session, and broadcast resulting deltas to
     local/relay subscribers.
   - Ignore duplicate operation/cell replays without applying moves twice.

4. [done 2026-04-26] Stable session handover
   - Add an explicit client/session identity or Gateway handover protocol so
     post-route-switch ownership can be preserved without relying on actor
     claim-on-first-use.
   - Revisit the Gateway route-change close path after stable session transfer
     exists.

5. [done 2026-04-26] Explicit owner transfer
   - Include actor owner sessions in `HandoverReplay`.
   - Apply target owner map during replay before post-handover traffic arrives.
   - Preserve legacy claim-on-first-use only for replay payloads without owner
     entries.

## Verification

- `cargo test -p tessera-orch handover`
- `cargo test -p tessera-worker handover`
- `cargo xt`
- `cargo test`
- Runtime smoke after code changes:
  - `cargo xt dev up --with-orch`
  - `cargo run -p tessera-client -- ping --ts 123`
  - `cargo xt dev down --with-orch`
