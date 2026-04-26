# Tessera Handover Runtime

Last reviewed: 2026-04-26

## Scope

This document describes the current handover control-plane and the first runtime enforcement slices. Target routing migration is implemented through the assignment listing path, Gateway injects a stable session id per client connection, and source buffered moves are transferred to the target Worker through a worker-to-worker replay payload.

Implemented now:
- `SubmitHandoverCommand` gRPC command API on the Orchestrator.
- Ordered state machine: `PreCopy -> Freeze -> Diff -> Commit`, with `Abort` from any active state.
- Validation for operation identity, configured source/target workers, source ownership of the cell, and invalid state transitions.
- `ListAssignments`/`WatchAssignments` include active handover status so runtime components can enforce the advertised policy.
- Source Worker enforcement for `Freeze`/`Diff`: client moves are placed in a bounded FIFO buffer instead of being immediately rejected, then replayed when the handover status is released.
- `Commit` requires the target Worker to be registered, transfers the cell assignment from source to target, and publishes a listing update so Gateway routing and Worker owned-cell state follow the new owner.
- Explicit failure policy: buffer overflow/TTL expiry returns a retryable error, unregistered targets keep the handover in `Diffing` while commit retry budget remains, exhausted commit retries abort before assignment transfer, and source Workers stop applying buffered moves after committed ownership transfer.
- Target-side replay: after a committed assignment transfer, the source Worker sends the target Worker an idempotent `HandoverReplay` payload with the current actor snapshot and non-expired buffered moves. The target applies the replay only if it owns the cell, ignores duplicate operation/cell replays, and marks replayed actors claimable by the first post-handover client session. When traffic flows through Gateway, that claim uses the Gateway-injected stable session id rather than the target Worker's local connection id.

Not implemented in this slice:
- Explicit source-to-target ownership manifest transfer.
- Automatic control-plane retry loops for failed handovers.

## Current Policy

The safe V0 handover policy is conservative:
- `PreCopy`: accepts a new active handover if the source worker currently owns the cell and no handover is active for that cell.
- `Freeze`: moves the active operation to frozen state. The advertised client move policy is `REJECT_DURING_FREEZE`; source Workers interpret that as bounded buffering for client moves on the source-owned cell.
- `Diff`: moves the active operation to diffing state. Client move policy remains `REJECT_DURING_FREEZE`; source Worker buffering remains active.
- `Commit`: requires the target Worker to be registered, moves the cell from source to target in the Orchestrator assignment map, clears the active handover, and publishes the updated listing. If the target Worker is not registered, the Orchestrator keeps the handover in `Diffing` for a bounded retry budget and then aborts before assignment transfer. Gateway updates its route table from the listing/watch path; existing client connections may reconnect upstream or close according to the current route-change safety policy.
- `Abort`: clears the active operation without changing assignments.

The Orchestrator response reports `assignments_changed=true` only for a successful `Commit` that moves the cell. `Abort`, retryable commit failures, and retry-budget aborts leave assignments unchanged. Source Workers replay buffered moves when the policy is released and they still own the cell, such as on `Abort`; after a successful `Commit`, the source Worker sees the cell removed from its owned set and sends actor snapshot plus buffered moves to the target Worker. Expired buffered moves still return `handover_move_expired`, and missing/stale target replay routes return replay-target errors to the source-side client response channel when possible.

## Next Slice

Explicit ownership transfer should be implemented separately from target replay:
- Include source session ownership for replayed actors in the handover payload.
- Replace target claim-on-first-use with explicit transferred ownership.
- Keep the current Gateway stable session id as the compatibility path for client frames that arrive immediately after route switch.

Source-side buffering, target replay, and Gateway stable sessions reduce the user-visible handover glitch for normal short freeze/diff windows, and route switch now moves the authoritative owner. They do not remove every possible error path. Long freezes, exhausted buffers, missing target routes, and unavailable replay targets still need explicit reject or abort behavior.
