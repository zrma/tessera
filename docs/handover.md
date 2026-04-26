# Tessera Handover Runtime

Last reviewed: 2026-04-26

## Scope

This document describes the current handover control-plane and the first runtime enforcement slices. Target routing migration is implemented through the assignment listing path; target-side replay and automatic retry are still separate follow-up work.

Implemented now:
- `SubmitHandoverCommand` gRPC command API on the Orchestrator.
- Ordered state machine: `PreCopy -> Freeze -> Diff -> Commit`, with `Abort` from any active state.
- Validation for operation identity, configured source/target workers, source ownership of the cell, and invalid state transitions.
- `ListAssignments`/`WatchAssignments` include active handover status so runtime components can enforce the advertised policy.
- Source Worker enforcement for `Freeze`/`Diff`: client moves are placed in a bounded FIFO buffer instead of being immediately rejected, then replayed when the handover status is released.
- `Commit` requires the target Worker to be registered, transfers the cell assignment from source to target, and publishes a listing update so Gateway routing and Worker owned-cell state follow the new owner.
- Explicit failure policy: buffer overflow/TTL expiry returns a retryable error, unregistered targets keep the handover in `Diffing` while commit retry budget remains, exhausted commit retries abort before assignment transfer, and source Workers stop applying buffered moves after committed ownership transfer.

Not implemented in this slice:
- Target-side replay after a routing switch.
- Automatic retry loops for failed handovers.

## Current Policy

The safe V0 handover policy is conservative:
- `PreCopy`: accepts a new active handover if the source worker currently owns the cell and no handover is active for that cell.
- `Freeze`: moves the active operation to frozen state. The advertised client move policy is `REJECT_DURING_FREEZE`; source Workers interpret that as bounded buffering for client moves on the source-owned cell.
- `Diff`: moves the active operation to diffing state. Client move policy remains `REJECT_DURING_FREEZE`; source Worker buffering remains active.
- `Commit`: requires the target Worker to be registered, moves the cell from source to target in the Orchestrator assignment map, clears the active handover, and publishes the updated listing. If the target Worker is not registered, the Orchestrator keeps the handover in `Diffing` for a bounded retry budget and then aborts before assignment transfer. Gateway keeps the client socket and reconnects upstream on the next routed frame when the cell route changes.
- `Abort`: clears the active operation without changing assignments.

The Orchestrator response reports `assignments_changed=true` only for a successful `Commit` that moves the cell. `Abort`, retryable commit failures, and retry-budget aborts leave assignments unchanged. Source Workers replay buffered moves when the policy is released and they still own the cell, such as on `Abort`; after a successful `Commit`, the source Worker sees the cell removed from its owned set and rejects drained buffered entries with `handover_cell_not_owned` until target-side replay exists.

## Next Slice

Target-side replay and retry should be implemented separately from the route switch slice:
- Move replay from source-side post-release replay to a target-side ingest/replay path after commit.
- Add bounded retry around commit orchestration and abort back to the source route on repeated failure before assignment transfer.
- Preserve clear retryable errors for long freezes, buffer overflow, TTL expiry, unregistered target, and repeated commit failure.

Source-side buffering already reduces the user-visible handover glitch for normal short freeze/diff windows, and route switch now moves the authoritative owner. It does not remove every possible error path. Long freezes, repeated commit failures, exhausted buffers, and missing target replay still need explicit reject or abort behavior.
