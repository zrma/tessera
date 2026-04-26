# Tessera Handover Skeleton

Last reviewed: 2026-04-26

## Scope

This document describes the current handover control-plane skeleton and the first source-worker runtime enforcement slice. It does not claim that target routing migration is implemented yet.

Implemented now:
- `SubmitHandoverCommand` gRPC command API on the Orchestrator.
- Ordered state machine: `PreCopy -> Freeze -> Diff -> Commit`, with `Abort` from any active state.
- Validation for operation identity, configured source/target workers, source ownership of the cell, and invalid state transitions.
- `ListAssignments`/`WatchAssignments` include active handover status so runtime components can enforce the advertised policy.
- Source Worker enforcement for `Freeze`/`Diff`: client moves are placed in a bounded FIFO buffer instead of being immediately rejected, then replayed when the handover status is released.
- Explicit failure policy: assignments are unchanged by this skeleton, buffer overflow/TTL expiry returns a retryable error, and commit failure policy is source-route preservation plus abort.

Not implemented in this slice:
- Gateway routing switch to the target worker.
- Target-side replay after a routing switch.
- Automatic retry loops for failed handovers.

## Current Policy

The safe V0 handover policy is conservative:
- `PreCopy`: accepts a new active handover if the source worker currently owns the cell and no handover is active for that cell.
- `Freeze`: moves the active operation to frozen state. The advertised client move policy is `REJECT_DURING_FREEZE`; source Workers interpret that as bounded buffering for client moves on the source-owned cell.
- `Diff`: moves the active operation to diffing state. Client move policy remains `REJECT_DURING_FREEZE`; source Worker buffering remains active.
- `Commit`: completes the active operation and clears it from Orchestrator memory, but does not mutate the assignment map yet.
- `Abort`: clears the active operation without changing assignments.

The Orchestrator response always reports `assignments_changed=false` for this skeleton. Existing source ownership remains the source of truth, so buffered moves are replayed by the source Worker when `Commit` or `Abort` clears the active handover status.

## Next Slice

Routing switch and retry should be implemented separately from the source buffering slice:
- Change assignments only after target readiness is confirmed, then publish a listing update that lets Gateway route the cell to the target Worker.
- Move replay from source-side post-release replay to target-side replay after commit.
- Add bounded retry around commit/route switch and abort back to the source route on repeated failure.
- Preserve clear retryable errors for long freezes, buffer overflow, TTL expiry, and repeated commit failure.

Source-side buffering already reduces the user-visible handover glitch for normal short freeze/diff windows, but it does not remove every possible error path. Long freezes, repeated commit failures, exhausted buffers, and target route switch failures still need explicit reject or abort behavior.
