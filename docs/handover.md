# Tessera Handover Skeleton

Last reviewed: 2026-04-26

## Scope

This document describes the current handover control-plane skeleton. It does not claim that runtime routing migration is implemented yet.

Implemented now:
- `SubmitHandoverCommand` gRPC command API on the Orchestrator.
- Ordered state machine: `PreCopy -> Freeze -> Diff -> Commit`, with `Abort` from any active state.
- Validation for operation identity, configured source/target workers, source ownership of the cell, and invalid state transitions.
- Explicit response policy: assignments are unchanged by this skeleton, client moves are rejected during `Freeze`/`Diff`, and commit failure policy is source-route preservation plus abort.

Not implemented in this slice:
- Gateway routing switch to the target worker.
- Worker-side freeze enforcement.
- Buffered client move replay.
- Automatic retry loops for failed handovers.

## Current Policy

The safe V0 handover policy is conservative:
- `PreCopy`: accepts a new active handover if the source worker currently owns the cell and no handover is active for that cell.
- `Freeze`: moves the active operation to frozen state. The advertised client move policy is `REJECT_DURING_FREEZE`.
- `Diff`: moves the active operation to diffing state. Client move policy remains `REJECT_DURING_FREEZE`.
- `Commit`: completes the active operation and clears it from Orchestrator memory, but does not mutate the assignment map yet.
- `Abort`: clears the active operation without changing assignments.

The Orchestrator response always reports `assignments_changed=false` for this skeleton. Existing source ownership remains the source of truth.

## Next Slice

Move buffering and retry should be implemented separately from the skeleton:
- Buffer client move requests during the short `Freeze`/`Diff` window instead of immediately surfacing errors.
- Bound the buffer by cell/client count and elapsed time.
- Replay buffered moves in request order after a successful commit.
- If commit retry fails or the buffer overflows, preserve the source owner/route and surface a clear retryable error.

This should reduce the user-visible handover glitch for normal short migrations, but it will not remove every possible error path. Long freezes, repeated commit failures, and exhausted buffers still need explicit reject or abort behavior.
