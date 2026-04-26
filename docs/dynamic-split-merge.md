# Dynamic Split/Merge Design Note

Last reviewed: 2026-04-26

## Scope

This note fixes the V2 direction for quadtree-style cell split/merge. It is a
design contract only. The Orchestrator now has an inactive split planner
skeleton with deterministic ranking, hysteresis/cooldown, churn-budget, and
overlap tests, but runtime split/merge, automatic rebalancing, merge planning,
real metrics ingestion, and multi-depth assignment changes are not implemented
yet.

The design assumes the existing V0/V1 foundations stay intact:

- `CellId` is the routing key and every cell has one authoritative Worker.
- Gateway routing follows Orchestrator `ListAssignments`/`WatchAssignments`.
- Cell movement uses the existing handover path before ownership changes.
- Worker AOI can be bounded by radius, visibility radius, and max-cell cap.

## Goals

- Split hot cells before one Worker's tick loop or relay fanout becomes the
  bottleneck.
- Merge cold sibling cells to reduce scheduling, routing, and AOI overhead.
- Keep route churn bounded so Gateway, Worker, and Orchestrator convergence stay
  observable.
- Prefer deterministic plans over reactive per-tick reshuffling.

## Non-goals

- No automatic multi-region migration.
- No hard real-time guarantee during split/merge windows.
- No direct mutation of assignments without the handover state machine.
- No deep quadtree encoding change until `CellId.depth/sub` semantics are made
  explicit for multiple levels.

## Split Inputs

Split decisions should use rolling windows rather than instantaneous samples:

- Actor count per cell.
- Per-cell move/broadcast queue pressure.
- Tick stage duration once per-cell tick metrics exist.
- Relay fanout and remote AOI subscriber count.
- Handover retry/error rate around the candidate cell.

Initial suggested thresholds:

- Candidate only after the cell exceeds at least two pressure signals for three
  consecutive windows.
- Minimum cell age before split: 60 seconds.
- Minimum cooldown after any split or merge touching the cell family: 120 seconds.

## Merge Inputs

Merge is only valid for a complete sibling set owned by compatible assignments.
For the first implementation, "compatible" should mean all siblings are assigned
to the same Worker or all can be moved through handover without violating the
global churn budget.

Initial suggested thresholds:

- All siblings remain below low-water actor, tick, and fanout thresholds for five
  consecutive windows.
- No active handover in the parent or sibling set.
- No recent split/merge cooldown on the parent.

## Hysteresis

Use separate high-water and low-water thresholds:

- Split at high pressure.
- Merge only after sustained low pressure.
- Never split and merge the same parent in the same planning epoch.
- A failed split/merge plan should place the cell family into cooldown before the
  planner retries.

This prevents load oscillation when actors hover around cell borders or when AOI
fanout briefly spikes.

## Assignment Churn Limits

The Orchestrator planner should enforce a hard budget before emitting commands:

- Max active split/merge plans per world.
- Max handover operations started per planning interval.
- Max cells moved per planning interval.
- Max sibling families in cooldown.
- No overlapping plans that touch the same parent, child, source Worker, and
  target Worker pair unless explicitly allowed by policy.

The first safe default should be one active split/merge plan per world and one
handover commit at a time per cell family.

## Runtime Sequence

1. Observe rolling metrics and build a deterministic candidate list.
2. Reserve a split/merge plan with an operation id and cooldown marker.
3. Materialize target assignments in Orchestrator memory but do not publish them
   as active ownership yet.
4. Use `PreCopy -> Freeze -> Diff -> Commit` handover for each ownership move.
5. Publish updated assignments through existing listing/watch paths.
6. Monitor replay, route switch, AOI subscription counts, and retry/error rates.
7. Clear the plan or mark it failed with cooldown.

## Required Invariants

- A client-visible route change must be backed by a committed assignment change.
- A source Worker must not drop actors before target replay and owner transfer are
  attempted.
- A target Worker must reject replay for cells it does not own.
- AOI subscriptions must be recalculated after a split/merge assignment update.
- Planner output must be reproducible from the same metrics snapshot and config.

## Verification Plan

Before runtime implementation, keep or extend tests for:

- Split candidate ranking and hysteresis. (initial inactive skeleton covered)
- Merge candidate validation for complete sibling sets.
- Churn budget rejection. (initial inactive skeleton covered)
- No overlapping active plans for the same cell family. (initial inactive
  skeleton covered for exact CellId families)
- Assignment listing shape before and after a simulated split/merge.

Runtime implementation should additionally run the existing full gate:

- `cargo xt`
- `cargo test`
- `cargo xt dev up --with-orch`
- `cargo run -p tessera-client -- ping --ts 123`
- `cargo xt dev down --with-orch`
