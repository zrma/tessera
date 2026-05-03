# Multi-Depth CellId Decision

Last reviewed: 2026-05-03

Status: decided; core helpers and family validation are implemented, runtime
activation is not implemented.

## Problem

The current runtime split/merge slice uses a shallow `CellId` convention:

- Parent: `{world, cx, cy, depth: 0, sub: 0}`
- Children: same `world/cx/cy`, `depth: 1`, `sub: 0..=3`

That is sufficient for one split level, but it cannot represent an arbitrary
quadtree path. `sub` stores only the local quadrant, so `depth=2, sub=3` does
not say whether the path is `0->3`, `1->3`, `2->3`, or `3->3`.

## Decision

Use leaf-resolution coordinates as the canonical multi-depth cell key:

```text
CellId {
  world,
  cx: leaf_x_at_depth,
  cy: leaf_y_at_depth,
  depth,
  sub: 0
}
```

At `depth=d`, `cx/cy` are coordinates in the grid whose resolution is
`2^d` times the depth-0 grid. A depth-0 root cell keeps today’s
`CellId::grid(world, cx, cy)` shape. Splitting a parent `(cx, cy, depth=d)`
creates four canonical children at `depth=d+1`:

- lower-left: `(cx * 2, cy * 2, depth + 1)`
- lower-right: `(cx * 2 + 1, cy * 2, depth + 1)`
- upper-left: `(cx * 2, cy * 2 + 1, depth + 1)`
- upper-right: `(cx * 2 + 1, cy * 2 + 1, depth + 1)`

`sub` remains in the wire shape for compatibility, but canonical multi-depth
cells set `sub=0`. The quadrant can be derived from the child coordinate parity
relative to its parent. Existing shallow `depth=1, sub=0..=3` cells are a legacy
alias for the current P4/P5/P6 local smoke reports and must not be mixed with
canonical leaf-coordinate children in the same published family.

## Consequences

- Parent lookup is `parent = (floor(cx / 2), floor(cy / 2), depth - 1)`.
- Sibling lookup is the four children of that parent at the child depth.
- Gateway route matching should prefer the most specific canonical cell that
  contains the requested position/cell once client routing exposes nested
  coordinates.
- Worker AOI geometry can use leaf-coordinate rectangles directly instead of
  path decoding.
- Persisted assignment state remains a full assignment map, but future
  validators must reject mixed legacy/canonical siblings in one family.

## Migration Gate

Already in place:

- `tessera-core::CellId` has canonical helper methods for `leaf`,
  `canonical_child`, `canonical_parent`, `canonical_quadrant`,
  `canonical_siblings`, `canonical_children`, and the current
  `legacy_shallow_child`/`legacy_shallow_children` aliases.
- `CellId::child_family_kind` classifies a child set as either the legacy
  shallow family or the canonical leaf-coordinate family, and rejects mixed,
  duplicate, or incomplete child sets.
- Orchestrator split/merge activation derives the current child family through
  the legacy `CellId` helpers and rejects nested legacy `sub`-only parent
  requests without mutating assignment state.
- `SplitChildTarget` now has optional explicit child `cell` support. The
  Orchestrator parser validates that explicit child cells form exactly one
  legacy shallow or canonical leaf-coordinate family, rejects mixed
  explicit-cell/sub target modes, and rejects mixed/duplicate/incomplete child
  families without mutating assignment state.
- Canonical split activation requests can publish through the same
  default-off/manual `SubmitSplitActivation` surface when the request supplies
  explicit child cells that form a complete canonical family. Unit coverage
  verifies replay-ack publication and persisted Orchestrator restart recovery
  for canonical children.
- `cargo xt split-activation` can submit canonical explicit child-cell requests
  with repeated `--target-cell world,cx,cy,depth,sub=worker-id` values. The
  helper rejects mixed legacy `--target` and explicit `--target-cell` modes.
- Worker split replay batch construction can now classify the requested child
  family and partition parent actors/owners by exact child `CellId`, so
  canonical children that all carry `sub=0` are no longer collapsed by the
  legacy `sub` map.
- Gateway assignment listing updates keep using exact `CellId` route keys, with
  regression coverage for replacing a canonical parent route with four
  canonical child routes.
- Worker assignment refresh has regression coverage for replacing an owned
  canonical parent with canonical children and clearing parent subscriptions and
  client root interests. AOI helper coverage also verifies canonical depth leaf
  neighbor selection for depth greater than the legacy shallow alias.
- Unit tests cover negative coordinates, sibling derivation, rejection of
  legacy `sub`-only cells by canonical helpers, family validation, and the
  Orchestrator legacy-only publish boundary plus canonical Worker replay batch
  partitioning, Gateway exact route updates, Worker ownership refresh, and
  canonical AOI neighbor selection.
- `cargo xt dev multi-depth-activation-smoke` covers the first local
  end-to-end canonical split success path for `CellId::leaf(0, -2, 3, 2)`.
  It publishes explicit child cells, waits for exact Gateway child routes,
  verifies child Ping, a stable-session post-split Move, remote AOI resync, and
  route-change/relay metrics, then writes
  `.dev/reports/multi-depth-activation-smoke-latest.json`.
- `cargo xt dev multi-depth-activation-report-check` validates that local
  canonical success report shape.
- `cargo xt dev multi-depth-activation-failure-smoke` covers the local
  canonical target outage/recovery path. It keeps the published child
  assignment family in the Orchestrator listing, observes the target Worker
  child routes failing while source Worker child routes continue to answer, then
  restarts the target Worker and verifies all exact canonical child routes
  recover. `cargo xt dev multi-depth-activation-failure-report-check` validates
  that evidence.
- `cargo xt dev multi-depth-activation-restart-smoke` covers the local
  canonical restart path. It persists the canonical child assignment map,
  restarts the Orchestrator without the manual activation flag, verifies the
  recovered child listing, Worker assignment refresh, Gateway exact route
  convergence, child Ping, and remote AOI interest resync metrics.
  `cargo xt dev multi-depth-activation-restart-report-check` validates that
  evidence.
- `cargo xt dev multi-depth-activation-soak` covers the local canonical
  load/soak path. It publishes the same canonical split and runs sustained
  Ping/Move traffic against each exact child cell while asserting route
  convergence retention, remote AOI frame observation, Gateway latency
  histogram growth, and zero Gateway client close counters. `cargo xt dev
  multi-depth-activation-soak-report-check` validates that evidence.

Before claiming multi-depth runtime split activation complete:

1. Keep existing depth-1 legacy behavior for current P5 reports and helper
   defaults.
2. Add Worker merge sibling detection tests with canonical depth-2 cells before
   canonical merge activation is considered.
3. Extend internal MicroK8s helpers and report checkers before any internal
   multi-depth mutation is allowed.

## Non-Goals

- No automatic planner mutation is enabled by this decision.
- No cross-Worker merge replay is added by this decision.
- No persisted Worker actor state is implied by this decision.
