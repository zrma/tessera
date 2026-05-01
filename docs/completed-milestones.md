# Tessera Completed Milestones

Last reviewed: 2026-05-01

This document records completed milestone plans that used to live in active
`docs/todo-*` files. Current open work should stay in `docs/todo-next.md` and
`docs/todo-p4-next-milestones.md`.

## P0/P1 Runtime Baseline

Status: complete as of 2026-04-26.

Completed slices:

1. Orchestrator Prometheus exporter.
2. Worker/Gateway relay observability.
3. Handover command protocol skeleton.
4. Handover source move buffering.
5. Handover commit route switch.
6. Handover target-side replay and commit retry.
7. Stable session handover baseline.
8. Explicit ownership transfer.
9. AOI precision upgrade.
10. Multi-cell tick pipeline.
11. Dynamic split/merge design note.

Verification used for these slices included focused Orchestrator/Worker tests,
`cargo xt`, `cargo test`, and the local runtime smoke:

```sh
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

## Handover Replay Details

Status: complete as of 2026-04-26.

The completed handover replay work established these runtime guarantees:

1. Orchestrator commit retry budget keeps unregistered-target commits in
   `Diffing` while budget remains and aborts before assignment transfer once the
   budget is exhausted.
2. Source Workers send a worker-to-worker `HandoverReplay` payload after a
   successful ownership transfer, including actor snapshot, owner session
   manifest, and non-expired buffered moves.
3. Target Workers apply replay only when they own the cell, preserve FIFO move
   order, install owner sessions before post-handover traffic, and ignore
   duplicate operation/cell replays.
4. Gateway injects a stable session id per client connection so ownership can
   survive route changes without relying only on claim-on-first-use fallback.

Known remaining handover limits are documented in `docs/handover.md`: long
freeze windows, exhausted buffers, missing target routes, and unavailable replay
targets still need explicit reject or abort behavior.

## P2 Observability And Packaging

Status: complete as of 2026-04-26.

Completed slices:

1. `cargo xt dev metrics-smoke` starts the dev stack with Orchestrator, Gateway,
   and Worker metrics ports enabled, scrapes `/metrics`, and validates core
   metric families with numeric samples.
2. Gateway readiness and reconnect observability adds `GET /ready`,
   `tessera_gateway_ready`, upstream connect attempts, route-change reconnects,
   close reason counters, and close reason log fields.
3. Packaging samples add a multi-binary `Dockerfile`,
   `deploy/docker-compose.yml`, a non-production Kubernetes sample, and
   `docs/packaging.md`.
4. Gateway Ping/Pong latency histogram exposes
   `tessera_gateway_ping_roundtrip_seconds_*` and is asserted by metrics smoke.

Deferred from P2:

- Production manifests remain blocked on target cluster conventions.

## P3 Runtime Hardening

Status: complete as of 2026-04-28.

Completed slices:

1. Metrics smoke latency path proves the Ping/Pong histogram increments on a
   real Gateway path.
2. Split planner skeleton ranks inactive split candidates from a metrics
   snapshot with hysteresis, cooldown, churn-budget, and overlap tests.
3. Merge planner skeleton ranks safe inactive merge candidates for complete cold
   sibling sets without publishing assignment changes.
4. Split/merge dry-run preview exposes `GET /split-merge/preview` on the
   Orchestrator metrics listener with `mode="dry_run"` and
   `assignments_changed=false`.
5. Split/merge preview fixture smoke starts the Orchestrator with
   `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON` and verifies a non-empty dry-run
   split plan.

P3 intentionally did not implement runtime assignment mutation, production
exposure policy, target worker selection, or real rolling metrics ingestion.
Those remain P4 decision gates.

## P4.1 Non-Ping Request Latency Correlation

Status: complete as of 2026-05-01.

Completed slices:

1. `ClientEnvelope` accepts optional `request_id` with serde defaults for
   backward-compatible JSON frames.
2. Worker client replies are encoded as `ServerEnvelope` and echo `request_id`
   only on direct replies. Broadcast/AOI traffic keeps `request_id` unset.
3. Gateway assigns request ids to Join/Move requests, tracks pending requests,
   and records latency only when a server reply echoes the matching id.
4. Gateway Prometheus metrics expose
   `tessera_gateway_request_roundtrip_seconds{kind="join|move"}`.
5. `cargo xt dev metrics-smoke` exercises Ping, Join, and Move through the real
   Gateway/Worker path and asserts both Ping/Pong and Join/Move latency
   histograms increment.

Verification used for this slice:

```sh
cargo test
cargo xt
cargo xt dev metrics-smoke
```

## Active Follow-Up

Open P4 work now starts after P4.1:

1. Production Kubernetes manifests.
2. Runtime split/merge activation.

Use `docs/todo-next.md` for the current open-work index and
`docs/todo-p4-next-milestones.md` for the decision gates.
