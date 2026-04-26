# Tessera P2 Observability and Packaging Todo

Last reviewed: 2026-04-26

## Baseline

P0/P1 runtime milestones are complete through handover replay ownership, AOI
precision controls, per-cell tick flush structure, and dynamic split/merge design
notes. The remaining work is operational hardening rather than core protocol
shape.

No immediate user escalation is required. Escalate before adding provider- or
cluster-specific deployment manifests, because Kubernetes target conventions and
scrape annotations should match the actual cluster.

## P2.1 Long-lived quality loop

Goal: make observability regressions discoverable without relying on manual
inspection.

Status: done 2026-04-26.

Implemented slice:

1. `cargo xt dev metrics-smoke` starts the dev stack with Orchestrator, Gateway,
   and Worker metrics ports enabled.
2. It scrapes `/metrics` from all enabled components.
3. It asserts core metric families exist and contain parseable numeric samples.
4. It remains separate from default `cargo xt` for now, so local and CI runtime
   gates can opt in explicitly.

Completion conditions:

- Metrics smoke fails on missing endpoint, invalid text format, or missing core
  metric family.
- README documents the command and expected local ports.
- `cargo xt`, `cargo test`, and the metrics smoke pass.

## P2.2 Gateway readiness and reconnect metrics

Goal: make route health and reconnect behavior visible from Gateway metrics and
logs.

Status: done 2026-04-26.

Implemented slice:

1. `GET /ready` is served by the Gateway metrics listener when
   `TESSERA_GW_METRICS_ADDR` is set.
2. Readiness is derived from the current routing table: at least one loaded
   route is ready; an empty route table returns HTTP 503.
3. Gateway Prometheus output includes `tessera_gateway_ready`, upstream connect
   attempts, route-change reconnects, and client close reason counters.
4. Close logs include a `close_reason` field for retry exhaustion, no route,
   pending ping route change, and ambiguous upstream state.
5. `cargo xt dev metrics-smoke` asserts Gateway `/ready` in addition to core
   `/metrics` families.

Completion conditions:

- Readiness endpoint reports healthy only when at least one route is available.
- Reconnect-required close/reconnect paths have explicit metric counters.
- Unit tests cover metrics text and readiness state.

Deferred:

- Request/round-trip latency buckets need a simple histogram format for the
  current hand-rolled Prometheus exporter. Keep this as a separate P2 slice to
  avoid mixing endpoint readiness with timing semantics.

## P2.3 Container and Kubernetes packaging

Goal: document a deployable shape without pretending production automation is
done.

Status: done 2026-04-26.

Implemented slice:

1. `Dockerfile` builds all runtime binaries into one sample runtime image.
2. `deploy/docker-compose.yml` runs Orchestrator, Worker, and Gateway with
   metrics endpoints enabled.
3. `deploy/kubernetes/tessera-sample.yaml` documents a non-production
   single-worker shape with Prometheus scrape annotations, Gateway readiness,
   TCP liveness checks, and Orchestrator config mount.
4. `docs/packaging.md` records build, smoke, and Kubernetes sample commands.

Completion conditions:

- README links to packaging examples.
- Examples are marked sample/non-production unless tied to a real deployment
  target.
- `cargo xt` validates documentation discoverability.

Deferred:

- Production manifests remain blocked on target cluster conventions: image
  registry, ingress/Service type, resource requests, rollout policy,
  PodDisruptionBudget, and Prometheus discovery.

Original recommendation:

1. Keep Dockerfile build path current for all runtime binaries.
2. Add example environment config for Gateway, Worker, and Orchestrator.
3. Add Kubernetes examples only as non-authoritative samples unless a target
   cluster convention is provided.
4. Include Prometheus scrape annotations, readiness/liveness endpoints, and
   config mount examples once endpoints exist.

## Suggested Order

All currently listed P2 slices are complete as of 2026-04-26.

## P2.4 Gateway latency histogram

Goal: expose a concrete latency histogram from the Gateway metrics endpoint
without inventing a broader correlation protocol.

Status: done 2026-04-26.

Implemented slice:

1. Gateway tracks client Ping to upstream Pong round-trip latency.
2. Prometheus output uses a conventional histogram family:
   `tessera_gateway_ping_roundtrip_seconds_bucket`,
   `tessera_gateway_ping_roundtrip_seconds_sum`, and
   `tessera_gateway_ping_roundtrip_seconds_count`.
3. Bucket boundaries are fixed at 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms,
   500ms, 1s, 2.5s, and 5s.
4. `cargo xt dev metrics-smoke` asserts the histogram family exists.

Deferred:

- Non-Ping request latency remains deferred until the protocol carries an
  explicit request id or response correlation key. Snapshot/Delta frames may
  also be broadcast traffic, so FIFO matching would overcount unrelated pushes.
