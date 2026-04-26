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

## P2.2 Gateway latency and readiness

Goal: make route health and reconnect behavior visible from Gateway metrics and
logs.

Recommended slice:

1. Add a lightweight readiness endpoint when `TESSERA_GW_METRICS_ADDR` is set.
2. Track upstream connect attempts, connect failures, reconnect-required route
   switches, and close reasons as counters.
3. Add request/round-trip latency buckets only after choosing a simple histogram
   format that matches the current hand-rolled Prometheus text exporter.

Completion conditions:

- Readiness endpoint reports healthy only when at least one route is available
  or fallback routing is configured.
- Reconnect-required close/reconnect paths have explicit metric counters.
- Unit tests cover metrics text and readiness state.

## P2.3 Container and Kubernetes packaging

Goal: document a deployable shape without pretending production automation is
done.

Recommended slice:

1. Keep Dockerfile build path current for all runtime binaries.
2. Add example environment config for Gateway, Worker, and Orchestrator.
3. Add Kubernetes examples only as non-authoritative samples unless a target
   cluster convention is provided.
4. Include Prometheus scrape annotations, readiness/liveness endpoints, and
   config mount examples once endpoints exist.

Completion conditions:

- README links to packaging examples.
- Examples are marked sample/non-production unless tied to a real deployment
  target.
- `cargo xt` validates documentation discoverability.

## Suggested Order

1. P2.2 Gateway readiness and reconnect metrics.
2. P2.3 packaging examples after readiness endpoints exist.
