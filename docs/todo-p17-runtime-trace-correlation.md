# P17 Runtime Trace Correlation

Last reviewed: 2026-07-14

Status: active

## Objective

Turn the existing Gateway request ids and runtime tracing calls into bounded,
machine-checkable lifecycle evidence. P17 must prove that a Join or Move can be
correlated from Gateway forwarding through Worker handling and back to the
Gateway response without changing the wire protocol, exposing raw connection
inventory in an aggregate report, or turning local trace output into a
production telemetry policy.

P10 remains the owner of metrics, durable observability reports, and guarded
Kubernetes soak evidence. P17 is the narrower structured-trace and request
correlation regression lane.

## Architecture Boundary

- Compact human-readable logging remains the default. Structured JSON output is
  opt-in and configured consistently by Gateway, Worker, and Orchestrator.
- Correlation uses the existing Gateway-generated `session` and `request_id`
  envelope fields. P17 does not add a tracing header or change protocol schema.
- Runtime trace files are local-only diagnostic artifacts. The repository-owned
  smoke result contains aggregate event counts and correlation verdicts, not
  peer addresses, target endpoints, actor ids, or raw log lines.
- Trace events use stable event names and bounded scalar fields. Free-form
  request payloads and errors are excluded from the correlation contract.
- The local/CI profile remains bounded; external collectors, sampling policy,
  retention, alerting, and live-service tracing backends are out of scope.

## Milestone Queue

1. **Complete: opt-in structured runtime logging**
   - Add one shared `compact|json` configuration contract with compact as the
     default and fail-closed rejection of unsupported values.
   - Apply the same initializer behavior to Gateway, Worker, and Orchestrator.
   - Cover defaults, valid values, invalid values, and parseable startup JSON
     without retaining machine inventory in tracked artifacts.
   - Evidence: `RuntimeLogFormat` is shared through `tessera-core`; focused
     tests cover the exact configuration states; all three runtime crates
     reject unsupported values before binding and emitted parseable JSON during
     the bounded metrics smoke.
2. **Stable request lifecycle events**
   - Emit bounded Gateway forward/response and Worker receive/response events
     for request-id-bearing Join and Move operations.
   - Carry stable event name, component, operation kind, session id, request id,
     and cell coordinates while excluding actor, peer, address, payload, and raw
     error fields.
   - Add focused event-shape and direct-reply correlation tests.
3. **Two-Worker trace correlation smoke**
   - Add `cargo xt dev trace-correlation-smoke` with an isolated two-cell and
     two-Worker topology plus opt-in JSON logs.
   - Run a fixed simulator profile, parse only the stable lifecycle events, and
     prove every planned Join/Move request has exactly one ordered lifecycle on
     the correct cell owner.
   - Emit a compact privacy-safe aggregate verdict and remove raw logs with the
     managed dev stack.
4. **Bounded soak gate and closeout**
   - Repeat the correlation profile enough to catch missing/duplicate events
     while retaining explicit CI bounds.
   - Add the trace smoke to CI and `cargo xt harness`, document the command and
     non-goals, and close P17 only after remote CI executes the new lane.

## Verification

Every slice requires focused tests plus repository defaults:

```text
cargo test -p tessera-core
cargo test -p tessera-gateway
cargo test -p tessera-worker
cargo test -p tessera-orch
cargo test -p xtask
cargo xt
cargo test
```

Network/trace slices also require:

```text
cargo xt dev trace-correlation-smoke
```

Before each push, run both publication gates and wait for remote CI success.

## Completion Boundary

P17 is complete when all runtimes share a validated opt-in JSON format,
request-id-bearing Join/Move events correlate across Gateway and Worker in an
isolated two-Worker smoke, a bounded repeated profile runs in CI, and tracked
evidence remains aggregate and privacy-safe. Production telemetry backends and
policies remain outside this repository.
