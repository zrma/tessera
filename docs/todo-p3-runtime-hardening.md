# Tessera P3 Runtime Hardening Todo

Last reviewed: 2026-04-26

## Baseline

P0/P1 runtime milestones and P2 observability/packaging slices are complete:
handover replay ownership, stable Gateway sessions, AOI precision controls,
per-cell tick structure, metrics/readiness endpoints, sample packaging, and
Gateway Ping/Pong latency histogram.

The next milestone should prefer narrow runtime hardening slices before larger
V1/V2 rebalancing work. No immediate user escalation is required for local
smoke/test hardening or inactive planner modeling. Escalate before production
manifests, protocol-wide request correlation changes, or automatic split/merge
policy activation.

## P3.1 Metrics smoke exercises latency path

Goal: make `cargo xt dev metrics-smoke` prove that the Gateway latency histogram
is not only exported but also increments on a real Ping/Pong path.

Status: done 2026-04-26.

Implemented slice:

1. Start the metrics-enabled dev stack as it does today.
2. Run `tessera-client ping --ts <fixed>` against the Gateway.
3. Re-scrape Gateway `/metrics`.
4. Assert `tessera_gateway_ping_roundtrip_seconds_count` is at least `1`.

Completion conditions:

- `cargo xt dev metrics-smoke` fails if ping traffic does not reach the
  histogram.
- `cargo xt`, `cargo test`, and local runtime smoke pass.
- README keeps the metrics-smoke description aligned with the stronger check.

## P3.2 Non-Ping request latency correlation

Goal: measure Join/Move request latency without confusing request replies with
broadcast Snapshot/Delta frames.

Escalation required before implementation.

Open decision:

- Add a protocol-level request id/correlation key, or keep Gateway metrics scoped
  to Ping/Pong RTT until the protocol envelope changes for another reason.

Why this needs a decision:

- Worker responses and broadcast traffic currently share the same `ServerMsg`
  envelope shape. FIFO matching in the Gateway would overcount unrelated pushes.

## P3.3 Production deployment manifests

Goal: turn the sample Kubernetes manifest into a real deployment profile.

Escalation required before implementation.

Open decisions:

- Target registry and image tag convention.
- Ingress or Service type.
- Prometheus discovery convention.
- Resource requests/limits, rollout policy, and PodDisruptionBudget.
- Namespace, labels, and config management convention.

## P3.4 Split/merge planner skeleton

Goal: begin V2 dynamic split/merge safely with deterministic planner tests before
runtime ownership changes.

Status: done 2026-04-26 for the inactive planner skeleton.

Implemented first slice:

1. Add an Orchestrator-local planner model that ranks split candidates from a
   metrics snapshot.
2. Encode hysteresis/cooldown and churn-budget rejection.
3. Keep planner output inactive; do not publish assignment changes yet.
4. Verify deterministic ranking and budget rejection with unit tests.

Completion conditions:

- `docs/dynamic-split-merge.md` invariants remain true.
- No assignment listing/watch behavior changes until a later explicit runtime
  slice.

Deferred:

- Runtime split/merge activation still requires explicit approval.
- Multi-depth parent/child semantics and real metrics ingestion remain future
  slices.

## P3.5 Merge planner skeleton

Goal: extend the inactive split/merge planner so it can rank safe merge
candidates for complete sibling sets without publishing assignment changes.

Status: done 2026-04-26.

Implemented slice:

1. Add merge-specific low-water thresholds and sustained low-pressure windows to
   the Orchestrator-local planner config and metrics model.
2. Emit inactive `Merge` plans only when a complete sibling set is cold, old
   enough, out of cooldown, and has no active handover.
3. Charge merge plans against the existing churn budget by handover operations
   and moved cells.
4. Keep planner output disconnected from assignment listing/watch.

Completion conditions:

- Deterministic tests cover complete sibling validation, low-water hysteresis,
  budget rejection, active-plan overlap rejection, and listing non-interference.
- `docs/dynamic-split-merge.md` verification notes list merge candidate
  validation as covered by the inactive skeleton.
- `cargo xt`, `cargo test`, and the local runtime smoke pass.

Deferred:

- Runtime merge activation, target worker selection, real rolling metrics
  ingestion, and multi-depth child encoding beyond the current `CellId.depth/sub`
  shape still require a later explicit runtime slice.

## P3.6 Split/merge dry-run preview

Goal: expose the inactive planner through an operator-visible dry-run endpoint
without changing assignments or starting handovers.

Status: done 2026-04-26.

Implemented slice:

1. Add `GET /split-merge/preview` to the Orchestrator HTTP listener that is
   enabled by `TESSERA_ORCH_METRICS_ADDR`.
2. Return JSON with `mode="dry_run"`, `assignments_changed=false`, and planner
   output ranked from a metrics snapshot.
3. Use `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON` or
   `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_PATH` as optional dry-run inputs. When both
   are unset, preview the current assignment listing as a zero-metric snapshot.
4. Extend `cargo xt dev metrics-smoke` to assert the preview endpoint is served.

Completion conditions:

- Preview never mutates assignment listing/watch state.
- Invalid preview snapshot config fails the endpoint instead of publishing a
  partial plan.
- `cargo xt`, `cargo test`, and the local runtime smoke pass.

Deferred:

- Runtime split/merge activation, real metrics ingestion, target worker
  selection, and production exposure policy remain separate follow-up slices.
