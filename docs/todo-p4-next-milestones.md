# Tessera P4 Next Milestones

Last reviewed: 2026-04-29

## Baseline

P0 through P3 are complete through handover replay ownership, stable Gateway
sessions, AOI precision controls, per-cell tick structure, observability
endpoints, packaging samples, split/merge planner skeletons, and fixture-backed
dry-run preview smoke. Completed milestone details are archived in
`docs/completed-milestones.md`.

There is no uncommitted implementation work at this planning point. The next
substantial milestones all cross a decision boundary: protocol shape, production
cluster policy, or runtime assignment mutation.

## P4.1 Non-Ping Request Latency Correlation

Status: escalation required before implementation.

Decision needed:

- Add a protocol-level request id/correlation key to client/server envelopes, or
  keep Gateway latency metrics scoped to Ping/Pong RTT until another protocol
  change justifies the correlation field.

Why it is blocked:

- Join/Move responses and broadcast Snapshot/Delta traffic currently share the
  same `ServerMsg` stream. Gateway-side FIFO matching would count unrelated
  broadcast traffic as request latency.

Suggested implementation after approval:

1. Add explicit request correlation to `ClientEnvelope`/server replies.
2. Preserve backward-compatible decode defaults for existing scripted clients.
3. Track Gateway latency for Join/Move only when the reply correlation matches.
4. Extend tests and `cargo xt dev metrics-smoke`.

## P4.2 Production Kubernetes Manifests

Status: escalation required before implementation.

Decision needed:

- Target image registry and tag convention.
- Namespace and label convention in the live `../k8s` GitOps repo.
- Service exposure for Gateway TCP ingress.
- Prometheus discovery convention.
- Resource requests/limits, rollout policy, and PodDisruptionBudget.

Suggested implementation after approval:

1. Convert `../k8s/docs/todo-tessera-deploy/` decisions into raw manifests under
   the k8s repo's app layout.
2. Keep the first profile internal-only unless Gateway TCP ingress is explicitly
   selected.
3. Validate with the k8s repo policy checks before any ArgoCD sync.

## P4.3 Runtime Split/Merge Activation

Status: escalation required before implementation.

Decision needed:

- Whether the first runtime activation should support split-only, merge-only, or
  both.
- Target worker selection policy.
- Multi-depth `CellId.depth/sub` semantics beyond the current shallow shape.
- Whether runtime plans are manually submitted first or automatically emitted
  from observed metrics.

Suggested implementation after approval:

1. Keep assignment mutation behind an explicit runtime flag or manual command.
2. Materialize planned target assignments without publishing them.
3. Drive each ownership move through the existing handover state machine.
4. Publish assignments only after successful commit/replay.
5. Add route convergence, AOI resync, and rollback/error-path tests before
   enabling any automatic policy.

## Recommendation

The safest next milestone is P4.1 if the goal is local code progress, because it
keeps deployment and assignment mutation out of scope. If the goal is operating
Tessera on the existing cluster, choose P4.2 first. P4.3 should come last unless
runtime split/merge semantics are the immediate priority.
