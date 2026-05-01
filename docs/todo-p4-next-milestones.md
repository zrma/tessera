# Tessera P4 Next Milestones

Last reviewed: 2026-05-01

## Baseline

P0 through P4.1 are complete through handover replay ownership, stable Gateway
sessions, AOI precision controls, per-cell tick structure, observability
endpoints, packaging samples, split/merge planner skeletons, and fixture-backed
dry-run preview smoke, plus request-id-based Join/Move latency correlation.
Completed milestone details are archived in
`docs/completed-milestones.md`.

There is no additional autonomous implementation slice at this planning point
after P4.1. The next substantial milestones cross either production cluster
policy or runtime assignment mutation.

## 2026-05-01 Decision Checkpoint

No safe autonomous implementation slice remains before choosing the next P4
branch.
Pick one of these directions to unblock the next commit-sized milestone:

1. P4.2 for cluster rollout: provide the live GitOps target details for image,
   namespace, Gateway exposure, Prometheus discovery, resources, and rollout
   policy.
2. P4.3 for runtime split/merge: approve the first activation shape, target
   worker policy, multi-depth `CellId` semantics, and manual-vs-automatic plan
   submission.

Default recommendation is P4.2 if operating Tessera on the existing cluster is
the immediate goal. Otherwise keep P4.3 gated until runtime split/merge
semantics are explicitly chosen.

## P4.1 Non-Ping Request Latency Correlation

Status: complete as of 2026-05-01.

Decision chosen:

- Add optional envelope-level `request_id` correlation. Gateway assigns it to
  Join/Move requests, Workers echo it only on direct replies, and broadcast
  traffic keeps it unset.

Why explicit correlation was required:

- Join/Move responses and broadcast Snapshot/Delta traffic currently share the
  same `ServerMsg` stream. Gateway-side FIFO matching would count unrelated
  broadcast traffic as request latency.

Decision options considered:

1. Keep Ping/Pong-only latency metrics and leave non-Ping latency deferred.
   This avoids protocol churn, but does not advance P4.1 beyond the current
   baseline.
2. Infer Join/Move latency by FIFO response order at the Gateway. This is not
   recommended because broadcast `Snapshot`/`Delta`/`Despawn` frames can arrive
   between a request and its direct reply.
3. Reuse existing `seq`/`epoch` as a correlation key. This is not recommended
   because those fields currently describe stream ordering and connection epoch,
   not a reply-echo contract.
4. Add request ids to individual `ServerMsg` payload variants. This is possible
   but broadens the payload surface and mixes direct-reply metadata into
   broadcast message shapes.
5. Add separate Join/Move acknowledgement payloads. This gives the cleanest
   semantic split, but changes client-visible behavior more than the first P4.1
   baseline needs.
6. Recommended: add an optional envelope-level `request_id` that the Gateway
   assigns to Join/Move requests and Workers echo only on direct replies. Keep
   serde defaults/skip-when-none for backward-compatible JSON frames, and do not
   attach request ids to broadcast traffic.

Completed implementation:

1. Added optional `request_id` correlation to `ClientEnvelope` and server reply
   envelopes.
2. Preserved backward-compatible decode defaults for existing scripted clients.
3. Gateway tracks Join/Move latency only when the reply `request_id` matches a
   pending request.
4. Tests and `cargo xt dev metrics-smoke` cover the new metric path.

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

Choose P4.2 next if the goal is operating Tessera on the existing cluster.
Choose P4.3 only when runtime split/merge semantics are the immediate priority;
it should remain gated until target worker policy and assignment mutation rules
are explicit.
