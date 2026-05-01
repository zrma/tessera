# Tessera P4 Next Milestones

Last reviewed: 2026-05-01

## Baseline

P0 through P4.1 are complete through handover replay ownership, stable Gateway
sessions, AOI precision controls, per-cell tick structure, observability
endpoints, packaging samples, split/merge planner skeletons, and fixture-backed
dry-run preview smoke, plus request-id-based Join/Move latency correlation.
P4.2 internal GitOps manifests are committed, pushed, synced by ArgoCD, and
runtime-smoked on the MicroK8s cluster. Completed milestone details are
archived in `docs/completed-milestones.md`.

The next substantial milestone crosses runtime assignment mutation.

## 2026-05-01 Decision Checkpoint

P4.2 defaults were accepted for the first internal-only deployment slice, the
cluster rollout was verified, and `v2026.05.1` was published by GitHub Actions
and promoted through GitOps. The remaining substantial branch is P4.3 runtime
split/merge activation.

The first P4.3 activation shape is fixed in `docs/dynamic-split-merge.md`:
split-only, manual submission, default-off feature flag, explicit target map,
and one-level `CellId` activation from `depth=0/sub=0` to `depth=1/sub=0..3`.

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

Status: internal-only manifest slice pushed, ArgoCD synced, and runtime-smoked
as of 2026-05-01.

Implemented first-slice defaults:

1. Bootstrapped `harbor.1day1coding.com/1day1coding/tessera:ec8c42b4` locally,
   then rebuilt the runtime image as `linux/amd64` after the initial local
   image was `linux/arm64` only.
2. Added namespace `tessera` with restricted pod-security labels and Istio
   revision label matching the current app namespace pattern.
3. Added one Orchestrator, one Worker, and one Gateway, all behind ClusterIP
   services; no public Gateway API routing for the custom
   `4000/TCP` client protocol yet.
4. Enabled metrics ports `6100`/`5100`/`4100`, Gateway `/ready`, and existing
   `prometheus.io/*` annotations.
5. Used conservative per-component requests (`cpu: 100m`, `memory: 128Mi`) and
   `256Mi` memory limits; deferred PodDisruptionBudget while each component has
   one replica.

k8s GitOps repo files added or updated:

- `k8s/apps/tessera/manifests/`
- `k8s/common/namespaces/tessera.yaml`
- `k8s/common/netpol/tessera.yaml`
- `k8s/argocd/project-tessera.yaml`
- `k8s/argocd/project-common.yaml`

Verification used for manifest prep and rollout:

```sh
make validate
docker buildx imagetools inspect harbor.1day1coding.com/1day1coding/tessera:ec8c42b4
kubectl -n argocd get app tessera -o wide
kubectl -n tessera get pods,svc,externalsecret -o wide
cargo run -p tessera-client -- ping --ts 123
curl http://127.0.0.1:4100/ready
curl http://127.0.0.1:6100/split-merge/preview
```

Image publish and GitOps promotion:

```sh
gh workflow run tessera.build-push.yml --ref main
```

The workflow builds `linux/amd64` on GitHub Actions and pushes a `vYYYY.MM.N`
tag to Harbor. The current promoted image tag is `v2026.05.1`.

Completed image-promotion checks:

1. GitHub Actions run published `v2026.05.1` to Harbor.
2. k8s GitOps manifest tag was updated to `v2026.05.1` and pushed.
3. ArgoCD Application `tessera` reached `Synced / Healthy`.
4. `kubectl -n tessera get pods,deploy,svc` showed all components ready on
   `v2026.05.1`.
5. Port-forwarded the internal Gateway and verified `tessera-client ping --ts
   123`.
6. Port-forwarded Orchestrator metrics and verified `/split-merge/preview`
   reports `assignments_changed=false`.

## P4.3 Runtime Split/Merge Activation

Status: first activation shape fixed; implementation not started.

Chosen first slice:

1. Split-only activation. Merge remains dry-run/design-only until a later
   milestone chooses merge rollback and sibling coalescing behavior.
2. Manual activation. Planner output can recommend candidates, but it must not
   auto-submit runtime mutations from observed metrics.
3. Default-off feature flag. Mutating activation should reject unless an
   explicit manual activation flag/config is enabled.
4. Explicit target map. The command must name targets for all four child
   `sub` values, validate configured/registered Workers, reject missing or
   duplicate children, and keep source-only no-op plans as dry-run output.
5. One-level `CellId` semantics. Only `depth=0/sub=0` parents split into
   `depth=1/sub=0..3` children with the quadrant convention recorded in
   `docs/dynamic-split-merge.md`; nested `depth>1` activation is rejected until
   a future encoding change.
6. All-or-nothing publication. Staged child assignments are private until
   replay succeeds, parent removal and child publication happen atomically, and
   pre-publication failures abort with parent assignment unchanged.
7. Post-publication convergence failures are surfaced with cooldown and manual
   recovery; the first slice does not automatically merge back.

Suggested implementation after approval:

1. Add the manual activation command/API and feature flag.
2. Materialize planned target child assignments without publishing them.
3. Drive parent freeze, child replay, and owner/session transfer through the
   existing handover/replay contracts or an explicitly equivalent split replay
   path.
4. Publish child assignments only after all child replay paths succeed, and
   remove the parent assignment in the same publication step.
5. Add target validation, depth validation, route convergence, AOI resync,
   atomic rollback, and post-publish failure tests before enabling the flag in
   any environment.

Verification required for the implementation milestone:

```sh
cargo xt
cargo test
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

The implementation should also extend the existing split/merge preview smoke or
add a new activation smoke so the default-off flag, manual validation, successful
split, and failed replay/target paths are covered by automated evidence.

## Recommendation

Choose the P4.3 implementation milestone only when runtime assignment mutation
is the immediate priority. The approved first shape is intentionally narrow:
split-only, manual, default-off, one-level `CellId`, and no automatic merge
rollback.
