# Tessera P4 Next Milestones

Last reviewed: 2026-05-02

## Baseline

P0 through P4.1 are complete through handover replay ownership, stable Gateway
sessions, AOI precision controls, per-cell tick structure, observability
endpoints, packaging samples, split/merge planner skeletons, and fixture-backed
dry-run preview smoke, plus request-id-based Join/Move latency correlation.
P4.2 internal GitOps manifests are committed, pushed, synced by ArgoCD, and
runtime-smoked on the MicroK8s cluster. Completed milestone details are
archived in `docs/completed-milestones.md`.

The current P4.3 implementation has crossed from private split staging into
manual runtime assignment publication, replay, and a local two-Worker
convergence smoke. The next substantial decision is post-publish convergence
failure handling and whether merge activation should remain design-only.

## 2026-05-01 Decision Checkpoint

P4.2 defaults were accepted for the first internal-only deployment slice, the
cluster rollout was verified, and `v2026.05.1` was published by GitHub Actions
and promoted through GitOps. P4.3 has now added the default-off manual
replay/publish slice.

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

Status: default-off manual replay/publish slice plus planner-to-operator plan
evidence, local route convergence, remote AOI resync, post-publish target
outage/restart recovery smoke, and local load/soak observation smoke
implemented; P5 rollback policy is
`operator_recovery_no_automatic_merge_rollback_v1`.
`cargo xt k8s activation-smoke` now provides the internal MicroK8s
port-forwarded operator helper, but the actual cluster evidence remains open
until the approved two-Worker GitOps topology and image are synced.
The 2026-05-02 plan-only run against the current live topology stopped before
mutation with `no_split_candidate`, because the live preview source is
`assignment_listing_zero_metrics` and the cluster has no controlled pressure
fixture or real metrics candidate.

Chosen first slice:

1. Split-only activation. Merge remains dry-run/design-only until a later
   milestone chooses sibling coalescing behavior and a separate merge activation
   safety model.
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
7. Post-publication convergence failures are surfaced as operator evidence with
   manual target Worker restoration. The first slice does not automatically
   merge back or enforce runtime cooldown state; controlled-smoke rollback is a
   GitOps backout of image/topology/fixture/flag changes.

Implemented slices:

1. Add the manual activation command/API and feature flag.
2. Materialize planned target child assignments without publishing them.
3. Validate the full target map, parent depth/sub, configured and registered
   targets, source-only no-op maps, active family handovers, published child
   overlap, and already staged split families.
4. Keep failed activation attempts assignment-safe with
   `assignments_changed=false` and unchanged `ListAssignments` output.
5. Prepare target Workers for child replay before assignment publication.
6. Ask the source Worker to freeze the parent, fold buffered moves into the
   parent snapshot, partition actors/owners into child replay payloads, and send
   acked replay frames to every child target.
7. Publish the four child assignments and remove the parent assignment only
   after all child replay paths ack success.
8. Abort prepared target replay payloads and keep the parent assignment
   unchanged when prepare or source replay fails.

Remaining implementation:

1. Run internal-cluster activation evidence before enabling the flag in any
   environment beyond controlled smoke runs. The helper exists as
   `cargo xt k8s activation-smoke`, but the cluster topology/image/flag slice is
   not applied yet, and the smoke window also needs a controlled preview fixture
   or real metrics candidate.
2. Keep merge activation disabled until sibling coalescing and merge-runtime
   safety semantics are chosen in a later milestone.

Internal MicroK8s activation preflight on 2026-05-02:

1. `kubectl config current-context` reported `microk8s-ts`.
2. `kubectl -n tessera get deploy,po,svc,cm -o wide` showed
   `tessera-orch`, `tessera-worker`, and `tessera-gateway` all ready on
   `harbor.1day1coding.com/1day1coding/tessera:v2026.05.1`, with only one
   Worker deployment/service.
3. `../k8s/k8s/apps/tessera/manifests/tessera-runtime.yaml` configures only
   `worker-a` and does not set
   `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`.

Conclusion: internal activation smoke is intentionally blocked by the current
single-worker, preview-only GitOps slice. The next cluster slice must be
explicitly approved because it changes runtime topology: publish an image that
contains the activation evidence harness, add a second Worker/service and
Orchestrator target config, enable the manual activation flag for the controlled
smoke environment, sync ArgoCD, then run success/failure/recovery smoke through
port-forward or an in-cluster Job. The concrete command/checklist plan lives in
`docs/internal-microk8s-activation-smoke.md`.

Verification required for the implementation milestone:

```sh
cargo xt
cargo test
cargo xt dev activation-plan-smoke
cargo xt dev activation-smoke
cargo xt dev activation-failure-smoke
cargo xt dev activation-soak
cargo xt dev activation-report-check
cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera --require-target-worker
cargo xt k8s activation-smoke --context microk8s-ts --namespace tessera --allow-activation --with-failure --allow-scale
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

The current operator/dev flow has `cargo xt split-activation-plan` for
preview-to-operator evidence without mutation, `cargo xt split-activation` for
explicit manual submission, and `cargo xt dev activation-plan-smoke` to prove a
preview split candidate becomes a ready operator plan while assignments remain
unchanged. `cargo xt dev activation-smoke` covers successful split publication,
Gateway route convergence, source/target Worker owned-cell refresh, target
replay, stable-session post-split Move, remote AOI resync snapshot, and local
JSON evidence. `cargo xt dev activation-failure-smoke` adds post-publish target
outage detection, failed child convergence evidence, no-automatic-rollback
observation, and target Worker restart recovery evidence. `cargo xt dev
activation-soak` adds sustained child Ping/Move traffic, route convergence
retention, remote AOI frame observation, Gateway latency histogram growth, zero
Gateway close counters, and local JSON evidence. `cargo xt k8s
activation-smoke` adds the guarded internal operator flow: service
port-forward, mutation-free plan, explicit activation publish, optional target
Worker scale-down failure detection, scale-up recovery, and cluster JSON
evidence once the controlled topology exists.

## Recommendation

Continue P4.3 only for approved cluster activation evidence. The implemented
shape is intentionally narrow: split-only, manual, default-off, one-level
`CellId`, acked replay before atomic publication, local route
convergence/load-soak smoke, guarded internal smoke helper, target Worker
restoration as recovery, GitOps controlled-smoke backout, and no automatic
merge rollback.
