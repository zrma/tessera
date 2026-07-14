# Tessera Packaging Examples

Last reviewed: 2026-07-14

This document records the repository-owned packaging boundary for local
containers and Kubernetes templates. Tessera should provide portable
Gateway/Worker/Orchestrator packaging that demonstrates the horizontally
deployable cell-orchestration architecture. It should not own a specific live
service's cluster inventory, alerting, ingress, certificate, registry, secret,
or incident-response policy.

## Container Image

Build the sample image from the repository root:

```sh
docker build -t tessera:local .
```

The image includes these binaries:

- `tessera-gateway`
- `tessera-worker`
- `tessera-orch`
- `tessera-client`
- `tessera-sim`

The default command is `tessera-gateway`; override it for the target role:

```sh
docker run --rm tessera:local tessera-orch
docker run --rm tessera:local tessera-worker
docker run --rm tessera:local tessera-gateway
```

## Registry Publication Boundary

This public repository does not publish to a private registry and does not
store private registry coordinates, credentials, image paths, or deployment
promotion metadata. Build and test images locally with the commands above.

If a private deployment needs a published image, its source of truth owns the
registry login, image naming, promotion, and rollout evidence outside this
repository and its public CI logs.

## Docker Compose Smoke

`deploy/docker-compose.yml` runs one orchestrator, one worker, and one gateway
with metrics endpoints enabled:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

All three services use compact logs by default. After supplying the existing
runtime authentication environment variables, opt into JSON for a bounded
local diagnostic with one shared override:

```sh
TESSERA_LOG_FORMAT=json docker compose -f deploy/docker-compose.yml up --build
```

The override accepts only the runtime's exact lowercase `compact|json`
contract; unsupported values make each runtime fail startup.

Useful local checks:

```sh
curl http://127.0.0.1:4100/ready
curl http://127.0.0.1:4100/metrics
curl http://127.0.0.1:5100/metrics
curl http://127.0.0.1:6100/metrics
cargo run -p tessera-client -- ping --ts 123
```

Stop and remove the sample stack:

```sh
docker compose -f deploy/docker-compose.yml down
```

## Kubernetes Sample And Template Boundary

`deploy/kubernetes/tessera-sample.yaml` is a non-production single-worker
sample. It shows:

- one namespace and one orchestrator config `ConfigMap`
- separate Deployments and Services for Orchestrator, Worker, and Gateway
- Prometheus scrape annotations for each component metrics endpoint
- Gateway readiness via `GET /ready`
- TCP liveness probes for runtime ports
- explicit compact log format for all three runtime Deployments

The tracked static sample intentionally stays compact. For a bounded local JSON
diagnostic, change all three `TESSERA_LOG_FORMAT` values together in a temporary
copy; do not commit that diagnostic override or its raw output.

Apply it only after replacing `ghcr.io/example/tessera:dev` with an image that
exists in the target registry:

```sh
kubectl apply -f deploy/kubernetes/tessera-sample.yaml
```

Sample checks:

```sh
kubectl -n tessera-sample get pods,svc
kubectl -n tessera-sample port-forward svc/tessera-gateway 4100:4100
curl http://127.0.0.1:4100/ready
```

P13 promotes this sample into the reusable Helm chart at
`deploy/helm/tessera`. The repository-owned template covers:

- Gateway, Worker, and Orchestrator workloads.
- Services for client ingress, internal component traffic, and optional
  metrics.
- Configurable image, runtime addresses, advertised Worker addresses, worker
  ids, assignment seed config, and default-off mutation flags.
- Gateway readiness, TCP liveness probes, and optional metrics annotations.
- Optional state mounts for Orchestrator assignment state and operation ledger
  files.
- Schema validation for supported values and template validation that keeps
  private inventory, credentials, concrete hostnames, and site-specific
  operations policy out of tracked artifacts. The deterministic render/policy
  check is `scripts/check-k8s-packaging.py`.

Cluster-specific live-service manifests belong outside this repository or in a
separate environment-owned layer. That layer should decide ingress, Service
type, resource requests, PodDisruptionBudget, rollout policy, certificate
handling, registry credentials, scrape discovery, alert routing, and incident
process.

## P13 Helm Chart Contract

The reusable package is a Helm v3 application chart rooted at
`deploy/helm/tessera`. Helm owns value substitution and Kubernetes object
rendering only; it is not the production deployment source of truth.

The chart contract is:

- The caller supplies the release name and namespace. The chart does not create
  a `Namespace` or assume cluster-scoped permissions.
- The default render uses a placeholder image and refers to an existing runtime
  authentication `Secret`. The chart never creates credentials or embeds token
  values.
- Gateway, Worker, and Orchestrator ports, probes, metrics annotations, log
  level, `compact|json` log format, image coordinates, and replica counts are
  values-backed.
- Worker identity and advertised addresses are deterministic Kubernetes DNS
  names. Initial cell assignments are explicit values and must refer to a
  declared Worker identity.
- Orchestrator assignment state and operation ledger storage are disabled by
  default. A caller may opt into an `emptyDir`, an existing claim, or a
  chart-created claim without providing any site-specific storage class.
- When durable assignment state is enabled, Worker-list changes follow the
  compatibility contract in `docs/p6-durable-split-state.md`: new Workers must
  start with no static cells, and removed Workers must already be drained.
- Mutation remains default-off. `splitMergeActivation` and
  `operationExecution` default to `disabled`; enabling either one is a
  controlled runtime decision outside install/render behavior.
- Services remain `ClusterIP`. Ingress, load balancers, certificates, registry
  credentials, scheduling policy, disruption budgets, alert rules, and scrape
  discovery are environment-owned overlays.

The supported values are validated by `values.schema.json`. Unknown top-level
or component keys are rejected so a misspelled safety value cannot silently
render with an unintended default.

The required render matrix is intentionally cluster-free:

```sh
helm lint deploy/helm/tessera
helm template tessera-example deploy/helm/tessera \
  --namespace tessera-example > <rendered-output>
helm template tessera-scale deploy/helm/tessera \
  --namespace tessera-scale \
  --values deploy/helm/tessera/ci/scale-out-values.yaml > <rendered-output>
helm template tessera-json deploy/helm/tessera \
  --namespace tessera-json \
  --set-string logFormat=json > <rendered-output>
```

Repository validation must render each case twice, prove byte-stable output,
validate the Kubernetes object shape, and run the publication-boundary checks.
It must not contact a live cluster.

## Render And Validate

Helm v3 is required for the chart gate. Run the repository-owned gate from the
repository root:

```sh
python3 scripts/check-k8s-packaging.py
cargo xt harness
```

To inspect a default render without contacting a cluster:

```sh
helm template tessera-example deploy/helm/tessera \
  --namespace tessera-example > <rendered-output>
kubectl apply --dry-run=client --validate=false -f <rendered-output>
```

`deploy/helm/tessera/ci/scale-out-values.yaml` is a committed validation fixture,
not a production values file. It proves that independent Worker ids, advertised
Service addresses, assignment seeds, Gateway replicas, and state persistence
render coherently. It also opts into JSON so the render matrix proves both the
default compact and explicit JSON contracts across every runtime Deployment.

## Example Install And Smoke

The following is an example namespace flow, not a production rollout contract.
The caller must supply a reachable image and runtime authentication material.
Keep token files and environment-specific values outside the repository.

Create the caller-owned namespace and Secret from local files:

```sh
kubectl create namespace tessera-example
kubectl -n tessera-example create secret generic tessera-runtime-auth \
  --from-file=operator-token=<operator-token-file> \
  --from-file=worker-relay-token=<worker-relay-token-file>
```

Install with an explicit image. Use an environment-owned values file for any
topology or persistence overrides:

```sh
helm upgrade --install tessera-example deploy/helm/tessera \
  --namespace tessera-example \
  --set-string image.repository='<image-repository>' \
  --set-string image.tag='<image-tag>'
```

For a caller-owned structured-log integration, add
`--set-string logFormat=json`. This changes runtime output only; collectors,
sampling, retention, indexing, and alerting remain environment-owned. Raw JSON
logs are local-only unless the owning environment applies its own reviewed data
handling policy.

Check the three default workloads, then forward the Gateway client and metrics
ports in a separate terminal:

```sh
kubectl -n tessera-example rollout status deployment/tessera-example-orchestrator
kubectl -n tessera-example rollout status deployment/tessera-example-worker-a
kubectl -n tessera-example rollout status deployment/tessera-example-gateway
kubectl -n tessera-example port-forward service/tessera-example-gateway 4000:4000 4100:4100
```

With the port-forward active:

```sh
curl -fsS http://127.0.0.1:4100/ready
cargo run -p tessera-client -- --addr 127.0.0.1:4000 ping --ts 123
```

The chart does not create a `Namespace` or Secret and does not delete either on
uninstall. A chart-created state claim is retained by default. Cleanup therefore
has two explicit ownership steps:

```sh
helm uninstall tessera-example --namespace tessera-example
kubectl -n tessera-example get persistentvolumeclaim
```

Delete a retained claim, the runtime Secret, or the namespace only when the
environment owner has separately approved destroying that state.
