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
  operations policy out of tracked artifacts. The dedicated deterministic
  render/policy check is the next P13 slice.

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
  level, image coordinates, and replica counts are values-backed.
- Worker identity and advertised addresses are deterministic Kubernetes DNS
  names. Initial cell assignments are explicit values and must refer to a
  declared Worker identity.
- Orchestrator assignment state and operation ledger storage are disabled by
  default. A caller may opt into an `emptyDir`, an existing claim, or a
  chart-created claim without providing any site-specific storage class.
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
```

Repository validation must render each case twice, prove byte-stable output,
validate the Kubernetes object shape, and run the publication-boundary checks.
It must not contact a live cluster.
