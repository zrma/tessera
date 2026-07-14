# Tessera Packaging Examples

Last reviewed: 2026-04-26

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

P13 should promote this sample into a reusable chart/template boundary. The
repository-owned template should cover:

- Gateway, Worker, and Orchestrator workloads.
- Services for client ingress, internal component traffic, and optional
  metrics.
- Configurable image, runtime addresses, advertised Worker addresses, worker
  ids, assignment seed config, and default-off mutation flags.
- Gateway readiness, TCP liveness probes, and optional metrics annotations.
- Optional state mounts for Orchestrator assignment state and operation ledger
  files.
- Render validation that keeps private inventory, credentials, concrete
  hostnames, and site-specific operations policy out of tracked artifacts.

Cluster-specific live-service manifests belong outside this repository or in a
separate environment-owned layer. That layer should decide ingress, Service
type, resource requests, PodDisruptionBudget, rollout policy, certificate
handling, registry credentials, scrape discovery, alert routing, and incident
process.
