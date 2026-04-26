# Tessera Packaging Examples

Last reviewed: 2026-04-26

This document records sample packaging entry points for local container and
Kubernetes experiments. These files are examples, not a production deployment
contract. Replace image names, resource policy, networking, storage, and scrape
metadata with the target cluster convention before treating them as live
infrastructure.

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

## Kubernetes Sample

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

Cluster-specific production manifests should be added separately once the
target ingress, Service type, resource requests, PodDisruptionBudget, rollout
policy, and Prometheus discovery convention are known.
