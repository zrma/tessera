# P13 Kubernetes Packaging Template

Last reviewed: 2026-07-14

## Objective

P13 turns Tessera's container-oriented Gateway, Worker, and Orchestrator runtime
into a reusable Kubernetes packaging surface. The package should make the
seamless-cell architecture horizontally deployable without making this
repository responsible for a specific live service, cluster inventory, alert
backend, or incident process.

The target is a portable chart/template contract for:

- Gateway, Worker, and Orchestrator workloads.
- Services for client ingress, internal Worker/Orchestrator traffic, and
  optional metrics.
- Configurable runtime addresses, worker ids, advertised addresses, assignment
  state, operation ledger state, metrics ports, and default-off mutation flags.
- Readiness/liveness probes and optional persistent state mounts.
- Render-time validation that blocks private inventory, credentials, and
  site-specific operations policy from entering tracked artifacts.

## Explicit Non-Goals

- Owning a production cluster's live operations.
- Choosing alerting, paging, notification, or incident-response backends.
- Encoding private registry coordinates, cluster names, node inventory,
  hostnames, internal endpoints, secrets, or webhook URLs.
- Promising zero-downtime production rollout semantics.
- Replacing a hosting environment's GitOps, policy, or SRE process.

## Source Evidence

- Current packaging sample: `docs/packaging.md` and
  `deploy/kubernetes/tessera-sample.yaml`.
- Runtime boundaries: `README.md`, `docs/dynamic-split-merge.md`,
  `docs/handover.md`, and `docs/multi-depth-cellid-decision.md`.
- Completed runtime evidence: P6 through P12 completion reports and audits.
- Publication boundary: `scripts/check-publication-boundary.py`.

## Candidate Slices

1. **Packaging contract**: define the chart/template shape, supported values,
   required placeholders, and validation commands.
2. **Template structure**: add reusable Gateway, Worker, and Orchestrator
   workload templates with Services, probes, metrics ports, and config wiring.
3. **Topology values**: model worker roles, worker replica/identity policy,
   Gateway routing inputs, Orchestrator assignment seed config, and optional
   state mounts without hardcoding private topology.
4. **Render validation**: add a repo-native render check that verifies generated
   manifests are deterministic, placeholder-safe, and publication-boundary clean.
5. **Container smoke docs**: document how to render/apply the template in an
   example namespace and run Gateway ping/readiness checks without requiring any
   live production cluster.
6. **Runtime follow-up map**: capture remaining cell-orchestration hardening
   work that packaging exposes, such as packet backpressure, route convergence,
   assignment-state compatibility, and scale-out worker identity.

## Done Criteria

P13 can close when:

1. `docs/packaging.md` describes the reusable chart/template boundary and the
   explicit non-goals.
2. The repository contains portable Kubernetes packaging artifacts for Gateway,
   Worker, and Orchestrator with placeholders only.
3. A render/policy check proves the packaging output is stable and contains no
   private inventory or credentials.
4. `cargo xt harness` and the publication boundary check pass.
5. Runtime code changes, if any, also pass `cargo xt` and `cargo test`.

## Guardrails

- Do not require access to a live cluster to validate the template.
- Do not add site-specific ingress, certificate, alert, registry, or secret
  policy.
- Do not treat P13 as a production operations milestone. It is a packaging and
  container architecture milestone.
- Keep private deployment evidence outside tracked repository artifacts.
