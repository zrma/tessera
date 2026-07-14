# P18 Packaged Log Format Parity

Last reviewed: 2026-07-14

Status: active

## Objective

Carry P17's shared `TESSERA_LOG_FORMAT=compact|json` runtime contract through
every repository-owned deployment surface. P18 must keep compact output as the
default, make the opt-in JSON choice explicit and schema-validated, and prove
that Gateway, Worker, and Orchestrator receive the same value without adding a
collector, retention policy, or environment-specific inventory.

## Architecture Boundary

- The runtime contract remains owned by `tessera-core`; packaging only maps a
  validated caller choice into `TESSERA_LOG_FORMAT`.
- Helm remains the reusable package and rejects unsupported values before
  rendering. Docker Compose and the static Kubernetes sample remain examples,
  not production deployment sources of truth.
- `compact` remains the default in every surface. `json` is an explicit
  diagnostics/integration choice and does not imply a production telemetry
  backend.
- Render evidence may retain only the expected setting and aggregate workload
  coverage. Runtime logs, endpoints, cluster names, and deployment inventory
  remain local-only.
- External collectors, sampling, retention, indexing, alerting, and
  live-service rollout policy are out of scope.

## Milestone Queue

1. **Complete: contract and gap review**
   - Record the P17-to-packaging mismatch and make P18 the active boundary.
   - Keep automatic handover policy, production telemetry, and live cluster
     operation outside this milestone.
2. **Helm schema and render parity**
   - Add a `compact|json` value with compact default and exact schema enum.
   - Render `TESSERA_LOG_FORMAT` into every Gateway, Worker, and Orchestrator
     container.
   - Extend deterministic render validation across the default compact case,
     an opt-in JSON case, and an invalid-value negative case.
3. **Example surface parity**
   - Expose a compact-default caller override in Docker Compose.
   - Make the static Kubernetes sample explicit about compact output for all
     three runtimes.
   - Add a repo-native check that prevents those examples from drifting away
     from the shared contract.
4. **Runbook and closeout**
   - Document Helm, Compose, and static-manifest configuration without
     presenting JSON output as a production policy.
   - Run packaging, harness, repository, and publication gates; close P18 only
     after remote CI validates the updated deployment contract.

## Verification

Every implementation slice requires the narrow packaging gate plus repository
defaults:

```text
scripts/check-k8s-packaging.py
cargo xt harness
cargo xt
cargo test
```

Before each push, run both publication gates and wait for remote CI success.

## Completion Boundary

P18 is complete when all repository-owned deployment surfaces set the exact
shared log-format environment variable, Helm proves compact-default and
JSON-opt-in renders plus fail-closed invalid input, sample drift is checked,
and the runbook preserves the local-only/raw-log boundary. Production
telemetry infrastructure and policy remain outside this repository.
