# Tessera Smoke Runbook

Last reviewed: 2026-07-14

This runbook holds the command catalog that used to make `README.md` hard to
scan. Prefer the narrowest smoke that covers the changed surface, then close a
milestone with its completion audit.

## Baseline Gates

```sh
cargo xt
cargo test
cargo xt harness
```

For runtime or network changes, also run the local dev ping smoke:

```sh
cargo xt dev up --with-orch
cargo run -p tessera-client -- ping --ts 123
cargo xt dev down --with-orch
```

## Local Dev Stack

```sh
cargo xt dev up
cargo xt dev up --with-orch
cargo xt dev logs --target all --follow
cargo xt dev down --with-orch
cargo xt dev simulation-smoke
cargo xt dev distributed-simulation-smoke
```

Useful client commands:

```sh
cargo run -p tessera-client -- ping --ts 123
cargo run -p tessera-client -- join --actor 1 --x 0 --y 0
cargo run -p tessera-client -- move --actor 1 --dx 1 --dy 0.5
cargo run -p tessera-client -- repl --actor 1
```

P15 simulator commands:

```sh
cargo run -p tessera-sim -- plan --seed 7 --clients 4 --cells 2
cargo run -p tessera-sim -- run --seed 7 --clients 4 --cells 1 --moves-per-client 2 --max-concurrency 2
cargo run -p tessera-sim -- run --clients 4 --moves-per-client 2 --json --max-failed-clients 0 --max-p95-latency-ms 100
```

`plan` is network-free. `run` requires the local Gateway path above and keeps
each player on an independent connection. Both commands reject scenarios or
execution limits outside the repository-owned bounds.

The JSON form is `tessera.sim.result.v1` and contains aggregate counts,
classified failures, elapsed time, throughput, operation latency percentiles in
microseconds, the caller's thresholds, and stable threshold violations. It does
not retain the target address or raw network errors. Threshold failure returns a
non-zero exit after writing the result to stdout.

`cargo xt dev distributed-simulation-smoke` owns an isolated Orchestrator,
Gateway, and two-Worker stack. It assigns two root cells to distinct Worker
identities, runs the fixed four-client profile across both cells, validates
canonical complete cell coverage, and proves both Workers accepted the planned
sessions. It then restarts the second Worker on a new local advertised address
with the same identity and cell, waits for Orchestrator and Gateway convergence,
and requires the exact deterministic profile to pass again before teardown.

## Runtime Logging

Gateway, Worker, and Orchestrator use compact logs by default. Set the shared
format contract to exact lowercase `json` when a local diagnostic or bounded
smoke needs machine-readable events:

```sh
TESSERA_LOG_FORMAT=json cargo xt dev up --with-orch
cargo xt dev logs --target all --follow
cargo xt dev down --with-orch
```

Exact `compact` is also accepted; empty, differently cased, or unsupported
values fail before runtime startup. Raw JSON logs are local-only diagnostics
and can contain runtime inventory. Do not copy them into tracked reports; P17
smoke evidence retains only bounded aggregate counts and verdicts.

Request-id-bearing Join and Move operations emit the following ordered event
contract under the `tessera.request.lifecycle` target:

```text
gateway.request.forwarded
worker.request.received
worker.response.sent
gateway.response.forwarded
```

Each event contains only `event`, `component`, `operation`, `session_id`,
`request_id`, and the scalar `cell_world`, `cell_cx`, `cell_cy`, `cell_depth`,
and `cell_sub` fields. Actor ids, peer or Worker addresses, payloads, and raw
errors are not part of this event contract.

## Metrics And Readiness

```sh
cargo xt dev metrics-smoke
TESSERA_GW_METRICS_ADDR=127.0.0.1:4100 cargo run -p tessera-gateway
TESSERA_WORKER_METRICS_ADDR=127.0.0.1:5100 cargo run -p tessera-worker
TESSERA_ORCH_METRICS_ADDR=127.0.0.1:6100 cargo run -p tessera-orch
```

Manual checks:

```sh
curl http://127.0.0.1:4100/ready
curl http://127.0.0.1:4100/metrics
curl http://127.0.0.1:5100/metrics
curl http://127.0.0.1:6100/metrics
curl http://127.0.0.1:6100/split-merge/preview
```

## Split And Merge Activation

Plan-only and policy-gated helpers:

```sh
cargo xt dev activation-plan-smoke
cargo xt dev activation-live-plan-smoke
cargo xt dev activation-live-metrics-smoke
cargo xt dev planner-mutation-smoke
cargo xt dev activation-live-planner-mutation-smoke
cargo xt split-activation-plan --preview-addr 127.0.0.1:6100 --orch-addr 127.0.0.1:6000
cargo xt merge-activation-plan --preview-addr 127.0.0.1:6100 --orch-addr 127.0.0.1:6000
```

Runtime-affecting local smokes:

```sh
cargo xt dev activation-smoke
cargo xt dev activation-failure-smoke
cargo xt dev activation-restart-smoke
cargo xt dev activation-soak
cargo xt dev merge-activation-smoke
cargo xt dev merge-activation-cross-worker-smoke
cargo xt dev merge-activation-failure-smoke
cargo xt dev merge-activation-restart-smoke
cargo xt dev merge-activation-soak
cargo xt dev canonical-merge-activation-smoke
cargo xt dev canonical-merge-activation-failure-smoke
cargo xt dev canonical-merge-activation-restart-smoke
cargo xt dev canonical-merge-activation-soak
```

Report checks:

```sh
cargo xt dev activation-report-check
cargo xt dev activation-report-check --require-live-metrics-plan
cargo xt dev canonical-merge-activation-report-check
cargo xt dev canonical-merge-activation-failure-report-check
cargo xt dev canonical-merge-activation-restart-report-check
cargo xt dev canonical-merge-activation-soak-report-check --min-iterations 32
```

## Multi-Depth Activation

```sh
cargo xt dev multi-depth-activation-smoke
cargo xt dev multi-depth-activation-report-check
cargo xt dev multi-depth-activation-failure-smoke
cargo xt dev multi-depth-activation-failure-report-check
cargo xt dev multi-depth-activation-restart-smoke
cargo xt dev multi-depth-activation-restart-report-check
cargo xt dev multi-depth-activation-soak
cargo xt dev multi-depth-activation-soak-report-check --min-iterations 32
```

## P7 Operation Loop

Local gates:

```sh
cargo xt dev p7-operation-loop-smoke
cargo xt dev p7-operation-execution-smoke
cargo xt dev p7-operation-split-execution-smoke
cargo xt dev p7-operation-multi-depth-execution-smoke
cargo xt dev p7-operation-observation-smoke
cargo xt dev p7-operation-recovery-smoke
cargo xt dev p7-operation-restart-smoke
cargo xt dev p7-operation-soak-smoke
cargo xt dev p7-operation-split-observation-smoke
cargo xt dev p7-operation-split-recovery-smoke
cargo xt dev p7-operation-split-restart-smoke
cargo xt dev p7-operation-split-soak-smoke
cargo xt dev p7-operation-multi-depth-observation-smoke
cargo xt dev p7-operation-multi-depth-recovery-smoke
cargo xt dev p7-operation-multi-depth-restart-smoke
cargo xt dev p7-operation-multi-depth-soak-smoke
cargo xt p7-completion-audit --json
```

Ledger checks use `cargo xt p7-operation-ledger-check` with the matching latest
ledger path and the required evidence flags, such as `--require-approval`,
`--require-published-execution`, `--require-completed-observation`, or
`--require-recovery-required`.

## P8 Closed-Loop Cadence

```sh
cargo xt dev p8-cadence-plan-smoke
cargo xt dev p8-cadence-proposal-smoke
cargo xt dev p8-cadence-approval-smoke
cargo xt dev p8-cadence-gate-smoke
cargo xt dev p8-cadence-execution-smoke
cargo xt dev p8-cadence-recovery-smoke
cargo xt dev p8-cadence-restart-smoke
cargo xt dev p8-cadence-soak-smoke
cargo xt p8-completion-audit --json
```

Internal runtime-affecting P8 smoke requires a deployment-controlled window:

```sh
cargo xt k8s p8-cadence-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-execution
cargo xt k8s p8-cadence-cleanup-check --context example-cluster --namespace tessera --expected-image <new-tag>
```

## P9 Recommend Mode

```sh
cargo xt dev p9-recommend-loop-soak
cargo xt dev p9-replay-audit
cargo xt dev p9-policy-regression-smoke
cargo xt p9-completion-audit --json
```

Internal P9 evidence:

```sh
cargo xt k8s p9-recommend-soak --context example-cluster --namespace tessera --expected-image <new-tag>
cargo xt k8s p9-controlled-spot-check-report \
  --source-report .dev/reports/guarded-kubernetes-operation-smoke-latest.json \
  --expected-image <new-tag>
```

P9 is complete as of `v2026.05.8`. Use these commands for regression or when a
later milestone intentionally reuses the recommend/replay evidence path.

## P10 Observability And Soak

P10 is complete as of `v2026.05.9`; use these commands for regression or when
the next milestone intentionally reuses the observability/soak evidence path:

```sh
cargo xt p10-completion-audit --json
```

Local gates:

```sh
cargo xt dev p10-observability-soak --iterations 2 --sleep-ms 1
cargo xt dev p10-ghost-relay-soak --iterations 2 --sleep-ms 1
cargo xt dev p10-replay-audit
```

Implemented local reports:

- `.dev/reports/p10-observability-soak-latest.json`
- `.dev/reports/p10-observability-ledger-latest.json`
- `.dev/reports/p10-recommend-history-latest.json`
- `.dev/reports/p10-ghost-relay-soak-latest.json`
- `.dev/reports/p10-replay-audit-latest.json`
- `.dev/reports/p10-gitops-rollout-latest.json`
- `.dev/reports/guarded-kubernetes-p10-observability-soak-latest.json`

Internal gate after image publish and deployment promotion:

```sh
cargo xt k8s p10-observability-soak --context example-cluster --namespace tessera --expected-image registry.example.com/example/tessera:v2026.05.9
```

## P11 Endurance And Recovery

The P11 audit exists first and fails closed until the required endurance and
recovery reports are implemented and captured:

```sh
cargo xt p11-completion-audit --json
```

Local gates:

```sh
cargo xt dev p11-endurance-soak
cargo xt dev p11-restart-recovery-smoke
cargo xt dev p11-transient-failure-recovery-smoke
```

`p11-endurance-soak` runs the P10 observability and ghost-relay dev smokes,
validates both source reports, then writes
`.dev/reports/p11-endurance-soak-latest.json`.

`p11-restart-recovery-smoke` runs the P7 split operation restart smoke for
persisted assignment/operation state evidence, adds a P11 Gateway/Worker restart
probe, then writes `.dev/reports/p11-restart-recovery-latest.json`.

`p11-transient-failure-recovery-smoke` creates a controlled target Worker
outage, verifies Gateway failure observation, restores the Worker, and writes
`.dev/reports/p11-transient-failure-recovery-latest.json` after clean
post-recovery traffic.

Planned reports:

- `.dev/reports/p11-endurance-soak-latest.json`
- `.dev/reports/p11-restart-recovery-latest.json`
- `.dev/reports/p11-transient-failure-recovery-latest.json`
- `.dev/reports/p11-gitops-rollout-latest.json`
- `.dev/reports/guarded-kubernetes-p11-endurance-recovery-latest.json`

Planned internal gate after image publish and deployment promotion:

```sh
cargo xt p6-rollout-report \
  --context example-cluster \
  --namespace tessera \
  --image <new-tag> \
  --rollout-revision <k8s-rollout-rev> \
  --cleanup-revision <k8s-cleanup-rev> \
  --image-published \
  --gitops-rollout-approved \
  --post-smoke-default-off-cleanup \
  --manual-activation-default-off \
  --preview-fixture-removed \
  --out .dev/reports/p11-gitops-rollout-latest.json

cargo xt k8s p11-endurance-recovery-smoke \
  --context example-cluster \
  --namespace tessera \
  --expected-image <new-tag> \
  --allow-pod-restart \
  --allow-controlled-failure
```

## P12 Read-Only Operator Evidence

P12 read-only evidence is generated from local report writers. It is support
material for runtime and packaging work, not a live-service operations track:

```sh
cargo xt dev p12-local-report-replay
cargo xt dev p12-slo-alert-candidates
cargo xt dev p12-runbook-drill
cargo xt dev p12-decision-packet
cargo xt p12-readiness-audit --json
```

Implemented P12 reports:

- `.dev/reports/p12-operator-readiness-latest.json`
- `.dev/reports/p12-source-replay-latest.json`
- `.dev/reports/p12-slo-alert-candidates-latest.json`
- `.dev/reports/p12-runbook-drill-latest.json`
- `.dev/reports/p12-decision-packet-latest.json`

## P13 Kubernetes Packaging Template

P13 portable Helm packaging is complete. Its cluster-free gate is part of
`cargo xt harness`:

```sh
scripts/check-k8s-packaging.py
cargo xt harness
```

Render inspection and the caller-owned example install/smoke/cleanup flow are
documented in `docs/packaging.md`. The chart does not own private registry,
host, ingress, certificate, alert, credential, namespace, or production
rollout policy.

## Guarded Kubernetes Activation

Read-only preflight:

```sh
cargo xt k8s activation-smoke --context example-cluster --namespace tessera --require-target-worker
cargo xt k8s activation-smoke --context example-cluster --namespace tessera --require-target-worker --use-live-worker-metrics --live-min-pressure-signals 1
cargo xt k8s activation-report-check --report .dev/reports/guarded-kubernetes-activation-smoke-latest.json --require-live-metrics-plan
```

Controlled runtime windows require explicit approval and matching `--allow-*`
flags:

```sh
cargo xt k8s activation-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-activation --with-failure --allow-scale
cargo xt k8s activation-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-activation --with-restart --allow-rollout-restart
cargo xt k8s merge-activation-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-activation --with-failure --allow-scale --with-restart --allow-rollout-restart --with-soak
cargo xt k8s multi-depth-activation-smoke --context example-cluster --namespace tessera --expected-image <new-tag> --allow-activation --with-failure --allow-scale --with-restart --allow-rollout-restart --with-soak
```

Always follow a controlled window with deployment cleanup and the relevant
completion audit.

## Completion Audits

```sh
cargo xt p6-completion-audit --json
cargo xt p7-completion-audit --json
cargo xt p8-completion-audit --json
cargo xt p9-completion-audit --json
cargo xt p10-completion-audit --json
cargo xt p11-completion-audit --json
cargo xt p12-readiness-audit --json
```

The audits are evidence aggregators. A green local smoke does not replace the
internal rollout, cleanup, or completion report required by the corresponding
milestone contract.
