# P10 Runtime Observability And Soak Hardening

Last reviewed: 2026-05-09

## Objective

P10 turns the completed P9 recommend-only control plane into a long-running
runtime evidence loop. The milestone must prove that Tessera can repeatedly
observe live Gateway, Worker, and Orchestrator state, preserve durable
replayable evidence, and keep runtime mutation default-off while request
latency, ghost relay behavior, route convergence, and assignment stability are
under sustained load:

```text
metrics + assignments + operation/recommend history
-> durable observability report
-> ghost relay and request-latency soak
-> replay audit
-> image publish and deployment rollout
-> guarded Kubernetes observability soak
-> default-off cleanup
-> completion audit
```

P10 is not an automatic mutation milestone. Runtime assignment changes remain
default-off outside explicit controlled windows, and the P10 happy path should
not require execution attempts.

## Completion Criteria

P10 is complete only when all of these are true:

1. A repo-native local observability soak samples Gateway, Worker, and
   Orchestrator metrics over repeated iterations.
2. The local soak records request latency histograms, ghost relay counters,
   assignment snapshots, operation ledger state when available, and P9
   recommend history when available into a durable report.
3. The durable report is replayable and can prove stable report hashes without
   touching runtime state.
4. Local/dev smoke covers sustained request latency, route convergence,
   close-counter cleanliness, assignment stability, and runtime mutation
   default-off.
5. A focused ghost relay soak covers fanout, backpressure, reconnect counters,
   route convergence, close-counter cleanliness, assignment stability, and
   runtime mutation default-off.
6. A P10 runtime image is published only after local evidence is green.
7. The image is promoted through the private deployment source of truth with deployment controller
   `Synced / Healthy`, deployment image match, and default-off cleanup evidence.
8. Guarded Kubernetes validates the same observability/soak contract with live
   runtime metrics, Gateway smoke, durable report capture, and final cleanup.
9. `cargo xt p10-completion-audit --json` maps every requirement above to
   concrete reports and returns `complete=true` only after all P10 evidence is
   present.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Fail-closed audit | `cargo xt p10-completion-audit --json` | returns `complete=false` until all P10 report files validate |
| Local observability soak | `cargo xt dev p10-observability-soak` | `.dev/reports/p10-observability-soak-latest.json` validates Gateway/Worker/Orchestrator metrics, request latency, ghost relay counters, assignments, histories, route convergence, close counters, default-off state, and durable replayability |
| Ghost relay soak | `cargo xt dev p10-ghost-relay-soak` | `.dev/reports/p10-ghost-relay-soak-latest.json` validates fanout/backpressure/reconnect counters, route convergence, close counters, assignment stability, durable report, and default-off state |
| Replay audit | `cargo xt dev p10-replay-audit` | `.dev/reports/p10-replay-audit-latest.json` proves stable report hashes and no runtime mutation |
| deployment rollout | private deployment image promotion | `.dev/reports/p10-gitops-rollout-latest.json` records image publish, rollout rev, cleanup rev, deployment controller health, image match, and default-off cleanup |
| Internal observability soak | `cargo xt k8s p10-observability-soak` | `.dev/reports/guarded-kubernetes-p10-observability-soak-latest.json` validates the promoted image, deployment controller health, Gateway smoke, durable report capture, default-off cleanup, and the same runtime observability contract |
| Completion audit | `cargo xt p10-completion-audit --json` | audit returns `complete=true` only after every P10 gate has real evidence |

Current completion audit:

```sh
cargo xt p10-completion-audit --json
```

P10 is complete as of the `v2026.05.9` evidence set. The current audit returns
`complete=true` with local observability, ghost relay, replay, deployment rollout,
guarded Kubernetes observability soak, and default-off cleanup evidence.

## Initial Implementation Order

1. **Contract and audit skeleton**: mark P10 active in docs, add this contract,
   and add fail-closed `cargo xt p10-completion-audit --json`.
2. **Local observability report schema**: done. `cargo xt dev
   p10-observability-soak --iterations 2 --sleep-ms 1` writes
   `.dev/reports/p10-observability-soak-latest.json`,
   `.dev/reports/p10-observability-ledger-latest.json`, and
   `.dev/reports/p10-recommend-history-latest.json` with Gateway, Worker,
   Orchestrator, request latency, assignment, operation, recommend, route,
   close-counter, and default-off evidence.
3. **Ghost relay soak**: done. `cargo xt dev p10-ghost-relay-soak
   --iterations 2 --sleep-ms 1` writes
   `.dev/reports/p10-ghost-relay-soak-latest.json` from an initial
   cross-worker child-route topology without runtime mutation, covering relay
   fanout, backpressure counters, reconnect counters, assignment stability,
   route convergence, and clean Gateway close counters.
4. **Replay audit**: done. `cargo xt dev p10-replay-audit` writes
   `.dev/reports/p10-replay-audit-latest.json` after rereading the local
   observability report, ghost relay report, operation ledger, and recommend
   history, then verifying stable report hashes without touching runtime state.
5. **Image publish and deployment rollout**: done. The build workflow published
   `registry.example.com/example/tessera:v2026.05.9`
   (`sha256:6909ef3c682b10e36a9934c4a1deb26db8d46dfa13f3e80b0cab100f90d33e80`),
   the private deployment source of truth promoted it; deployment controller
   returned
   `Synced / Healthy`, and `.dev/reports/p10-gitops-rollout-latest.json`
   validates deployment image match plus default-off cleanup.
6. **Internal observability soak**: done. `cargo xt k8s
   p10-observability-soak --context example-cluster --namespace tessera
   --expected-image registry.example.com/example/tessera:v2026.05.9
   --iterations 2 --sleep-ms 100` writes
   `.dev/reports/guarded-kubernetes-p10-observability-soak-latest.json` with
   deployment controller health, Gateway ping/move smoke, Gateway/Worker/Orchestrator metrics,
   latency samples, relay counter sampling, assignment stability, and
   default-off cleanup.
7. **Completion audit**: done. `cargo xt p10-completion-audit --json` returns
   `complete=true` and `findings=[]`.

## Guardrails

- P10 local and internal observability paths must not require automatic
  mutation or operation execution.
- Runtime-affecting windows still require explicit operator approval and
  matching `--allow-*` flags.
- Durable evidence reports must be replayable and strict enough for future
  audits.
- Each logical slice should be committed and pushed before the next gate begins.
