# P11 Operational Endurance And Failure Recovery

Last reviewed: 2026-06-04

## Objective

P11 turns the completed P10 observability loop into an operational endurance and
failure-recovery proof. The milestone must show that Tessera can keep Gateway
routing, Worker relay state, Orchestrator assignments, operation ledger
durability, request latency, close counters, and ghost relay counters stable
under repeated load, reconnects, restarts, and controlled component failures:

```text
P10 observability baseline
-> local endurance soak
-> restart recovery smokes
-> transient failure and reconnect recovery
-> image publish and deployment rollout
-> guarded Kubernetes endurance/recovery smoke
-> default-off cleanup
-> completion audit
```

P11 keeps runtime mutation default-off by default. Controlled failure windows
may restart local processes or Kubernetes pods, but assignment mutation and
operation execution must remain explicit and evidence-backed.

## Completion Criteria

P11 is complete only when all of these are true:

1. A repo-native local/dev endurance soak records repeated load and reconnect
   evidence while Gateway routes, Worker relay state, Orchestrator assignments,
   operation ledger state, request latency, close counters, and ghost relay
   counters stay within the expected stable envelope.
2. Focused local/dev restart recovery smokes cover Gateway restart, Worker
   restart, and Orchestrator restart with persisted assignment and operation
   state.
3. Focused local/dev transient failure smokes cover target Worker
   unavailability, controlled component failure, port-forward reconnect
   behavior, and post-recovery route/assignment convergence.
4. Durable reports are strict enough for replay/audit and record default-off
   runtime state without relying on automatic mutation.
5. A P11 runtime image is published only after local endurance evidence is
   green.
6. The image is promoted through the private deployment source of truth with deployment controller
   `Synced / Healthy`, deployment image match, and default-off cleanup evidence.
7. Guarded Kubernetes validates the same endurance/recovery contract with pod
   restarts, controlled failures, Gateway smoke, durable report capture, and
   final cleanup.
8. `cargo xt p11-completion-audit --json` maps every requirement above to
   concrete reports and returns `complete=true` only after all P11 evidence is
   present.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Fail-closed audit | `cargo xt p11-completion-audit --json` | returns `complete=false` until all P11 report files validate |
| Local endurance soak | `cargo xt dev p11-endurance-soak` | `.dev/reports/p11-endurance-soak-latest.json` validates repeated load, reconnects, route stability, Worker relay stability, Orchestrator assignment stability, ledger durability, latency, close counters, ghost relay counters, and default-off state |
| Local restart recovery | `cargo xt dev p11-restart-recovery-smoke` | `.dev/reports/p11-restart-recovery-latest.json` validates Gateway/Worker/Orchestrator restart recovery plus persisted assignment and operation state |
| Local transient failure recovery | `cargo xt dev p11-transient-failure-recovery-smoke` | `.dev/reports/p11-transient-failure-recovery-latest.json` validates target Worker unavailability, controlled component failure, port-forward reconnect recovery, and post-recovery convergence |
| deployment rollout | private deployment image promotion | `.dev/reports/p11-gitops-rollout-latest.json` records image publish, rollout rev, cleanup rev, deployment controller health, image match, and default-off cleanup |
| Internal endurance/recovery | `cargo xt k8s p11-endurance-recovery-smoke --allow-pod-restart --allow-controlled-failure` | `.dev/reports/guarded-kubernetes-p11-endurance-recovery-latest.json` validates the promoted image, deployment controller health, deployment self-heal-safe pod restarts, controlled failures, Gateway smoke, operation ledger durability, route/assignment convergence, durable report capture, and default-off cleanup |
| Completion audit | `cargo xt p11-completion-audit --json` | audit returns `complete=true` only after every P11 gate has real evidence |

Current completion audit:

```sh
cargo xt p11-completion-audit --json
```

The audit returns `complete=true` and `findings=[]` for the `v2026.05.10`
evidence set.

## Initial Implementation Order

1. **Contract and audit skeleton**: mark P11 active in docs, add this contract,
   and add fail-closed `cargo xt p11-completion-audit --json`.
2. **Local endurance soak**: `cargo xt dev p11-endurance-soak` composes the P10
   observability and ghost-relay loops into repeated load, reconnect, routing,
   relay, assignment, ledger, latency, close-counter, and default-off evidence.
3. **Restart recovery smokes**: `cargo xt dev p11-restart-recovery-smoke`
   composes a P7 operation restart source with a P11 component restart probe to
   cover Gateway, Worker, and Orchestrator restart recovery plus persisted
   assignment and operation state.
4. **Transient failure recovery**:
   `cargo xt dev p11-transient-failure-recovery-smoke` creates a controlled
   target Worker outage, confirms the failure window, restores the Worker, and
   records post-recovery Gateway/assignment convergence with clean counters.
5. **Image publish and deployment rollout**: done. The build workflow published
   `registry.example.com/example/tessera:v2026.05.10`, the private deployment source of truth recorded the rollout;
   deployment controller returned `Synced / Healthy`, and
   `.dev/reports/p11-gitops-rollout-latest.json` validates deployment image
   match plus default-off cleanup.
6. **Internal endurance/recovery smoke**: done. `cargo xt k8s
   p11-endurance-recovery-smoke --context example-cluster --namespace tessera
   --expected-image registry.example.com/example/tessera:v2026.05.10
   --allow-pod-restart --allow-controlled-failure` writes
   `.dev/reports/guarded-kubernetes-p11-endurance-recovery-latest.json` with
   deployment controller health, Gateway smoke, pod restart recovery, controlled target Worker
   failure recovery, assignment/route convergence, operation ledger durability,
   and final default-off cleanup.
7. **Completion audit**: done. `cargo xt p11-completion-audit --json` returns
   `complete=true` and `findings=[]`.

## Guardrails

- P11 endurance paths must not require automatic assignment mutation or
  automatic operation execution.
- Restart/failure windows must be explicit, bounded, and followed by
  convergence checks.
- Runtime-affecting internal Kubernetes checks must use deployment promotion and
  verify deployment controller `Synced / Healthy` before producing completion evidence.
- Each logical slice should be committed and pushed before the next gate begins.
