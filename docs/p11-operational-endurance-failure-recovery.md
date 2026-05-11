# P11 Operational Endurance And Failure Recovery

Last reviewed: 2026-05-11

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
-> image publish and GitOps rollout
-> internal MicroK8s endurance/recovery smoke
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
6. The image is promoted through the k8s GitOps repo with ArgoCD
   `Synced / Healthy`, deployment image match, and default-off cleanup evidence.
7. Internal MicroK8s validates the same endurance/recovery contract with pod
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
| GitOps rollout | k8s repo image promotion | `.dev/reports/p11-gitops-rollout-latest.json` records image publish, rollout rev, cleanup rev, ArgoCD health, image match, and default-off cleanup |
| Internal endurance/recovery | `cargo xt k8s p11-endurance-recovery-smoke` | `.dev/reports/internal-microk8s-p11-endurance-recovery-latest.json` validates the promoted image, ArgoCD health, pod restarts, controlled failures, Gateway smoke, durable report capture, and default-off cleanup |
| Completion audit | `cargo xt p11-completion-audit --json` | audit returns `complete=true` only after every P11 gate has real evidence |

Current completion audit:

```sh
cargo xt p11-completion-audit --json
```

The audit is expected to fail closed until local endurance, restart recovery,
transient failure recovery, GitOps, and internal MicroK8s P11 evidence reports
exist and validate.

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
4. **Transient failure recovery**: add target Worker unavailability and
   port-forward reconnect checks with post-recovery convergence evidence.
5. **Image publish and GitOps rollout**: publish a P11 runtime image, promote it
   through the k8s GitOps repo, verify ArgoCD and image state, and record a P11
   rollout/default-off report.
6. **Internal endurance/recovery smoke**: validate the same contract in internal
   MicroK8s against the promoted image, then restore/verify default-off cleanup.
7. **Completion audit**: close with `cargo xt p11-completion-audit --json`,
   logical commits/pushes, CI verification, ArgoCD health, and final default-off
   state.

## Guardrails

- P11 endurance paths must not require automatic assignment mutation or
  automatic operation execution.
- Restart/failure windows must be explicit, bounded, and followed by
  convergence checks.
- Runtime-affecting internal Kubernetes checks must use GitOps promotion and
  verify ArgoCD `Synced / Healthy` before producing completion evidence.
- Each logical slice should be committed and pushed before the next gate begins.
