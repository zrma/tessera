# P9 Operation Control-Plane Readiness

Last reviewed: 2026-05-08

## Objective

P9 turns the completed P8 cadence into an operator-ready control plane that can
run for long periods in recommend-only mode, preserve replayable operation
history, and prove policy decisions without enabling automatic mutation:

```text
live metrics + assignments -> recommend-only history -> replay audit
-> policy regression -> internal recommend soak -> controlled spot-check
-> default-off cleanup -> completion audit
```

The default live state remains no automatic mutation. Runtime assignment changes
are allowed only inside an operator-approved controlled window, and every window
must be followed by GitOps cleanup that restores manual execution/activation and
preview fixtures to default-off.

## Completion Criteria

P9 is complete only when all of these are true:

1. A repo-native recommend loop records repeated live Worker metrics, current
   assignment snapshots, selected/skipped candidate decisions, policy gate
   inputs, and stable batch keys without assignment mutation or execution.
2. The recommend loop writes durable operation history that can be replayed
   after process restart with stable proposal hashes and operation ids.
3. Split, merge, and canonical multi-depth candidates are all observed from the
   same durable history contract.
4. Policy regression covers default-off execution, explicit approval, deny
   evidence, cooldown, budget, and concurrency gates.
5. Local replay/audit verification proves the durable history can reconstruct
   proposal, approval/deny, execution-skipped, observation, and recovery records
   without requiring live cluster mutation.
6. A P9 runtime image is published and promoted through the k8s GitOps repo with
   ArgoCD `Synced / Healthy`, deployment image match, and default-off cleanup
   evidence.
7. Internal MicroK8s recommend-mode soak runs against live Worker metrics and
   durable storage, proves replay, and leaves assignment state unchanged.
8. A short controlled mutation spot-check proves the operation history can cover
   approval, bounded execution, observation, restart/replay, and cleanup.
9. `cargo xt p9-completion-audit --json` maps every requirement above to
   concrete reports and fails until all required evidence exists.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Recommend-only loop | `cargo xt dev p9-recommend-loop-soak` | `.dev/reports/p9-recommend-loop-soak-latest.json` has live metrics, assignment snapshots, stable candidate keys, durable history, and no mutation |
| Replay audit | `cargo xt dev p9-replay-audit` | `.dev/reports/p9-replay-audit-latest.json` proves history replay, stable proposal hashes, stable operation ids, restart recovery, and no execution |
| Policy regression | `cargo xt dev p9-policy-regression-smoke` | `.dev/reports/p9-policy-regression-latest.json` covers default-off, approval, deny, cooldown, budget, and concurrency gates |
| GitOps rollout | k8s repo image promotion | `.dev/reports/p9-gitops-rollout-latest.json` records image publish, rollout rev, cleanup rev, ArgoCD health, and default-off cleanup |
| Internal recommend soak | `cargo xt k8s p9-recommend-soak` | `.dev/reports/internal-microk8s-p9-recommend-soak-latest.json` proves live recommend mode, durable history, replay, image match, and no mutation |
| Controlled spot-check | `cargo xt k8s p9-controlled-spot-check-report --source-report .dev/reports/internal-microk8s-operation-smoke-latest.json` | `.dev/reports/internal-microk8s-p9-controlled-spot-check-latest.json` proves approval, bounded execution, observation, replay after restart, and cleanup |
| Completion audit | `cargo xt p9-completion-audit --json` | audit returns `complete=true` only after every P9 gate has real evidence |

Current completion audit:

```sh
cargo xt p9-completion-audit --json
```

At P9 start this is intentionally incomplete. It should stay `complete=false`
until the local recommend-loop/replay/policy reports, P9 rollout report, and
internal MicroK8s recommend/controlled reports are all present and valid.

## Initial Implementation Order

1. **Status refresh and audit skeleton**: mark P8 complete, add this P9 contract,
   and add `cargo xt p9-completion-audit --json` so the milestone has a
   fail-closed verifier before new runtime work begins.
2. **Local recommend-loop history**: add the repo-native recommend-only loop and
   report schema, proving repeated live metrics and assignment snapshots produce
   stable split/merge/canonical multi-depth candidates with no assignment
   mutation.
3. **Replay audit**: persist enough durable operation history to replay the
   recommend-loop records after restart and verify stable proposal hashes and
   operation ids.
4. **Policy regression**: make default-off, explicit approval, deny,
   cooldown, budget, and concurrency evidence independently verifiable.
5. **Image publish and GitOps rollout**: publish a P9 runtime image, promote it
   through the k8s GitOps repo, wait for ArgoCD `Synced / Healthy`, and record a
   P9 rollout/default-off report.
6. **Internal recommend soak**: run `cargo xt k8s p9-recommend-soak` in
   internal MicroK8s against live Worker metrics and durable storage, then
   replay the history without assignment mutation.
7. **Controlled spot-check**: open a short operator-approved window, execute one
   bounded mutation path, observe/replay it, and restore default-off cleanup.
8. **Completion audit**: close with `cargo xt p9-completion-audit --json`,
   logical commits/pushes, CI verification, ArgoCD health, and final default-off
   state.

## Guardrails

- Recommend mode must not mutate assignments or attempt execution.
- Runtime-affecting tests require an explicit controlled window and an
  `--allow-execution` style flag.
- Controlled windows must be followed by GitOps cleanup and audit evidence that
  manual execution/activation and preview fixtures returned to default-off.
- Durable history is part of the product contract: report schemas should remain
  replayable, diffable, and strict enough for future audits.
- Each logical slice should be committed and pushed separately before the next
  runtime gate begins.
