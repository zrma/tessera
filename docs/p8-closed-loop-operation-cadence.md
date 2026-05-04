# P8 Closed-Loop Operation Cadence

Last reviewed: 2026-05-04

## Objective

P8 turns the P7 policy-gated operation loop into a bounded operator cadence:

```text
live evidence -> candidate batch -> durable proposal ledger -> explicit approval
-> budget/cooldown/concurrency gate -> controlled execution window
-> observation/recovery/soak -> audit -> default-off cleanup
```

The goal is not automatic mutation. The default live state remains
default-off/no automatic mutation. P8 only allows runtime changes when an
operator explicitly opens a controlled window and the operation passes policy
id, approval, cooldown, budget, concurrency, and observation gates.

## Completion Criteria

P8 is complete only when all of these are true:

1. A repo-native planner cadence can collect live Worker metrics and current
   assignment state repeatedly, then emit split, merge, and canonical
   multi-depth candidates without assignment mutation.
2. Candidate batches are stable and auditable. Each batch records source
   metrics, assignment snapshot summary, policy inputs, candidate hashes, and
   reasons for selected or skipped operations.
3. Proposal writes are idempotent. Re-running the cadence does not duplicate
   operation records for the same proposal hash and candidate content.
4. Approval remains explicit and durable. No cadence path can execute without
   an approval record that matches the proposal hash, policy id, TTL, cooldown,
   and budget metadata.
5. Cooldown, budget, and concurrency limits are enforced before execution, and
   blocked operations leave machine-checkable ledger/audit evidence.
6. Approved execution windows are bounded. The executor runs only the selected
   operation set, records every state transition, and stops after the configured
   window or operation count.
7. Observation gates still decide closure. Route convergence, Worker refresh,
   traffic, close counters, restart/recovery evidence, and soak evidence must be
   present before a completed audit can pass.
8. Failure handling remains operator-visible. P8 does not introduce automatic
   rollback unless a later milestone adds a separate policy and evidence set.
9. Local/dev smoke covers read-only cadence, approved bounded cadence,
   cooldown/budget/concurrency blocking, failure/recovery, restart recovery,
   soak, and idempotent reruns.
10. Internal MicroK8s smoke covers the runtime-affecting cadence path after image
    publish and GitOps rollout, then restores mutating flags to default-off and
    leaves ArgoCD `Synced / Healthy`.
11. A P8 completion audit maps every requirement above to concrete artifacts and
    fails until the required local, rollout, internal, cleanup, and audit
    evidence exists.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Read-only cadence | cadence planner command/report | repeated live metrics + assignment snapshots produce candidate batches with `assignments_changed=false` |
| Stable candidates | candidate hash and batch schema | duplicate cadence run reuses or skips existing proposal records without ledger duplication |
| Durable proposals | proposal writer integration | ledger checker proves split, merge, and canonical multi-depth proposal records |
| Explicit approval | approval selection command/API | missing, expired, mismatched, or wrong-policy approvals are blocked with evidence |
| Cooldown/budget/concurrency | preflight gate evaluator | blocked reports identify the exact gate and leave no assignment mutation |
| Bounded execution | controlled cadence executor | approved window runs only the configured operation count and records idempotent state transitions |
| Observation/recovery | report fields and checkers | completed/recovery-required reports include route, Worker, traffic, counter, restart, and soak evidence |
| Internal rollout | k8s GitOps smoke window | image publish, rollout rev, cleanup rev, ArgoCD `Synced / Healthy`, live smoke, and default-off cleanup are verified |
| Completion audit | `cargo xt p8-completion-audit --json` or equivalent | audit returns `complete=true` only after every P8 gate has real evidence |

## Initial Implementation Order

1. **Status refresh**: close stale P7 docs against the `v2026.05.6` evidence set
   and point the next milestone at this P8 contract.
2. **Read-only cadence report**: `cargo xt dev p8-cadence-plan-smoke` runs
   multiple planner ticks from live Worker metrics and assignment listing, writes
   `.dev/reports/p8-cadence-plan-smoke-latest.json`, and proves no assignment
   mutation or execution attempt.
3. **Proposal ledger idempotency**: connect cadence output to durable proposal
   writes with `cargo xt dev p8-cadence-proposal-smoke`, proving repeated
   proposal ticks reuse one durable operation record without assignment mutation
   or execution attempt. Evidence is
   `.dev/reports/p8-cadence-proposal-smoke-latest.json` plus
   `.dev/reports/p8-cadence-proposal-ledger-latest.json`.
4. **Approval selection**: `cargo xt dev p8-cadence-approval-smoke`
   records a live-metrics proposal, persists an operator approval with policy,
   cooldown, and budget keys, proves repeat approval is idempotent, and keeps
   unapproved/missing-policy/wrong-policy/default-off execution attempts
   mutation-free. Evidence is
   `.dev/reports/p8-cadence-approval-smoke-latest.json` plus
   `.dev/reports/p8-cadence-approval-ledger-latest.json`.
5. **Gate enforcement**: `cargo xt dev p8-cadence-gate-smoke` proves
   cooldown, budget, and concurrency policy blocks run before any execution
   window and before the default-off executor block. Evidence is
   `.dev/reports/p8-cadence-gate-smoke-latest.json` plus per-case gate ledger
   files.
6. **Bounded local execution cadence**: `cargo xt dev
   p8-cadence-execution-smoke` runs a small approved cadence window from a
   live-metrics proposal, executes one operation set, observes it to completion,
   and proves repeated execution is idempotent. Evidence is
   `.dev/reports/p8-cadence-execution-smoke-latest.json` plus
   `.dev/reports/p8-cadence-execution-ledger-latest.json`.
7. **Failure/restart/soak cadence evidence**: `cargo xt dev
   p8-cadence-recovery-smoke` extends the bounded cadence through a target
   Worker outage, records `recovery_required`, and proves operator-visible
   Worker restart recovery without automatic rollback. The remaining local
   follow-ups are Orchestrator restart and soak reports.
8. **Image and GitOps rollout**: publish a new runtime image, promote through
   the k8s GitOps repo, wait for ArgoCD `Synced / Healthy`, and verify image
   match.
9. **Internal controlled cadence smoke**: run the approved bounded cadence
   against MicroK8s, record report checker evidence, then clean up mutating
   flags and preview fixtures to default-off.
10. **Completion audit**: add the P8 audit gate and map every criterion to
    concrete local, internal, cleanup, and CI evidence.

## Guardrails

- Default live state stays no automatic mutation.
- Controlled windows must be short-lived, explicit, and followed by cleanup.
- Runtime-affecting slices must follow local smoke -> image publish -> GitOps
  rollout -> ArgoCD health -> internal smoke -> default-off cleanup.
- Each logical slice should be committed and pushed separately before the next
  runtime gate begins.
- Existing P7 operation ids are deterministic from proposal content, so internal
  smoke windows that intentionally replay a scenario should use fresh ledger
  paths or prove idempotent reuse explicitly.
