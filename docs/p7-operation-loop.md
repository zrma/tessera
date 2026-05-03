# P7 Operation Loop

Last reviewed: 2026-05-03

## Objective

P7 turns the P6 durable/manual/default-off split, merge, and multi-depth control
plane into an operator-facing closed loop:

```text
planner proposal -> approval -> execution -> observation -> recovery/backout -> audit report
```

The goal is not unguarded autoscaling. The goal is a durable, policy-gated,
observable operation system where planner output can be reviewed, approved,
executed, recovered, and audited repeatedly.

## Completion Criteria

P7 is complete only when all of these are true:

1. Live Worker metrics and current assignment state can produce split and merge
   operation proposals without mutating assignments.
2. Every proposal is written to a durable operation record with stable operation
   id, proposal source, target cells, worker roles, preconditions, and
   submission template.
3. Operator approval is explicit and durable. Approval records include policy
   id, approver, timestamp, TTL, expected proposal hash, cooldown budget, and
   allowed mutation kind.
4. The executor is default-off unless a policy gate is enabled. It executes only
   approved, unexpired, idempotent operations that pass cooldown and budget
   checks.
5. Split, merge, and canonical multi-depth operations record execution phases:
   preflight, replay/prepare, publish, observation, failure detection,
   recovery/backout, and final audit.
6. Failure handling remains operator-visible. P7 does not introduce automatic
   rollback unless a later policy explicitly adds it with separate evidence.
7. Gateway route convergence, Worker assignment refresh, AOI resync,
   latency/close counters, and operation state transitions are recorded in
   reports and validated by machine checkers.
8. Local/dev smoke covers proposal, blocked execution, approved execution,
   failure/recovery, restart recovery, and soak.
9. Internal MicroK8s smoke covers the same runtime-affecting paths after image
   publish and GitOps rollout.
10. Post-smoke cleanup restores mutating flags to default-off and leaves ArgoCD
    `Synced / Healthy`.
11. A P7 completion audit maps every requirement above to concrete artifacts
    and fails until all required evidence exists.

## Prompt-To-Artifact Checklist

| Requirement | First artifact | Completion evidence |
| --- | --- | --- |
| Planner proposal records | operation record schema and writer | report checker proves `activation_mutated=false` proposal evidence from live metrics and assignment listing |
| Durable operation state | file-backed operation ledger or equivalent repo-local persistence | restart test proves proposals and approvals survive Orchestrator restart |
| Explicit approval | CLI/API for approval records | tests reject missing, expired, mismatched, or wrong-policy approvals |
| Policy/cooldown/budget gate | executor preflight validator | local smoke records blocked-by-policy and approved execution reports |
| Idempotent execution | stable operation id and replay-safe phase transitions | duplicate execution test/report proves no double publish |
| Failure and recovery | operation phase updates for target/owner outage | smoke report records failure detection, no automatic rollback, operator recovery, and final convergence |
| Observation | metrics/report fields for route count, AOI frames, latency, close counters | verifier rejects missing route convergence, Worker refresh, AOI, or counter evidence |
| Internal rollout | k8s GitOps smoke window runbook | image publish, GitOps rollout rev, cleanup rev, ArgoCD `Synced / Healthy`, default-off cleanup report |
| Completion audit | `cargo xt p7-completion-audit` or equivalent | audit returns `complete=true` only after every P7 gate is backed by real evidence |

## Slice Cadence

Each slice should be self-contained:

1. Inspect current docs/code/reports before editing.
2. Implement the smallest useful artifact.
3. Run `cargo xt` and relevant tests.
4. Run local/dev smoke when runtime behavior changes.
5. Commit with `<type>: <summary>` and the Codex co-author trailer.
6. Push the relevant bookmark.
7. For runtime-affecting slices, publish an image, promote through GitOps, wait
   for ArgoCD `Synced / Healthy`, run internal smoke, write verifier reports,
   and clean up default-off state.

## Initial Implementation Order

1. **Operation record model**: define durable proposal and execution records
   without changing runtime behavior. The initial opt-in surface is
   `TESSERA_ORCH_OPERATION_LEDGER_PATH` plus the read-only Orchestrator
   `GET /operations` endpoint.
2. **Ledger append/update path**: let Orchestrator-owned proposal and phase
   writes persist durable records across restart, using the repo's existing
   conservative persistence style before introducing heavier storage.
3. **Proposal writer**: convert live metrics split candidates and assignment
   listing merge candidates into durable proposals.
4. **Approval gate**: add explicit approval records with TTL, policy id,
   expected proposal hash, cooldown, and budget constraints.
5. **Executor dry run**: report blocked execution by default and approved
   execution only under explicit policy.
6. **Closed-loop smoke**: verify proposal-to-approval-to-execution locally for
   split, merge, and canonical multi-depth paths.
7. **Internal rollout**: repeat the controlled image/GitOps/smoke/cleanup flow
   and add the P7 audit gate.

## Guardrails

- Do not make planner-selected mutation automatic by default.
- Do not weaken P6 rollback policy by silently merging back after a failure.
- Do not count read-only proposal evidence as execution evidence.
- Do not count local smoke as internal MicroK8s evidence.
- Do not count ArgoCD `Synced / Healthy` as runtime success without the
  operation-specific smoke report.
- Keep docs, verifier contracts, and GitOps cleanup state synchronized with the
  actual code and reports.
