# P7 Operation Loop

Last reviewed: 2026-05-04

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

Current ledger verifier:

```sh
cargo xt p7-operation-ledger-check \
  --ledger .dev/operation-ledger.json \
  --require-approval \
  --require-blocked-execution
```

This validates the first proposal -> approval -> default-off blocked execution
artifact. It is not a replacement for runtime execution, observation,
recovery, internal rollout, or final P7 completion audit evidence.
Approved execution ledgers use the same checker with
`--require-published-execution`.

Current local closed-loop smoke:

```sh
cargo xt dev p7-operation-loop-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-loop-ledger-latest.json \
  --require-approval \
  --require-blocked-execution
```

This starts Orchestrator-only dev stacks with registered two-Worker listings and
split, merge, and canonical multi-depth preview candidates, records proposal ->
approval -> default-off execution block for each path, and writes
`.dev/reports/p7-operation-loop-smoke-latest.json` plus
`.dev/reports/p7-operation-loop-ledger-latest.json`. It still covers only
default-off execution.

Current approved execution smoke:

```sh
cargo xt dev p7-operation-execution-smoke
cargo xt p7-operation-ledger-check \
  --ledger .dev/reports/p7-operation-execution-ledger-latest.json \
  --require-approval \
  --require-published-execution
```

This starts an Orchestrator-only dev stack with
`TESSERA_ORCH_OPERATION_EXECUTION=manual` and
`TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`, records proposal -> approval ->
published execution for an approved same-Worker merge operation, verifies that a
repeat execution returns `already_published` without another mutation, and
writes `.dev/reports/p7-operation-execution-smoke-latest.json` plus
`.dev/reports/p7-operation-execution-ledger-latest.json`. Split runtime
execution, canonical multi-depth runtime execution, observation, recovery,
restart, soak, and internal MicroK8s evidence remain explicit follow-up gates.

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
   conservative persistence style before introducing heavier storage. The
   first write surface is `POST /operations/proposals`, which appends planner
   proposals only and leaves assignment mutation disabled.
3. **Proposal writer**: convert live metrics split candidates and assignment
   listing merge candidates into durable proposals with stable operation ids,
   proposal hashes, target cells, worker roles, preconditions, and submission
   commands.
4. **Approval gate**: add explicit approval records with TTL, policy id,
   expected proposal hash, cooldown, budget constraints, and allowed mutation
   kind. The first write surface is `POST /operations/approvals?...`; it
   approves an existing proposal only when the supplied proposal hash still
   matches and still leaves execution mutation disabled.
5. **Executor dry run and first manual execution**: report blocked execution by
   default and approved execution only under explicit policy. The first surface is
   `POST /operations/executions?...`, which evaluates proposal hash,
   approval, policy, TTL, cooldown, and budget metadata. Default-off execution
   records a durable `blocked_by_policy` phase/status. A controlled execution
   window additionally requires `TESSERA_ORCH_OPERATION_EXECUTION=manual` and
   `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`; the first runtime mutation path
   is approved same-Worker merge publish with idempotent repeat execution.
6. **Closed-loop smoke**: verify proposal-to-approval-to-execution locally for
   split, merge, and canonical multi-depth paths. The first repo-native smoke is
   `cargo xt dev p7-operation-loop-smoke`, which covers split/merge/canonical
   multi-depth proposal -> approval -> default-off execution block without
   assignment mutation.
7. **Approved merge execution smoke**: verify that an approved same-Worker merge
   operation can publish once through the P7 executor and that duplicate execute
   calls are idempotent. The first repo-native smoke is
   `cargo xt dev p7-operation-execution-smoke`.
8. **Internal rollout**: repeat the controlled image/GitOps/smoke/cleanup flow
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
