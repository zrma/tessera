# Tessera Documentation Index

Last reviewed: 2026-06-04

This directory separates current operating truth from historical milestone
records. Start here when deciding what to read next.

## Current Source Of Truth

| Need | Document |
| --- | --- |
| Common agent harness interface and Tessera overlay | `agent-harness.md` |
| Current repo status and next open boundary | `todo-next.md` |
| Agent autonomy, quality gates, and crate boundary policy | `quality.md` |
| Local/dev and guarded deployment smoke command catalog | `smoke-runbook.md` |
| Packaging examples and non-production deployment samples | `packaging.md` |
| Completed milestone archive through P4.3 | `completed-milestones.md` |

`README.md` is the front door. It should stay short enough to answer what the
project is, how to run the local stack, what is currently complete, and where to
find deeper runbooks.

## Active Planning

`todo-next.md` is the active open-work index. As of this review, P12 read-only
Operator Readiness and Alert Handoff is closed; the remaining boundary is
optional read-only Kubernetes snapshot evidence or explicit operator decisions
for external observability/live alert wiring.

When a new milestone is chosen, update `todo-next.md` first, then add a focused
milestone contract only if the new work needs its own checklist, evidence map,
or completion audit.

## Milestone Contracts And Evidence

| Milestone | Current state | Document |
| --- | --- | --- |
| P5 split activation | Complete; historical boundary | `p5-completion-audit.md` |
| P6+ durable split/merge/multi-depth control plane | Complete as `v2026.05.3` | `p6-completion-audit.md` |
| P7 policy-gated operation loop | Complete as `v2026.05.6` | `p7-operation-loop.md` |
| P8 closed-loop operation cadence | Complete as `v2026.05.7` | `p8-closed-loop-operation-cadence.md` |
| P9 operation control-plane readiness | Complete as `v2026.05.8` | `p9-operation-control-plane-readiness.md` |
| P10 runtime observability and soak hardening | Complete as `v2026.05.9` | `p10-runtime-observability-soak-hardening.md` |
| P11 operational endurance and failure recovery | Complete as `v2026.05.10` | `p11-operational-endurance-failure-recovery.md` |
| P12 operator readiness and alert handoff | Complete read-only handoff; external wiring unresolved | `todo-p12-ops-readiness.md` |

The P6/P7/P8/P9/P10/P11 documents are retained because they define evidence
contracts, report shapes, and completion gates. P12 records the current
read-only operator handoff and unresolved external decisions. Older milestone
docs should not be treated as active TODO lists unless `todo-next.md` explicitly
reopens a boundary.

## Design Notes

| Topic | Document |
| --- | --- |
| Dynamic split/merge design and historical activation lanes | `dynamic-split-merge.md` |
| Multi-depth `CellId` decision | `multi-depth-cellid-decision.md` |
| Handover runtime baseline | `handover.md` |
| P6 durable assignment state details | `p6-durable-split-state.md` |
| Public packaging and Kubernetes samples | `packaging.md` |

These files contain useful implementation detail and evidence history, but
new work should read them through the current status in `todo-next.md`.

## Documentation Hygiene

- Keep `README.md` concise and link to deeper docs instead of expanding command
  catalogs inline.
- Keep active work in `todo-next.md`; move closed work into the matching
  milestone/evidence document.
- Use "historical" or "closed" language when preserving older decision gates,
  so completed P6+ through P9 work is not mistaken for open scope.
- After docs changes, run `cargo xt harness`. If code or harness contracts were
  touched, run the broader `cargo xt` and relevant tests.
