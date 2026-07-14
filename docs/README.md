# Tessera Documentation Index

Last reviewed: 2026-07-14

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

`todo-next.md` is the active open-work index. P12 read-only operator evidence,
P13 portable Kubernetes packaging, P14 runtime hardening, and P15 deterministic
simulation harness work are closed. P16 distributed simulation and topology
convergence is also closed. P17 runtime trace correlation is closed; the next
open boundary is P18 packaged log-format parity in `todo-next.md`.

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
| P12 read-only operator evidence | Complete support packet; live operations out of scope | `todo-p12-ops-readiness.md` |
| P13 Kubernetes packaging template | Complete; portable chart and render gate | `todo-p13-k8s-packaging.md` |
| P14 runtime hardening | Complete; packet, route/state, replay, and planner hardening | `todo-p14-runtime-hardening.md` |
| P15 deterministic simulation harness | Complete; plans, bounded execution, result gates, and CI smoke | `todo-p15-simulation-harness.md` |
| P16 distributed simulation and topology convergence | Complete; cell coverage, two-Worker workload, address convergence, and CI gate | `todo-p16-distributed-simulation.md` |
| P17 runtime trace correlation | Complete; structured logs, lifecycle events, two-Worker trace smoke, and repeated CI gate | `todo-p17-runtime-trace-correlation.md` |
| P18 packaged log-format parity | Active; Helm schema/render and example-surface parity | `todo-p18-packaged-log-format.md` |

The P6/P7/P8/P9/P10/P11 documents are retained because they define evidence
contracts, report shapes, and completion gates. P12 records a completed
read-only support packet. P13 records the completed portable packaging
boundary, P14 records the completed runtime-hardening evidence, and P15 records
the completed simulator contract. P16 records the completed distributed
simulator evidence, and P17 records the completed runtime trace-correlation
gate. Older milestone docs should not be treated as active TODO lists unless
`todo-next.md` explicitly reopens a boundary.

## Design Notes

| Topic | Document |
| --- | --- |
| Dynamic split/merge design and historical activation lanes | `dynamic-split-merge.md` |
| Multi-depth `CellId` decision | `multi-depth-cellid-decision.md` |
| Handover runtime baseline | `handover.md` |
| P6 durable assignment state details | `p6-durable-split-state.md` |
| Public packaging and Kubernetes samples | `packaging.md` |
| P13 packaging template plan | `todo-p13-k8s-packaging.md` |
| P14 runtime hardening plan | `todo-p14-runtime-hardening.md` |
| P16 distributed simulation plan | `todo-p16-distributed-simulation.md` |
| P17 runtime trace correlation plan | `todo-p17-runtime-trace-correlation.md` |
| P18 packaged log-format parity plan | `todo-p18-packaged-log-format.md` |

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
