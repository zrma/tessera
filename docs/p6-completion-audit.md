# P6+ Completion Audit

Last reviewed: 2026-05-03

P6+ is complete as of the `v2026.05.3` internal MicroK8s rollout and
post-smoke default-off cleanup.

The machine-readable completion gate is:

```sh
cargo xt p6-completion-audit --json
```

The expected result is:

```json
{
  "complete": true,
  "findings": [],
  "report_dir": ".dev/reports",
  "schema": "tessera.p6_completion_audit.v1"
}
```

## Success Criteria

P6+ is counted complete because all of these criteria now have concrete
evidence:

1. Published split assignment state survives Orchestrator restart.
2. Restart recovery is verified locally and in internal MicroK8s.
3. Live Worker metrics can produce an operator-reviewed split plan, and
   mutation remains default-off or policy-gated.
4. Runtime merge activation is implemented with safe policy gates.
5. Split and merge have success, failure/recovery, restart, load/soak, and
   rollback/backout evidence.
6. Gateway route convergence, Worker assignment refresh, AOI resync, and report
   verifiers cover both local/dev and internal evidence.
7. GitOps rollout state is captured for the internal gates.
8. Multi-depth split activation has a chosen `CellId`/quadtree encoding and
   verified runtime behavior.

## Evidence Map

| Gate | Evidence |
| --- | --- |
| P5/P6 split publish, failure, restart, live-metrics plan | `.dev/reports/internal-microk8s-activation-smoke-latest.json` and `cargo xt k8s activation-report-check --require-published --require-failure --require-restart --require-live-metrics-plan --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.3` |
| Runtime merge internal publish/failure/restart/soak | `.dev/reports/internal-microk8s-merge-activation-smoke-latest.json` and `cargo xt k8s merge-activation-report-check --require-ready-plan --require-published --require-failure --require-restart --require-soak --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.3` |
| Canonical multi-depth internal publish/failure/restart/soak | `.dev/reports/internal-microk8s-multi-depth-activation-smoke-latest.json` and `cargo xt k8s multi-depth-activation-report-check --require-ready-plan --require-published --require-failure --require-restart --require-soak --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.3` |
| Planner mutation policy evidence | `.dev/reports/internal-microk8s-planner-activation-latest.json` and `cargo xt k8s planner-activation-report-check --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.3` |
| GitOps rollout evidence | `.dev/reports/p6-gitops-rollout-latest.json` and `cargo xt p6-rollout-report-check --expected-image harbor.1day1coding.com/1day1coding/tessera:v2026.05.3` |
| Final completion gate | `cargo xt p6-completion-audit --json` returns `complete=true` and `findings=[]` |

## Rollout State

- Published image: `harbor.1day1coding.com/1day1coding/tessera:v2026.05.3`
- P6 rollout revision: `c1b296f42e4d544aaae966d888bd31a4d1b3b586`
- P6 cleanup revision: `69ade59159747fa73f0eb472c474c96e604aa04f`
- Cleanup state: manual activation default-off, preview fixture removed,
  standard assignment state path restored, ArgoCD `tessera` `Synced / Healthy`

## Next Boundary

P6+ closed the durable/manual/default-off dynamic cell control plane. The next
milestone is P7: Closed-Loop, Policy-Gated Dynamic Cell Operations. P7 should
begin with durable operation records and proposal/approval/execution audit
state, not with unguarded autonomous mutation. See `docs/p7-operation-loop.md`.

