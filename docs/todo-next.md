# Tessera Next Todo

Last reviewed: 2026-05-01

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- P4.2는 internal-only GitOps manifest commit/push, ArgoCD sync, runtime smoke, GitHub Actions `v2026.05.1` image publish, k8s GitOps tag promotion까지 완료됐다.
- P4.3 runtime split/merge activation의 첫 shape는 `docs/dynamic-split-merge.md`에 split-only/manual/default-off feature flag/one-level `CellId` 기준으로 고정됐다. 구현은 아직 시작하지 않았다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 다음 milestone: P4.3 split-only manual activation 구현.
- 에스컬레이션 필요: runtime assignment mutation 구현 착수 및 실제 activation flag/API 표면 확정.
