# Tessera Next Todo

Last reviewed: 2026-04-30

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- 현재 active todo는 P4뿐이다. P4의 큰 작업은 protocol shape, production cluster policy, runtime assignment mutation 중 하나를 건드리므로 구현 전 사용자 판단이 필요하다.
- 2026-04-30 재검토 기준, repo에는 이어서 처리할 미커밋 구현 작업이 없다. 다음 진행은 P4 선택지 중 하나를 승인받은 뒤 작은 커밋 단위로 재개한다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 에스컬레이션 필요: 비-Ping request latency correlation, production Kubernetes manifests, runtime split/merge activation.
