# Tessera Next Todo

Last reviewed: 2026-05-01

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- 현재 active todo는 P4.2/P4.3뿐이다. 남은 큰 작업은 production cluster policy 또는 runtime assignment mutation을 건드리므로 구현 전 사용자 판단이 필요하다.
- 2026-05-01 재검토 기준, P4.1 뒤에 자율적으로 이어서 처리할 구현 slice는 없다. 다음 진행은 P4.2 또는 P4.3 선택지를 승인받은 뒤 작은 커밋 단위로 재개한다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 에스컬레이션 필요: production Kubernetes manifests, runtime split/merge activation.
