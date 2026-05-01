# Tessera Next Todo

Last reviewed: 2026-05-02

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- P4.2는 internal-only GitOps manifest commit/push, ArgoCD sync, runtime smoke, GitHub Actions `v2026.05.1` image publish, k8s GitOps tag promotion까지 완료됐다.
- P4.3 runtime split/merge activation의 첫 slice는 split-only/manual/default-off `SubmitSplitActivation` gRPC surface, target map/depth/registration validation, private staged child assignment model, 실패 시 assignments unchanged 테스트까지 구현됐다. 실제 child assignment publish와 Worker replay는 다음 slice다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 다음 milestone: P4.3 staged split의 child assignment publish 및 Worker replay 연결.
- 에스컬레이션 필요: staged split을 실제 assignment publication으로 넘길 replay/partition contract와 post-publish convergence failure 정책 확정.
