# Tessera Next Todo

Last reviewed: 2026-05-01

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- P4.2는 internal-only GitOps manifest commit/push, ArgoCD sync, runtime smoke까지 완료됐다.
- GitHub Actions image publish workflow는 추가됐지만 Harbor login이 `unauthorized`로 실패 중이다. `HARBOR_USERNAME`/`HARBOR_PASSWORD` 또는 해당 Harbor 계정의 push 권한을 정정한 뒤 workflow를 재실행하고, 성공한 version tag를 k8s GitOps manifest로 별도 promote한다.
- P4.3 runtime split/merge activation은 target worker policy와 assignment mutation rules 결정 전까지 gated 상태다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 다음 검증: Harbor secret/권한 정정 후 GitHub Actions image publish 재실행 및 version-tag GitOps promotion.
- 에스컬레이션 필요: GitHub Actions Harbor push credential, runtime split/merge activation.
