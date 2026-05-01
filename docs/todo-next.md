# Tessera Next Todo

Last reviewed: 2026-05-02

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- P4.2는 internal-only GitOps manifest commit/push, ArgoCD sync, runtime smoke, GitHub Actions `v2026.05.1` image publish, k8s GitOps tag promotion까지 완료됐다.
- P4.3 runtime split activation의 publish/replay/convergence slice는 split-only/manual/default-off `SubmitSplitActivation` gRPC surface, planner-to-operator `split-activation-plan` evidence helper, target map/depth/registration validation, target Worker replay prepare, source Worker parent snapshot/buffered move partition, child replay ack, atomic child assignment publish, replay 실패 시 assignments unchanged rollback, 로컬 two-Worker activation plan smoke/Gateway route convergence/source-target Worker refresh/stable-session post-split Move/remote AOI resync snapshot, post-publish target outage 감지 및 Worker restart recovery smoke, local load/soak observation smoke, internal MicroK8s port-forward smoke helper까지 구현됐다.
- 2026-05-02 read-only internal MicroK8s preflight 결과, live `tessera` namespace와 GitOps manifest는 아직 one Orchestrator / one Worker / one Gateway 구조이고 `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual`이 꺼져 있다. 이어서 `cargo xt k8s activation-smoke` plan-only helper는 mutation 없이 중단했고, 현재 preview는 `assignment_listing_zero_metrics` 기반 `no_split_candidate`였다. 따라서 실제 publish/failure/recovery run은 새 image promotion, second Worker/Service, Orchestrator config target map, manual activation flag, controlled preview fixture 또는 실제 metrics candidate를 포함한 별도 GitOps slice 승인 후 실행해야 한다. 실행 계획은 `docs/internal-microk8s-activation-smoke.md`에 둔다.
- P5 completion audit와 prompt-to-artifact checklist는 `docs/p5-completion-audit.md`에 둔다. P5 split activation rollback policy는 `operator_recovery_no_automatic_merge_rollback_v1`로 고정했다. P5는 internal publish/failure/recovery evidence와 final report verifier가 닫히기 전까지 완료로 보지 않는다.
- `../k8s`에는 승인 대기용 `v2026.05.2` two-Worker GitOps draft가 로컬 diff로 준비되어 있으며, 단일 파일 pre-commit 검증은 통과했다. matching image tag publish와 controlled smoke window 승인 전에는 push하지 않는다.

## P4

- 실행 계획: `docs/todo-p4-next-milestones.md`
- 다음 milestone: internal MicroK8s activation GitOps slice 승인/적용/검증.
- 에스컬레이션 필요: Tessera `v2026.05.2` image publish, k8s GitOps smoke topology push/sync, and controlled `--allow-activation --with-failure --allow-scale` execution.
