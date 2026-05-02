# Tessera Next Todo

Last reviewed: 2026-05-02

## Baseline

- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- P0/P1/P2/P3는 handover replay ownership, stable Gateway sessions, AOI precision, per-cell tick pipeline, observability/packaging sample, split/merge planner skeleton, fixture-backed dry-run preview smoke까지 완료됐다. 완료 상세 기록은 `docs/completed-milestones.md`로 옮겼다.
- P4.1은 optional envelope-level `request_id`를 통한 Join/Move latency correlation까지 완료됐다.
- P4.2는 internal-only GitOps manifest commit/push, ArgoCD sync, runtime smoke, GitHub Actions `v2026.05.1` image publish, k8s GitOps tag promotion까지 완료됐다.
- P4.3 runtime split activation의 publish/replay/convergence slice는 split-only/manual/default-off `SubmitSplitActivation` gRPC surface, planner-to-operator `split-activation-plan` evidence helper, target map/depth/registration validation, target Worker replay prepare, source Worker parent snapshot/buffered move partition, child replay ack, atomic child assignment publish, replay 실패 시 assignments unchanged rollback, 로컬 two-Worker activation plan smoke/Gateway route convergence/source-target Worker refresh/stable-session post-split Move/remote AOI resync snapshot, post-publish target outage 감지 및 Worker restart recovery smoke, local load/soak observation smoke, internal MicroK8s port-forward smoke helper까지 구현됐다.
- 2026-05-02 internal MicroK8s P5 gate가 완료됐다. Tessera `v2026.05.2` image를 GitHub Actions로 publish했고, k8s GitOps controlled smoke revision에서 two-Worker topology, manual activation flag, preview fixture를 적용한 뒤 `cargo xt k8s activation-smoke --allow-activation --with-failure --allow-scale`와 final `activation-report-check --require-published --require-failure`가 통과했다.
- P5 completion audit와 prompt-to-artifact checklist는 `docs/p5-completion-audit.md`에 둔다. P5 split activation rollback policy는 `operator_recovery_no_automatic_merge_rollback_v1`로 고정했고, final internal report는 `remaining_uncovered=[]`를 기록한다.
- Post-smoke cleanup도 완료됐다. k8s GitOps cleanup revision은 `v2026.05.2` two-Worker topology를 유지하면서 `TESSERA_ORCH_SPLIT_MERGE_ACTIVATION`과 `TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON`을 제거했고, ArgoCD `tessera`는 `Synced / Healthy`에 도달했다.

## Next

- 다음 milestone은 P5 이후 범위로 분리한다: automatic planner submission, runtime merge activation, multi-depth split, or persistent split state 중 하나를 별도 decision gate로 선택한다.
- 실행 기록과 후속 boundary는 `docs/todo-p4-next-milestones.md`, `docs/p5-completion-audit.md`, `docs/internal-microk8s-activation-smoke.md`를 기준으로 본다.
