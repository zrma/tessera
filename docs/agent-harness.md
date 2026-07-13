# Agent Harness

## Interface

- Structure ID: `agent-harness-v1`.
- Baseline ID: `openai-gpt-5.6-2026-07-11`.
- Convergence stage: `canonical`.
- Target stage: `canonical`.
- Canonical check: `scripts/check-agent-harness-interface.sh`.
- Publication class: `public`.
- Publication boundary check: `scripts/check-publication-boundary.py`.

`AGENTS.md`가 공통 GPT-5.6 계약을 소유하고, 이 문서는 Tessera distributed-runtime overlay와 quality/operations 문서로 가는 canonical 진입점이다.

Publication class는 현재 저장소 자체의 공개 경계만 선언한다. public gate는 다른 저장소의 inventory를 기록하지 않고 직접 식별자, checkout 경로, 외부 revision, local draft 상태, 개인 운영 endpoint, 머신별 Kubernetes context, hardcoded private registry project를 차단한다. 공개 문서에는 `example` 값과 식별 불가능한 책임 경계만 남긴다.

Tracked artifact contract: raw tool output와 정확한 로컬 환경 evidence는 local-only로 취급한다. 공개 가능한 기록에는 repository-owned 결정, 필요한 명령 이름, redacted 검증 판정만 남기고 경로·호스트·주소·클러스터 값은 placeholder로 바꾼다.

## Project Objective

Gateway, Worker, Orchestrator로 구성된 spatial runtime을 검증 가능한 split/merge/handover와 운영 evidence를 갖춘 분산 시스템으로 발전시킨다.

## Source Of Truth

- 현재 상태와 roadmap: `README.md`.
- crate/runtime 동작: `crates/`, `proto/`, `xtask/`와 실제 CLI help.
- 품질·검증 정책: `docs/quality.md`; 상세 runtime/operations 문서는 `docs/README.md`에서 탐색한다.
- 다음 작업/decision gate: `docs/todo-next.md`와 활성 milestone 문서.

## Autonomy And Permissions

- 목표와 검증 경로가 명확한 로컬·가역 작업은 추가 승인 없이 구현, 검증, 문서화, local change 정리까지 진행한다.
- 외부 write, secret, 비용, 파괴적 작업, 제품 방향 변경, 승인되지 않은 원격 변경은 에스컬레이션한다.
- live Kubernetes mutation, scale/restart, default-off execution gate는 해당 `cargo xt k8s` allow flag와 운영 승인 경계를 지킨다.

## Execution Loop

1. `jj status`, README status, task-relevant quality/operation 문서를 확인한다.
2. core/proto/gateway/worker/orchestrator/client/xtask 중 소유 경계를 고정한다.
3. protocol, durability, failure/recovery, evidence contract를 먼저 정의한다.
4. focused unit/integration check와 함께 최소 범위로 구현한다.
5. 필요한 local dev smoke와 report checker로 runtime 결과를 검증한다.
6. implemented/planned 상태와 durable operator knowledge를 같은 change에서 갱신한다.
7. 하나의 runtime 목적을 가진 `jj` change로 닫는다.

## Verification And Evidence

- Publication boundary: `scripts/check-publication-boundary.py`; 공개 출고 전에는 권한 있는 local private-inventory guard도 실행한다.
- Harness interface: `scripts/check-agent-harness-interface.sh`; repo harness: `cargo xt harness`.
- 기본 code gate: `cargo xt`와 `cargo test`.
- runtime/network 변경: `cargo xt dev up --with-orch`, test client, 관련 activation/merge/handover smoke, cleanup.
- guarded Kubernetes 변경: guarded helper와 report checker, deployment controller/health, post-smoke default-off 상태.
- 최종 증거에는 report shape, runtime result, local/remote bookmark, CI를 포함한다.

## Escalation

요구사항에 따라 architecture가 크게 갈리는 공백, secret/credential, 비용·운영 리스크, 파괴적 live 작업, published history rewrite, 승인되지 않은 push가 필요한 경우에만 사용자에게 최소 판단을 요청한다.

## VCS And Publish

- 로컬 VCS는 `jj`를 사용하고 change description은 `<type>: <summary>`와 Codex trailer 규칙을 따른다.
- crate/runtime 경계와 기존 사용자 변경을 보존해 논리 change를 나눈다.
- 검증된 마일스톤만 로컬 `main`으로 전진시킨다.
- push 권한이 주어진 경우 원격 freshness, commit, GitHub Actions, 필요한 runtime rollout evidence를 확인한다.

## Harness Evaluation And Improvement

대표 distributed-runtime task에서 완료성, evidence 품질, 회귀율, smoke latency, 비용을 평가한다. 반복 실패는 `cargo xt harness`, report checker, focused smoke 또는 concise quality rule로 기계화한다.

## Convergence

- `bridge`: 이 문서가 공통 인터페이스를 제공하고 기존 상세 문서를 연결한다.
- `normalized`: 중복된 autonomy, execution, verification, escalation, VCS 정책을 이 문서의 동일 섹션으로 이동한다.
- `canonical`: 프로젝트 목적, source, command, domain invariant는 같은 섹션 계약 안의 local content로 유지하고 공통 baseline, 제목 순서, 검사 골격은 동일하게 잠근다.
- 단계 전환은 현재 저장소의 Structure ID, 섹션 순서, canonical check 결과로 검증하며 다른 저장소의 이름·개수·로컬 경로·공개 여부를 전제하지 않는다.

## Project Overlay

- core/proto는 상위 runtime crate에 의존하지 않고 gateway/worker/orchestrator는 서로 직접 의존하지 않는다.
- assignment, handover, split/merge 상태 변화는 failure/recovery/restart evidence를 요구한다.
- 계획을 구현으로 표시하지 않고 operator mutation은 명시적 default-off gate를 유지한다.

## Related Documents

- Status and architecture: `README.md`.
- Quality and verification: `docs/quality.md`, `docs/smoke-runbook.md`.
- Documentation map: `docs/README.md`.
- Operations progression: `docs/p7-operation-loop.md`, `docs/p8-closed-loop-operation-cadence.md`, `docs/p9-operation-control-plane-readiness.md`.
