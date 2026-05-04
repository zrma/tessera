# Tessera — Agent Quick Context

이 리포지토리에서 자동화 에이전트가 작업할 때 필요한 최소 컨텍스트. “최소 읽기 세트”: `README.md`(현재 상태/로드맵) + AGENTS.md.
Harness나 아키텍처/검증 정책을 건드릴 때는 `docs/quality.md`도 함께 확인한다.

## 읽기 우선
- 작업 전 `README.md`의 Status/Design Overview/Run Locally를 확인.
- 이 문서(AGENTS.md)에서 명령/지침/금지사항을 확인.

## 자율 수행 원칙
- 기본값은 자율 진행이다. 요청이 목표와 제약을 충분히 주면, 추가 확인 없이 repo의 기존 패턴을 읽고 합리적인 가정으로 구현/검증/문서화까지 진행한다.
- 사용자를 호출하는 경우는 결과가 크게 달라지는 요구사항 공백, 되돌리기 어려운 파괴적 작업, published history rewrite, 명시되지 않은 원격 bookmark 이동, 외부 비용/운영 리스크가 있는 작업, secret/credential 취급처럼 실제 위험이 있는 경우로 제한한다.
- 진행 중 발견한 작은 불확실성은 작업을 멈추지 말고 코드/문서/최종 보고에 가정과 검증 결과를 남긴다.
- 중간 보고는 짧게 현재 작업과 발견 사실 중심으로 하고, 최종 보고는 변경 파일·검증 명령·남은 리스크를 포함한다.

## OpenAI GPT-5.5 기준
- OpenAI 모델/API/프롬프트/에이전트 기준을 갱신할 때는 `openai-docs` 스킬과 공식 OpenAI developer docs를 먼저 확인한다.
- 최신 OpenAI 기준을 요구받으면 `gpt-5.5`를 baseline으로 보되, 활성 model string과 직접 연결된 prompt/harness 문구만 좁게 갱신한다.
- GPT-5.5용 지침은 outcome-first로 작성한다. 목표, 성공 기준, 허용되는 부작용, 검증 증거, 중단/에스컬레이션 조건, 출력 형태를 먼저 고정한다.
- reasoning/verbosity/Responses API/tool wiring은 이 저장소가 실제 통합 지점을 노출할 때만 바꾼다. API surface나 tool handler 변경이 필요하면 blocker로 기록한다.

## 현재 범위 (요약)
- V0 구현: 고정 그리드 셀, Gateway↔Worker TCP 파이프라인, Orchestrator `ListAssignments` 스냅샷 + `WatchAssignments` 스트림 + 주기적 재조회(`TESSERA_GW_REFRESH_SECS`).
- 주요 크레이트: `tessera-gateway`, `tessera-worker`, `tessera-orch`, 공용 타입 `tessera-core`, 테스트 클라이언트 `tessera-client`, 자동화 `xtask`.

## 로컬 실행/종료
- 빠른 기동: `cargo xt dev up` (worker+gateway), Orchestrator까지: `cargo xt dev up --with-orch [--orch-config path]`.
- 종료: `cargo xt dev down` / `cargo xt dev down --with-orch`.
- 로그: `.dev/logs/{worker,gateway,orch}.log`; PID는 `.dev/pids/`. 기동 후 `tail -f`로 정상 시작 여부 확인.
- 로그 helper: `cargo xt dev logs --target all --follow` 또는 `--target gateway|worker|orch`.
- 개별 실행 예: `cargo run -p tessera-orch`, `cargo run -p tessera-gateway`, `cargo run -p tessera-worker`, `cargo run -p tessera-client -- ...`.

## 기본 환경 변수
- `TESSERA_GW_ADDR` 기본 `127.0.0.1:4000`
- `TESSERA_GW_REFRESH_SECS` 기본 `5` (Gateway 라우팅 재조회 주기)
- `TESSERA_WORKER_ADDR` 기본 `127.0.0.1:5001`
- `TESSERA_WORKER_ADVERTISE_ADDR` 기본 `TESSERA_WORKER_ADDR` (오케스트레이터 등록용 워커 주소)
- `TESSERA_WORKER_ID` 기본 `worker-local`
- `TESSERA_ORCH_ADDR` 기본 `127.0.0.1:6000`
- `TESSERA_ORCH_ASSIGNMENT_STATE_PATH` 기본 unset/disabled (설정 시 Orchestrator가 handover/split/merge publish 후 assignment map을 JSON으로 저장하고 재시작 시 복구)
- `TESSERA_ORCH_OPERATION_LEDGER_PATH` 기본 unset/disabled (설정 시 P7 operation ledger JSON을 초기화/로드하고 `GET /operations` read-only snapshot, `POST /operations/proposals` planner proposal writer, `POST /operations/approvals?...` approval writer, `POST /operations/executions?...` default-off execution dry-run writer를 노출)
- `TESSERA_ORCH_OPERATION_COOLDOWN_ACTIVE_KEYS` 기본 unset (comma-separated cooldown key 목록, matching approval execution preflight를 mutation 없이 block)
- `TESSERA_ORCH_OPERATION_BUDGET_LIMITS` 기본 unset (comma-separated `budget-key=limit` 목록, 같은 budget key의 다른 non-failed records가 limit 이상이면 execution preflight를 mutation 없이 block)
- `TESSERA_ORCH_OPERATION_MAX_IN_FLIGHT_PER_BUDGET_KEY` 기본 unset (같은 budget key의 다른 executing/observing operation count limit)
- `RUST_LOG` 기본 `info`

## 작업 지침 (Do/Don’t)
- Do: README의 ✅(구현) / 🚧(계획) 구분을 지키고, 아키텍처 변화 시 Design Overview 갱신.
- Do: 내부 crate 의존 방향을 지킨다. `tessera-core`/`tessera-proto`는 내부 Tessera crate에 의존하지 않고, gateway/worker/orch는 서로 직접 의존하지 않는다.
- Do: 단일 파일 수정은 `apply_patch` 우선, 검색은 `rg`/`rg --files` 우선.
- Do: 최소 `cargo fmt`, 관련 `cargo test`; 필요 시 `cargo check --workspace`. dev 헬퍼는 `cargo xt`/`cargo xt dev ...` 활용하고, 작업 마무리 시에는 `cargo xt`와 `cargo test`를 반드시 돌려 결과를 확인한다. `cargo xt`는 fmt/clippy/check와 `cargo xt harness`를 포함한다.
- Do: 런타임/네트워크/dev helper를 바꾸면 `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`, `cargo xt dev down --with-orch` 스모크를 추가로 확인한다. split activation restart/durability를 바꾸면 `cargo xt dev activation-restart-smoke`도 확인하고, merge activation/coalescing/failure를 바꾸면 `cargo xt dev merge-activation-smoke`, `cargo xt dev merge-activation-failure-smoke`, 필요 시 `cargo xt dev merge-activation-restart-smoke`도 확인한다. internal MicroK8s restart helper를 바꾸면 `cargo xt k8s activation-report-check --require-restart`가 검증하는 report shape도 함께 갱신한다.
- Do: 커밋 메시지 `type: summary`(예: `feat: refresh gateway routing`). JJ는 `jj commit -m "..."`; 북마크 이동은 지시가 있을 때만(`jj bookmark set main -r <rev>`).
- Don’t: 기존 변경을 덮어쓰거나 파괴적 명령(`git reset --hard`, 무단 삭제 등) 실행 금지.
- Don’t: 계획을 구현된 것처럼 문서화하지 말 것.

## 완료 조건
- 코드 변경: `cargo xt`와 `cargo test`가 통과해야 한다. 실패하거나 비용이 너무 크면 이유와 가장 가까운 대체 검증을 남긴다.
- 런타임 변경: dev stack이 실제로 살아 있고 테스트 클라이언트가 gateway에 연결되는지 확인한다.
- 문서 변경: README/AGENTS/docs의 구현/계획/명령이 현재 동작과 어긋나지 않는지 확인하고, `cargo xt harness`로 discoverability와 crate boundary를 확인한다.
- VCS 정리: `jj status`와 `jj diff`로 변경 범위를 확인하고, 사용자가 push까지 요청한 경우 `jj git fetch` 후 지정 bookmark를 push한다.

## 소통/결정 기록
- 혼자 토이 프로젝트 기준: 결정/메모는 README(Design Overview) 또는 AGENTS에 간단히 남기면 충분. 필요 시 ADR 추가.
