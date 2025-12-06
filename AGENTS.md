# Tessera — Agent Quick Context

이 리포지토리에서 자동화 에이전트가 작업할 때 필요한 최소 컨텍스트. “최소 읽기 세트”: `README.md`(현재 상태/로드맵) + AGENTS.md.

## 읽기 우선
- 작업 전 `README.md`의 Status/Design Overview/Run Locally를 확인.
- 이 문서(AGENTS.md)에서 명령/지침/금지사항을 확인.

## 현재 범위 (요약)
- V0 구현: 고정 그리드 셀, Gateway↔Worker TCP 파이프라인, Orchestrator `ListAssignments` 스냅샷 + 주기적 재조회(`TESSERA_GW_REFRESH_SECS`). *(다음 목표: 라우팅 watch/스트리밍 적용)*
- 주요 크레이트: `tessera-gateway`, `tessera-worker`, `tessera-orch`, 공용 타입 `tessera-core`, 테스트 클라이언트 `tessera-client`, 자동화 `xtask`.

## 로컬 실행/종료
- 빠른 기동: `cargo xt dev up` (worker+gateway), Orchestrator까지: `cargo xt dev up --with-orch [--orch-config path]`.
- 종료: `cargo xt dev down` / `cargo xt dev down --with-orch`.
- 로그: `.dev/logs/{worker,gateway,orch}.log`; PID는 `.dev/pids/`. 기동 후 `tail -f`로 정상 시작 여부 확인.
- 개별 실행 예: `cargo run -p tessera-orch`, `cargo run -p tessera-gateway`, `cargo run -p tessera-worker`, `cargo run -p tessera-client -- ...`.

## 기본 환경 변수
- `TESSERA_GW_ADDR` 기본 `127.0.0.1:4000`
- `TESSERA_GW_REFRESH_SECS` 기본 `5` (Gateway 라우팅 재조회 주기)
- `TESSERA_WORKER_ADDR` 기본 `127.0.0.1:5001`
- `TESSERA_WORKER_ID` 기본 `worker-local`
- `TESSERA_ORCH_ADDR` 기본 `127.0.0.1:6000`
- `RUST_LOG` 기본 `info`

## 작업 지침 (Do/Don’t)
- Do: README의 ✅(구현) / 🚧(계획) 구분을 지키고, 아키텍처 변화 시 Design Overview 갱신.
- Do: 단일 파일 수정은 `apply_patch` 우선, 검색은 `rg`/`rg --files` 우선.
- Do: 최소 `cargo fmt`, 관련 `cargo test`; 필요 시 `cargo check --workspace`. dev 헬퍼는 `cargo xt`/`cargo xt dev ...` 활용하고, 작업 마무리 시에는 `cargo xt`와 `cargo test`를 반드시 돌려 결과를 확인한다.
- Do: 커밋 메시지 `type: summary`(예: `feat: refresh gateway routing`). JJ는 `jj commit -m "..."`; 북마크 이동은 지시가 있을 때만(`jj bookmark set main -r <rev>`).
- Don’t: 기존 변경을 덮어쓰거나 파괴적 명령(`git reset --hard`, 무단 삭제 등) 실행 금지.
- Don’t: 계획을 구현된 것처럼 문서화하지 말 것.

## 소통/결정 기록
- 혼자 토이 프로젝트 기준: 결정/메모는 README(Design Overview) 또는 AGENTS에 간단히 남기면 충분. 필요 시 ADR 추가.
