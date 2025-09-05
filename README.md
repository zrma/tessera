# Tessera

Cell-based world orchestration for real-time servers in Rust.

**Tessera**는 심리스(Seamless) 오픈월드를 위해 월드를 **셀(Cell)** 로 분할하고,
셀의 **소유권 이전(Handover)**, **AOI/ghost 동기화**, **동적 분할(쿼드트리)** 을 제공하는
실시간 서버 프레임워크입니다. 목표는 “MMO급 시뮬레이션”의 **기본 뼈대**입니다.

- 언어/런타임: Rust + Tokio
- 배포: Self-hosted / Kubernetes 친화
- 통신: 클라↔게이트웨이(TCP/QUIC), 서버↔서버(gRPC stream 또는 NATS)

## Workspace
- `crates/tessera-gateway`: 클라이언트 입출력 게이트웨이(Stateless)
- `crates/tessera-worker`: 셀 소유자/틱 루프(Stateful)
- `crates/tessera-orch`: 오케스트레이터(Control-plane)
- `crates/tessera-core`: 공용 타입/프레이밍/CellId
- `crates/tessera-proto`: (선택) gRPC/IDL 코드젠
- `crates/tessera-sim`: 부하/플레이어 시뮬레이터
- `crates/tessera-client`: 테스트용 CLI 클라이언트
- `xtask`: 포맷/린트/체크 헬퍼

## Quick Start
- 빌드: `cargo build`
- 검증: `cargo xt` (fmt → clippy → check)
- 실행 예시:
  - `cargo run -p tessera-gateway`
  - `cargo run -p tessera-worker`
  - `cargo run -p tessera-orch`

## Run Locally
- 일괄 실행/정지(Worker+Gateway):
  - 올리기: `cargo xt dev up`
  - 내리기: `cargo xt dev down`
  - 로그: `.dev/logs/{worker,gateway}.log`
- 로그 보기: `cargo xt dev logs --target all --follow` (또는 `--target gateway|worker`, `--lines 200`)
- 환경변수(옵션):
  - `TESSERA_GW_ADDR` 기본 `127.0.0.1:4000`
  - `TESSERA_WORKER_ADDR` 기본 `127.0.0.1:5001`
  - `RUST_LOG` 기본 `info`

## Test Client
- Ping: `cargo run -p tessera-client -- ping --ts 123`
- Join: `cargo run -p tessera-client -- join --actor 1 --x 0 --y 0`
- Move: `cargo run -p tessera-client -- move --actor 1 --dx 1 --dy 0.5`
- REPL: `cargo run -p tessera-client -- repl --actor 1`
- 스크립트: `cargo run -p tessera-client -- script ./script.txt --actor 1`

자세한 설계와 범위는 `docs/overview.md` 를 참고하세요.
