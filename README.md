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
  - `TESSERA_WORKER_ID` 기본 `worker-local`
  - `TESSERA_ORCH_ADDR` 기본 `127.0.0.1:6000`
  - `RUST_LOG` 기본 `info`
- 게이트웨이는 Orchestrator 라우팅 스냅샷이 실패할 경우 `TESSERA_WORKER_ADDR` 단일 워커로 폴백
- 오케스트레이터 실행: `cargo run -p tessera-orch` (기본 `TESSERA_ORCH_ADDR=127.0.0.1:6000`)
- 오케스트레이터 설정: 기본값은 `worker-local → CellId::grid(0, 0, 0)`이며, `TESSERA_ORCH_CONFIG`(파일 경로) 또는 `TESSERA_ORCH_CONFIG_JSON`(직접 JSON)으로 커스텀 매핑 가능
- 설정 예시:
```json
{
  "workers": [
    {
      "id": "worker-a",
      "addr": "127.0.0.1:5001",
      "cells": [
        {"world": 0, "cx": 0, "cy": 0},
        {"world": 0, "cx": 1, "cy": 0, "depth": 1, "sub": 0}
      ]
    }
  ]
}
```

## Test Client
- Ping: `cargo run -p tessera-client -- ping --ts 123`
- Join: `cargo run -p tessera-client -- join --actor 1 --x 0 --y 0`
- Move: `cargo run -p tessera-client -- move --actor 1 --dx 1 --dy 0.5`
- REPL: `cargo run -p tessera-client -- repl --actor 1` (history, `help` 명령 지원)
- 스크립트: `cargo run -p tessera-client -- script ./script.txt --actor 1`

## Status Snapshot

### ✅ Implemented (V0 scope)
- Core 타입/프레이밍 및 Envelope 래핑(`CellId`, `ClientMsg/ServerMsg`, length-prefixed JSON)
- Gateway↔Worker TCP 프록시 파이프라인 (Join/Move/Ping 처리)
- Gateway: Orchestrator에서 `ListAssignments` 스냅샷을 받아 셀→워커 라우팅 적용 (실패 시 단일 워커로 폴백)
- Worker: 부팅 시 `RegisterWorker`로 셀 소유권 스냅샷 취득 후 해당 셀만 처리
- Orchestrator: `RegisterWorker`/`GetAssignments`/`ListAssignments` gRPC 엔드포인트 제공
- 테스트 클라이언트(REPL/스크립트), `cargo xt` dev 툴킷

### 🚧 Planned / Upcoming
- Gateway 라우팅 테이블 실시간 갱신(주기적 `ListAssignments`, 스트리밍 watch)
- Orchestrator 메트릭 집계/헬스 체크 및 리밸런싱 명령(`PreCopy/Freeze/Diff/Commit`)
- Worker 셀 단위 AOI/ghost 브로드캐스트 강화 및 다셀 틱 파이프라인 구조화
- Prometheus 지표, 리밸런싱 자동화, 동적 분할(V1/V2) 등은 아직 미구현

## Protocol Snapshot
- Envelope: `cell: CellId`, `seq: u64`, `epoch: u32`, `payload: ClientMsg|ServerMsg`
- 멱등·역전 처리의 기반으로 `seq/epoch` 사용(현재 워커는 응답 `seq` 증가)
- 클라 옵션: `--world --cx --cy --epoch`로 Envelope 기본값 설정

## Troubleshooting
- 포트 점유: `TESSERA_GW_ADDR`, `TESSERA_WORKER_ADDR`를 변경하거나 점유 프로세스 종료
- 로그 확인: `cargo xt dev logs --target all --follow`
- clippy 경고: `cargo xt`는 `-D warnings`로 엄격 체크. 경고 메시지에 따라 수정

자세한 설계와 범위는 `docs/overview.md` 를 참고하세요.

## Contributing & Workflow
- 팀 작업 관행(커밋 컨벤션, 테스트, 문서 업데이트 등)은 `docs/contributing.md`에 정리돼 있습니다.
- 새 기능을 추가하거나 문서를 수정하기 전에 해당 가이드를 확인해주세요.
