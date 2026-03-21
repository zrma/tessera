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
  - Orchestrator까지 포함: `cargo xt dev up --with-orch [--orch-config .dev/orch-config.json]`
  - 내리기: `cargo xt dev down`
  - Orchestrator 종료: `cargo xt dev down --with-orch`
  - 로그: `.dev/logs/{worker,gateway}.log`
- 로그 보기: `cargo xt dev logs --target all --follow` (또는 `--target gateway|worker`, `--lines 200`)
- 환경변수(옵션):
  - `TESSERA_GW_ADDR` 기본 `127.0.0.1:4000`
  - `TESSERA_GW_REFRESH_SECS` 기본 `5`초(Orchestrator 라우팅 스냅샷 재조회 주기)
  - `TESSERA_WORKER_ADDR` 기본 `127.0.0.1:5001`
  - `TESSERA_WORKER_ADVERTISE_ADDR` 기본 `TESSERA_WORKER_ADDR` (오케스트레이터에 등록할 워커 주소, `TESSERA_WORKER_ADDR`가 `0.0.0.0`/`::`인 경우 반드시 지정)
  - `TESSERA_WORKER_ID` 기본 `worker-local`
  - `TESSERA_WORKER_REFRESH_SECS` 기본 `5`초(Orchestrator 연결 실패 시 워커가 할당 재등록을 재시도하는 주기)
  - `TESSERA_WORKER_AOI_RADIUS_CELLS` 기본 `1` (현재 셀 기준 AOI ghost로 구독할 인접 셀 반경, `1`이면 3x3)
  - `TESSERA_WORKER_AOI_CELL_SPAN_UNITS` 기본 `32` (경계 기반 AOI에서 사용할 셀 로컬 좌표 범위)
  - `TESSERA_WORKER_AOI_EDGE_MARGIN_UNITS` 기본 unset (설정 시 root actor가 셀 경계에 가까울 때만 해당 방향 ghost 셀을 구독, 좌표계는 `0..TESSERA_WORKER_AOI_CELL_SPAN_UNITS`)
  - `TESSERA_ORCH_ADDR` 기본 `127.0.0.1:6000`
  - `RUST_LOG` 기본 `info`
- 게이트웨이는 Orchestrator 라우팅 스냅샷이 실패할 경우 `TESSERA_WORKER_ADVERTISE_ADDR`(설정 시) 또는 `TESSERA_WORKER_ADDR` 단일 워커로 폴백
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
- Gateway: Orchestrator `WatchAssignments` 스트림으로 셀→워커 라우팅 즉시 반영(실패 시 단일 워커 폴백) + `ListAssignments` 주기 재조회(`TESSERA_GW_REFRESH_SECS`)
- Worker: 부팅 시 `RegisterWorker`로 셀 소유권 스냅샷 취득 후 해당 셀만 처리, 셀별 이동 브로드캐스트를 actor별 최신 상태로 틱 큐에서 flush하며 동일 worker가 소유한 인접 셀의 `Snapshot/Delta/Despawn`를 AOI ghost로 전달하고 assignment refresh 및 root actor 이동 시 기존 연결의 AOI 구독도 재동기화하며, Orchestrator listing으로 remote peer route와 remote AOI interest를 추적하고 worker 간 `Subscribe/Unsubscribe/Snapshot/Delta/Despawn` ghost relay를 실제 TCP로 중계하며 peer-shared 세션/집계 구독과 remote actor cache로 중복 연결을 줄임
- Orchestrator: `RegisterWorker`/`GetAssignments`/`ListAssignments`/`WatchAssignments` gRPC 엔드포인트 제공
- 테스트 클라이언트(REPL/스크립트), `cargo xt` dev 툴킷

### 🚧 Planned / Upcoming
- Orchestrator 메트릭 집계/헬스 체크 및 리밸런싱 명령(`PreCopy/Freeze/Diff/Commit`)
- Worker AOI/ghost를 셀 경계 기반에서 더 정밀한 거리/가시성 기반으로 고도화하고, 다셀 틱 파이프라인을 구조화
- Worker 간 ghost relay의 backpressure/헬스/재연결 관측성을 메트릭으로 노출
- Prometheus 지표, 리밸런싱 자동화, 동적 분할(V1/V2) 등은 아직 미구현

## Protocol Snapshot
- Envelope: `cell: CellId`, `seq: u64`, `epoch: u32`, `payload: ClientMsg|ServerMsg`
- 멱등·역전 처리의 기반으로 `seq/epoch` 사용(현재 워커는 응답 `seq` 증가)
- 클라 옵션: `--world --cx --cy --epoch`로 Envelope 기본값 설정

## Troubleshooting
- 포트 점유: `TESSERA_GW_ADDR`, `TESSERA_WORKER_ADDR`를 변경하거나 점유 프로세스 종료
- 로그 확인: `cargo xt dev logs --target all --follow`
- clippy 경고: `cargo xt`는 `-D warnings`로 엄격 체크. 경고 메시지에 따라 수정
- 게이트웨이 라우팅/업스트림 실패가 반복되면 클라이언트 연결을 종료하므로, 클라는 재접속이 필요할 수 있음

## Design Overview
- 문제: 단일 프로세스/샤드 구조는 심리스 월드에서 병목과 끊김을 만든다. 목표는 셀 단위 분할/이동/분해로 부하를 흡수하고, 클라는 단일 소켓을 유지한다.
- Goals: V0 고정 그리드+정적 매핑(구현), V1 셀 리밸런싱·Handover, V1 AOI/ghost 최적화, V2 동적 분할(쿼드트리) 등.
- Non-goals(초기): 완성형 게임 서버 기능, 완전 무중단 마이그레이션, 멀티리전 일관성.
- 핵심 개념: `CellId{world,cx,cy,depth,sub}` 단일-writer, Gateway는 Orchestrator watch/스냅샷 기반 라우팅, Worker는 틱 루프에서 셀별 이동 브로드캐스트를 actor별 최신 상태로 flush하고 동일 worker가 소유한 인접 셀의 `Snapshot/Delta/Despawn`를 AOI ghost로 전달하며 assignment refresh와 root actor 이동 때 기존 세션의 AOI 구독을 다시 맞추고, 필요하면 셀 경계 기반 edge margin으로 ghost 범위를 좁히며, Orchestrator listing으로 remote peer route와 remote AOI interest를 추적하고 worker 간 ghost `Subscribe/Unsubscribe/Snapshot/Delta/Despawn`를 TCP relay로 주고받되 route별 shared 세션과 remote actor cache로 fanout을 재사용하며, Orchestrator는 `cell→worker` 레지스트리.
- 데이터 흐름: Gateway 입력 → 대상 셀 워커 전달 → Worker 틱/델타 → 클라·인접 워커 전파 → 필요 시 Handover(`PreCopy→Freeze→Diff/Commit→라우팅 교체`).
- 운영 메모: 틱 20–30Hz, 셀 크기는 AOI의 2–3배 권장, 위험요소는 게이트웨이 병목·Handover 순서·분할/병합 플래핑·AOI 폭주(rate cap 필요).

## Contributing & Workflow
- 기본 브랜치 `main`; 커밋 메시지 포맷은 `type: summary`(예: `feat: refresh gateway routing`), 한 커밋에 명확한 변경 세트만 담습니다.
- 코드 변경 시 문서/테스트를 함께 갱신하고, README의 ✅(구현)/🚧(계획) 구분을 유지합니다.
- 제출 전 `cargo fmt`, 관련 `cargo test`, 필요 시 `cargo check --workspace`를 실행합니다.
- 추가 지침과 에이전트 컨텍스트는 `AGENTS.md`를 참고하세요.
