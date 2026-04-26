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
- `xtask`: 포맷/린트/체크/harness 헬퍼

## Quick Start
- 빌드: `cargo build`
- 검증: `cargo xt` (fmt → clippy → check → harness)
- Harness만 확인: `cargo xt harness`
- 전체 테스트: `cargo test`
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
  - 로그: `.dev/logs/{worker,gateway,orch}.log`
- 로그 보기: `cargo xt dev logs --target all --follow` (또는 `--target gateway|worker|orch`, `--lines 200`)
- 로컬 스모크:
  - `cargo xt dev up --with-orch`
  - `cargo run -p tessera-client -- ping --ts 123`
  - `cargo xt dev down --with-orch`
- 환경변수(옵션):
  - `TESSERA_GW_ADDR` 기본 `127.0.0.1:4000`
  - `TESSERA_GW_REFRESH_SECS` 기본 `5`초(Orchestrator 라우팅 스냅샷 재조회 주기)
  - `TESSERA_GW_METRICS_ADDR` 기본 unset (설정 시 Gateway가 `GET /metrics` Prometheus text endpoint를 함께 노출, 예: `127.0.0.1:4100`)
  - `TESSERA_WORKER_ADDR` 기본 `127.0.0.1:5001`
  - `TESSERA_WORKER_ADVERTISE_ADDR` 기본 `TESSERA_WORKER_ADDR` (오케스트레이터에 등록할 워커 주소, `TESSERA_WORKER_ADDR`가 `0.0.0.0`/`::`인 경우 반드시 지정)
  - `TESSERA_WORKER_ID` 기본 `worker-local`
  - `TESSERA_WORKER_REFRESH_SECS` 기본 `5`초(Orchestrator 연결 실패 시 워커가 할당 재등록을 재시도하는 주기)
  - `TESSERA_WORKER_METRICS_ADDR` 기본 unset (설정 시 Worker가 `GET /metrics` Prometheus text endpoint를 함께 노출, 예: `127.0.0.1:5100`)
  - `TESSERA_WORKER_AOI_RADIUS_CELLS` 기본 `1` (현재 셀 기준 AOI ghost로 구독할 인접 셀 반경, `1`이면 3x3)
  - `TESSERA_WORKER_AOI_CELL_SPAN_UNITS` 기본 `32` (경계 기반 AOI에서 사용할 셀 로컬 좌표 범위)
  - `TESSERA_WORKER_AOI_EDGE_MARGIN_UNITS` 기본 unset (visibility radius가 unset일 때, root actor가 셀 경계에 가까울 때만 해당 방향 ghost 셀을 구독)
  - `TESSERA_WORKER_AOI_VISIBILITY_RADIUS_UNITS` 기본 unset (설정 시 actor 위치에서 후보 셀 사각형까지의 거리 기반으로 ghost 셀을 구독)
  - `TESSERA_WORKER_AOI_MAX_CELLS` 기본 `64` (actor별 AOI 후보 셀 cap, 가까운 셀 우선)
  - `TESSERA_WORKER_HANDOVER_MOVE_BUFFER_CAPACITY` 기본 `128` (handover `Freeze`/`Diff` 중 source worker가 cell별로 보관할 client move 최대 개수)
  - `TESSERA_WORKER_HANDOVER_MOVE_BUFFER_TTL_MS` 기본 `2000` (buffered move가 replay를 기다릴 수 있는 최대 시간)
  - `TESSERA_ORCH_ADDR` 기본 `127.0.0.1:6000`
  - `TESSERA_ORCH_METRICS_ADDR` 기본 unset (설정 시 Orchestrator가 `GET /metrics` Prometheus text endpoint를 함께 노출, 예: `127.0.0.1:6100`)
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
- Worker: 부팅 시 `RegisterWorker`로 셀 소유권 스냅샷 취득 후 해당 셀만 처리, 셀별 이동 브로드캐스트를 actor별 최신 상태로 per-cell tick flush batch에서 처리하며 동일 worker가 소유한 인접 셀의 `Snapshot/Delta/Despawn`를 AOI ghost로 전달하고 assignment refresh 및 root actor 이동 시 기존 연결의 AOI 구독도 재동기화한다. AOI는 cell radius, edge margin, visibility radius, max-cell cap으로 제한할 수 있으며, Orchestrator listing으로 remote peer route와 remote AOI interest를 추적하고 worker 간 `Subscribe/Unsubscribe/Snapshot/Delta/Despawn` ghost relay를 실제 TCP로 중계하며 peer-shared 세션/집계 구독과 remote actor cache로 fanout을 재사용하고 opt-in `/metrics`로 relay fanout/backpressure/reconnect 카운터를 노출
- Orchestrator/Gateway: Orchestrator는 `RegisterWorker`/`GetAssignments`/`ListAssignments`/`WatchAssignments`와 `GetHealth`/`GetMetrics`/`SubmitHandoverCommand` gRPC 엔드포인트를 제공하고, Orchestrator/Gateway 모두 opt-in Prometheus `/metrics` exporter 제공
- Handover runtime baseline: `PreCopy → Freeze → Diff → Commit`/`Abort` control-plane 상태머신과 validation을 제공하고, Orchestrator `ListAssignments`/`WatchAssignments`가 active handover status를 내려주며, source Worker는 `Freeze`/`Diff` 중 bounded buffer에 client move를 보관한다. `Abort`처럼 source가 계속 cell을 소유하면 buffered move를 FIFO로 로컬 replay하고, `Commit`은 target Worker 등록과 bounded retry budget을 확인한 뒤 assignment를 target으로 전환한다. source Worker는 commit release 시 actor snapshot, actor별 owner session manifest, buffered move를 target Worker에 `HandoverReplay` relay payload로 넘기며, target은 replay를 idempotent하게 적용하고 owner map을 즉시 구성한다.
- 테스트 클라이언트(REPL/스크립트), `cargo xt` dev 툴킷

### 🚧 Planned / Upcoming
- Worker 간 ghost relay의 장기 scrape/tracing assertions와 health policy 고도화
- 리밸런싱 자동화, 동적 분할(V1/V2) 등은 아직 미구현

## Protocol Snapshot
- Envelope: `cell: CellId`, `seq: u64`, `epoch: u32`, `payload: ClientMsg|ServerMsg`
- Client ingress envelope: Gateway는 client connection별 stable `session` id를 Worker로 주입해 route switch 뒤에도 actor ownership을 이어간다. 직접 Worker로 들어오는 legacy client frame은 session 없이도 계속 허용된다.
- 멱등·역전 처리의 기반으로 `seq/epoch` 사용(현재 워커는 응답 `seq` 증가)
- 클라 옵션: `--world --cx --cy --epoch`로 Envelope 기본값 설정
- Handover control-plane: `SubmitHandoverCommand`는 `PreCopy`, `Freeze`, `Diff`, `Commit`, `Abort`를 순서 검증하고, `Commit`은 registered target Worker로 assignment를 전환하며, `ListAssignments`/`WatchAssignments`는 active handover status와 최신 assignment를 함께 전달한다. Commit 전 target 미등록은 bounded retry budget 안에서 `Diffing`을 유지하고, budget 소진 시 assignment transfer 전에 abort한다. Commit 후 Worker `HandoverReplay`는 actor snapshot, owner session manifest, buffered move를 target으로 넘긴다. 자세한 정책은 `docs/handover.md`에 둔다.

## Troubleshooting
- 포트 점유: `TESSERA_GW_ADDR`, `TESSERA_WORKER_ADDR`를 변경하거나 점유 프로세스 종료
- 로그 확인: `cargo xt dev logs --target all --follow`
- Orchestrator metrics 확인: `TESSERA_ORCH_METRICS_ADDR=127.0.0.1:6100 cargo run -p tessera-orch` 후 `curl http://127.0.0.1:6100/metrics`
- Gateway metrics 확인: `TESSERA_GW_METRICS_ADDR=127.0.0.1:4100 cargo run -p tessera-gateway` 후 `curl http://127.0.0.1:4100/metrics`
- Worker metrics 확인: `TESSERA_WORKER_METRICS_ADDR=127.0.0.1:5100 cargo run -p tessera-worker` 후 `curl http://127.0.0.1:5100/metrics`
- clippy 경고: `cargo xt`는 `-D warnings`로 엄격 체크. 경고 메시지에 따라 수정
- 게이트웨이 라우팅/업스트림 실패가 반복되면 클라이언트 연결을 종료하므로, 클라는 재접속이 필요할 수 있음

## Automation Harness
- 에이전트 기본 루프: `README.md`와 `AGENTS.md`를 읽고, 기존 패턴을 따라 구현한 뒤, `cargo xt`, `cargo test`, 필요 시 로컬 스모크로 검증한다.
- `docs/quality.md`는 자율 수행 계약, feedback loop, crate boundary policy의 repo-local 기준 문서다.
- `cargo xt harness`는 README/AGENTS/docs/CI discoverability와 내부 크레이트 의존 방향을 검사한다.
- 현재 기계적 crate boundary: `tessera-core`/`tessera-proto`는 내부 Tessera crate에 의존하지 않고, `tessera-gateway`/`tessera-worker`/`tessera-orch`는 `tessera-core`와 `tessera-proto`만 공유 의존성으로 사용하며 서로 직접 의존하지 않는다.
- CI는 push/PR에서 `cargo xt`, `cargo test`, `cargo xt dev up --with-orch` + `cargo run -p tessera-client -- ping --ts 123` 스모크를 실행한다.

## Design Overview
- 문제: 단일 프로세스/샤드 구조는 심리스 월드에서 병목과 끊김을 만든다. 목표는 셀 단위 분할/이동/분해로 부하를 흡수하고, 클라는 단일 소켓을 유지한다.
- Goals: V0 고정 그리드+정적 매핑(구현), V1 셀 리밸런싱·Handover, V1 AOI/ghost 최적화, V2 동적 분할(쿼드트리) 등.
- Non-goals(초기): 완성형 게임 서버 기능, 완전 무중단 마이그레이션, 멀티리전 일관성.
- 핵심 개념: `CellId{world,cx,cy,depth,sub}` 단일-writer, Gateway는 Orchestrator watch/스냅샷 기반 라우팅, Worker는 틱 루프에서 셀별 이동 브로드캐스트를 actor별 최신 상태로 flush하고 동일 worker가 소유한 인접 셀의 `Snapshot/Delta/Despawn`를 AOI ghost로 전달하며 assignment refresh와 root actor 이동 때 기존 세션의 AOI 구독을 다시 맞춘다. AOI는 셀 반경, edge margin, actor-to-cell-rect visibility radius, max-cell cap으로 제한하며, Orchestrator listing으로 remote peer route와 remote AOI interest를 추적하고 worker 간 ghost `Subscribe/Unsubscribe/Snapshot/Delta/Despawn`를 TCP relay로 주고받되 route별 shared 세션과 remote actor cache로 fanout을 재사용하며, Orchestrator는 `cell→worker` 레지스트리.
- 데이터 흐름: Gateway 입력 → 대상 셀 워커 전달 → Worker 틱/델타 → 클라·인접 워커 전파 → 필요 시 Handover(`PreCopy→Freeze→Diff/Commit→라우팅 교체`).
- 운영 메모: 틱 20–30Hz, 셀 크기는 AOI의 2–3배 권장, 위험요소는 게이트웨이 병목·Handover 순서·분할/병합 플래핑·AOI 폭주(`TESSERA_WORKER_AOI_MAX_CELLS` cap 필요).

## Contributing & Workflow
- 기본 브랜치 `main`; 커밋 메시지 포맷은 `type: summary`(예: `feat: refresh gateway routing`), 한 커밋에 명확한 변경 세트만 담습니다.
- 코드 변경 시 문서/테스트를 함께 갱신하고, README의 ✅(구현)/🚧(계획) 구분을 유지합니다.
- 제출 전 `cargo xt`와 `cargo test`를 실행합니다. `cargo xt`에는 `cargo xt harness`가 포함됩니다. 런타임/네트워크/dev helper 변경은 Run Locally의 로컬 스모크까지 확인합니다.
- 자동화 에이전트는 목표와 제약이 충분하면 자율적으로 구현/검증/문서화를 진행하고, 파괴적 작업·published history rewrite·명시되지 않은 원격 bookmark 이동·외부 운영/비용 리스크처럼 되돌리기 어려운 경우에만 사용자 확인을 요청합니다.
- 추가 지침과 에이전트 컨텍스트는 `AGENTS.md`를 참고하세요.
