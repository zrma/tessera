# Tessera Overview

## 0. 한 줄 요약
심리스 오픈월드를 위해 월드를 **셀(Cell)** 로 나누고,
각 셀을 **단일-writer**가 소유해 **낮은 지연·예측 가능한 일관성**을 제공하는 서버 프레임워크.
핵심: ① 셀 파티셔닝 ② Handover ③ AOI/ghost ④ 동적 분할(쿼드트리)

---

## 1. 문제 정의 (왜 필요한가)
- 단일 프로세스는 심리스 월드에서 장애/성능의 단일 병목입니다.
- “샤드/방”은 공간이 끊겨 경계 이동 시 지연·재접속이 발생합니다.
- 목표: **셀 단위 분할/이동/분해**로 부하 흡수, 클라는 **소켓 1개**만 유지.

---

## 2. 목표(Goals) / 비목표(Non-goals)

### Goals
- 셀 파티셔닝: 고정 그리드(V0) → 재배치(V1) → 쿼드트리(V2)
- Handover: pre-copy → freeze → diff → commit
- AOI/ghost: 이웃만 증분 전파(브로드캐스트 금지)
- K8s 친화: L4 노출, StatefulSet, preStop drain
- 낮은 진입장벽 스켈레톤: Gateway/Worker/Orch/Sim 모노레포

### Non-goals (초기 버전)
- 완성형 게임 서버(전투/AI/스킬 제공 ❌)
- 모든 변형의 완전 무중단 마이그레이션(필요 시 재접속/롤백)
- 멀티-리전 복제/글로벌 일관성(후속)

---

## 3. 핵심 개념

### 3.1 Cell (셀)
- 공간 파티셔닝 단위. 키: `CellId{ world, cx, cy, depth, sub }`
- Single-writer: 한 시점에 **하나의 Worker**만 쓰기 가능
- Ghost: 이웃에 배포되는 **읽기 전용** 상태(AOI 범위만 갱신)

### 3.2 Gateway / Worker / Orchestrator
- Gateway(Stateless): 클라와 **단일 소켓** 유지, 여러 셀 업데이트를 머지하여 전송
- Worker(Stateful): 셀 **소유자**, 틱 루프 + AOI 브로드캐스트 + 이웃 ghost 동기화
- Orchestrator(Control-plane): `cell → worker` 레지스트리, 리밸런싱/분할 결정·지시

---

## 4. 데이터 흐름 (요약)
1) Gateway가 입력 수집 → `CellId` 기반으로 소유 Worker에 전달
2) Worker 틱 업데이트 → AOI 대상에 델타 생성
3) 전파: (a) Gateway(클라) (b) 인접 Worker(ghost)
4) 필요 시 Handover: `PreCopy → Freeze → Diff/Commit → 라우팅 교체 → Unfreeze`

---

## 5. 프로토콜(초안)

### 5.1 내부 Control (Orchestrator ↔ Worker)
- gRPC (bidi stream)
- 메시지: `AssignCell`, `PreCopy{snapshot}`, `Freeze{epoch}`, `Diff{from_seq}`, `Commit{new_owner, epoch}`, `Ack/Abort`

### 5.2 데이터 채널
- 클라 ↔ Gateway: TCP/QUIC + length‑prefixed 바이너리(Protobuf/FlatBuffers)
- Worker ↔ Worker(ghost): gRPC stream 또는 NATS(stream)
- 모든 메시지: `seq`, `epoch`, `cell_id` 포함(멱등/역전 처리)

### 5.3 Envelope (V0 적용)
- 구조: `Envelope{ cell: CellId, seq: u64, epoch: u32, payload }`
- 역할: 라우팅·재전송·역전 처리의 공통 래퍼. 현재 V0에서는 워커가 응답 `seq`를 증가시키며 에코.
- 클라: 기본 `cell/epoch`를 커맨드라인에서 지정(`--world --cx --cy --epoch`).

---

## 6. AOI & 셀 크기(초기 값)
- 틱: 20–30Hz (50–33ms)
- 셀 크기: AOI 반경의 2–3배(경계 thrash 최소화)
- 예시: 1km² → 200m 셀(5×5=25) → Worker 3–6개, 연속 블록 할당

---

## 7. 로드스케일/리밸런싱 로직(개요)
- 지표: `tick_time_ms`, `tick_overrun%`, `actors`, `pps`, `out_bytes`
- 스코어: `0.5*actors + 0.3*pps + 0.2*(p99_tick/budget)` 등 가중 합
- 리밸런싱(V1): 임계 초과(지속) 시 인접 Worker로 셀 이동(쿨다운/히스테리시스)
- 분할(V2): 편차·과부하 시 2×2 서브셀로 분해 후 일부 재배치

---

## 8. MVP 범위 (V0 → V2)

### V0 — 고정 그리드 + 정적 매핑 + 관측성
- `CellId`, 패킷 프레이밍, Worker 틱 루프, Gateway 머지 경로
- 정적 registry(ConfigMap/파일) → Gateway 라우팅
- Prometheus 지표

### V1 — 리밸런싱(셀 단위 이동)
- Orchestrator 메트릭 집계 → `PreCopy/Freeze/Diff/Commit`
- 라우팅 원자 교체(소켓 유지), 롤백(Abort)

### V2 — 동적 분할(쿼드트리)
- `depth/sub` 추가, 상/하위 셀 간 handover
- 병합 조건/쿨다운

---

## 9. 위험/제약
- 게이트웨이 병목: 멀티 인스턴스/샤딩(클라→GW 해시)
- 정합성: handover 중 순서 보장, Freeze 최소화
- 운영 복잡도: 분할/병합 플래핑 방지(히스테리시스)
- AOI 폭주: 전파 상한선(rate cap) 필요

---

## 10. 폴더 구조
- `crates/tessera-core` — 공용 타입/프레이밍/CellId
- `crates/tessera-proto` — (선택) gRPC/IDL 코드젠
- `crates/tessera-gateway` — 게이트웨이(Stateless)
- `crates/tessera-worker` — 워커(Stateful, 틱 루프)
- `crates/tessera-orch` — 오케스트레이터(Control-plane)
- `crates/tessera-sim` — 부하/플레이어 시뮬레이터
- `proto/` — 프로토 파일(선택)
- `xtask/` — 개발 자동화(fmt/clippy/check)
- `docs/` — 문서(overview/dev-guide/architecture/adr/...)

---

## 11. 용어집 (Glossary)
- **Cell**: 공간 파티션 단위, 단일-writer
- **Handover**: 셀 소유권 이전 절차(PreCopy → Freeze → Diff → Commit)
- **Ghost**: 이웃 셀에 배포되는 읽기 전용 상태
- **AOI**: 관심영역(전파 대상 범위)
- **Registry**: `cell → worker` 매핑 소스(trust anchor)

---

## 부록: 로컬 실행(요약)
- 올리기: `cargo xt dev up` / 내리기: `cargo xt dev down`
- 로그: `cargo xt dev logs --target all --follow` (또는 `gateway|worker`)
- 클라 예시: `cargo run -p tessera-client -- repl --actor 1`
- 스크립트: `cargo run -p tessera-client -- script ./script.txt --actor 1`
