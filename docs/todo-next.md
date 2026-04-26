# Tessera Next Todo

Last reviewed: 2026-04-26

## Baseline

- 작업 시작 시점에는 Orchestrator Prometheus exporter 미완성 diff가 있었고, 이번 pass에서 이를 검증 가능한 변경으로 마무리했다.
- V0 범위는 고정 그리드 셀, Gateway/Worker TCP 파이프라인, Orchestrator assignment snapshot/watch, Worker AOI ghost relay까지 구현된 상태다.
- 즉시 사용자 판단이 필요한 에스컬레이션은 없다. 다만 handover 순서와 동적 분할 정책은 구현 전 의미론 결정을 다시 확인하는 편이 안전하다.

## P0

1. [done 2026-04-26] Orchestrator Prometheus exporter
   - 목표: 기존 `GetMetrics` snapshot을 Prometheus text format으로 노출한다.
   - 완료 조건: `TESSERA_ORCH_METRICS_ADDR` 설정 시 `GET /metrics`가 `tessera_orch_*` gauge/counter를 반환하고, 기본 gRPC 경로는 기존처럼 동작한다.
   - 검증: unit/HTTP smoke test, `cargo xt`, `cargo test`, metrics-enabled dev smoke.

2. [done 2026-04-26] Worker/Gateway relay observability
   - 목표: remote ghost relay의 연결 상태, 재연결 횟수, 구독 fanout, dropped/backpressure 이벤트를 메트릭으로 볼 수 있게 한다.
   - 완료 조건: Worker와 Gateway가 각자 runtime-local counters와 opt-in `/metrics` endpoint를 갖고, 기존 relay/connection behavior를 유지한다.
   - 검증: unit metrics format tests, `cargo xt`, `cargo test`, metrics-enabled dev smoke.

3. [done 2026-04-26] Handover command protocol skeleton
   - 목표: `PreCopy`, `Freeze`, `Diff`, `Commit` 명령의 proto 타입과 Orchestrator 상태 모델을 추가하되 실제 무중단 이동은 feature slice로 나눈다.
   - 완료 조건: 명령 타입, validation, rejected-state tests, 문서화가 먼저 들어가고 runtime routing 전환은 다음 change로 분리한다.
   - 정책: freeze/diff 중 client move는 현재 slice에서 reject하고, commit failure는 기존 source owner/route 유지 후 abort한다.

4. [done 2026-04-26] Handover source move buffering
   - 목표: freeze/diff 중 즉시 reject되는 client move를 source Worker의 bounded buffer에 보관하고 handover status가 해제되면 순서대로 재적용해 client-visible error를 완화한다.
   - 완료 조건: buffer capacity/TTL/overflow 정책, Orchestrator listing의 handover status propagation, Worker replay ordering test가 고정된다.
   - 주의: buffering은 짧은 handover window를 완화하지만, 장시간 freeze·buffer overflow·반복 commit 실패에는 여전히 reject/abort가 필요하다.

5. [done 2026-04-26] Handover commit route switch
   - 목표: target readiness 확인 후 assignment/listing을 target Worker로 전환하고, Gateway route switch와 Worker owned-cell 갱신을 기존 watch/listing 경로에 연결한다.
   - 완료 조건: target Worker가 등록되지 않은 commit은 retryable reject로 남고, 등록된 target으로 commit하면 Orchestrator assignment가 이동하며, source Worker는 더 이상 이전 cell을 소유한 것으로 처리하지 않는다.
   - 주의: 이번 slice는 route/ownership 전환까지이며 source buffered move의 target-side replay는 아직 없다. commit 이후 source buffer drain은 `handover_cell_not_owned` error path를 유지한다.

6. [next] Handover target-side replay and commit retry
   - 목표: source buffered move를 target Worker에 넘기는 replay 경로와 commit retry/abort 정책을 runtime 경로에 연결한다.
   - 작업 계획: `docs/todo-handover-replay.md`에 retry budget, replay payload, target apply, stable session 후속 작업을 분리했다.
   - 완료 조건: target replay ordering, retry budget/timeout, abort-before-assignment-transfer tests가 고정된다.
   - 주의: client socket 유지, source/target 중복 적용 방지, stale route 차단이 핵심 리스크다. 이번 milestone은 replayed actor를 target에서 claim-on-first-use로 처리하고, stable session handover는 별도 후속으로 둔다.

## P1

1. AOI precision upgrade
   - 목표: 현재 셀 경계 기반 edge margin을 거리/가시성 기반으로 확장한다.
   - 완료 조건: centered/edge/distant actor cases가 deterministic test로 고정되고, AOI 폭주 방지 cap이 문서화된다.

2. Multi-cell tick pipeline
   - 목표: Worker 내부의 셀별 tick, broadcast flush, relay fanout 단계를 더 명시적인 pipeline으로 나눈다.
   - 완료 조건: 기존 client/ghost behavior test가 유지되고, per-cell stage를 개별적으로 테스트할 수 있다.

3. Dynamic split/merge design note
   - 목표: quadtree split/merge 조건, hysteresis, assignment churn 제한을 문서로 고정한다.
   - 완료 조건: README의 V2 동적 분할 목표와 일치하는 `docs/` 설계 노트가 생기고, 구현 전제와 non-goals가 분리된다.

## P2

- Long-lived quality loop: metrics scrape/tracing assertion을 `cargo xt harness` 또는 별도 smoke로 넣을지 결정한다.
- Gateway latency/readiness 관측: request latency histogram, readiness endpoint, reconnect-required close reason을 metric/log field로 정리한다.
- Container/Kubernetes packaging: Prometheus scrape annotation, readiness/liveness endpoint, config mount 예시를 추가한다.
