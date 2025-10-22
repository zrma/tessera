# Tessera Contributing Guide

이 문서는 Tessera 리포지토리에서 작업할 때 따라야 할 기본 관행을 정리합니다. 새 세션을 열거나 새로운 팀원이 합류했을 때 참고할 수 있는 온보딩 자료 역할을 합니다.

## Branch & Commit
- 기본 브랜치는 `main`; 기능 작업은 가능하면 주제별 브랜치에서 진행합니다.
- 커밋 메시지는 `type: summary` 형태를 권장합니다.
  - 예: `feat: integrate worker startup with orchestrator assignments`
  - 주요 prefix: `feat`, `fix`, `docs`, `refactor`, `test`, `chore` 등
- 하나의 커밋에는 의미가 명확한 변경 세트를 담습니다. 코드‧문서 혼합 가능하지만 이유가 드러나도록 요약에 반영하세요.
- PR 제목은 첫 커밋 메시지 요약을 그대로 사용하거나, 여러 커밋을 묶는 경우 대표 요약으로 정리합니다.

## Workflow
1. `README.md`와 `docs/overview.md`를 꼭 읽고 현재 구현 범위/로드맵을 파악합니다.
2. 작업 전 현재 상태를 확인 (`git status`, `cargo check` 등) 후 새 브랜치에서 수정합니다.
3. 코드 변경 시 관련 문서/테스트를 함께 갱신합니다. 문서에서 구현/계획 항목 구분을 명확히 유지하세요.
4. `cargo fmt`, `cargo test`, `cargo check --workspace`를 실행해 유효성을 확인합니다.
5. 커밋 메시지는 위의 규칙을 따르고, PR에 변경 요약 + 테스트 내용(`cargo test` 결과 등)을 적습니다.

## Testing
- 기본 원칙: 코드 변경 후에는 최소 `cargo fmt`와 관련 `cargo test`를 실행합니다.
- 신규 기능은 가능한 테스트(단위, 통합, 스냅샷 등)를 동반합니다.
- 테스트가 어려운 경우 PR 설명에 이유와 수동 확인 방법을 기록합니다.

## Documentation
- README는 “현재 상태(✅)”와 “향후 계획(🚧)”을 명확하게 유지합니다.
- 아키텍처/로드맵 변경 시 `docs/overview.md`를 업데이트하고 상태 태그를 적절히 조정합니다.
- 새 프로토콜/CLI/환경 변수 등을 추가하거나 수정할 때는 관련 문서 섹션에 해당 내용을 반영합니다.

## Release & Versioning
- 현재는 사내 개발 단계이므로 태그/릴리즈 주기는 미정입니다. 향후 릴리즈 절차가 정해지면 이 문서를 업데이트합니다.
- 외부 공유가 필요한 변경은 README와 Contributing 문서를 먼저 최신 상태로 맞춘 뒤 공지합니다.

## 기타 참고
- 질의응답/토론은 우선 Slack `#tessera-dev` 채널(가정)에 남기고, 의사결정 결과는 문서(overview.md 또는 관련 ADR)에 기록합니다.
- 반복 작업은 `xtask`에 명령으로 추가해 자동화합니다.
