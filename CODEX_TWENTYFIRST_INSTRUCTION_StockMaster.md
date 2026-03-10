# Codex First Instruction After KRX Approval — StockMaster

작업 루트는 `D:\MyApps\StockMaster` 입니다.

루트에 있는 기존 문서들을 먼저 읽고, 특히 아래 문서를 source of truth로 사용하세요.

- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_019_KRX_Live_API_Activation_and_Market_Data_Integration.md`
- `TICKET_020_KRX_Post_Approval_Enablement_and_Smoke.md`
- `README.md`
- `docs/DB_CONTRACT_MATRIX.md`
- `docs/SCHEDULER_AUTOMATION.md`
- `docs/SCHEDULER_SERVER_RUNBOOK.md`

이번 작업의 목표는 **KRX 활용신청 승인 후 live activation을 안전하게 연결하는 것**입니다.

## 현재 전제
사용자는 아래 8개 서비스에 대해 활용신청을 완료했고, 이 티켓은 **승인 완료 후** 적용하는 단계입니다.

- 유가증권 일별매매정보
- 코스닥 일별매매정보
- 유가증권 종목기본정보
- 코스닥 종목기본정보
- KRX 시리즈 일별시세정보
- KOSPI 시리즈 일별시세정보
- KOSDAQ 시리즈 일별시세정보
- ETF 일별매매정보

## 반드시 지킬 원칙
- 승인 서비스만 호출 가능하도록 service allowlist를 둡니다.
- `ENABLE_KRX_LIVE=false`면 KRX live 호출을 하지 않습니다.
- KRX 실패는 시스템 전체 fatal로 만들지 말고 fallback으로 처리합니다.
- KIS 실시간/장중 경로는 유지합니다.
- KRX는 reference/statistics/daily market data 중심으로 씁니다.
- 출처 표기(`한국거래소 통계정보`)를 화면/리포트/문서에 반영합니다.
- 요청 예산 가드를 둡니다.
- 현재 `main.duckdb` single-writer discipline을 깨지 마세요.

## 구현해야 할 것
1. KRX service registry 추가
2. env validation 및 allowlist 구조 추가
3. 승인 서비스별 smoke test 스크립트 추가
4. universe/reference/index/ETF 경로에 live-first/fallback-second 연결
5. KRX request log / budget snapshot / service status 저장
6. Ops / Health / Docs / report attribution 반영
7. tests / docs / validation scripts 추가

## 예상 파일 범위
- `app/providers/krx/registry.py`
- `app/providers/krx/client.py`
- `app/providers/krx/reference.py`
- `app/settings.py`
- `app/ingestion/...`
- `app/ui/pages/01_Ops.py`
- `app/ui/pages/10_Health_Dashboard.py`
- `app/ui/pages/11_Docs_Help.py`
- `scripts/krx_smoke_test.py`
- `scripts/krx_smoke_test_all_allowed.py`
- `scripts/validate_krx_live_configuration.py`
- `scripts/render_krx_service_status_report.py`
- 관련 테스트와 문서

## 완료 기준
- 승인 서비스 8개가 canonical slug로 registry에 등록됨
- `ENABLE_KRX_LIVE=true` 상태에서 smoke test 수행 가능
- live 실패 시 fallback 동작 확인 가능
- Ops/Health/Docs에서 KRX live 상태 확인 가능
- request budget / request log / service status가 저장됨
- 출처 표기가 화면 또는 리포트에 반영됨
- 테스트와 validation script가 통과함

## 보고 방식
작업 후 아래를 요약해서 보고하세요.
- 추가/수정 파일 목록
- 승인 서비스 registry 표
- smoke test 결과
- fallback 동작 확인 결과
- request budget snapshot 결과
- UI/Ops/Docs 반영 내용
- 실행한 검증 명령과 결과
- 남은 limitation / follow-up

작업 트리에만 반영하고, 커밋/푸시는 하지 마세요.
