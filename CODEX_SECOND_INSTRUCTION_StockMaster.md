# Codex 두 번째 전달용 지시서 — TICKET-001 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation을 깨지 않는 선에서 **TICKET-001** 을 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`

이번 작업의 목표는 **실제 공식 provider를 최소 가동 상태로 만들고**, 리서치 플랫폼의 기준 데이터인 **종목마스터(dim_symbol)** 와 **거래일 캘린더(dim_trading_calendar)** 를 채우는 것입니다.

이번 작업에서 반드시 구현할 것:
- KIS provider 인증/토큰 캐시/health check/읽기 전용 시세 probe
- DART provider corpCode 다운로드/파싱/매핑/company overview 또는 기초 재무 probe
- 종목마스터 정규화 및 `dim_symbol` upsert
- `vw_universe_active_common_stock` view 생성
- 거래일 캘린더 생성 및 `dim_trading_calendar` upsert
- `scripts/sync_universe.py`
- `scripts/sync_trading_calendar.py`
- `scripts/provider_smoke_check.py`
- raw 저장 규칙 반영
- run manifest 기록
- Ops/UI에 유니버스/캘린더/provider health 요약 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- 자동매매/주문/체결 기능 금지
- 이번 티켓에서 전 종목 일봉/재무/뉴스 대량 적재까지 확장하지 말 것
- 공식 소스 우선
- KRX 자동화가 당장 완전하지 않으면 adapter + seed fallback 구조는 허용
- 불확실한 corp_code 매핑은 null 처리
- 기존 기획 문서 원본은 이동/삭제/개명하지 말 것

이번 작업 완료의 핵심 기준:
1. `python scripts/bootstrap.py`
2. `python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31`
3. `python scripts/sync_universe.py`
4. `python scripts/provider_smoke_check.py --symbol 005930`
5. `streamlit run app/ui/Home.py`

위 흐름이 재현 가능하도록 만들어 주세요.
README에는 환경변수, 실행 명령, fallback 사용법, 확인 포인트를 적어 주세요.

가능하면 작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 실행 순서
- 현재 남아 있는 TODO
- TICKET-002 진입 전 주의사항

---

