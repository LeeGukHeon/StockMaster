# Codex 세 번째 전달용 지시서 — TICKET-002 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation 을 깨지 않는 선에서 **TICKET-002** 를 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **핵심 연구 데이터 3축** 을 적재 가능한 상태로 만드는 것입니다.

반드시 구현할 것:
- 전 종목 또는 subset 대상 일봉 적재 파이프라인
- `fact_daily_ohlcv` 적재 + raw 저장 + 검증 + idempotent upsert
- DART 재무 수집/정규화/스냅샷 materialization
- `fact_fundamentals_snapshot` 적재
- 뉴스 메타데이터 수집 + dedupe + symbol linking
- `fact_news_item` 적재
- `scripts/sync_daily_ohlcv.py`
- `scripts/sync_fundamentals_snapshot.py`
- `scripts/sync_news_metadata.py`
- `scripts/backfill_core_research_data.py`
- run manifest 기록
- Ops / Research 화면에 데이터 신선도와 최근 적재 결과 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- 자동매매/주문/체결 기능 금지
- 뉴스 본문 전문 저장 금지
- 재무 snapshot 에 미래 공시가 섞이지 않도록 availability rule 을 명시적으로 구현
- 공격적인 fuzzy symbol linking 금지
- 이번 티켓에서 피처 전체/랭킹 전체까지 과도 확장하지 말 것
- 공식 source 우선

이번 작업 완료의 핵심 기준:
1. `python scripts/sync_daily_ohlcv.py --date 2026-03-06 --limit-symbols 50`
2. `python scripts/sync_fundamentals_snapshot.py --as-of-date 2026-03-06 --limit-symbols 50`
3. `python scripts/sync_news_metadata.py --date 2026-03-06 --mode market_and_focus --limit-symbols 50`
4. `python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50`
5. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- 환경변수
- 각 파이프라인 실행 순서
- backfill 예시
- 뉴스 query pack 조정 방법
- 재무 availability / snapshot rule
- 현재 known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 실행 순서
- 데이터 적재 확인 방법
- 아직 남은 TODO
- TICKET-003 진입 전 주의사항

---

