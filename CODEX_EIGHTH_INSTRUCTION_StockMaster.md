# Codex 여덟 번째 전달용 지시서 — TICKET-007 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic / ML alpha model v1 / selection engine v2 를 깨지 않는 선에서 **TICKET-007** 을 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
- `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
- `TICKET_006_ML_Alpha_Uncertainty_Disagreement_Selection_v2.md`
- `TICKET_007_Intraday_Candidate_Assist_Engine.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **장중 후보군 보조 엔진 v1 + 1분봉/체결강도/호가 요약 기반 진입 타이밍 레이어** 를 만드는 것입니다.

반드시 구현할 것:
- 전일 selection engine v2 결과에서 장중 후보군 session materialization 구현
- `scripts/materialize_intraday_candidate_session.py`
- 후보군 대상 1분봉 적재/백필 구현
- `scripts/backfill_intraday_candidate_bars.py`
- 후보군 대상 체결 요약 적재/백필 구현
- `scripts/backfill_intraday_candidate_trade_summary.py`
- 후보군 대상 호가 요약 적재/백필 구현
- `scripts/backfill_intraday_candidate_quote_summary.py`
- 장중 collector 또는 동등한 수집 루프 구현
- `scripts/run_intraday_candidate_collector.py`
- checkpoint 시그널 materialization 구현
- `scripts/materialize_intraday_signal_snapshots.py`
- entry timing decision 구현
- `scripts/materialize_intraday_entry_decisions.py`
- timing layer 기초 평가 구현
- `scripts/evaluate_intraday_timing_layer.py`
- 장중 모니터 리포트 renderer 구현
- `scripts/render_intraday_monitor_report.py`
- `fact_intraday_candidate_session` 또는 동등한 session candidate 저장 계약 구축
- `fact_intraday_bar_1m` 또는 동등한 1분봉 저장 계약 구축
- `fact_intraday_trade_summary` 또는 동등한 체결 요약 저장 계약 구축
- `fact_intraday_quote_summary` 또는 동등한 호가 요약 저장 계약 구축
- `fact_intraday_signal_snapshot` 또는 동등한 신호 스냅샷 저장 계약 구축
- `fact_intraday_entry_decision` 또는 동등한 진입 판단 저장 계약 구축
- `fact_intraday_timing_outcome` 또는 동등한 사후평가 저장 계약 구축
- Streamlit 장중 페이지 추가 (`Intraday Console` 또는 동등 페이지)
- Ops 화면 확장: collector/provider latency/data quality/checkpoint health 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- 자동매매/주문 API 연동 금지
- 포지션 관리 시스템 구현 금지
- 전 종목 장중 전수수집 금지. 반드시 candidate-only 저장 전략을 사용할 것
- selection engine v2 를 무시한 독립 intraday stock picking 금지
- raw websocket/tick packet 장기보관 금지
- quote/trade data 부족 시 fallback / signal quality 저하를 명시적으로 드러낼 것
- UI 접속 시 collector 자동 실행 금지
- entry timing layer 는 우선 **rule-based + deterministic** 하게 구현할 것
- 장중 타이밍 점수는 selection v2 를 대체하는 것이 아니라 보조/조정 계층일 것
- 뉴스 본문 전문 저장/전송 금지
- aggressive over-engineering 금지

세부 요구:
- session candidate 는 `selection_date -> next trading day session_date` 규칙을 따를 것
- 기본 checkpoint 는 `09:05`, `09:15`, `09:30`, `10:00`, `11:00`
- action 은 최소한 `ENTER_NOW`, `WAIT_RECHECK`, `AVOID_TODAY`, `DATA_INSUFFICIENT` 를 지원할 것
- 최소 signal family 는 아래를 포함할 것
  - gap/opening quality
  - VWAP/micro-trend
  - relative volume/activity
  - orderbook imbalance / spread
  - execution strength
  - risk/friction/shock
- quote summary unavailable 시 null 허용 + penalty/fallback 표시
- trade summary unavailable 시 proxy 또는 unavailable 표시
- timing evaluation 은 최소한 naive open 대비 timing edge 를 볼 수 있어야 함

이번 작업 완료의 핵심 기준:
1. `python scripts/materialize_intraday_candidate_session.py --selection-date 2026-03-06 --horizons 1 5 --max-candidates 30`
2. `python scripts/backfill_intraday_candidate_bars.py --session-date 2026-03-09 --horizons 1 5`
3. `python scripts/backfill_intraday_candidate_trade_summary.py --session-date 2026-03-09 --horizons 1 5`
4. `python scripts/backfill_intraday_candidate_quote_summary.py --session-date 2026-03-09 --horizons 1 5`
5. `python scripts/materialize_intraday_signal_snapshots.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5`
6. `python scripts/materialize_intraday_entry_decisions.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5`
7. `python scripts/evaluate_intraday_timing_layer.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5`
8. `python scripts/render_intraday_monitor_report.py --session-date 2026-03-09 --checkpoint 09:30 --dry-run`
9. `streamlit run app/ui/Home.py`

가능하면 아래도 지원해 주세요.
- `python scripts/run_intraday_candidate_collector.py --session-date 2026-03-09 --poll-seconds 15 --dry-run`

README에는 최소한 아래를 적어 주세요.
- intraday candidate session 개념
- selection v2 와 intraday timing layer 관계
- 자동매매가 아님을 명시
- candidate-only 저장 전략
- 1분봉 / trade summary / quote summary 저장 원칙
- intraday signal family 구성
- action 정의
- checkpoint 정의
- fallback / signal quality 정책
- TTL / storage policy
- collector 실행 예시
- current known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- session candidate 생성 규칙
- intraday collector / backfill 흐름
- signal snapshot 계산 방식 요약
- entry action 규칙 요약
- fallback / signal quality 정책 요약
- Intraday Console 에서 확인할 위치
- naive open 대비 timing evaluation 요약
- 아직 남은 TODO
- TICKET-008 진입 전 주의사항

---
