# Codex 아홉 번째 전달용 지시서 — TICKET-008 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic / ML alpha model v1 / selection engine v2 / intraday candidate assist engine v1 을 깨지 않는 선에서 **TICKET-008** 을 진행하세요.

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
- `TICKET_008_Intraday_Postmortem_Regime_Aware_Strategy_Comparison.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
- `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **장중 타이밍 레이어 사후평가 고도화 + selection v2 결합 전략 비교 + regime-aware intraday adjustment** 를 만드는 것입니다.

반드시 구현할 것:
- 장중 market context snapshot materialization
- `scripts/materialize_intraday_market_context_snapshots.py`
- intraday regime adjustment materialization
- `scripts/materialize_intraday_regime_adjustments.py`
- raw decision 기반 adjusted decision materialization
- `scripts/materialize_intraday_adjusted_entry_decisions.py`
- intraday decision matured outcome 계산
- `scripts/materialize_intraday_decision_outcomes.py`
- strategy comparison 평가 구현
- `scripts/evaluate_intraday_strategy_comparison.py`
- intraday timing calibration / diagnostic 구현
- `scripts/materialize_intraday_timing_calibration.py`
- 장중 postmortem 리포트 renderer 구현
- `scripts/render_intraday_postmortem_report.py`
- 선택적 Discord publisher 구현
- `scripts/publish_discord_intraday_postmortem.py`
- validation / sanity script 구현
- `scripts/validate_intraday_strategy_pipeline.py`
- `fact_intraday_market_context_snapshot` 또는 동등한 저장 계약 구축
- `fact_intraday_regime_adjustment` 또는 동등한 저장 계약 구축
- `fact_intraday_adjusted_entry_decision` 또는 동등한 저장 계약 구축
- `fact_intraday_strategy_result` 또는 동등한 저장 계약 구축
- `fact_intraday_strategy_comparison` 또는 동등한 저장 계약 구축
- `fact_intraday_timing_calibration` 또는 동등한 저장 계약 구축
- Streamlit `Intraday Console` 확장: raw vs adjusted action / regime family / strategy trace 표시
- `Evaluation` 페이지 확장: strategy comparison / regime matrix / timing edge / skip diagnostic 표시
- `Stock Workbench` 확장: 종목별 intraday raw/adjusted timeline 과 realized edge 표시
- `Ops` 페이지 확장: market context / adjustment / comparison / publish 상태 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- 자동매매 / 주문 API 연동 금지
- intraday policy ML / RL / online learning 금지
- selection v2 를 무시한 독립 intraday stock picking 금지
- 전 종목 장중 전수저장 금지. 반드시 candidate-only 원칙을 유지할 것
- raw decision overwrite 금지. adjusted decision 은 별도 저장할 것
- exit 기준은 기존 selection baseline 과 동일해야 함. entry만 달라져야 함
- `DATA_INSUFFICIENT` 를 `ENTER_NOW` 로 승격하지 말 것
- `AVOID_TODAY -> ENTER_NOW` 같은 공격적 역전은 기본 구현에서 금지 또는 매우 제한적으로 처리할 것
- data quality 가 약한 경우 보수적 fallback 을 명시적으로 드러낼 것
- UI 접속 시 evaluation / publish 자동 실행 금지
- 뉴스 본문 전문 저장/전송 금지
- aggressive over-engineering 금지

세부 요구:
- 기본 checkpoint 는 `09:05`, `09:15`, `09:30`, `10:00`, `11:00`
- 최소 regime family 는 아래를 지원할 것
  - `PANIC_OPEN`
  - `WEAK_RISK_OFF`
  - `NEUTRAL_CHOP`
  - `HEALTHY_TREND`
  - `OVERHEATED_GAP_CHASE`
  - `DATA_WEAK`
- 최소 adjustment profile 은 아래를 지원할 것
  - `DEFENSIVE`
  - `NEUTRAL`
  - `SELECTIVE_RISK_ON`
  - `GAP_CHASE_GUARD`
  - `DATA_WEAK_GUARD`
- raw vs adjusted action 을 같은 row 기준으로 비교 가능해야 할 것
- 최소 전략은 아래를 지원할 것
  - `SEL_V2_OPEN_ALL`
  - `SEL_V2_TIMING_RAW_FIRST_ENTER`
  - `SEL_V2_TIMING_ADJ_FIRST_ENTER`
- 가능하면 아래도 지원해 주세요
  - `SEL_V2_TIMING_ADJ_0930_ONLY`
  - `SEL_V2_TIMING_ADJ_1000_ONLY`
- strategy comparison 은 최소한 아래를 계산할 것
  - executed_count
  - execution_rate
  - mean/median realized excess return
  - hit rate
  - mean/median timing edge vs open
  - positive timing edge rate
  - skip saved loss rate
  - missed winner rate
- intraday outcome 의 exit date 는 기존 label/selection outcome 과 동일한 종료일을 재사용할 것
- no-entry 도 결과로 평가할 것. 단순히 drop 하지 말 것
- data quality / provider coverage 가 약할 때는 reason code 와 flag 를 남길 것

이번 작업 완료의 핵심 기준:
1. `python scripts/materialize_intraday_market_context_snapshots.py --session-date 2026-03-09 --checkpoints 09:05 09:15 09:30 10:00 11:00`
2. `python scripts/materialize_intraday_regime_adjustments.py --session-date 2026-03-09 --checkpoints 09:05 09:15 09:30 10:00 11:00 --horizons 1 5`
3. `python scripts/materialize_intraday_adjusted_entry_decisions.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5`
4. `python scripts/materialize_intraday_decision_outcomes.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5`
5. `python scripts/evaluate_intraday_strategy_comparison.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5 --cutoff 11:00`
6. `python scripts/materialize_intraday_timing_calibration.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5`
7. `python scripts/render_intraday_postmortem_report.py --session-date 2026-03-09 --horizons 1 5 --dry-run`
8. `python scripts/publish_discord_intraday_postmortem.py --session-date 2026-03-09 --horizons 1 5 --dry-run`
9. `python scripts/validate_intraday_strategy_pipeline.py --session-date 2026-03-09 --horizons 1 5`
10. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- intraday market context 개념
- regime family 정의
- raw timing vs adjusted timing 차이
- selection v2 와 intraday timing 결합 원칙
- strategy id 정의
- same-exit comparison 규칙
- no-entry 해석과 skip diagnostic 정의
- candidate-only 저장 전략 유지 원칙
- data quality / fallback 정책
- dry-run / publish 예시
- current known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- market context snapshot 계산 방식 요약
- regime family 와 adjustment profile 정의 요약
- raw→adjusted action transition 규칙 요약
- strategy id 및 entry rule 요약
- same-exit outcome 계산 규칙 요약
- strategy comparison 주요 결과 요약
- UI에서 확인할 위치
- known limitations
- TICKET-009 진입 전 주의사항

