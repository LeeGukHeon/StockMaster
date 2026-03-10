# Codex 여섯 번째 전달용 지시서 — TICKET-005 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / Discord 장후 리포트 를 깨지 않는 선에서 **TICKET-005** 를 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
- `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **selection/prediction 사후 평가 + calibration 검증 + postmortem 리포트** 를 만드는 것입니다.

반드시 구현할 것:
- `fact_selection_outcome` 또는 동등한 symbol-level outcome 저장 계약 구축
- selection 당시 snapshot vs realized outcome 비교 로직 구현
- `scripts/materialize_selection_outcomes.py`
- `scripts/backfill_selection_outcomes.py`
- `fact_evaluation_summary` 또는 동등한 cohort/rolling 집계 저장 계약 구축
- `scripts/materialize_prediction_evaluation.py`
- selection engine v1 vs explanatory ranking v0 baseline 비교 집계 구현
- `fact_calibration_diagnostic` 또는 동등한 calibration diagnostic 저장 계약 구축
- `scripts/materialize_calibration_diagnostics.py`
- postmortem report renderer 구현
- `scripts/render_postmortem_report.py`
- Discord postmortem publisher 구현 (`dry-run` 필수)
- `scripts/publish_discord_postmortem_report.py`
- evaluation consistency validator 구현
- `scripts/validate_evaluation_pipeline.py`
- Evaluation / Stock Workbench / Leaderboard / Ops 화면 확장
- README 갱신
- 관련 테스트 작성

중요 제약:
- evaluation 시점에 과거 예측값/밴드를 다시 계산해서 덮어쓰지 말 것
- selection 당시 `fact_ranking` / `fact_prediction` snapshot 을 기준으로 평가할 것
- realized outcome 계산은 TICKET-003의 label 정의(next open → future close)와 동일해야 함
- selection engine v1 과 explanatory ranking v0 는 비교하되 섞지 말 것
- `pre-cost evaluation` 임을 README 에 명시할 것
- 아직 없는 거래비용 simulator 나 ML uncertainty 를 있는 것처럼 쓰지 말 것
- Discord publish 실패가 전체 배치 실패로 번지지 않게 할 것
- 뉴스 본문 전문 저장/전송 금지
- aggressive over-engineering 금지

이번 작업 완료의 핵심 기준:
1. `python scripts/materialize_selection_outcomes.py --selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
2. `python scripts/backfill_selection_outcomes.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
3. `python scripts/materialize_prediction_evaluation.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --rolling-windows 20 60`
4. `python scripts/materialize_calibration_diagnostics.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --bin-count 10`
5. `python scripts/render_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run`
6. `python scripts/publish_discord_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run`
7. `python scripts/validate_evaluation_pipeline.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5`
8. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- selection snapshot freeze 원칙
- realized return / realized excess return 정의
- D+1 / D+5 평가 시점 정의
- band hit / above upper / below lower 정의
- selection v1 vs explanatory v0 비교 방식
- rolling evaluation window 정의
- calibration diagnostic 정의
- pre-cost evaluation 주의사항
- postmortem dry-run / publish 사용법
- current known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- matured outcome 계산 순서
- evaluation summary / calibration diagnostic 생성 순서
- selection v1 vs explanatory v0 비교 확인 방법
- postmortem preview / publish 확인 방법
- 아직 남은 TODO
- TICKET-006 진입 전 주의사항

---
