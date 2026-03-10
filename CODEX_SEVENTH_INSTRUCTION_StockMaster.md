# Codex 일곱 번째 전달용 지시서 — TICKET-006 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic 를 깨지 않는 선에서 **TICKET-006** 을 진행하세요.

반드시 먼저 읽을 문서:
- `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
- `TICKET_000_Foundation_and_First_Work_Package.md`
- `TICKET_001_Universe_Calendar_Provider_Activation.md`
- `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
- `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
- `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
- `TICKET_005_Postmortem_Evaluation_Calibration_Report.md`
- `TICKET_006_ML_Alpha_Uncertainty_Disagreement_Selection_v2.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **정식 ML alpha model v1 + uncertainty/disagreement 기초 + selection engine v2** 를 만드는 것입니다.

반드시 구현할 것:
- supervised training dataset assembly 구현
- `scripts/build_model_training_dataset.py`
- horizon 별 ML alpha model v1 학습 파이프라인 구현
- `scripts/train_alpha_model_v1.py`
- time-aware OOF 또는 validation prediction backfill 구현
- `scripts/backfill_alpha_oof_predictions.py`
- inference materialization 구현
- `scripts/materialize_alpha_predictions_v1.py`
- `fact_model_training_run` 또는 동등한 model registry 저장 계약 구축
- `fact_model_member_prediction` 또는 동등한 member prediction 저장 계약 구축
- `fact_model_metric_summary` 또는 동등한 metric 저장 계약 구축
- `fact_prediction` 확장: predicted excess return / interval / uncertainty / disagreement / fallback metadata
- model-calibrated uncertainty v1 구현
- disagreement v1 구현
- selection engine v2 구현
- `scripts/materialize_selection_engine_v2.py`
- model validation / comparison script 구현
- `scripts/validate_alpha_model_v1.py`
- `scripts/compare_selection_engines.py`
- model diagnostic report renderer 구현
- `scripts/render_model_diagnostic_report.py`
- Leaderboard / Stock Workbench / Evaluation / Ops 화면 확장
- README 갱신
- 관련 테스트 작성

중요 제약:
- 기본 경로는 sklearn-only 로 동작해야 함
- LightGBM/XGBoost/SHAP 같은 외부 의존성은 optional 이어야 하며 필수로 만들지 말 것
- time-aware split 을 사용할 것. 랜덤 셔플 CV 를 기본으로 쓰지 말 것
- label 정의는 기존 TICKET-003의 `next open -> future close` excess return 규칙을 유지할 것
- explanatory score 는 selection engine v2 의 core alpha 대체재가 아님
- uncertainty 와 disagreement 를 같은 값으로 취급하지 말 것
- 학습 실패 / 데이터 부족 시 fallback 정책을 명시적으로 구현할 것
- fallback row 를 숨기지 말 것
- UI 로딩 시 학습이 돌지 않게 할 것
- evaluation 시점에 과거 prediction snapshot 을 재계산/덮어쓰기 하지 말 것
- 뉴스 본문 전문 저장/전송 금지
- aggressive over-engineering 금지

모델 family 기본 요구:
- `ElasticNetCV` 또는 동등한 선형 baseline
- `HistGradientBoostingRegressor` 또는 동등한 boosting baseline
- `RandomForestRegressor` 또는 `ExtraTreesRegressor` 중 하나의 bagged tree baseline
- horizon 별 separate model
- ensemble weight 저장

이번 작업 완료의 핵심 기준:
1. `python scripts/build_model_training_dataset.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120`
2. `python scripts/train_alpha_model_v1.py --train-end-date 2026-03-06 --horizons 1 5 --min-train-days 120 --validation-days 20`
3. `python scripts/backfill_alpha_oof_predictions.py --start-train-end-date 2026-02-14 --end-train-end-date 2026-03-06 --horizons 1 5 --limit-models 3`
4. `python scripts/materialize_alpha_predictions_v1.py --as-of-date 2026-03-06 --horizons 1 5`
5. `python scripts/materialize_selection_engine_v2.py --as-of-date 2026-03-06 --horizons 1 5`
6. `python scripts/validate_alpha_model_v1.py --as-of-date 2026-03-06 --horizons 1 5`
7. `python scripts/compare_selection_engines.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5`
8. `python scripts/render_model_diagnostic_report.py --train-end-date 2026-03-06 --horizons 1 5 --dry-run`
9. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- ML alpha model v1 목적과 한계
- excess return label 정의
- train/validation/OOF 규칙
- base model family 구성
- ensemble weighting 방식
- uncertainty v1 정의
- disagreement v1 정의
- selection engine v2 정의
- fallback 정책
- artifact / model registry 구조
- 실행 명령(train / infer / compare / diagnostic)
- current known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- 학습 데이터셋 생성 순서
- 모델 학습/OOF/추론/materialization 순서
- uncertainty / disagreement 계산 방식 요약
- selection engine v2 수식 요약
- fallback 정책 요약
- UI 에서 확인할 위치
- 아직 남은 TODO
- TICKET-007 진입 전 주의사항

---
