# Codex 열한 번째 전달용 지시서 — TICKET-010 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic / ML alpha model v1 / selection engine v2 / intraday candidate assist engine v1 / regime-aware intraday comparison / intraday policy calibration framework 를 깨지 않는 선에서 **TICKET-010** 을 진행하세요.

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
- `TICKET_009_Policy_Calibration_Regime_Tuning_Experiment_Ablation.md`
- `TICKET_010_Policy_Meta_Model_ML_Timing_Classifier_v1.md`
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
- `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
- `CODEX_NINTH_INSTRUCTION_StockMaster.md`
- `CODEX_TENTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **장중 Policy Meta-Model / ML Timing Classifier v1** 을 만드는 것입니다.

반드시 구현할 것:
- matured intraday snapshot 기반 meta training dataset assembly
- `ENTER_PANEL`, `WAIT_PANEL` 분리 학습 구조
- `scripts/build_intraday_meta_training_dataset.py`
- `scripts/validate_intraday_meta_dataset.py`
- sklearn 기반 panel-specific classifier training
- `scripts/train_intraday_meta_models.py`
- walk-forward evaluation
- `scripts/run_intraday_meta_walkforward.py`
- probability/threshold calibration
- `scripts/calibrate_intraday_meta_thresholds.py`
- model evaluation script
- `scripts/evaluate_intraday_meta_models.py`
- active model inference materializer
- `scripts/materialize_intraday_meta_predictions.py`
- final action decision materializer
- `scripts/materialize_intraday_final_actions.py`
- active meta-model freeze command
- `scripts/freeze_intraday_active_meta_model.py`
- active meta-model rollback command
- `scripts/rollback_intraday_active_meta_model.py`
- meta-model report renderer
- `scripts/render_intraday_meta_model_report.py`
- optional Discord summary publisher
- `scripts/publish_discord_intraday_meta_summary.py`
- framework validation script
- `scripts/validate_intraday_meta_model_framework.py`
- generic model registry 확장 또는 동등 계약 구축
- `fact_intraday_meta_prediction` 또는 동등한 저장 계약 구축
- `fact_intraday_meta_decision` 또는 동등한 저장 계약 구축
- `fact_intraday_active_meta_model` 또는 동등한 저장 계약 구축
- Research Lab / Model Diagnostics 페이지 구현 또는 확장
- Intraday Console 확장: adjusted action + ML predicted class + final action 표시
- Evaluation 페이지 확장: policy-only vs meta-overlay 비교
- Ops 페이지 확장: active meta-model / fallback / rollback history 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- selection engine v2 와 active intraday policy 가 여전히 모체여야 함
- meta-model 단독으로 후보군 선정하거나 독립 action 생성 금지
- 자동매매 / 주문 API 연동 금지
- deep learning / transformer / RL / online learning 금지
- `AVOID_TODAY` 와 `DATA_INSUFFICIENT` 를 upward override 하는 로직 금지
- hard guard block 시 override 금지
- matured outcome 만 학습/평가 입력으로 사용할 것
- live scoring 시 as-of feature discipline 엄수
- UI 접속 시 training / calibration 자동 실행 금지
- auto-promotion / auto-rollback 금지
- 전 종목 장중 전수저장 금지. candidate-only 철학 유지
- 뉴스 본문 전문 저장/전송 금지
- 기존 raw/adjusted decision overwrite 금지

세부 요구:
- 최소 panel 은 아래를 지원할 것
  - `ENTER_PANEL`
  - `WAIT_PANEL`
- 최소 class 체계는 아래를 지원할 것
  - `ENTER_PANEL`: `KEEP_ENTER`, `DOWNGRADE_WAIT`, `DOWNGRADE_AVOID`
  - `WAIT_PANEL`: `KEEP_WAIT`, `UPGRADE_ENTER`, `DOWNGRADE_AVOID`
- 최소 model family 는 아래 중 2개 이상을 panel 별 지원할 것
  - `LogisticRegression`
  - `HistGradientBoostingClassifier`
  - `RandomForestClassifier` 또는 `ExtraTreesClassifier`
- ensemble 은 soft-voting 또는 weighted voting 으로 충분
- calibration 은 sigmoid 또는 isotonic 기반이면 충분
- uncertainty/disagreement 는 v1 에서 member dispersion + margin 기반 proxy 로 충분
- final action layer 는 아래만 허용
  - adjusted `ENTER_NOW` -> keep / wait / avoid
  - adjusted `WAIT_RECHECK` -> keep / enter / avoid
  - adjusted `AVOID_TODAY` / `DATA_INSUFFICIENT` -> keep only
- fallback 은 정상 동작으로 취급하고 반드시 reason 저장
- freeze/rollback 은 scope overlap 없이 동작해야 함

저장 계약 최소 요구:
- `fact_model_training_run` 확장 또는 동등 구조
- `fact_model_metric_summary` 확장 또는 동등 구조
- `fact_intraday_meta_prediction`
- `fact_intraday_meta_decision`
- `fact_intraday_active_meta_model`
- feature importance / diagnostics 저장 경로 또는 테이블

평가 최소 요구:
- panel 별 macro F1 / class precision-recall / log loss
- policy-only 대비 meta-overlay same-exit lift
- downgrade precision / upgrade precision
- saved loss / missed winner
- override rate / fallback rate
- regime family / checkpoint band breakdown

UI 최소 요구:
- Research Lab / Model Diagnostics:
  - model version / panel / horizon 선택
  - calibration / confusion matrix / feature importance
- Intraday Console:
  - active policy action
  - ML class probabilities
  - confidence margin
  - uncertainty / disagreement
  - final action / override / fallback
- Evaluation:
  - policy-only vs meta-overlay
- Ops:
  - active meta-model registry
  - last training/scoring run
  - fallback / rollback history

완료 후 반드시 남길 것:
- 추가/수정 파일 목록
- 스크립트 목록
- 학습/평가/추론 흐름 요약
- 저장 테이블/컬럼 요약
- final action 규칙 요약
- known limitation
- 다음 티켓으로 넘길 메모

주의:
- 구현은 보수적으로 하세요.
- 이번 티켓의 목적은 “정책을 대체하는 AI”가 아니라 “정책 위에 얹는 bounded ML overlay” 입니다.
- 코드/설계 선택에서 애매하면 더 단순하고 재현 가능한 쪽을 택하세요.
