# Codex 전달용 — TICKET-016 실행 지시서

아래 지시를 그대로 따른다.

## 1. 작업 위치
- 루트 경로: `D:\MyApps\StockMaster`
- 이번 작업은 **TICKET-016 — DB 개선 / TICKET-000~013 통합 점검 체크리스트 / 케이스별 개선방안** 을 구현하는 작업이다.

## 2. 먼저 읽을 문서
반드시 아래 문서를 먼저 읽고 source of truth 로 사용한다.

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
- `TICKET_011_Integrated_Portfolio_Capital_Allocation_Risk_Budget.md`
- `TICKET_012_Operational_Stability_Batch_Recovery_Disk_Guard_Monitoring_Health_Dashboard.md`
- `TICKET_013_Final_User_Workflow_Dashboard_Report_Polish.md`
- `TICKET_016_DB_Audit_Integration_Checklist_Gap_Remediation_000_013.md`
- `USER_MANUAL_KR_StockMaster.md`
- `DATA_TRAINING_GUIDE_KR_StockMaster.md`

또한 현재 working tree 와 DuckDB/Parquet/artifact 상태를 직접 확인해라.

## 3. 현재 상황 요약
- TICKET-013 은 완료되었고, release/report/snapshot schema mismatch 는 정리된 상태다.
- release candidate 는 critical 0, warning 2, 현재 알려진 주요 warning 은 weekend freshness 관련이다.
- 이번 작업은 **새 예측 기능**을 만드는 것이 아니라 **DB/저장 계약/lineage/latest layer/report artifact/reference integrity** 를 통합 점검하고 보강하는 것이다.

## 4. 이번 작업의 핵심 목표
반드시 아래를 달성해라.

1. TICKET-000~013 전 구간 핵심 테이블의 grain, unique key, required columns, rerun rule, lineage 를 inventory 화한다.
2. `fact_latest_app_snapshot`, `fact_latest_report_index`, `fact_release_candidate_check`, `fact_ui_data_freshness_snapshot` 를 포함한 latest/release layer consistency 를 자동 검증한다.
3. 대표 curated/release/ops tables 의 duplicate/lineage/reference/artifact/freshness 문제를 자동 탐지한다.
4. TICKET-000~013 checklist 를 PASS/WARN/FAIL 형태로 문서화한다.
5. 즉시 고칠 수 있는 P0/P1 문제는 수정하고, 나머지는 backlog/runbook 으로 남긴다.

## 5. 구현해야 할 최소 산출물
### 문서
- `docs/AUDIT_T000_T013_STATUS.md`
- `docs/DB_CONTRACT_MATRIX.md`
- `docs/GAP_REMEDIATION_BACKLOG.md`
- `docs/CASE_RUNBOOK_T000_T013.md`

### 코드/스크립트
- `app/audit/contracts.py`
- `app/audit/checks.py`
- `app/audit/reporting.py`
- 필요 시 `app/audit/remediation.py`
- `scripts/audit_t000_t013_integrity.py`
- `scripts/validate_db_contracts.py`
- `scripts/validate_latest_layer_consistency.py`
- `scripts/validate_artifact_reference_integrity.py`
- `scripts/validate_ticket_coverage_checklist.py`
- `scripts/render_audit_summary_report.py`

### 테스트
- `tests/unit/test_db_contract_registry.py`
- `tests/integration/test_audit_t000_t013_framework.py`
- 필요 시 보조 테스트 추가

## 6. 반드시 점검할 핵심 항목
최소 아래 항목은 구현과 문서 둘 다에 반영해라.

- canonical latest snapshot uniqueness
- canonical report index integrity
- artifact path → 실제 파일 존재 여부
- weekend/holiday freshness classification
- active policy/model/portfolio id consistency
- duplicate row detection for representative tables
- rerun idempotency representative check
- release/report/ui/source consistency
- active/latest referenced artifact cleanup safety

대표 테이블은 최소 아래를 포함해라.

- `dim_symbol`
- `dim_trading_calendar`
- `fact_daily_ohlcv`
- `fact_fundamentals_snapshot`
- `fact_news_item`
- `fact_feature_snapshot`
- `fact_forward_return_label`
- `fact_market_regime_snapshot`
- `fact_investor_flow`
- `fact_prediction`
- `fact_ranking`
- `fact_selection_outcome`
- `fact_evaluation_summary`
- `fact_calibration_diagnostic`
- `fact_model_training_run`
- `fact_model_member_prediction`
- `fact_intraday_candidate_session`
- `fact_intraday_final_action`
- `fact_intraday_meta_prediction`
- `fact_intraday_meta_decision`
- `fact_intraday_active_meta_model`
- `fact_portfolio_target_book`
- `fact_portfolio_rebalance_plan`
- `fact_portfolio_position_snapshot`
- `fact_portfolio_nav_snapshot`
- `fact_job_run`
- `fact_job_step_run`
- `fact_health_snapshot`
- `fact_latest_app_snapshot`
- `fact_latest_report_index`
- `fact_release_candidate_check`
- `fact_ui_data_freshness_snapshot`

## 7. 작업 순서
1. 문서와 현재 코드를 읽고 테이블/스크립트 inventory 작성
2. DB contract registry 초안 작성
3. audit/validation 스크립트 작성
4. 실제 실행 후 PASS/WARN/FAIL 수집
5. 즉시 수정 가능한 P0/P1 문제 수정
6. docs 생성
7. 테스트 추가 및 실행
8. README 또는 docs/help 에 audit 진입점 링크 추가가 필요하면 최소 범위로 반영

## 8. 하지 말아야 할 것
- alpha/selection/portfolio 성능 향상 로직을 새로 넣지 마라.
- OCI 배포 작업을 섞지 마라.
- 백테스트 엔진 구현을 섞지 마라.
- 기존 핵심 테이블명을 이유 없이 대량 변경하지 마라.
- historical data 를 파괴적으로 재작성하지 마라.

## 9. 완료 보고 형식
완료 시 아래 형식으로 보고해라.

1. 추가/수정 파일 목록
2. DB contract matrix 핵심 요약
3. PASS/WARN/FAIL 요약 수치
4. 즉시 수정한 P0/P1 항목
5. backlog 로 남긴 항목
6. 실행한 스크립트와 결과
7. 실행한 테스트와 결과
8. 남은 known limitation

## 10. 품질 기준
- audit 결과가 코드로 재현 가능해야 한다.
- 문서와 실제 검사 결과가 일치해야 한다.
- weekend freshness, latest layer consistency, artifact integrity 는 반드시 자동 검증되어야 한다.
- TICKET-014/015 전에 읽을 수 있는 수준의 운영 문서가 생성되어야 한다.
