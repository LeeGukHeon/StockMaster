# CODEX 전달용 지시서 — TICKET-015 백테스트 / 워크포워드 검증 / Research Lab

아래 지시를 그대로 수행하세요.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업 중입니다.
루트에는 지금까지 누적된 구현 방향 문서와 티켓 문서들이 이미 존재합니다.

이번 작업의 source of truth 는 아래 문서들입니다.

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
- `TICKET_014_OCI_Deployment_External_Access_Runbook.md`
- `TICKET_015_Backtest_WalkForward_Validation_Research_Lab.md`

## 이번 작업 목표

`TICKET_015_Backtest_WalkForward_Validation_Research_Lab.md` 를 기준으로, StockMaster 내부 데이터 계약에 맞춘 **전용 워크포워드 백테스트 / 비교 실험 / Research Lab UI** 를 구현하세요.

핵심 원칙:

- generic 장난감 백테스터 금지
- `as_of_date` discipline 필수
- matured-only 평가 필수
- same-exit comparison 우선
- net-of-cost 결과를 기본 summary 로 제공
- 외부 라이브러리는 optional adapter 만 허용

## 꼭 해야 할 일

1. 백테스트용 데이터 계약/테이블 추가
   - `fact_backtest_run`
   - `fact_backtest_fold`
   - `fact_backtest_trade`
   - `fact_backtest_position_day`
   - `fact_backtest_nav_day`
   - `fact_backtest_summary`
   - `fact_backtest_diagnostic`
   - `dim_backtest_scenario`

2. scenario registry 구현
   - scenario config snapshot 저장
   - scenario family / horizon / entry / exit / cost profile / liquidity profile 를 명시적 config 로 다룰 것

3. backtest engine 구현
   - selection v2 open D+1
   - selection v2 open D+5
   - selection v2 + timing raw D+1
   - selection v2 + timing adjusted D+1
   - portfolio balanced long-only D+5

4. 비교 실행 기능 구현
   - 여러 scenario 를 같은 기간에 비교 실행
   - open vs timing raw vs timing adjusted vs portfolio 비교 가능해야 함

5. summary bundle 생성
   - markdown summary
   - html summary
   - parquet/csv outputs
   - charts
   - manifest

6. UI 추가
   - Research Lab > Backtest
   - Fold Diagnostics
   - Strategy Comparison
   - Portfolio Backtest Replay
   - Ops > Backtest Queue / Artifacts

7. 테스트 추가
   - leakage guard
   - same-exit correctness
   - cost/slippage correctness
   - turnover/liquidity cap correctness
   - manifest reproducibility

## 구현 순서 권장

1. schema / repository / scenario registry
2. backtest engine core
3. selection/timing compare scenarios
4. portfolio replay scenario
5. summary/diagnostics writer
6. UI pages
7. tests / fixtures / sample scenario runs

## 하지 말 것

- 주문 기능 추가 금지
- 실시간 자동매매 기능 추가 금지
- 외부 라이브러리 종속 구조로 핵심 엔진 작성 금지
- future leakage 허용 금지
- 비용 미반영 결과만 main summary 로 노출 금지
- 기존 selection/timing/portfolio source-of-truth 를 우회하는 별도 임시 로직 금지

## 완료 후 남겨야 할 것

- 변경된 폴더/파일 목록
- 실행 명령 예시
- sample backtest scenario 실행 결과 요약
- 남은 한계와 후속 권장사항
- docs 반영 여부

가능하면 구현 후 README / docs 에도 backtest 사용법을 짧게 반영하세요.

