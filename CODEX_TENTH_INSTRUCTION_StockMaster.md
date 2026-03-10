# Codex 열 번째 전달용 지시서 — TICKET-009 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic / ML alpha model v1 / selection engine v2 / intraday candidate assist engine v1 / regime-aware intraday comparison 을 깨지 않는 선에서 **TICKET-009** 을 진행하세요.

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
- `CODEX_FIRST_INSTRUCTION_StockMaster.md`
- `CODEX_SECOND_INSTRUCTION_StockMaster.md`
- `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
- `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
- `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
- `CODEX_NINTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **장중 타이밍 레이어 ML화 전 단계: policy calibration 자동화 + regime parameter tuning + 실험 관리/ablation 프레임** 을 만드는 것입니다.

반드시 구현할 것:
- intraday policy parameter schema / candidate registry 구축
- `scripts/materialize_intraday_policy_candidates.py`
- matured-only 기반 calibration 실행기 구현
- `scripts/run_intraday_policy_calibration.py`
- anchored/rolling walk-forward 실행기 구현
- `scripts/run_intraday_policy_walkforward.py`
- policy ablation evaluator 구현
- `scripts/evaluate_intraday_policy_ablation.py`
- recommendation materializer 구현
- `scripts/materialize_intraday_policy_recommendations.py`
- active policy freeze command 구현
- `scripts/freeze_intraday_active_policy.py`
- active policy rollback command 구현
- `scripts/rollback_intraday_active_policy.py`
- policy research report renderer 구현
- `scripts/render_intraday_policy_research_report.py`
- optional Discord summary publisher 구현
- `scripts/publish_discord_intraday_policy_summary.py`
- framework validation script 구현
- `scripts/validate_intraday_policy_framework.py`
- `fact_intraday_policy_experiment_run` 또는 동등한 저장 계약 구축
- `fact_intraday_policy_candidate` 또는 동등한 저장 계약 구축
- `fact_intraday_policy_evaluation` 또는 동등한 저장 계약 구축
- `fact_intraday_policy_ablation_result` 또는 동등한 저장 계약 구축
- `fact_intraday_policy_selection_recommendation` 또는 동등한 저장 계약 구축
- `fact_intraday_active_policy` 또는 동등한 저장 계약 구축
- Streamlit `Research Lab` 또는 동등 페이지 구현/확장
- `Intraday Console` 확장: active policy / fallback trace / tuned action 표시
- `Evaluation` 페이지 확장: policy version comparison / walk-forward / ablation 표시
- `Ops` 페이지 확장: experiment runs / active registry / rollback history 표시
- README 갱신
- 관련 테스트 작성

중요 제약:
- 자동매매 / 주문 API 연동 금지
- intraday policy ML / RL / online learning 금지
- UI 접속 시 calibration / promotion / rollback 자동 실행 금지
- selection v2 와 same-exit comparison 원칙 유지
- matured outcome 만 tuning 입력으로 사용할 것
- active policy auto promotion 금지
- data weak 상태에서 공격적 승격 허용 금지
- 전 종목 장중 전수저장 금지. candidate-only 철학 유지
- 뉴스 본문 전문 저장/전송 금지
- 기존 raw / adjusted decision 을 overwrite 하지 말 것
- active scope overlap 금지
- aggressive over-engineering 금지

세부 요구:
- 최소 policy template 은 아래를 지원할 것
  - `BASE_DEFAULT`
  - `DEFENSIVE_LIGHT`
  - `DEFENSIVE_STRONG`
  - `RISK_ON_LIGHT`
  - `GAP_GUARD_STRICT`
  - `FRICTION_GUARD_STRICT`
  - `COHORT_GUARD_STRICT`
  - `FULL_BALANCED`
- 최소 scope 는 아래를 지원할 것
  - `GLOBAL`
  - `HORIZON`
  - `HORIZON_CHECKPOINT`
  - `HORIZON_REGIME_CLUSTER`
  - 가능하면 `HORIZON_CHECKPOINT_REGIME_FAMILY`
- regime cluster 기본 매핑은 아래를 지원할 것
  - `RISK_OFF`: `PANIC_OPEN`, `WEAK_RISK_OFF`
  - `NEUTRAL`: `NEUTRAL_CHOP`
  - `RISK_ON`: `HEALTHY_TREND`, `OVERHEATED_GAP_CHASE`
  - `DATA_WEAK`: `DATA_WEAK`
- 기본 split 모드는 아래를 지원할 것
  - `ANCHORED_WALKFORWARD`
  - `ROLLING_WALKFORWARD`
- 기본 split 예시는 아래를 지원할 것
  - train 40 sessions
  - validation 10 sessions
  - test 10 sessions
  - step 5 sessions
- policy comparison 에 최소한 아래 지표를 계산할 것
  - executed_count
  - execution_rate
  - mean/median realized excess return
  - hit_rate
  - mean_timing_edge_vs_open
  - positive_timing_edge_rate
  - skip_saved_loss_rate
  - missed_winner_rate
  - left_tail_proxy
  - stability_score
  - objective_score
- sample 부족 시 manual review required 로 남길 것
- family 표본 부족 시 cluster/global fallback 을 명시적으로 저장할 것
- recommendation 과 active registry 는 분리할 것
- freeze / rollback 은 명시적 CLI 명령으로만 실행할 것

이번 작업 완료의 핵심 기준:
1. `python scripts/materialize_intraday_policy_candidates.py --search-space-version pcal_v1 --horizons 1 5 --checkpoints 09:05 09:15 09:30 10:00 11:00 --scopes GLOBAL HORIZON HORIZON_CHECKPOINT HORIZON_REGIME_CLUSTER`
2. `python scripts/run_intraday_policy_calibration.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --horizons 1 5 --checkpoints 09:05 09:15 09:30 10:00 11:00 --objective-version ip_obj_v1 --split-version wf_40_10_10_step5 --search-space-version pcal_v1`
3. `python scripts/run_intraday_policy_walkforward.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --mode rolling --train-sessions 40 --validation-sessions 10 --test-sessions 10 --step-sessions 5 --horizons 1 5`
4. `python scripts/evaluate_intraday_policy_ablation.py --start-session-date 2026-01-05 --end-session-date 2026-03-20 --horizons 1 5 --base-policy-source latest_recommendation`
5. `python scripts/materialize_intraday_policy_recommendations.py --as-of-date 2026-03-20 --horizons 1 5 --minimum-test-sessions 10`
6. `python scripts/freeze_intraday_active_policy.py --as-of-date 2026-03-20 --promotion-type MANUAL_FREEZE --source latest_recommendation --note "Promote after review"`
7. `python scripts/rollback_intraday_active_policy.py --as-of-date 2026-03-24 --horizons 1 5 --note "Rollback due to weak execution stability"`
8. `python scripts/render_intraday_policy_research_report.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run`
9. `python scripts/publish_discord_intraday_policy_summary.py --as-of-date 2026-03-20 --horizons 1 5 --dry-run`
10. `python scripts/validate_intraday_policy_framework.py --as-of-date 2026-03-20 --horizons 1 5`
11. `streamlit run app/ui/Home.py`

README에는 최소한 아래를 적어 주세요.
- intraday policy calibration 목적
- matured-only tuning 원칙
- same-exit comparison 원칙
- policy template / search space 설명
- objective function 개요
- walk-forward split 규칙
- regime cluster/family fallback 구조
- recommendation vs active policy 차이
- freeze / rollback 사용법
- current known limitations

작업 후 아래를 간단히 정리해 주세요.
- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- policy parameter schema 요약
- search space template 와 범위 요약
- objective function 구성요소 요약
- walk-forward split 방식 요약
- regime fallback 규칙 요약
- recommendation / freeze / rollback 흐름 요약
- UI에서 확인할 위치
- known limitations
- TICKET-010 진입 전 주의사항
