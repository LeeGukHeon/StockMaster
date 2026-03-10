# Codex 열두 번째 전달용 지시서 — TICKET-011 연결용

아래 내용을 Codex에 그대로 전달하면 됩니다.

---

당신은 `D:\MyApps\StockMaster` 루트에서 작업하는 엔지니어입니다.
먼저 저장소 루트의 아래 문서를 읽고, 이미 구현된 foundation / provider activation / core data ingestion / feature store / selection engine v1 / evaluation / calibration diagnostic / ML alpha model v1 / selection engine v2 / intraday candidate assist engine v1 / regime-aware intraday comparison / intraday policy calibration framework / intraday meta-model v1 을 깨지 않는 선에서 **TICKET-011** 을 진행하세요.

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
- `TICKET_011_Integrated_Portfolio_Capital_Allocation_Risk_Budget.md`
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
- `CODEX_ELEVENTH_INSTRUCTION_StockMaster.md`

이번 작업의 목표는 **통합 포트폴리오 / 자본배분 / 리스크 버짓 레이어** 를 만드는 것입니다.

반드시 구현할 것:
- portfolio candidate assembly layer
- `scripts/build_portfolio_candidate_book.py`
- `scripts/validate_portfolio_candidate_book.py`
- active portfolio policy config loader + registry
- `config/portfolio_policies/balanced_long_only_v1.yaml`
- 가능하면 `config/portfolio_policies/defensive_long_only_v1.yaml`
- active portfolio policy freeze command
- `scripts/freeze_active_portfolio_policy.py`
- active portfolio policy rollback command
- `scripts/rollback_active_portfolio_policy.py`
- deterministic portfolio allocation engine v1
- `scripts/materialize_portfolio_target_book.py`
- deterministic rebalance planner
- `scripts/materialize_portfolio_rebalance_plan.py`
- position snapshot materializer
- `scripts/materialize_portfolio_position_snapshots.py`
- portfolio nav materializer
- `scripts/materialize_portfolio_nav.py`
- portfolio walk-forward runner
- `scripts/run_portfolio_walkforward.py`
- portfolio evaluation script
- `scripts/evaluate_portfolio_policies.py`
- portfolio report renderer
- `scripts/render_portfolio_report.py`
- optional Discord summary publisher
- `scripts/publish_discord_portfolio_summary.py`
- framework validation script
- `scripts/validate_portfolio_framework.py`
- `fact_portfolio_policy_registry` 또는 동등 저장 계약
- `fact_portfolio_candidate` 또는 동등 저장 계약
- `fact_portfolio_target_book` 또는 동등 저장 계약
- `fact_portfolio_rebalance_plan` 또는 동등 저장 계약
- `fact_portfolio_position_snapshot` 또는 동등 저장 계약
- `fact_portfolio_nav_snapshot` 또는 동등 저장 계약
- `fact_portfolio_constraint_event` 또는 동등 저장 계약
- `fact_portfolio_evaluation_summary` 또는 동등 저장 계약
- Portfolio Studio / Portfolio Evaluation / Ops 화면 구현 또는 기존 화면 확장
- README 갱신
- 관련 테스트 작성

중요 제약:
- portfolio layer 는 selection engine v2 와 intraday timing layer 의 downstream 이어야 함
- selection 자체를 portfolio layer 안에서 다시 학습/재생성 금지
- long-only only
- no leverage / no margin / no short / no derivatives
- 자동매매 / 주문 API 연동 금지
- future leakage 금지
- UI 접속 시 walk-forward / 대량 materialization 자동 실행 금지
- negative weight 금지
- gross exposure 100% 초과 금지
- active policy 가 없어도 dry-run 검증 가능하도록 설계
- intraday timing output 이 없어도 `OPEN_ALL` 포트폴리오 기능이 돌아가야 함
- 전 종목 장중 전수저장 금지 철학 유지
- 기존 selection / prediction / intraday raw tables overwrite 금지

세부 요구:
- 최소 execution mode 는 아래를 지원할 것
  - `OPEN_ALL`
  - `TIMING_ASSISTED` (intraday final action 이 있을 때)
- 최소 rebalance action 은 아래를 지원할 것
  - `BUY_NEW`
  - `ADD`
  - `HOLD`
  - `TRIM`
  - `EXIT`
  - `SKIP`
  - `NO_ACTION`
- 신규진입 기준과 보유유지 기준은 분리할 것
- hard exit 규칙은 명시적으로 둘 것
- cash budget 은 regime-aware 여야 함
- single-name cap / sector cap / KOSDAQ cap / turnover cap / liquidity cap 을 지원할 것
- weight 계산은 deterministic 이어야 함
- candidate tie-breaker 를 코드상 명시할 것
- target notional / target shares 계산을 지원할 것
- fractional share 금지
- cash residual 추적 필수
- holdings carry / hold hysteresis 를 넣어 churn 을 줄일 것
- turnover budget 때문에 못 들어간 종목은 waitlist 또는 skip 로 남길 것

저장 계약 최소 요구:
- policy registry
- candidate book
- target book
- rebalance plan
- position snapshot
- nav snapshot
- constraint event
- evaluation summary

권장 구현 방향:
- `effective_alpha_long` 는 primary horizon alpha 를 중심으로 하되 tactical alpha / lower band / flow / regime 를 보조로 쓰고, uncertainty / disagreement / implementation penalty 를 차감하는 형태
- `risk_scaled_conviction = effective_alpha_long / max(volatility_proxy, vol_floor)` 형태의 단순 스케일링이면 충분
- target weight 는 normalized score + cap-aware iterative allocation 으로 충분
- current holdings 는 신규진입보다 완화된 hold threshold 를 적용
- forced exit / forced trim / hold keep / add / new entry / cash 순으로 rebalance sequencing
- `TIMING_ASSISTED` 는 신규진입/추가매수만 timing final action 으로 gating 하면 충분

평가 최소 요구:
- cumulative return
- annualized volatility 또는 동등 지표
- Sharpe-like ratio
- max drawdown
- average turnover
- average cash weight
- average holding count
- concentration stats
- open-all vs timing-assisted 비교
- equal-weight baseline 비교

UI 최소 요구:
- Portfolio Studio:
  - active policy
  - target holdings
  - target weight / notional / shares
  - waitlist / blocked / cash
  - constraint summary
- Rebalance Monitor 또는 동등 화면:
  - BUY/ADD/HOLD/TRIM/EXIT/SKIP
  - execution mode
  - gate status
  - turnover / cash delta
- Portfolio Evaluation:
  - NAV
  - drawdown
  - turnover
  - avg holdings
  - policy comparison
- Ops:
  - active policy registry
  - latest target/rebalance/nav run
  - validation / rollback history

완료 후 반드시 남길 것:
- 추가/수정 파일 목록
- config 파일 목록
- allocation flow 요약
- rebalance sequencing 요약
- weight calculation 요약
- 저장 테이블/컬럼 요약
- execution mode 차이 요약
- known limitation
- 다음 티켓으로 넘길 메모

주의:
- 이번 티켓의 목적은 “자동매매”가 아니라 “들고 갈 포트폴리오 제안” 입니다.
- 복잡한 optimizer 보다 단순하고 재현 가능한 allocator 를 우선하세요.
- intraday timing 결과는 portfolio engine 을 지배하는 것이 아니라 신규진입/추가매수 execution gating 에만 사용하세요.
