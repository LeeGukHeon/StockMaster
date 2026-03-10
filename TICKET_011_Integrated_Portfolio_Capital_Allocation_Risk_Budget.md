# TICKET-011 — 통합 포트폴리오 / 자본배분 / 리스크 버짓 레이어

- 문서 목적: TICKET-010 이후, Codex가 바로 이어서 구현할 **통합 포트폴리오 구성 / 자본배분 / 리스크 버짓 / 리밸런싱 / 포트폴리오 레벨 평가 계층** 의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
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
  - `CODEX_ELEVENTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
  - TICKET-006 ML alpha model v1 / uncertainty-disagreement / selection engine v2 완료
  - TICKET-007 intraday candidate assist engine v1 완료
  - TICKET-008 intraday postmortem / regime-aware comparison / strategy comparison 완료
  - TICKET-009 intraday policy calibration / tuning / ablation / freeze-rollback 프레임 완료
  - TICKET-010 intraday policy meta-model / ML timing classifier v1 완료
- 우선순위: 최상
- 기대 결과: **종목 레벨 selection / timing 결과를 실제로 들고 갈 long-only 포트폴리오 제안으로 바꾸고, 자본배분·리스크 버짓·현금 비중·리밸런싱·사후 평가가 모두 재현 가능한 계약 아래 관리되는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“좋은 종목 후보를 고르는 시스템”을 “실제로 몇 종목을 어떤 비중으로 들고 갈지 제안하는 포트폴리오 시스템”으로 연결하는 작업** 이다.

핵심은 다음 다섯 가지다.

1. selection engine v2 / intraday timing overlay 결과를 포트폴리오 관점에서 조합한다.
2. `target book`, `rebalance plan`, `position snapshot`, `NAV snapshot` 을 만든다.
3. 가중치 배분이 단순 equal weight 가 아니라 **alpha, uncertainty, implementation, volatility, regime** 를 반영하도록 만든다.
4. long-only / no leverage / cash buffer / turnover cap / concentration cap / liquidity cap 을 갖춘다.
5. 포트폴리오 수준에서 **open-all / timing-assisted / baseline** 을 비교 평가할 수 있게 한다.

즉, 이번 티켓의 목적은 “자동매매”가 아니라 **재현 가능한 paper portfolio / recommended book / capital allocation layer** 를 완성하는 것이다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 selection engine v2 와 intraday overlay 는 upstream 이고, portfolio layer 는 downstream 이다
이번 티켓의 portfolio layer 는 새 예측 엔진이 아니다.

반드시 아래 순서를 유지한다.

1. 장후 `selection engine v2` 가 후보와 점수를 만든다.
2. 필요 시 장중 `intraday policy + meta overlay` 가 entry timing final action 을 만든다.
3. portfolio layer 는 이 결과를 바탕으로 **무엇을, 얼마나, 어떤 우선순위로** 들고 갈지 결정한다.

즉 portfolio layer 는:
- 종목 selection 자체를 대체하지 않는다.
- intraday timing 을 독립적으로 재학습하지 않는다.
- raw 시장 데이터만으로 직접 포트폴리오를 구성하지 않는다.
- upstream 결과를 **weight / hold / trim / exit / cash** 로 번역하는 계층이다.

### 2.2 v1 은 long-only, no leverage, no short, no auto-order 여야 한다
이번 티켓의 기본 전제는 보수적 개인 리서치 플랫폼이다.

따라서 v1 은 반드시 아래를 따른다.

- long-only
- leverage 금지
- short 금지
- 파생상품 포지션 금지
- 주문 API / 자동매매 연동 금지
- 추천 포트폴리오 / paper portfolio / 리밸런싱 제안까지만 구현

### 2.3 포트폴리오는 “점수 순으로 N개 담기”가 아니라 “위험과 실행가능성을 반영한 제약 있는 자본배분”이어야 한다
이번 티켓에서 가장 중요한 설계 원칙은 이것이다.

좋은 종목 순위를 만드는 것과 실제 자본을 나누는 것은 다르다.

반드시 아래가 반영되어야 한다.

- 불확실성이 높을수록 비중 축소
- implementation penalty 가 높을수록 비중 축소
- 변동성이 높을수록 동일 conviction 에서 비중 축소
- 장세가 불안할수록 cash buffer 확대
- 집중도가 높아지면 cap 적용
- turnover 가 지나치면 진입/교체를 제한
- liquidity / tradability 가 부족하면 신규진입 제한 또는 skip

### 2.4 포트폴리오 엔진 v1 은 deterministic heuristic allocator 여야 한다
이번 티켓에서는 과도한 최적화기를 도입하지 않는다.

권장 구현은 아래와 같다.

- score-to-weight 변환
- inverse-vol 또는 volatility-aware scaling
- cap-aware iterative water-filling
- sector/market concentration control
- turnover budget 기반 new entry 제한
- deterministic tie-breaker

허용:
- 단순 선형/비선형 휴리스틱
- greedy allocator
- cap-aware rebalance engine
- lightweight optimizer 가 필요하면 선택적 사용

이번 티켓에서 하지 않는 것:
- 무거운 수학 최적화 프레임 필수화
- 복잡한 covariance matrix optimizer 를 기본 의존성으로 강제
- leverage, margin, VaR 최적화, 옵션 헤지
- 포트폴리오 RL

### 2.5 포트폴리오 엔진은 “entry threshold” 와 “hold threshold” 를 분리해야 한다
turnover 를 낮추기 위해 신규진입 기준과 보유지속 기준은 다르게 두는 것이 맞다.

권장 원칙:
- 신규진입은 더 엄격하다.
- 기존 보유는 약간 완화된 기준에서 유지할 수 있다.
- 단, hard exit 신호는 예외 없이 우선한다.

예시 개념:
- new entry floor: `A` 또는 `A-` + 양의 adjusted alpha + tradable
- hold floor: `B` 이상 + no hard exit
- hard exit: `C`, `prediction_interval_lower << 0`, severe liquidity failure, hard block

### 2.6 cash 는 leftover 가 아니라 의도적 risk budget 이다
현금 비중은 남는 돈이 아니라 리스크 버짓의 일부여야 한다.

따라서 v1 에서도 다음이 있어야 한다.

- regime-aware target cash floor
- regime-aware target cash ceiling
- candidate 부족 / uncertainty 확장 / turnover cap binding 시 cash 유지
- 포트폴리오가 억지로 fully-invested 되지 않도록 설계

### 2.7 intraday timing overlay 는 execution gating 으로만 연결한다
이번 티켓에서 장중 엔진은 포트폴리오 구조를 바꾸는 것이 아니라 **신규진입 / 추가매수 실행 타이밍을 보조** 하는 용도로 연결한다.

즉:
- 전일 장후 target book 은 존재해야 한다.
- 다음 세션에 `OPEN_ALL` 가 기본 execution mode 이다.
- intraday output 이 있으면 `TIMING_ASSISTED` execution mode 를 추가 비교한다.
- `WAIT_RECHECK` / `AVOID_TODAY` 는 당일 신규진입 notional 을 현금으로 남기게 할 수 있다.
- 기존 보유 청산/축소는 v1 에서 open-based deterministic execution 으로 충분하다.

### 2.8 active portfolio policy / freeze / rollback 이 있어야 한다
selection, timing, meta-model 에 version 관리가 들어간 만큼 포트폴리오 정책도 동일하게 관리해야 한다.

반드시 아래가 필요하다.

- active portfolio policy registry
- freeze command
- rollback command
- policy scope
- policy version / config hash / created_by / activated_at
- no auto-promotion
- no auto-rollback

### 2.9 virtual capital 과 수량 계산은 반드시 설정 기반이어야 한다
포트폴리오는 weight 만으로 끝나면 불충분하다.

최소한 아래를 지원해야 한다.

- `virtual_capital_krw`
- 기준 가격(보통 next-open reference 또는 latest close reference)
- target notional
- target shares
- fractional share 금지
- rounding residual cash
- 잔여 현금 추적

### 2.10 as-of discipline 과 reproducibility 를 최우선으로 한다
포트폴리오 생성 시점에 알 수 있는 정보만 사용해야 한다.

- `as_of_date` 장후 target book 생성 시점에는 그 시점까지 materialized 된 selection / prediction / regime / feature / latest holdings 만 사용
- next session execution simulation 에서는 정해진 execution assumption 만 사용
- 이후 정보를 역으로 target book 에 섞으면 안 된다
- 과거 target book / position / nav snapshot 을 재계산 후 overwrite 하면 안 된다

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 Portfolio candidate book assembly
포트폴리오 후보군 조립 레이어를 만든다.

입력은 최소한 아래를 사용할 수 있어야 한다.

- `fact_ranking` 의 selection engine v2 결과
- `fact_prediction` 의 alpha / interval / uncertainty / disagreement
- `fact_market_regime_snapshot`
- `fact_feature_snapshot` 의 volatility / turnover / liquidity proxy
- 기존 portfolio holdings 또는 직전 position snapshot
- 필요 시 `dim_symbol` 의 market / sector / issuer 정보

출력은 최소한 아래를 포함해야 한다.

- `as_of_date`
- `symbol`
- `portfolio_policy_id`
- `primary_horizon`
- `tactical_horizon`
- `selection_grade`
- `selection_value_v2`
- `predicted_excess_return_primary`
- `predicted_excess_return_tactical`
- `prediction_interval_lower_primary`
- `uncertainty_score`
- `disagreement_score`
- `implementation_penalty_score`
- `flow_score`
- `regime_fit_score`
- `volatility_proxy`
- `adv20_notional` 또는 동등 liquidity proxy
- `is_current_holding`
- `entry_eligibility_flag`
- `hold_eligibility_flag`
- `hard_exit_flag`
- `candidate_reason_json`

권장 상태 분류:
- `NEW_ENTRY_CANDIDATE`
- `HOLD_CANDIDATE`
- `TRIM_CANDIDATE`
- `EXIT_CANDIDATE`
- `WATCH_ONLY`
- `BLOCKED`

### 3.2 Portfolio policy schema 와 active registry
최소 하나 이상의 포트폴리오 정책 템플릿을 만든다.

권장 기본 템플릿:
- `balanced_long_only_v1`
- `defensive_long_only_v1` (가능하면 같이)

각 policy 는 최소한 아래를 가져야 한다.

- `portfolio_policy_id`
- `portfolio_policy_version`
- `primary_horizon` (권장 `5`)
- `tactical_horizon` (권장 `1`)
- `virtual_capital_krw`
- `target_cash_floor_by_regime`
- `target_cash_ceiling_by_regime`
- `min_names`
- `max_names`
- `max_single_weight`
- `max_sector_weight`
- `max_market_weight_kosdaq`
- `max_new_entries_per_day`
- `max_gross_turnover_per_rebalance`
- `max_adv_participation`
- `new_entry_grade_floor`
- `hold_grade_floor`
- `hard_exit_rules`
- `weighting_method`
- `volatility_scaling_method`
- `hysteresis_buffer`
- `created_at`

정책 파일은 `config/portfolio_policies/` 아래 YAML 또는 JSON 으로 두는 것이 권장된다.

### 3.3 Portfolio allocation engine v1
실제 자본배분 엔진을 만든다.

반드시 지원해야 하는 흐름:

1. eligible candidate pool 생성
2. current holdings / carry-over 상태 확인
3. regime-aware target gross exposure 와 cash budget 결정
4. 각 종목의 `effective conviction` 계산
5. volatility-aware score scaling
6. 신규진입 / 기존보유 / 청산 우선순위 결정
7. concentration cap / liquidity cap / turnover cap 적용
8. 최종 `target_weight_final` 계산
9. 잔여는 cash 로 유지

권장 수식 예시(형태만 맞으면 됨):

```text
alpha_primary = max(predicted_excess_return_d5, 0)
alpha_tactical = max(predicted_excess_return_d1, 0)

robust_alpha =
    alpha_primary
    + w_tactical * alpha_tactical
    + w_lower * max(prediction_interval_lower_primary, 0)
    + w_flow * flow_score
    + w_regime * regime_fit_score
    - w_uncertainty * uncertainty_score
    - w_disagreement * disagreement_score
    - w_impl * implementation_penalty_score

effective_alpha_long = max(robust_alpha, 0)

risk_scaled_conviction =
    effective_alpha_long / max(volatility_proxy, vol_floor)
```

그 다음 normalized score 를 weight 로 변환하되:
- `max_single_weight`
- `max_sector_weight`
- `max_market_weight_kosdaq`
- `max_adv_participation`
- `max_names`
- `cash floor`
- `cash ceiling`
을 만족해야 한다.

### 3.4 Deterministic rebalance sequencing
리밸런싱은 반드시 재현 가능해야 한다.

권장 우선순위는 아래와 같다.

1. **forced exit**
   - hard exit
   - tradability failure
   - severe cap violation
2. **forced trim**
   - single-name cap 초과
   - sector cap 초과
3. **hold keep**
   - 기존 보유 중 keep 조건을 만족하는 종목 유지
4. **add existing winners**
   - 기존 보유 중 conviction 이 높은 종목 증액
5. **new entries**
   - 신규진입 후보를 priority 순으로 채움
6. **residual cash**
   - 남는 비중은 현금

반드시 `rebalance_reason` 과 `constraint_binding_json` 을 남긴다.

### 3.5 Open-all 과 timing-assisted 두 execution mode 지원
최소한 아래 두 가지 execution mode 를 비교할 수 있어야 한다.

- `OPEN_ALL`
  - 장후 target book 기준 신규진입/추가매수는 다음 세션 open 에 모두 실행한다고 가정
- `TIMING_ASSISTED`
  - 장중 final action 이 `ENTER_NOW` 인 신규진입/추가매수만 실제 실행
  - `WAIT_RECHECK` 는 당일 미집행 cash 로 남김
  - `AVOID_TODAY` 는 skip
  - 기존 청산/축소는 v1 에서 open execution 으로 충분

`TIMING_ASSISTED` 는 TICKET-010 output 이 존재할 때만 활성화되면 된다.  
없으면 `OPEN_ALL` 만으로도 이번 티켓은 성립한다.

### 3.6 Position snapshot / NAV snapshot materialization
포트폴리오가 실제로 어떻게 움직였는지 남겨야 한다.

최소한 아래가 가능해야 한다.

- decision day 기준 target book 저장
- next execution day 기준 rebalance plan 저장
- execution mode 별 position snapshot 저장
- execution mode 별 NAV / daily return / cash weight / turnover 저장

권장 지원 필드:
- `position_status`
- `entry_date`
- `days_held`
- `shares`
- `avg_cost_assumption`
- `market_value_close`
- `weight_close`
- `cash_weight`
- `gross_exposure`
- `daily_return`
- `cum_nav`

### 3.7 Portfolio walk-forward evaluation
포트폴리오 레벨 백테스트/평가를 추가한다.

최소 비교 대상:
- `PORTFOLIO_ACTIVE_OPEN_ALL`
- `PORTFOLIO_ACTIVE_TIMING_ASSISTED` (가능 시)
- `SELECTION_V2_TOPN_EQW`
- `SELECTION_V1_TOPN_EQW` 또는 `EXPLANATORY_TOPN_EQW` (가능 시)
- `CASH_ONLY`

최소 평가 지표:
- cumulative return
- annualized volatility
- Sharpe-like ratio
- max drawdown
- average gross turnover
- average cash weight
- average number of holdings
- concentration statistics
- hit rate
- realized implementation usage
- timing-assisted vs open-all 차이

### 3.8 Portfolio report / UI / Discord summary
포트폴리오 레벨 결과를 사람이 볼 수 있어야 한다.

최소한 아래를 제공한다.

- 현재 target book
- 오늘 rebalance plan
- cash / concentration / sector exposure 요약
- top holdings 와 allocation rationale
- open-all vs timing-assisted 차이
- 최근 NAV / drawdown / turnover
- active policy 정보

필요 시 Discord 로는 간단 요약만 보내면 된다.

### 3.9 Freeze / rollback / validation framework
포트폴리오 정책도 운영 계층으로 관리되어야 한다.

반드시 아래가 필요하다.

- active policy freeze
- active policy rollback
- validation script
- config hash 기록
- registry history
- scope overlap validation
- dry-run support

---

## 4. 포트폴리오 엔진 v1의 추천 동작 규칙

### 4.1 권장 기본 구조
v1 은 하나의 단일 book 이지만, 내부적으로는 아래 두 입력을 조합해도 된다.

- **primary sleeve**: D+5 alpha / lower band / selection grade
- **tactical overlay**: D+1 alpha / flow / timing 친화성

단, 결과는 하나의 long-only target book 이면 충분하다.

### 4.2 신규진입 기본 규칙
권장 신규진입 최소 조건:

- `selection_grade in {A, A-}`
- `predicted_excess_return_primary > 0`
- `effective_alpha_long > 0`
- `hard_exit_flag = false`
- `tradability_ok = true`
- `liquidity_ok = true`
- turnover budget 내
- portfolio slot available

### 4.3 보유유지 기본 규칙
권장 보유유지 최소 조건:

- 기존 보유
- `selection_grade in {A, A-, B}`
- `hard_exit_flag = false`
- severe tradability failure 없음

즉 신규진입보다 보유유지가 약간 완화된다.

### 4.4 강제 청산 기본 규칙
권장 hard exit 예시:

- `selection_grade = C`
- `effective_alpha_long <= 0` 가 일정 기준 이하
- `prediction_interval_lower_primary` 가 강하게 음수
- severe liquidity/tradability failure
- data integrity failure
- symbol inactive / suspended

### 4.5 cash buffer 기본 규칙
권장 기본 매핑 예시:

- `OFFENSE` / `CALM`: cash floor 5% ~ 10%
- `NEUTRAL`: cash floor 10% ~ 15%
- `CAUTION`: cash floor 15% ~ 25%
- `PANIC`: cash floor 25% ~ 40%

실제 수치는 policy file 에 두고, 코드에서 하드코딩하지 않는 것이 좋다.

### 4.6 concentration 제약
최소한 아래는 있어야 한다.

- 단일 종목 비중 상한
- 동일 sector 비중 상한
- KOSDAQ 전체 비중 상한
- 최대 보유 종목 수
- 최소 보유 종목 수(가능하면)

### 4.7 liquidity / tradability 제약
최소한 아래 프록시 중 일부를 써야 한다.

- `adv20_notional`
- 최근 거래대금 percentile
- 거래정지/관리/이상 상태 플래그
- target notional / adv20_notional 비율

권장 신규진입 제약:
- `target_notional <= adv20_notional * max_adv_participation`

### 4.8 turnover 제약
turnover 제약은 반드시 있어야 한다.

권장 구현:
- gross turnover budget
- 신규진입 종목 수 상한
- 작은 차이는 rebalance 하지 않는 hysteresis band
- top-ranked hold 에 가산점 또는 hold bonus

### 4.9 deterministic tie-breaker
동점 처리 순서를 코드로 명시한다.

권장 예시:
1. higher selection grade
2. higher effective alpha
3. lower uncertainty
4. lower implementation penalty
5. higher liquidity
6. symbol code ascending

---

## 5. 저장 계약과 데이터 구조

### 5.1 기존 테이블 재사용 원칙
가능하면 아래를 재사용한다.

- `fact_ranking`
- `fact_prediction`
- `fact_market_regime_snapshot`
- `fact_feature_snapshot`
- `fact_selection_outcome`
- `fact_intraday_final_action` 또는 동등 구조
- `dim_symbol`
- `dim_trading_calendar`

### 5.2 이번 티켓에서 권장되는 새 테이블

#### 5.2.1 `fact_portfolio_policy_registry`
권장 최소 컬럼:
- `portfolio_policy_id`
- `portfolio_policy_version`
- `policy_scope`
- `config_path`
- `config_hash`
- `status` (`draft`, `active`, `archived`, `rolled_back`)
- `created_at`
- `activated_at`
- `deactivated_at`
- `rollback_of_policy_version`
- `notes`

#### 5.2.2 `fact_portfolio_candidate`
권장 최소 컬럼:
- `as_of_date`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `symbol`
- `primary_horizon`
- `tactical_horizon`
- `selection_engine_version`
- `selection_grade`
- `selection_value`
- `predicted_excess_return_primary`
- `predicted_excess_return_tactical`
- `prediction_interval_lower_primary`
- `uncertainty_score`
- `disagreement_score`
- `implementation_penalty_score`
- `flow_score`
- `regime_fit_score`
- `volatility_proxy`
- `adv20_notional`
- `is_current_holding`
- `entry_eligibility_flag`
- `hold_eligibility_flag`
- `hard_exit_flag`
- `candidate_state`
- `candidate_reason_json`
- `created_at`

#### 5.2.3 `fact_portfolio_target_book`
권장 최소 컬럼:
- `as_of_date`
- `decision_run_id`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `execution_mode_default`
- `symbol`
- `book_state` (`TARGET_HOLD`, `TARGET_NEW_ENTRY`, `TARGET_TRIM`, `TARGET_EXIT`, `WAITLIST`, `CASH_RESERVE`)
- `slot_rank`
- `effective_alpha_long`
- `risk_scaled_conviction`
- `target_weight_raw`
- `target_weight_capped`
- `target_weight_final`
- `target_notional_krw`
- `reference_price`
- `target_shares`
- `cash_floor_applied`
- `constraint_binding_json`
- `allocation_reason_json`
- `created_at`

#### 5.2.4 `fact_portfolio_rebalance_plan`
권장 최소 컬럼:
- `trade_date`
- `decision_as_of_date`
- `rebalance_run_id`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `execution_mode`
- `symbol`
- `prev_weight`
- `target_weight`
- `delta_weight`
- `rebalance_action` (`BUY_NEW`, `ADD`, `HOLD`, `TRIM`, `EXIT`, `SKIP`, `NO_ACTION`)
- `expected_price_reference`
- `target_notional_krw`
- `target_shares`
- `gate_status`
- `rebalance_reason`
- `constraint_binding_json`
- `created_at`

#### 5.2.5 `fact_portfolio_position_snapshot`
권장 최소 컬럼:
- `holding_date`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `execution_mode`
- `symbol`
- `position_status`
- `shares`
- `avg_cost_assumption`
- `entry_date`
- `days_held`
- `market_value_open`
- `market_value_close`
- `weight_open`
- `weight_close`
- `unrealized_pnl`
- `created_at`

#### 5.2.6 `fact_portfolio_nav_snapshot`
권장 최소 컬럼:
- `holding_date`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `execution_mode`
- `nav_open`
- `nav_close`
- `daily_return`
- `cum_return`
- `cash_weight`
- `gross_exposure`
- `net_exposure`
- `gross_turnover`
- `num_holdings`
- `largest_weight`
- `top3_weight_sum`
- `created_at`

#### 5.2.7 `fact_portfolio_constraint_event`
권장 최소 컬럼:
- `as_of_date`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `symbol`
- `constraint_name`
- `constraint_value`
- `constraint_limit`
- `binding_state`
- `action_taken`
- `created_at`

#### 5.2.8 `fact_portfolio_evaluation_summary`
권장 최소 컬럼:
- `evaluation_run_id`
- `portfolio_policy_id`
- `portfolio_policy_version`
- `execution_mode`
- `start_date`
- `end_date`
- `metric_name`
- `metric_value`
- `metric_scope`
- `created_at`

### 5.3 artifact / report 경로 권장안
예시:

```text
artifacts/portfolio/policies/balanced_long_only_v1/
artifacts/portfolio/reports/as_of_date=2026-03-06/
artifacts/portfolio/evaluation/portfolio_policy_id=balanced_long_only_v1/
```

---

## 6. 스크립트와 구현 단위

이번 티켓에서 Codex가 구현해야 할 최소 스크립트는 아래와 같다.

### 6.1 후보군 조립
- `scripts/build_portfolio_candidate_book.py`
- `scripts/validate_portfolio_candidate_book.py`

### 6.2 정책 활성화 / 롤백
- `scripts/freeze_active_portfolio_policy.py`
- `scripts/rollback_active_portfolio_policy.py`

### 6.3 target book / rebalance / position / nav
- `scripts/materialize_portfolio_target_book.py`
- `scripts/materialize_portfolio_rebalance_plan.py`
- `scripts/materialize_portfolio_position_snapshots.py`
- `scripts/materialize_portfolio_nav.py`

### 6.4 walk-forward / 평가
- `scripts/run_portfolio_walkforward.py`
- `scripts/evaluate_portfolio_policies.py`

### 6.5 리포트 / 전송
- `scripts/render_portfolio_report.py`
- `scripts/publish_discord_portfolio_summary.py`

### 6.6 프레임 검증
- `scripts/validate_portfolio_framework.py`

가능하면 아래 config 도 포함한다.
- `config/portfolio_policies/balanced_long_only_v1.yaml`
- `config/portfolio_policies/defensive_long_only_v1.yaml`

---

## 7. UI / 화면 요구사항

### 7.1 Portfolio Studio 또는 동등 페이지
최소한 아래가 보여야 한다.

- active portfolio policy
- virtual capital
- current regime
- target cash floor / applied cash
- target holdings table
- symbol 별 target weight / target notional / target shares
- allocation rationale 요약
- constraint hit 표시
- waitlist / blocked candidates

### 7.2 Rebalance Monitor 또는 동등 페이지
최소한 아래가 보여야 한다.

- 오늘/다음 세션 rebalance plan
- `BUY_NEW`, `ADD`, `HOLD`, `TRIM`, `EXIT`, `SKIP`
- `OPEN_ALL` vs `TIMING_ASSISTED`
- gate status
- gross turnover estimate
- cash delta

### 7.3 Portfolio Evaluation 페이지
최소한 아래가 보여야 한다.

- portfolio NAV curve
- drawdown curve
- average holdings / average cash
- turnover
- concentration
- execution mode 비교
- policy 비교
- equal-weight baseline 대비 비교

### 7.4 Stock Workbench 확장
개별 종목 화면에서 최소한 아래가 보여야 한다.

- 현재 포트폴리오 포함 여부
- current target weight
- rebalance action
- entry/hold/exit reason
- portfolio-level constraint 영향 여부

### 7.5 Ops 페이지 확장
최소한 아래가 보여야 한다.

- active policy registry
- latest target book run
- latest rebalance run
- latest position/nav materialization run
- validation health
- rollback history
- constraint event summary

---

## 8. README / 문서화 요구사항

README 또는 동등 문서에 최소한 아래를 정리한다.

- portfolio engine v1 의 목적
- upstream dependency (selection v2 / timing overlay)
- long-only / no leverage / no auto-order 원칙
- policy config 구조
- target book 생성 규칙
- new entry / hold / hard exit 규칙
- weighting / scaling / cap 방식
- cash budget 규칙
- open-all / timing-assisted 차이
- walk-forward 실행 순서
- known limitations

---

## 9. 테스트 요구사항

최소한 아래 테스트가 있어야 한다.

### 9.1 Candidate assembly test
- selection/prediction/regime/feature join 이 성공하는지
- hold/new entry/hard exit flag 가 의도대로 나오는지
- missing optional field 시 fallback 이 동작하는지

### 9.2 Policy config validation test
- YAML/JSON policy 가 검증되는지
- 필수 필드 누락 시 실패하는지
- freeze/rollback registry 가 정상 동작하는지

### 9.3 Allocation engine smoke test
- 소규모 샘플로 target book 생성이 되는지
- target weights 합이 cash 포함 100% 내인지
- single-name cap / sector cap / turnover cap 이 적용되는지
- no negative weight / no leverage 를 지키는지

### 9.4 Rebalance plan test
- 기존 보유에서 BUY/ADD/HOLD/TRIM/EXIT 분류가 가능한지
- delta weight 와 target notional / shares 계산이 맞는지
- skip / blocked 상태가 남는지

### 9.5 Position/NAV materialization test
- execution mode 별 position snapshot 이 생성되는지
- NAV 가 누적 계산되는지
- cash weight / turnover 가 기록되는지

### 9.6 Portfolio evaluation test
- baseline 과 비교 결과가 생성되는지
- cumulative return / drawdown / turnover / avg holdings 가 계산되는지
- timing-assisted 가 없는 경우 open-all only 로 fallback 되는지

### 9.7 UI smoke test
- Portfolio Studio / Evaluation / Ops 가 최신 필드를 읽는지

---

## 10. 완료 기준 (Definition of Done)

아래가 모두 충족되면 이번 티켓을 완료로 본다.

1. `python scripts/build_portfolio_candidate_book.py --as-of-date 2026-03-06 --portfolio-policy balanced_long_only_v1`
2. `python scripts/validate_portfolio_candidate_book.py --as-of-date 2026-03-06 --portfolio-policy balanced_long_only_v1`
3. `python scripts/freeze_active_portfolio_policy.py --config config/portfolio_policies/balanced_long_only_v1.yaml`
4. `python scripts/materialize_portfolio_target_book.py --as-of-date 2026-03-06 --portfolio-policy balanced_long_only_v1 --execution-mode OPEN_ALL`
5. `python scripts/materialize_portfolio_rebalance_plan.py --decision-as-of-date 2026-03-06 --portfolio-policy balanced_long_only_v1 --execution-mode OPEN_ALL`
6. `python scripts/materialize_portfolio_position_snapshots.py --start-date 2026-02-17 --end-date 2026-03-06 --portfolio-policy balanced_long_only_v1 --execution-mode OPEN_ALL`
7. `python scripts/materialize_portfolio_nav.py --start-date 2026-02-17 --end-date 2026-03-06 --portfolio-policy balanced_long_only_v1 --execution-mode OPEN_ALL`
8. `python scripts/run_portfolio_walkforward.py --start-date 2026-02-17 --end-date 2026-03-06 --portfolio-policy balanced_long_only_v1`
9. `python scripts/evaluate_portfolio_policies.py --start-date 2026-02-17 --end-date 2026-03-06 --portfolio-policy balanced_long_only_v1`
10. `python scripts/render_portfolio_report.py --as-of-date 2026-03-06 --portfolio-policy balanced_long_only_v1 --dry-run`
11. `python scripts/validate_portfolio_framework.py --start-date 2026-02-17 --end-date 2026-03-06 --portfolio-policy balanced_long_only_v1`
12. `streamlit run app/ui/Home.py`
13. UI 에서 target book / rebalance plan / portfolio evaluation 확인 가능
14. README 갱신 완료
15. 테스트 통과

`TIMING_ASSISTED` 모드는 intraday final action output 이 존재하면 추가로 검증한다.
존재하지 않더라도 `OPEN_ALL` 기준 완료는 인정한다.

---

## 11. 하지 말아야 할 것

이번 티켓에서 아래는 금지한다.

- 자동 주문 / 자동매매 연동
- leverage / margin / short / derivatives
- target book 생성 시 미래 정보 사용
- selection engine 을 portfolio engine 안에서 재학습
- UI 접속 시 walk-forward 자동 실행
- 음수 weight / 100% 초과 gross exposure 허용
- random tie-break 로 결과 비결정화
- history overwrite
- 뉴스 본문 전문 저장/전송
- heavy optimizer 를 필수 dependency 로 강제
- intraday timing 부재 시 전체 포트폴리오 기능이 깨지도록 만들기

---

## 12. 작업 완료 후 Codex가 남겨야 할 요약

작업이 끝나면 최소한 아래를 짧게 정리한다.

- 새로 추가된 파일 목록
- portfolio policy config 목록
- target book 생성 순서
- rebalance sequencing 요약
- weight calculation 요약
- cash / concentration / turnover 규칙 요약
- 저장 테이블/컬럼 요약
- execution mode 차이 요약
- UI 에서 확인할 위치
- known limitation
- 다음 티켓으로 넘길 메모

---

## 13. 다음 티켓으로 자연스럽게 이어질 가능성이 높은 방향

다음 티켓 후보는 아래 중 하나가 자연스럽다.

1. **운영 안정화 / 배치 복구 / 디스크 가드 / 모니터링 / health dashboard**
2. **최종 사용자 워크플로우 정리 / 장후-장중-사후평가 통합 대시보드 마감**
3. **portfolio policy tuning / regime-aware capital schedule 고도화**

이번 티켓은 “종목 추천 시스템”을 “실제로 들고 갈 포트폴리오 제안 시스템”으로 연결하는 마감 성격의 티켓이므로, 구현 시 설계의 보수성과 재현성을 우선한다.
