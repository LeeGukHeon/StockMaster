# TICKET-009 — 장중 타이밍 레이어 ML화 전 단계: Policy Calibration 자동화 + Regime Parameter Tuning + 실험 관리/Ablation 프레임

- 문서 목적: TICKET-008 이후, Codex가 바로 이어서 구현할 **장중 타이밍 정책의 자동 calibration / regime-aware parameter tuning / experiment registry / ablation framework** 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
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
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
  - `CODEX_NINTH_INSTRUCTION_StockMaster.md`
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
- 우선순위: 최상
- 기대 결과: **장중 timing layer 를 손으로 만지는 규칙 묶음이 아니라, 누수 없이 실험·비교·선정·동결(freeze)·승격(promote)할 수 있는 정책 연구 프레임으로 전환하는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“장중 타이밍 정책을 사람이 감으로 조절하는 단계”에서 “같은 exit 정의와 walk-forward 원칙 아래 여러 정책/파라미터/ablation 을 자동으로 비교하고, 그 결과를 근거로 active policy 를 동결·교체하는 단계”** 로 가는 전환점이다.

즉, 이번 티켓의 목표는 아래 아홉 가지를 안정적으로 만드는 것이다.

1. 장중 policy parameter 와 adjustment profile 을 **명시적 스키마** 로 정의한다.
2. 실험에 사용할 policy candidate search space 를 생성하고 저장한다.
3. matured outcome 만 사용해서 정책 후보를 평가하는 **offline calibration** 파이프라인을 만든다.
4. global / regime-cluster / regime-family / horizon / checkpoint 단위의 tuning 을 지원한다.
5. walk-forward split 으로 후보를 비교하고, leakage 없는 방식으로 **선정 결과** 를 기록한다.
6. full tuned policy 뿐 아니라 주요 guard / penalty / gate 의 기여를 따로 보는 **ablation framework** 를 만든다.
7. 실험 결과를 바탕으로 특정 기간에 유효한 active policy 를 **freeze / promote / rollback** 할 수 있게 한다.
8. UI / 리포트 / Ops 에서 현재 active policy, 후보 성과, ablation 결과, fallback 상태를 사람이 이해 가능하게 보여준다.
9. 다음 티켓에서 intraday timing layer 의 ML화 또는 meta-policy 모델로 넘어갈 수 있도록 실험 데이터 계약을 고정한다.

이번 티켓이 끝나면 다음 단계에서는 작은 분류기나 meta-model 이 들어오더라도, 최소한 **동일한 실험/승격/rollback 체계** 위에서 움직일 수 있어야 한다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 이번 티켓은 여전히 “pre-ML policy research” 단계다
이번 티켓은 이름 그대로 **ML화 전 단계** 다.

허용되는 것:
- deterministic parameter search
- grid search / constrained search / simple heuristic search
- walk-forward evaluation
- regime-specific threshold tuning
- ablation / sensitivity analysis
- active policy freeze / promote / rollback

이번 티켓에서 하지 않는 것:
- reinforcement learning
- online learning
- 장중 실시간 self-update
- UI 접속 시 자동 튜닝
- 미성숙(matured 되지 않은) outcome 을 섞은 적응형 업데이트

### 2.2 tuning 과 evaluation 은 반드시 matured outcome 만 사용한다
반드시 아래를 지킨다.

- 정책 후보 튜닝에는 `fact_intraday_strategy_result` 또는 동등한 **matured outcome** 만 사용한다.
- 아직 exit 가 닫히지 않은 session 은 calibration 입력으로 쓰지 않는다.
- `effective_from_date` 이후에 발생한 outcome 을 써서 같은 날짜 정책을 선택하면 안 된다.
- 정책 승격은 **effective_from 이전에 확정된 정보만** 기반으로 해야 한다.

즉, 이번 티켓의 가장 중요한 목표 중 하나는 **정책 누수 방지** 다.

### 2.3 same-exit comparison 원칙을 유지한다
정책 비교는 모두 entry 정책만 다르고 exit 기준은 동일해야 한다.

- `SEL_V2_OPEN_ALL` baseline 과 tuned policy 는 같은 horizon 종료일을 사용한다.
- `raw timing`, `adjusted timing`, `tuned timing` 모두 exit 정의를 바꾸지 않는다.
- holding period 를 정책마다 다르게 바꾸는 것은 이번 티켓 범위가 아니다.

### 2.4 정책 선택도 “coverage”와 “stability”를 같이 본다
장중 timing policy 는 mean return 하나로 선택하면 쉽게 과최적화된다.

따라서 정책 후보 비교는 최소한 아래를 같이 봐야 한다.

- mean / median realized excess return
- hit rate
- mean timing edge vs open
- execution rate
- skip saved loss rate
- missed winner rate
- downside/tail proxy
- regime bucket 별 안정성
- 최근/과거 창 간 일관성

즉, 좋은 정책은 단순히 수익률이 높은 정책이 아니라, **지나치게 execution 이 붕괴하지 않고, 특정 며칠에만 우연히 좋아 보이는 정책이 아니어야** 한다.

### 2.5 regime-specific tuning 은 허용하되, fallback 계층을 분명히 둔다
한국 장세처럼 regime 전환이 빠를 때는 `PANIC_OPEN` 과 `HEALTHY_TREND` 에 같은 threshold 를 쓰는 것이 비효율적일 수 있다.

하지만 regime family 별 표본이 작을 수 있으므로, 반드시 fallback 계층을 둔다.

기본 fallback 순서는 아래를 권장한다.

1. `(horizon, checkpoint, regime_family)` tuned policy
2. `(horizon, regime_cluster)` tuned policy
3. `(horizon)` global tuned policy
4. TICKET-008 deterministic default profile

즉, regime-specific tuning 은 가능하지만, 샘플이 부족할 때는 반드시 더 상위 scope 로 부드럽게 fallback 되어야 한다.

### 2.6 실험 관리(experiment management)는 일급 객체여야 한다
이번 티켓에서는 “몇 개 스크립트를 돌려 본 결과”가 아니라 **재현 가능한 실험 시스템** 을 만들어야 한다.

각 실험은 최소한 아래를 남겨야 한다.

- experiment id / run id
- search space definition hash
- data window
- split definition
- horizon / checkpoint / regime scope
- objective function version
- candidate count
- selected candidate
- fallback 발생 여부
- metric summary
- artifact path / report path

### 2.7 active policy 는 자동 교체하지 않는다
자동매매가 아니더라도 active policy 를 자동 교체하는 것은 위험하다.

이번 티켓에서는 다음만 허용한다.

- 후보 생성
- 평가 / 선정
- 추천(active candidate suggestion) 산출
- 사람이 확인 가능한 freeze / promote 명령
- rollback 명령

즉, calibration run 의 결과가 좋다고 해서 곧바로 운영 policy 를 자동 교체하지 않는다.

### 2.8 ablation 은 “무엇이 성과를 만들었는지”를 보여줘야 한다
풀 정책이 좋아 보여도 아래를 모르면 유지보수가 어렵다.

- regime specialization 이 성과를 만든 것인지
- friction guard 가 손실을 줄인 것인지
- gap chase guard 가 실질적으로 기여하는지
- cohort breadth / market context guard 가 필요한지

따라서 ablation 은 단순 옵션이 아니라, 정책을 믿을 수 있게 만드는 핵심 근거다.

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 Policy parameter schema 고정
다음이 가능한 상태를 만든다.

- 장중 timing policy 의 조정값들을 구조화된 파라미터로 정의할 수 있어야 한다.
- 파라미터는 json blob 하나로 끝내지 말고, 최소 핵심 필드는 명시 칼럼으로도 저장한다.
- parameter hash 를 계산해 동일 파라미터 중복을 식별할 수 있어야 한다.

최소 파라미터 범주는 다음을 포함한다.

- `enter_threshold_delta`
- `wait_threshold_delta`
- `avoid_threshold_delta`
- `min_selection_confidence_gate`
- `min_signal_quality_gate`
- `uncertainty_penalty_weight`
- `spread_penalty_weight`
- `friction_penalty_weight`
- `gap_chase_penalty_weight`
- `cohort_weakness_penalty_weight`
- `market_shock_penalty_weight`
- `data_weak_guard_strength`
- `max_gap_up_allowance_pct`
- `min_execution_strength_gate`
- `min_orderbook_imbalance_gate`
- `allow_enter_under_data_weak` (기본 False)
- `allow_wait_override` (제한적)
- `selection_rank_cap` 또는 equivalent gate

파라미터는 scope 단위로 적용할 수 있어야 한다.

예시 scope:
- `GLOBAL`
- `HORIZON`
- `HORIZON_CHECKPOINT`
- `HORIZON_REGIME_CLUSTER`
- `HORIZON_CHECKPOINT_REGIME_FAMILY`

### 3.2 Search space generation
다음이 가능한 상태를 만든다.

- 기본 policy template 와 제한된 search range 를 기반으로 calibration candidate 집합을 생성할 수 있어야 한다.
- search space 는 무한정 커지지 않도록 제약된 grid / bounded search 여야 한다.
- 각 candidate 는 `policy_candidate_id`, `parameter_hash`, `scope_key`, `template_id`, `search_space_version` 을 가져야 한다.

최소한 다음 종류의 template 을 지원한다.

- `BASE_DEFAULT`
- `DEFENSIVE_LIGHT`
- `DEFENSIVE_STRONG`
- `RISK_ON_LIGHT`
- `GAP_GUARD_STRICT`
- `FRICTION_GUARD_STRICT`
- `COHORT_GUARD_STRICT`
- `FULL_BALANCED`

Search space 는 아래 원칙을 지킨다.

- 극단적 공격형 정책은 기본 grid 에 넣지 않는다.
- `DATA_WEAK` 에서 `ENTER_NOW` 를 쉽게 허용하는 후보는 생성 금지 또는 별도 experimental flag 로 격리한다.
- `AVOID_TODAY -> ENTER_NOW` 역전 가능성을 넓게 허용하지 않는다.
- execution rate 를 0에 가깝게 만드는 과보수 후보가 너무 많아지지 않도록 제한한다.

### 3.3 Experiment registry / result tables
다음 저장 계약 또는 동등한 구조를 만든다.

#### 3.3.1 `fact_intraday_policy_experiment_run`
최소 필드 예시:
- `experiment_run_id`
- `experiment_name`
- `experiment_type` (`CALIBRATION`, `ABLATION`, `WALKFORWARD`, `FREEZE_RECOMMENDATION`)
- `search_space_version`
- `objective_version`
- `split_version`
- `start_session_date`
- `end_session_date`
- `train_start_date`
- `train_end_date`
- `validation_start_date`
- `validation_end_date`
- `test_start_date`
- `test_end_date`
- `horizon`
- `checkpoint_scope`
- `regime_scope`
- `candidate_count`
- `status`
- `selected_policy_candidate_id`
- `fallback_used_flag`
- `artifact_path`
- `created_at`

#### 3.3.2 `fact_intraday_policy_candidate`
최소 필드 예시:
- `policy_candidate_id`
- `template_id`
- `scope_type`
- `scope_key`
- `horizon`
- `checkpoint`
- `regime_cluster`
- `regime_family`
- `parameter_hash`
- 핵심 파라미터 칼럼들
- `parameters_json`
- `search_space_version`
- `created_at`

#### 3.3.3 `fact_intraday_policy_evaluation`
최소 필드 예시:
- `experiment_run_id`
- `policy_candidate_id`
- `split_name` (`TRAIN`, `VALIDATION`, `TEST`, `FULL`)
- `horizon`
- `checkpoint`
- `regime_scope_key`
- `session_count`
- `candidate_count`
- `executed_count`
- `execution_rate`
- `mean_realized_excess_return`
- `median_realized_excess_return`
- `hit_rate`
- `mean_timing_edge_vs_open`
- `positive_timing_edge_rate`
- `skip_saved_loss_rate`
- `missed_winner_rate`
- `left_tail_proxy`
- `stability_score`
- `objective_score`
- `coverage_penalty`
- `sample_penalty`
- `notes_json`

#### 3.3.4 `fact_intraday_policy_ablation_result`
최소 필드 예시:
- `experiment_run_id`
- `ablation_id`
- `ablation_label`
- `base_policy_candidate_id`
- `variant_policy_candidate_id`
- `dimension_removed_or_changed`
- `metric_name`
- `metric_base`
- `metric_variant`
- `metric_delta`
- `session_count`
- `candidate_count`
- `regime_scope_key`
- `horizon`
- `checkpoint`

#### 3.3.5 `fact_intraday_active_policy`
최소 필드 예시:
- `policy_active_id`
- `policy_candidate_id`
- `promotion_type` (`MANUAL_FREEZE`, `MANUAL_ROLLBACK`, `RECOMMENDED_ONLY`)
- `effective_from_date`
- `effective_to_date`
- `scope_type`
- `scope_key`
- `horizon`
- `checkpoint`
- `regime_scope`
- `source_experiment_run_id`
- `approval_note`
- `is_active`
- `created_at`

#### 3.3.6 `fact_intraday_policy_selection_recommendation`
최소 필드 예시:
- `recommendation_date`
- `experiment_run_id`
- `scope_type`
- `scope_key`
- `recommended_policy_candidate_id`
- `fallback_chain_used`
- `reason_codes_json`
- `recommendation_confidence`
- `manual_review_required_flag`
- `created_at`

### 3.4 Objective function v1 구현
다음이 가능한 상태를 만든다.

- 정책 후보를 하나의 score 로 정렬할 수 있어야 하되, score 자체의 구성요소를 함께 보여줘야 한다.
- objective 는 평균 수익률 하나가 아니라 **성과 + coverage + stability + downside control** 을 같이 반영해야 한다.

권장 예시 형태:

`objective_score =`
- `w1 * mean_realized_excess_return`
- `+ w2 * mean_timing_edge_vs_open`
- `+ w3 * skip_saved_loss_rate`
- `+ w4 * hit_rate`
- `- w5 * missed_winner_rate`
- `- w6 * left_tail_proxy`
- `- w7 * execution_collapse_penalty`
- `- w8 * instability_penalty`
- `- w9 * low_sample_penalty`

반드시 필요한 점:
- objective version 을 명시한다.
- objective score 만 저장하지 말고 component 별 값도 저장한다.
- execution rate 가 지나치게 낮은 정책이 평균수익만으로 상위에 오는 것을 막는다.
- 샘플 수가 작을수록 penalty 가 늘어나야 한다.

### 3.5 Walk-forward calibration
다음이 가능한 상태를 만든다.

- 특정 기간의 matured data 를 train/validation/test 또는 rolling walk-forward 구조로 나눠 정책 후보를 평가할 수 있어야 한다.
- 기본은 calendar date 기준이 아니라 trading session 기준으로 분할한다.
- split 정의는 고정 문자열이 아니라 versioned config 로 저장한다.

최소 지원 형태:
- `ANCHORED_WALKFORWARD`
- `ROLLING_WALKFORWARD`

권장 기본값 예시:
- train 40 sessions
- validation 10 sessions
- test 10 sessions
- step 5 sessions

단, history 가 부족하면 아래처럼 동작한다.
- 충분한 train/validation/test window 가 안 되면 **research-only run** 으로 남긴다.
- active promotion recommendation 은 `manual_review_required_flag = true` 로 둔다.
- fallback 정책을 제안하되 자동 승격하지 않는다.

### 3.6 Regime cluster / family tuning
다음이 가능한 상태를 만든다.

- TICKET-008 의 regime family 를 더 상위의 regime cluster 로 묶을 수 있어야 한다.

권장 예시:
- `RISK_OFF`: `PANIC_OPEN`, `WEAK_RISK_OFF`
- `NEUTRAL`: `NEUTRAL_CHOP`
- `RISK_ON`: `HEALTHY_TREND`, `OVERHEATED_GAP_CHASE`
- `DATA_WEAK`: `DATA_WEAK`

이 구조를 두는 이유는 regime family 단위 표본이 모자랄 때 cluster 단위 tuning/fallback 을 하기 위해서다.

최소 구현 사항:
- family → cluster mapping 테이블 또는 deterministic mapping 함수
- cluster 단위 policy evaluation
- family 샘플 부족 시 cluster fallback 로직
- fallback 발생 내역 로그/리포트화

### 3.7 Ablation framework
다음이 가능한 상태를 만든다.

- active 또는 추천 policy 를 기준으로 주요 guard 의 기여를 떼어보고 비교할 수 있어야 한다.
- ablation 은 full tuned policy 하나만 비교하는 것이 아니라, 어떤 구성요소가 어떤 효과를 냈는지 드러내야 한다.

최소 ablation id 예시:
- `ABL_RAW_BASELINE`
- `ABL_DEFAULT_ADJUSTED`
- `ABL_NO_REGIME_SPECIALIZATION`
- `ABL_NO_GAP_GUARD`
- `ABL_NO_FRICTION_GUARD`
- `ABL_NO_COHORT_GUARD`
- `ABL_NO_DATA_WEAK_GUARD`
- `ABL_FULL_TUNED`

Ablation 결과에서는 최소한 아래를 보여줘야 한다.
- mean / median realized excess return delta
- timing edge delta
- execution rate delta
- skip saved loss delta
- missed winner delta
- regime bucket 별 delta

### 3.8 Policy recommendation, freeze, rollback
다음이 가능한 상태를 만든다.

- calibration 결과를 바탕으로 **추천 정책** 을 생성할 수 있어야 한다.
- 추천 정책을 사람이 명시적으로 active 로 승격(freeze/promote)할 수 있어야 한다.
- 문제가 생기면 직전 policy 로 rollback 할 수 있어야 한다.

최소 명령 흐름:
1. calibration / ablation run 수행
2. 추천 policy materialization
3. 사람이 추천 결과 검토
4. `freeze_intraday_active_policy.py` 로 active 반영
5. 필요 시 `rollback_intraday_active_policy.py` 로 이전 버전 복귀

중요 제약:
- auto promote 금지
- 동시에 겹치는 active scope 가 둘 생기지 않도록 방지
- rollback 시 source policy id 와 reason note 저장

### 3.9 Research report / UI / Ops 반영
다음이 가능한 상태를 만든다.

#### Research / Policy Lab 페이지
최소 표시 항목:
- 최근 experiment run 목록
- horizon / checkpoint / regime scope 필터
- objective leaderboard
- candidate grid 요약
- selected vs runner-up 비교
- ablation chart/table
- walk-forward window 별 성과
- fallback chain 발생률
- manual review required 항목

#### Intraday Console
최소 표시 항목:
- 현재 active policy id
- 현재 session 에 적용된 scope / fallback source
- raw action / adjusted action / tuned action 비교
- 적용된 threshold / guard summary
- data quality / regime family / policy reason code

#### Evaluation 페이지
최소 표시 항목:
- policy version 별 strategy comparison
- freeze 전/후 비교
- horizon / regime cluster 별 성과 히트맵
- ablation 결과 요약
- tuning edge vs open 추세

#### Ops 페이지
최소 표시 항목:
- latest experiment status
- candidate generation count
- split coverage
- artifact/report path
- active policy registry
- rollback history
- stale policy warning

### 3.10 Report renderer / optional Discord summary
다음이 가능한 상태를 만든다.

- calibration / ablation / 추천 결과를 요약한 HTML 리포트를 생성할 수 있어야 한다.
- 필요하면 Discord 로 짧은 summary 를 dry-run 또는 publish 할 수 있어야 한다.

Discord 요약에는 아래 정도만 포함한다.
- experiment id
- selected candidate id
- objective score
- baseline 대비 핵심 개선/악화
- manual review 필요 여부
- report link/path 또는 요약 위치

뉴스 본문 전문, 대량 표, 과도한 raw data dump 는 보내지 않는다.

---

## 4. 정책 후보 설계 가이드

### 4.1 기본 정책 템플릿
Codex는 최소한 아래 템플릿을 기본 후보군에 포함해야 한다.

1. `BASE_DEFAULT`
   - TICKET-008 기본 deterministic profile 과 거의 동일
2. `DEFENSIVE_LIGHT`
   - enter threshold 소폭 상향
   - friction penalty 강화
3. `DEFENSIVE_STRONG`
   - enter threshold 강한 상향
   - data weak / shock guard 강화
4. `RISK_ON_LIGHT`
   - strong trend regime 에서만 selection confidence 상위권에 threshold 소폭 완화
5. `GAP_GUARD_STRICT`
   - 고갭 추격 강한 제한
6. `FRICTION_GUARD_STRICT`
   - spread / imbalance / execution strength 미달 시 진입 강하게 억제
7. `COHORT_GUARD_STRICT`
   - 후보군 전반이 약하면 enter 억제
8. `FULL_BALANCED`
   - 위 guard 를 균형형으로 조합한 기본 튜닝 후보

### 4.2 추천 objective 해석
정책 추천은 “무조건 가장 수익률 높은 후보”를 고르는 것이 아니다.

추천 후보는 보통 아래 특성을 만족해야 한다.
- baseline 대비 edge 가 개선됨
- execution rate 가 완전히 붕괴하지 않음
- tail risk proxy 가 심하게 나빠지지 않음
- regime 별 성과가 한두 버킷에만 몰리지 않음
- 최근 window 와 직전 window 사이의 급격한 성과 붕괴가 적음

### 4.3 overfitting 방지 장치
반드시 아래 장치를 둔다.

- search space 크기 상한
- low sample penalty
- execution collapse penalty
- walk-forward 검증
- family 표본 부족 시 cluster/global fallback
- recommendation 은 하되 auto promotion 금지
- report 에 runner-up 과 차이를 함께 표시

---

## 5. 스크립트 요구사항

아래 스크립트 또는 동등한 역할의 엔트리포인트를 반드시 만든다.

### 5.1 Search space / candidate generation
- `scripts/materialize_intraday_policy_candidates.py`
  - 역할: search space version, template set, scope 설정을 바탕으로 정책 후보 생성

### 5.2 Calibration / evaluation
- `scripts/run_intraday_policy_calibration.py`
  - 역할: 특정 기간, horizon, checkpoint, scope 에 대해 candidate 평가 실행
- `scripts/run_intraday_policy_walkforward.py`
  - 역할: anchored / rolling walk-forward split 반복 실행

### 5.3 Ablation
- `scripts/evaluate_intraday_policy_ablation.py`
  - 역할: full tuned policy 대비 특정 구성요소 제거/변형 실험 수행

### 5.4 Recommendation / freeze / rollback
- `scripts/materialize_intraday_policy_recommendations.py`
  - 역할: calibration/walk-forward 결과 기반 추천 후보 산출
- `scripts/freeze_intraday_active_policy.py`
  - 역할: 추천 또는 특정 candidate 를 active policy 로 동결
- `scripts/rollback_intraday_active_policy.py`
  - 역할: 이전 active policy 로 복귀

### 5.5 Report / validation
- `scripts/render_intraday_policy_research_report.py`
  - 역할: calibration + ablation + recommendation 요약 리포트 생성
- `scripts/publish_discord_intraday_policy_summary.py`
  - 역할: 요약본 Discord dry-run/publish
- `scripts/validate_intraday_policy_framework.py`
  - 역할: 후보 생성 / evaluation / recommendation / active registry 의 정합성 검사

---

## 6. 최소 CLI 예시

아래 명령이 돌아가는 상태를 목표로 한다.

1. 후보 생성
```bash
python scripts/materialize_intraday_policy_candidates.py \
  --search-space-version pcal_v1 \
  --horizons 1 5 \
  --checkpoints 09:05 09:15 09:30 10:00 11:00 \
  --scopes GLOBAL HORIZON HORIZON_CHECKPOINT HORIZON_REGIME_CLUSTER
```

2. calibration run
```bash
python scripts/run_intraday_policy_calibration.py \
  --start-session-date 2026-01-05 \
  --end-session-date 2026-03-20 \
  --horizons 1 5 \
  --checkpoints 09:05 09:15 09:30 10:00 11:00 \
  --objective-version ip_obj_v1 \
  --split-version wf_40_10_10_step5 \
  --search-space-version pcal_v1
```

3. walk-forward run
```bash
python scripts/run_intraday_policy_walkforward.py \
  --start-session-date 2026-01-05 \
  --end-session-date 2026-03-20 \
  --mode rolling \
  --train-sessions 40 \
  --validation-sessions 10 \
  --test-sessions 10 \
  --step-sessions 5 \
  --horizons 1 5
```

4. ablation
```bash
python scripts/evaluate_intraday_policy_ablation.py \
  --start-session-date 2026-01-05 \
  --end-session-date 2026-03-20 \
  --horizons 1 5 \
  --base-policy-source latest_recommendation
```

5. recommendation 생성
```bash
python scripts/materialize_intraday_policy_recommendations.py \
  --as-of-date 2026-03-20 \
  --horizons 1 5 \
  --minimum-test-sessions 10
```

6. active freeze
```bash
python scripts/freeze_intraday_active_policy.py \
  --as-of-date 2026-03-20 \
  --promotion-type MANUAL_FREEZE \
  --source latest_recommendation \
  --note "Promote after review"
```

7. rollback
```bash
python scripts/rollback_intraday_active_policy.py \
  --as-of-date 2026-03-24 \
  --horizons 1 5 \
  --note "Rollback due to weak execution stability"
```

8. research report
```bash
python scripts/render_intraday_policy_research_report.py \
  --as-of-date 2026-03-20 \
  --horizons 1 5 \
  --dry-run
```

9. Discord dry-run
```bash
python scripts/publish_discord_intraday_policy_summary.py \
  --as-of-date 2026-03-20 \
  --horizons 1 5 \
  --dry-run
```

10. validation
```bash
python scripts/validate_intraday_policy_framework.py \
  --as-of-date 2026-03-20 \
  --horizons 1 5
```

11. UI
```bash
streamlit run app/ui/Home.py
```

---

## 7. 기본 평가 척도 요구사항

Codex는 최소한 아래 지표를 계산하고 저장해야 한다.

### 7.1 성과 지표
- `mean_realized_excess_return`
- `median_realized_excess_return`
- `hit_rate`
- `mean_timing_edge_vs_open`
- `positive_timing_edge_rate`

### 7.2 execution / coverage 지표
- `executed_count`
- `execution_rate`
- `session_coverage_rate`
- `candidate_coverage_rate`

### 7.3 손실 회피 / 기회 손실 지표
- `skip_saved_loss_rate`
- `skip_saved_loss_mean`
- `missed_winner_rate`
- `missed_winner_mean`

### 7.4 안정성 / 리스크 proxy
- `left_tail_proxy`
- `execution_collapse_penalty`
- `low_sample_penalty`
- `stability_score`
- `regime_dispersion_score`
- `recent_window_decay_score`

### 7.5 monotonicity / action ordering 진단
가능하면 아래를 지원한다.
- `ENTER_NOW` 가 `WAIT_RECHECK` 보다 장기적으로 우월한지
- `WAIT_RECHECK` 가 `AVOID_TODAY` 보다 우월한지
- tuned action ordering 이 raw/adjusted ordering 보다 개선되었는지

---

## 8. UI 요구사항 세부

### 8.1 Research Lab 페이지
최소 섹션:
- latest experiments
- active recommendations
- objective leaderboard
- split summary
- ablation summary
- regime family/cluster heatmap
- promotion candidates requiring review

### 8.2 Intraday Console
최소 섹션:
- current active policy card
- scope resolution trace
- fallback trace
- raw vs adjusted vs tuned action table
- threshold summary
- policy reason code summary

### 8.3 Evaluation 페이지
최소 섹션:
- policy version comparison
- pre-freeze vs post-freeze performance
- walk-forward window chart/table
- regime cluster comparison
- missed winner / skip saved loss diagnostics

### 8.4 Ops 페이지
최소 섹션:
- experiment run health
- candidate generation health
- stale policy warning
- promotion/rollback registry
- artifact/report existence checks

---

## 9. 테스트 요구사항

최소한 아래 테스트를 작성한다.

1. search space candidate 중복 제거 테스트
2. parameter hash 안정성 테스트
3. matured-only filter 테스트
4. walk-forward split 누수 방지 테스트
5. regime family 부족 시 cluster/global fallback 테스트
6. objective score 계산 테스트
7. execution collapse penalty 테스트
8. recommendation 생성 시 manual review flag 테스트
9. active freeze 시 overlapping scope 방지 테스트
10. rollback 시 이전 active 복원 테스트
11. ablation metric delta 계산 테스트
12. UI helper/service 레벨 기본 smoke test

---

## 10. 이번 티켓에서 하지 말아야 할 것

- 자동매매 / 주문 API 연동
- 실시간 온라인 튜닝
- reinforcement learning / bandit / 정책 gradient
- intraday policy 자체의 ML 분류기 도입
- 전 종목 장중 전수저장
- 미성숙 outcome 을 섞은 즉시 policy 교체
- auto promotion
- 뉴스 본문 전문 저장/전송
- 과도한 리팩터링으로 기존 selection/evaluation/intraday 파이프라인 깨기

---

## 11. 완료 기준 (Definition of Done)

아래를 만족하면 이번 티켓 완료로 본다.

1. 정책 후보 search space 를 생성하고 중복 없이 저장할 수 있다.
2. matured outcome 만 사용한 calibration run 이 가능하다.
3. rolling/anchored walk-forward 결과가 저장된다.
4. regime-family / cluster / global scope 별 추천 후보를 생성할 수 있다.
5. fallback chain 이 로그와 리포트에 드러난다.
6. ablation 결과로 주요 guard 의 기여를 비교할 수 있다.
7. active policy 를 manual freeze / rollback 할 수 있다.
8. Streamlit 에서 active policy, recommendation, ablation, walk-forward 결과를 볼 수 있다.
9. Discord dry-run 요약이 동작한다.
10. validation script 가 정합성 문제를 잡아낸다.
11. README 에 calibration / promotion / rollback 흐름이 반영된다.

---

## 12. README 에 반드시 추가할 것

- intraday policy calibration 목적
- matured-only tuning 원칙
- same-exit comparison 원칙
- search space / template 개념
- objective function 개요
- walk-forward split 규칙
- regime family / regime cluster fallback 구조
- recommendation / freeze / rollback 흐름
- active policy registry 사용법
- current known limitations

---

## 13. Codex 작업 후 반드시 남겨야 하는 요약

작업 완료 후 Codex는 아래를 간단히 정리해야 한다.

- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- 정책 파라미터 스키마 요약
- search space template / 범위 요약
- objective function 구성요소 요약
- walk-forward split 방식 요약
- regime fallback 규칙 요약
- 추천/동결/롤백 흐름 요약
- UI 에서 확인할 위치
- known limitations
- TICKET-010 진입 전 주의사항

---

## 14. 다음 티켓으로의 연결 의도

이번 티켓이 끝나면 다음 단계에서는 아래 중 하나로 자연스럽게 이어질 수 있어야 한다.

1. **TICKET-010 — Intraday Policy Meta-Model / ML Timing Classifier v1**
   - 규칙형 tuned policy 를 baseline 으로 유지하면서, 작은 분류기/메타모델이 `ENTER_NOW`, `WAIT_RECHECK`, `AVOID_TODAY` 확률을 보조 산출하는 단계

2. **TICKET-010 대안 — Portfolio Construction / Capacity / Position Sizing Layer**
   - selection v2 + tuned intraday timing 을 포트폴리오 단위에서 정렬·제한·집행 우선순위화 하는 단계

이번 문서는 첫 번째 방향, 즉 **intraday policy 자체의 ML/meta-model 단계로 넘어가기 직전의 연구 인프라** 를 만드는 데 초점을 둔다.
