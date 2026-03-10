# TICKET-008 — 장중 타이밍 레이어 사후평가 고도화 + Selection v2 결합 전략 비교 + Regime-aware Intraday Adjustment

- 문서 목적: TICKET-007 이후, Codex가 바로 이어서 구현할 **장중 타이밍 레이어의 사후평가 고도화 + selection engine v2 와의 결합 전략 비교 + regime-aware intraday adjustment** 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
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
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
  - `CODEX_EIGHTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
  - TICKET-006 ML alpha model v1 / uncertainty-disagreement / selection engine v2 완료
  - TICKET-007 intraday candidate assist engine v1 완료
- 우선순위: 최상
- 기대 결과: **selection engine v2 기반 후보군을 대상으로 장중 진입 타이밍 레이어가 실제로 얼마나 개선을 만들었는지 평가하고, 시장 장세(regime)에 따라 장중 판단 기준을 동적으로 조정하며, baseline open 진입 / timing v1 / regime-aware timing 을 같은 프레임에서 비교할 수 있는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“장중 타이밍 레이어가 보기엔 그럴듯한가”를 넘어서, 실제로 selection v2 대비 성과 개선을 냈는지, 어떤 장세에서 도움이 되고 어떤 장세에서 망가지는지까지 남기는 첫 장중 검증/정책 티켓**이다.

즉, 이번 티켓의 목표는 아래 여덟 가지를 안정적으로 만드는 것이다.

1. 장중 checkpoint 시점의 **시장 컨텍스트(market context)** 를 구조화된 snapshot 으로 적재한다.
2. 시장 컨텍스트와 기존 장후 regime snapshot 을 결합해 **intraday regime family** 를 만든다.
3. 기존 timing layer v1 의 raw action 을 시장 장세에 따라 보수/중립/공격적으로 조정하는 **regime-aware adjustment layer** 를 구현한다.
4. raw decision 과 adjusted decision 을 모두 저장하고, 둘을 명시적으로 비교할 수 있게 한다.
5. selection v2 의 open baseline 과 timing layer 의 여러 실행 정책을 같은 기준으로 **전략 비교(strategy comparison)** 한다.
6. 전략 비교는 종목 단위뿐 아니라 일별 cohort, rolling window, regime bucket 기준으로도 가능해야 한다.
7. 장 마감 후 **intraday postmortem / strategy comparison report** 를 생성하고, 선택적으로 Discord에 전송할 수 있어야 한다.
8. UI / Ops 에서 장중 시장 상태, 조정 이유, 전략별 성과 차이, 현재 한계를 사람이 이해 가능하게 보여준다.

이 티켓이 끝나면 다음 티켓에서는 intraday timing layer 자체의 소형 ML화, regime-aware policy calibration 자동화, 미국/나스닥 확장, 포트폴리오 구성 실험 같은 방향으로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 selection engine v2 가 여전히 “모체”다
이번 티켓에서도 종목 선택의 주체는 selection engine v2 다.

장중 정책은 아래 원칙을 절대 넘어서면 안 된다.

- 장후 selection v2 를 무시하고 장중만으로 새 종목을 발굴하지 않는다.
- 장중 정책이 selection v2 와 완전히 반대 방향의 공격적 의사결정을 독자적으로 내리지 않는다.
- 장중 정책은 **언제 들어갈지 / 오늘은 보류할지 / 어떤 장세에서는 더 보수화할지**를 정하는 보조 계층이다.

### 2.2 모든 비교는 “동일한 exit 정의” 위에서 해야 한다
selection v2 의 baseline 과 intraday timing 전략을 비교할 때, exit 기준이 달라지면 비교가 무의미해진다.

반드시 아래를 지킨다.

- open baseline 은 기존 `fact_selection_outcome` 또는 동등한 matured outcome 과 같은 exit 정의를 사용한다.
- intraday entry 전략도 **같은 `future_close_date` / 같은 horizon 종료일** 을 사용한다.
- 즉, 차이는 entry 시점만 있어야 하고 exit 는 동일해야 한다.
- 임의로 exit를 재계산하거나 다른 holding period를 섞지 않는다.

### 2.3 raw action 과 adjusted action 을 혼동하지 않는다
이번 티켓의 핵심은 기존 TICKET-007의 timing layer v1 을 무효화하는 것이 아니다.

반드시 분리해 저장한다.

- `raw_action`: timing layer v1 이 원래 계산한 판단
- `adjusted_action`: regime-aware policy 가 조정한 판단
- `raw_timing_score`
- `adjusted_timing_score`
- `adjustment_profile`
- `adjustment_reason_codes`

UI와 리포트에서도 raw와 adjusted를 동시에 보여줘야 한다.

### 2.4 regime-aware adjustment 는 우선 rule-based / deterministic 해야 한다
이번 티켓에서는 intraday policy 를 ML이나 RL로 만들지 않는다.

허용되는 것:
- 규칙 기반 threshold shift
- penalty / bonus 재가중
- 시장 상태에 따른 enter / wait / avoid 기준 조정
- selection confidence 와 intraday signal quality 를 함께 보는 deterministic policy

이번 티켓에서 하지 않는 것:
- reinforcement learning
- online learning
- 실시간 self-updating policy
- UI 접속 시점의 자동 재학습

### 2.5 no-entry도 “결과”다
장중 timing layer 는 어떤 경우엔 진입을 막는 것이 목적일 수 있다.

따라서 평가에서는 다음이 모두 중요하다.

- 진입해서 얼마나 벌었는가
- 진입을 미뤄서 baseline open 대비 얼마나 개선됐는가
- 진입을 건너뛰어서 손실을 얼마나 회피했는가
- 너무 과보수적으로 굴어서 좋은 기회를 얼마나 놓쳤는가

즉, 실행 전략의 평가는 **진입 성과 + 손실 회피 + 기회손실**을 함께 본다.

### 2.6 저장 예산을 지키는 candidate-only 철학 유지
이번 티켓도 TICKET-007의 저장 원칙을 유지한다.

- 후보군 외 전 종목 장중 저장 금지
- raw websocket dump 장기보관 금지
- 장기 보관 대상은 context snapshot / adjusted decision / outcome / strategy comparison 중심
- 결과를 재현할 수 있는 요약 정보만 남긴다

---

## 3. 이번 티켓에서 반드시 끝내야 하는 것

### 3.1 Intraday market context snapshot
다음이 가능한 상태를 만든다.

- 각 `session_date`, `checkpoint` 마다 장중 시장 컨텍스트를 계산하고 저장할 수 있어야 한다.
- 컨텍스트는 최소한 아래 두 범위를 포함한다.
  1. **시장 전체 레벨(context-wide)**
  2. **후보군 레벨(candidate-cohort)**

최소 필드 예시는 다음과 같다.

- `session_date`
- `checkpoint_time`
- `market_session_state`
- `kospi_return_from_open`
- `kosdaq_return_from_open`
- `market_breadth_ratio` (상승종목 / 하락종목 비율 또는 동등 지표)
- `advancers_count`
- `decliners_count`
- `candidate_mean_return_from_open`
- `candidate_median_return_from_open`
- `candidate_hit_ratio_from_open`
- `candidate_mean_relative_volume`
- `candidate_mean_spread_bps`
- `candidate_mean_execution_strength`
- `candidate_mean_orderbook_imbalance`
- `market_shock_proxy`
- `intraday_volatility_proxy`
- `dispersion_proxy`
- `data_quality_flag`
- `context_reason_codes_json`

데이터가 충분치 않은 필드는 null 허용 가능하지만, null이어도 `data_quality_flag` 와 reason code 로 드러나야 한다.

### 3.2 Intraday regime family classification
다음이 가능한 상태를 만든다.

- 장후에 계산된 `fact_market_regime_snapshot` 또는 동등 테이블과 장중 market context snapshot 을 결합해 **장중 regime family** 를 생성할 수 있어야 한다.
- 최소한 아래 정도의 regime family 를 deterministic 하게 지원한다.
  - `PANIC_OPEN`
  - `WEAK_RISK_OFF`
  - `NEUTRAL_CHOP`
  - `HEALTHY_TREND`
  - `OVERHEATED_GAP_CHASE`
  - `DATA_WEAK`

regime family 는 아래 신호를 조합해 분류한다.

- 지수의 시가 대비 움직임
- breadth 악화/개선
- candidate cohort 의 동반 약세/강세
- intraday spread / friction 악화 여부
- gap 과 즉시 되밀림 여부
- execution strength / orderbook imbalance 의 질
- 기존 장후 regime snapshot (`panic`, `defensive`, `neutral`, `optimistic` 등)

분류는 완벽한 경제학적 진실이어야 하는 것이 아니라, **운영상 정책을 바꾸는 데 충분히 일관된 deterministic rule set** 이어야 한다.

### 3.3 Regime-aware adjustment profile
다음이 가능한 상태를 만든다.

- regime family 에 따라 timing layer raw score/action 을 조정하는 **adjustment profile** 을 가져야 한다.
- 최소한 아래 profile 을 지원한다.
  - `DEFENSIVE`
  - `NEUTRAL`
  - `SELECTIVE_RISK_ON`
  - `GAP_CHASE_GUARD`
  - `DATA_WEAK_GUARD`

profile 별로 아래를 조정할 수 있어야 한다.

- enter threshold
- wait-to-enter threshold
- avoid threshold
- uncertainty penalty weight
- spread / friction penalty weight
- gap chase penalty
- selection-confidence minimum gate
- signal-quality minimum gate

예시 원칙:

- `PANIC_OPEN`, `WEAK_RISK_OFF` 에서는 `DEFENSIVE`
  - enter threshold 상향
  - avoid 판정 강화
  - spread penalty 강화
  - signal quality 낮으면 거의 진입 금지
- `HEALTHY_TREND` 에서는 `SELECTIVE_RISK_ON`
  - 단, selection confidence 상위 후보에 한해 threshold 소폭 완화
  - 고갭/과열은 여전히 제한
- `OVERHEATED_GAP_CHASE` 에서는 `GAP_CHASE_GUARD`
  - high gap-up chasing 을 강하게 억제
- `DATA_WEAK` 에서는 `DATA_WEAK_GUARD`
  - raw signal 가 좋아 보여도 data quality 부족 시 쉽게 ENTER_NOW 를 주지 않음

### 3.4 Adjusted decision materialization
다음이 가능한 상태를 만든다.

- TICKET-007에서 계산한 raw entry decision 을 읽어 **adjusted decision** 을 materialize 할 수 있어야 한다.
- 동일 `(session_date, checkpoint, symbol, horizon)` 에 대해 아래를 저장한다.

최소 필드 예시:

- `session_date`
- `selection_date`
- `checkpoint_time`
- `symbol`
- `horizon`
- `selection_engine_version`
- `intraday_engine_version`
- `regime_profile_version`
- `market_regime_family`
- `adjustment_profile`
- `raw_timing_score`
- `adjusted_timing_score`
- `raw_action`
- `adjusted_action`
- `selection_confidence_bucket`
- `signal_quality_flag`
- `regime_adjustment_delta`
- `risk_penalty_delta`
- `gap_penalty_delta`
- `data_quality_penalty_delta`
- `adjustment_reason_codes_json`
- `decision_notes_json`

중요:
- raw action 이 `DATA_INSUFFICIENT` 인 경우 adjusted action 은 더 공격적으로 바뀌면 안 된다.
- raw action 이 `AVOID_TODAY` 인데 adjusted action 이 `ENTER_NOW` 로 역전되려면 매우 제한된 명시 규칙이 있어야 하며, 기본 구현에서는 허용하지 않는 편을 권장한다.
- 즉, adjustment 는 **주로 더 보수화하거나, 제한적으로 WAIT↔ENTER_NOW 를 조정**하는 쪽이어야 한다.

### 3.5 Matured intraday decision outcome
다음이 가능한 상태를 만든다.

- raw/adjusted intraday decision 이 실제로 어떤 결과를 냈는지 만기 도달 후 계산할 수 있어야 한다.
- outcome 계산 시 entry price 는 deterministic 해야 한다.

권장 우선순위:
1. checkpoint 에 해당하는 1분봉 `close`
2. checkpoint 에 해당하는 1분봉 `vwap`
3. 불가 시 null + `outcome_data_insufficient`

exit 기준은 반드시 selection baseline 과 동일하게 잡는다.

- `future_close_date` 는 기존 label/outcome 기준을 재사용한다.
- D+1 은 session_date 당일 종가
- D+5 는 selection horizon 기준 동일 종료일 종가

최소 필드 예시:

- `strategy_id`
- `selection_date`
- `session_date`
- `symbol`
- `horizon`
- `entry_checkpoint_time`
- `entry_timestamp`
- `entry_price`
- `exit_date`
- `exit_price`
- `executed_flag`
- `skip_reason_code`
- `realized_return`
- `realized_excess_return`
- `timing_edge_vs_open_bps`
- `skip_saved_loss_flag`
- `missed_positive_flag`
- `outcome_status`

### 3.6 Combined strategy comparison layer
다음이 가능한 상태를 만든다.

- selection v2 와 intraday timing 을 결합한 여러 실행 전략을 같은 프레임에서 비교할 수 있어야 한다.
- 최소한 아래 전략은 지원해야 한다.

#### 3.6.1 Baseline 전략
- `SEL_V2_OPEN_ALL`
  - selection v2 상위 후보를 다음 거래일 시가에 진입
  - 기존 selection outcome 과 같은 의미

#### 3.6.2 Timing raw 전략
- `SEL_V2_TIMING_RAW_FIRST_ENTER`
  - checkpoint 순서대로 최초 `raw_action == ENTER_NOW` 인 시점에 진입
  - cutoff 시각까지 ENTER_NOW 가 없으면 no-entry

#### 3.6.3 Timing adjusted 전략
- `SEL_V2_TIMING_ADJ_FIRST_ENTER`
  - checkpoint 순서대로 최초 `adjusted_action == ENTER_NOW` 인 시점에 진입
  - cutoff 시각까지 ENTER_NOW 가 없으면 no-entry

#### 3.6.4 Optional 고정 checkpoint 전략
가능하면 아래도 지원한다.
- `SEL_V2_TIMING_ADJ_0930_ONLY`
- `SEL_V2_TIMING_ADJ_1000_ONLY`

전략 비교는 최소한 아래 질문에 답할 수 있어야 한다.

- open baseline 대비 timing layer 가 평균적으로 개선을 냈는가
- raw timing 보다 adjusted timing 이 나은가
- 어떤 regime 에서 adjusted timing 이 특히 유효한가
- no-entry 증가가 손실 회피인지, 과보수적 기회손실인지
- horizon 1 과 5 에서 결과가 다르게 나오는가

### 3.7 Calibration / diagnostic for intraday timing
다음이 가능한 상태를 만든다.

- adjusted timing layer 가 진짜 보수화/완화 효과를 냈는지 진단할 수 있어야 한다.
- 최소한 아래를 계산한다.

#### 3.7.1 Action monotonicity
- `ENTER_NOW` 후보의 결과가 `WAIT_RECHECK` 보다 일반적으로 더 낫는가
- `AVOID_TODAY` 로 분류된 후보의 open baseline 성과가 실제로 나빴는가

#### 3.7.2 Timing edge distribution
- strategy 별 `timing_edge_vs_open_bps` 분포
- mean / median / p25 / p75
- positive edge rate

#### 3.7.3 Skip diagnostic
- no-entry 비율
- skipped negative rate (`skip_saved_loss_flag`)
- skipped winner rate (`missed_positive_flag`)
- skip precision / skip regret 에 준하는 해석 지표

#### 3.7.4 Regime x action matrix
- regime family 별 action 분포
- regime family 별 executed hit rate
- regime family 별 average timing edge
- defensive profile 이 실제로 방어 효과를 냈는지

#### 3.7.5 Selection-confidence interaction
- selection confidence bucket 상위 종목에서만 timing layer 가 유효한지
- low-confidence 후보에선 no-entry 증가가 자연스러운지

### 3.8 Intraday postmortem / strategy comparison report
다음이 가능한 상태를 만든다.

- 장 마감 후 intraday timing layer 의 결과를 요약한 **postmortem report** 를 렌더링할 수 있어야 한다.
- 리포트는 최소한 아래 내용을 포함한다.
  1. 평가된 session / horizon 범위
  2. market regime summary
  3. raw vs adjusted action 분포
  4. strategy comparison summary
  5. timing edge 요약
  6. skip saved loss / missed winner 요약
  7. 주요 성공 사례 / 주요 실패 사례
  8. 개선 포인트 / 운영 메모

리포트 형식은 최소한 아래 중 하나를 지원한다.
- markdown
- JSON + markdown preview

가능하면 Discord용 축약본도 만든다.

### 3.9 Discord postmortem publish
다음이 가능한 상태를 만든다.

- TICKET-004 / TICKET-005 의 Discord publisher 패턴을 재사용해 intraday postmortem 요약을 선택적으로 전송할 수 있어야 한다.
- 필수 조건:
  - `.env` 기반 on/off
  - `dry-run` 지원
  - publish 실패가 파이프라인 전체 실패로 이어지지 않음
  - payload preview path 또는 saved markdown path 기록

### 3.10 UI / Ops 강화
다음이 가능해야 한다.

#### 3.10.1 Intraday Console
- raw action / adjusted action / regime family 동시 표시
- market context summary card
- symbol-level decision trace
- checkpoint별 action transition 표시

#### 3.10.2 Evaluation
- intraday strategy comparison tab 추가
- strategy별 성과 비교
- regime별 성과 비교
- timing edge distribution
- skip diagnostic

#### 3.10.3 Stock Workbench
- 특정 종목의 selection v2 snapshot
- intraday raw/adjusted action history
- 실제 outcome 과 timing edge

#### 3.10.4 Ops
- 마지막 market context run
- 마지막 regime adjustment run
- 마지막 strategy comparison evaluation run
- Discord publish success/fail 상태
- data quality / coverage 경고

### 3.11 스크립트 / 엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/materialize_intraday_market_context_snapshots.py`
- `scripts/materialize_intraday_regime_adjustments.py`
- `scripts/materialize_intraday_adjusted_entry_decisions.py`
- `scripts/materialize_intraday_decision_outcomes.py`
- `scripts/evaluate_intraday_strategy_comparison.py`
- `scripts/materialize_intraday_timing_calibration.py`
- `scripts/render_intraday_postmortem_report.py`
- `scripts/publish_discord_intraday_postmortem.py`
- `scripts/validate_intraday_strategy_pipeline.py`

기존 TICKET-007 스크립트를 깨지 않도록 하고, 필요한 metadata 를 확장하는 수준에서 연동한다.

---

## 4. 이번 티켓의 범위와 비범위

### 4.1 이번 티켓의 범위
- intraday market context snapshot
- deterministic intraday regime family classification
- regime-aware adjustment profile
- raw vs adjusted decision materialization
- intraday matured outcome 계산
- selection v2 open baseline 대비 strategy comparison
- intraday timing calibration / diagnostic
- intraday postmortem report / optional Discord publish
- UI / Ops 확장
- README / 실행 가이드 갱신
- 테스트 작성

### 4.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- 자동매매 / 주문 API 연동
- intraday policy ML / RL / online learning
- 전 종목 장중 전수 실험
- 포트폴리오 최적화 / 자금배분 엔진
- 실시간 체결 엔진
- 미국주식 / 나스닥 확장
- 종목 발굴 자체를 intraday 쪽으로 이전
- 대용량 raw tick / full orderbook dump 장기 저장

---

## 5. 데이터 계약 및 저장 구조

### 5.1 필수 테이블 / 뷰
아래 이름을 권장하며, 저장소 기존 naming convention 이 있다면 동등한 이름으로 구현해도 된다. 다만 의미는 동일해야 한다.

#### 5.1.1 `fact_intraday_market_context_snapshot`
키 후보:
- `session_date`
- `checkpoint_time`
- `context_scope = market`

주요 필드:
- 시장 전체 요약
- 후보군 코호트 요약
- data quality
- context reason codes
- source/provider metadata

#### 5.1.2 `fact_intraday_regime_adjustment`
키 후보:
- `session_date`
- `checkpoint_time`
- `symbol`
- `horizon`

주요 필드:
- `market_regime_family`
- `adjustment_profile`
- raw/adjusted score
- deltas
- selection confidence bucket
- signal quality
- adjustment reasons

#### 5.1.3 `fact_intraday_adjusted_entry_decision`
키 후보:
- `session_date`
- `checkpoint_time`
- `symbol`
- `horizon`

주요 필드:
- `raw_action`
- `adjusted_action`
- raw/adjusted score
- final gating status
- eligible_to_execute_flag
- notes / reason codes

#### 5.1.4 `fact_intraday_strategy_result`
키 후보:
- `selection_date`
- `session_date`
- `symbol`
- `horizon`
- `strategy_id`

주요 필드:
- entry rule
- executed_flag
- entry time / price
- exit date / price
- realized return / excess return
- timing edge vs open
- skip diagnostics
- outcome status

#### 5.1.5 `fact_intraday_strategy_comparison`
키 후보:
- `session_date`
- `horizon`
- `strategy_id`
- `comparison_scope`

주요 필드:
- executed_count
- no_entry_count
- mean/median realized excess return
- hit rate
- mean/median timing edge
- skip saved loss rate
- missed winner rate
- data coverage
- notes

#### 5.1.6 `fact_intraday_timing_calibration`
키 후보:
- `as_of_window_end`
- `horizon`
- `grouping_key`

주요 필드:
- action monotonicity metrics
- regime bucket metrics
- confidence bucket metrics
- coverage / precision / regret summary

#### 5.1.7 권장 뷰
- `vw_intraday_strategy_daily_summary`
- `vw_intraday_regime_action_summary`
- `vw_intraday_symbol_decision_trace`
- `vw_intraday_skip_diagnostic_summary`

### 5.2 selection baseline 과의 연결
intraday 결과는 selection baseline 과 반드시 연결 가능해야 한다.

- `selection_date`
- `session_date`
- `symbol`
- `horizon`
- `selection_engine_version`
- `selection_value_v2`
- `selection_grade_v2`

가능하면 open baseline 결과 row 와 직접 join 가능한 key 를 남긴다.

### 5.3 run manifest 연결
각 materialization / evaluation run 은 기존 run manifest 체계와 연결되어야 한다.

최소 남겨야 하는 것:
- `run_id`
- `script_name`
- `run_started_at`
- `run_finished_at`
- `session_date`
- `checkpoint scope`
- `row_count`
- `warning_count`
- `source manifest / upstream run ids`

---

## 6. Regime-aware adjustment 세부 규칙

### 6.1 기본 checkpoint
기본 checkpoint 는 TICKET-007과 동일하게 아래를 유지한다.

- `09:05`
- `09:15`
- `09:30`
- `10:00`
- `11:00`

필요 시 향후 확장 가능하지만, 이번 티켓에서는 위 checkpoint 가 기본이다.

### 6.2 market regime family 예시 규칙
정확한 수치는 config 화하되, 아래 성격은 유지한다.

#### 6.2.1 `PANIC_OPEN`
예시 조건:
- 지수 하락폭이 크고
- breadth 가 급격히 나쁘며
- candidate cohort 도 동반 약세이고
- spread / friction 이 악화

정책:
- enter threshold 크게 상향
- low signal quality 는 거의 전부 `WAIT_RECHECK` 또는 `AVOID_TODAY`
- selection confidence 가 낮은 후보는 실행 금지에 가깝게 처리

#### 6.2.2 `WEAK_RISK_OFF`
예시 조건:
- 시장 전체는 급락까진 아니어도
- breadth 약세와 candidate weakness 가 동반
- execution / imbalance 지표가 미지근하거나 약함

정책:
- `DEFENSIVE`
- WAIT 비율 증가
- spread / uncertainty penalty 강화

#### 6.2.3 `NEUTRAL_CHOP`
예시 조건:
- 방향성이 약하고 혼조
- breadth 도 중립적
- candidate cohort 도 분산이 큼

정책:
- `NEUTRAL`
- raw action을 크게 뒤집지 않되, poor data quality 는 보수화

#### 6.2.4 `HEALTHY_TREND`
예시 조건:
- 시장/후보군이 동반 강세
- breadth 양호
- spread 양호
- execution / imbalance 가 추세 지속 쪽

정책:
- `SELECTIVE_RISK_ON`
- selection confidence 상위 + signal quality 양호한 후보에 한해 enter threshold 소폭 완화
- 단, 과열 갭추격은 별도 가드 적용

#### 6.2.5 `OVERHEATED_GAP_CHASE`
예시 조건:
- 후보가 큰 갭상승 후
- opening quality 가 과열이고
- VWAP 대비 chase risk 가 큼

정책:
- `GAP_CHASE_GUARD`
- raw 가 `ENTER_NOW` 여도 `WAIT_RECHECK` 로 완화 가능
- high gap penalty 부여

#### 6.2.6 `DATA_WEAK`
예시 조건:
- quote/trade summary 부족
- context coverage 낮음
- provider latency/health 저하

정책:
- `DATA_WEAK_GUARD`
- 쉽게 ENTER_NOW 를 주지 않음
- `DATA_INSUFFICIENT` 또는 `WAIT_RECHECK` 쪽으로 유도

### 6.3 조정 스코어 예시
정확한 산식은 구현체가 config 가능해야 하지만, 아래 구조를 권장한다.

```text
adjusted_timing_score
= raw_timing_score
+ regime_support_delta
- regime_risk_penalty
- data_quality_penalty
- gap_chase_penalty
- friction_penalty_delta
```

그리고 action 은 `adjusted_timing_score` 와 gate 로 결정한다.

```text
if signal_quality very weak -> DATA_INSUFFICIENT
elif adjusted_score >= enter_threshold and selection_confidence passes -> ENTER_NOW
elif adjusted_score <= avoid_threshold -> AVOID_TODAY
else -> WAIT_RECHECK
```

### 6.4 역전 규칙 제한
아래는 강하게 제한한다.

- `raw_action = AVOID_TODAY` 를 `adjusted_action = ENTER_NOW` 로 올리는 것
- `raw_action = DATA_INSUFFICIENT` 를 `adjusted_action = ENTER_NOW` 로 바꾸는 것
- selection confidence 하위 bucket 을 aggressive profile 로 승격하는 것

즉, adjustment 는 기본적으로 **정보가 나빠질수록 더 보수적** 이어야 한다.

---

## 7. 전략 비교 정의

### 7.1 공통 전제
모든 전략은 같은 candidate universe, 같은 exit date, 같은 horizon 을 사용한다.

- universe: 해당 `selection_date` 의 selection v2 상위 후보
- entry policy: 전략별로 다름
- exit policy: 동일
- 비교 단위: symbol / daily cohort / rolling window / regime bucket

### 7.2 필수 전략 정의

#### 7.2.1 `SEL_V2_OPEN_ALL`
- entry: 다음 거래일 시가
- executed_flag: 항상 true (단 데이터 부족 제외)
- 목적: 기준선

#### 7.2.2 `SEL_V2_TIMING_RAW_FIRST_ENTER`
- entry: checkpoint 순으로 최초 `raw_action = ENTER_NOW`
- cutoff: 기본 `11:00`
- executed_flag: cutoff 전 `ENTER_NOW` 있으면 true, 없으면 false

#### 7.2.3 `SEL_V2_TIMING_ADJ_FIRST_ENTER`
- entry: checkpoint 순으로 최초 `adjusted_action = ENTER_NOW`
- cutoff: 기본 `11:00`
- executed_flag: cutoff 전 `ENTER_NOW` 있으면 true, 없으면 false

### 7.3 Optional 전략
가능하면 아래도 추가한다.

- `SEL_V2_TIMING_ADJ_0930_ONLY`
- `SEL_V2_TIMING_ADJ_1000_ONLY`
- `SEL_V2_TIMING_ADJ_FORCE_OPEN_IF_TOP_CONFIDENCE`
  - 단, optional 이고 기본 범위 밖이어도 된다.

### 7.4 주요 비교 메트릭
전략 비교는 최소한 아래를 계산한다.

- `executed_count`
- `execution_rate`
- `mean_realized_excess_return`
- `median_realized_excess_return`
- `hit_rate`
- `mean_timing_edge_vs_open_bps`
- `median_timing_edge_vs_open_bps`
- `positive_timing_edge_rate`
- `skip_saved_loss_rate`
- `missed_winner_rate`
- `mean_selection_value_v2`
- `coverage_ok_rate`

---

## 8. UI / 리포트 요구사항

### 8.1 Intraday Console
최소한 아래가 보여야 한다.

- session_date / checkpoint 선택
- market regime family / adjustment profile
- raw vs adjusted action count
- candidate table
  - symbol
  - selection rank
  - selection grade
  - raw score/action
  - adjusted score/action
  - reason chips
  - signal quality
- action transition trace
- current known limitation note

### 8.2 Evaluation 페이지
최소한 아래가 보여야 한다.

- strategy comparison summary
- strategy x horizon matrix
- regime x strategy matrix
- timing edge distribution
- skip diagnostic
- raw vs adjusted action monotonicity summary

### 8.3 Stock Workbench
최소한 아래가 보여야 한다.

- 종목 단위 selection v2 snapshot
- intraday action timeline
- checkpoint별 raw/adjusted 변화
- realized outcome / edge vs open

### 8.4 Ops 페이지
최소한 아래가 보여야 한다.

- latest market context run
- latest regime adjustment run
- latest intraday outcome run
- latest strategy comparison run
- Discord publish status
- data coverage / weak data warnings

---

## 9. README / 운영 문서 요구사항

README 또는 동등 문서에는 최소한 아래를 반영한다.

- intraday market context 개념
- market regime family 정의
- raw timing vs adjusted timing 차이
- selection v2 와 intraday timing 결합 원칙
- strategy id 정의
- no-entry 해석 방법
- same-exit comparison 규칙
- skip saved loss / missed winner 의미
- candidate-only storage 원칙
- current known limitations
- dry-run / publish 예시

---

## 10. 테스트 요구사항

최소한 아래 테스트를 작성한다.

### 10.1 Unit tests
- market context aggregation rule
- regime family classifier
- adjustment profile application
- raw→adjusted action transition rule
- strategy entry resolver
- same-exit outcome calculation
- skip diagnostic flags

### 10.2 Integration tests
- fixture session date 로 market context → adjusted decision → outcome → strategy comparison 전체 흐름
- upstream selection v2 / intraday raw decision 과 join 가능 여부
- UI query layer 가 빈 데이터 / partial data 에서 깨지지 않는지

### 10.3 Regression / sanity checks
- `SEL_V2_OPEN_ALL` 결과가 기존 selection baseline 과 크게 어긋나지 않는지
- `DATA_WEAK` profile 에서 ENTER_NOW 비중이 과도하게 높지 않은지
- `PANIC_OPEN` 에서 aggressive action 남발이 없는지
- strategy comparison row count 와 candidate count 정합성

---

## 11. 완료 기준

아래 명령이 동작하거나, 동등한 엔트리포인트가 제공되어야 한다.

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

완료 판정의 핵심은 아래다.

- raw vs adjusted action 을 같은 row 기준으로 비교 가능하다.
- strategy별 실행/미실행과 realized outcome 이 계산된다.
- open baseline 대비 timing edge 를 볼 수 있다.
- regime bucket 별로 결과 차이를 볼 수 있다.
- UI / report 에서 이유와 한계가 투명하게 보인다.

---

## 12. 구현 시 주의사항

- 기존 selection / evaluation / intraday raw pipeline 을 깨지 않는다.
- raw decision 을 overwrite 하지 않는다. adjusted decision 은 별도 저장한다.
- 시장 컨텍스트가 약하거나 데이터가 빈약할 때는 과감히 보수적으로 처리한다.
- no-entry 를 실패로만 취급하지 않는다.
- 보고서에는 “더 잘 맞아보이는 숫자”가 아니라 실제 데이터가 보여주는 것을 우선한다.
- 과도한 최적화나 지나친 parameter 탐색을 이번 티켓의 핵심 목표로 두지 않는다.
- 모든 결과는 **재현 가능성**과 **운영상 해석 가능성**을 우선한다.

---

## 13. 작업 후 Codex가 정리해줘야 할 것

작업 후 아래를 간단히 정리해 남겨야 한다.

- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 뷰 목록
- market context snapshot 계산 방식 요약
- regime family 정의 요약
- adjustment profile 및 action transition 규칙 요약
- strategy id 와 각 전략의 entry rule 요약
- same-exit outcome 계산 규칙 요약
- strategy comparison 주요 메트릭 요약
- UI에서 확인할 위치
- known limitations
- TICKET-009 진입 전 주의사항

