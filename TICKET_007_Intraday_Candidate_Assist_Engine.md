# TICKET-007 — 장중 후보군 보조 엔진 v1 + 1분봉/체결강도/호가 요약 기반 진입 타이밍 레이어

- 문서 목적: TICKET-006 이후, Codex가 바로 이어서 구현할 **장중 후보군 보조 엔진 v1 + 1분봉/체결강도/호가 요약 기반 진입 타이밍 레이어** 의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
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
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SIXTH_INSTRUCTION_StockMaster.md`
  - `CODEX_SEVENTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
  - TICKET-005 postmortem / evaluation / calibration diagnostic 완료
  - TICKET-006 ML alpha model v1 / uncertainty-disagreement / selection engine v2 완료
- 우선순위: 최상
- 기대 결과: **전일 selection engine v2 가 뽑은 장중 후보군을 다음 거래일 장중에 추적하고, 1분봉·체결강도·호가 요약·VWAP·갭/변동성 정보를 바탕으로 “지금 진입 / 관찰 유지 / 오늘 보류” 수준의 진입 타이밍 판단을 제공하는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **StockMaster의 장후 리서치 중심 시스템 위에, “다음 날 실제로 언제 들어가는 것이 나았는가”를 보조해 주는 장중 타이밍 레이어를 얹는 티켓**이다.

즉, 이번 티켓의 목표는 아래 여덟 가지를 안정적으로 만드는 것이다.

1. 전일 selection engine v2 결과에서 **장중 추적 대상 후보군(session candidates)** 을 생성한다.
2. 후보군에 대해서만 **1분봉 / 체결 요약 / 호가 요약 / 장중 상태 플래그** 를 수집하거나 물질화한다.
3. 장중 시점별로 **VWAP, opening gap, relative volume, spread, orderbook imbalance, execution strength, micro-trend** 같은 신호를 계산한다.
4. 장중 신호를 바탕으로 **Entry Timing Layer v1** 을 만든다.
5. Entry Timing Layer v1 은 **selection engine v2 의 상위 후보를 보조**하는 역할만 하고, 별도 독립 종목 발굴 엔진 역할은 하지 않는다.
6. 자동매매/주문 제출 없이 **읽기 전용 연구 시스템**으로 유지한다.
7. 장중 판단 결과를 **재현 가능하게 저장**하고, 나중에 장 마감 후 “진입 타이밍 보조가 실제로 개선을 만들었는지” 평가할 수 있어야 한다.
8. UI / Ops / 리포트에서 장중 상태와 판단 근거를 사람이 이해할 수 있게 보여준다.

이번 티켓이 끝나면 다음 티켓에서는 장중 보조 엔진의 사후 평가 고도화, 시그널 실험 관리, regime-aware intraday adjustment, 미국주식/나스닥 확장 같은 방향으로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓의 핵심 원칙

### 2.1 이 티켓은 “자동매매” 티켓이 아니다
이번 티켓의 산출물은 아래까지다.

- 장중 후보군 생성
- 장중 데이터 수집/요약
- 장중 진입 타이밍 신호 계산
- 장중 진입 판단(`ENTER_NOW`, `WAIT`, `AVOID_TODAY` 등) 생성
- 장중 리서치 UI/리포트 생성

이번 티켓에서 **절대 하지 말아야 하는 것**:

- 실주문 전송
- 자동 주문 버튼 생성
- 무인 실행형 매매 전략 구현
- 계좌 주문 상태 추적/정정/취소 기능
- 포지션 PnL 실시간 추적 시스템 구축

이 티켓의 목적은 어디까지나 **selection engine v2 가 고른 종목을 “장중에 더 좋은 진입 시점으로 번역하는 연구 도구”** 를 만드는 것이다.

### 2.2 후보군 한정 저장 원칙
80GB 저장 예산을 지키기 위해 다음을 강제한다.

- 전 종목 장중 원시데이터 장기보관 금지
- **후보군(session candidates)만 장중 데이터 저장**
- 원시 웹소켓 패킷 장기보관 금지
- 장기보관 대상은 **요약/정규화된 1분봉 + 체결/호가 summary + decision snapshot** 중심
- 디버그용 raw dump 가 있더라도 매우 짧은 TTL 을 둔다

### 2.3 장후 selection 이 “모체”, 장중 엔진은 “보조층”
장중 엔진은 다음을 넘어서는 안 된다.

- 장후 alpha 모델과 selection engine v2 를 무시하고 단독으로 종목을 새로 발굴하는 것
- 장중 미세 신호만 보고 전날 장후 스냅샷과 반대되는 공격적 의사결정을 하는 것

기본 규칙:

- 장중 엔진은 **전일 selection engine v2 의 session candidates** 만 평가한다.
- 장중 엔진은 **selection confidence 를 존중하는 gating layer** 여야 한다.
- 장중 엔진은 **진입 시점 조정 / 관찰 지속 / 오늘 보류** 정도를 결정한다.

### 2.4 실시간성이 중요하지만, 구조는 재현 가능해야 한다
장중 엔진은 결국 실시간성도 필요하지만, 연구 플랫폼으로서 더 중요한 것은 재현 가능성이다.

따라서 반드시 남겨야 하는 것:

- 어떤 `selection_date` 결과가 어떤 `session_date` 후보군으로 넘어갔는지
- 각 checkpoint 시점에 어떤 입력값이 있었는지
- 어떤 규칙/모델 버전이 어떤 decision 을 만들었는지
- 이후 장 마감 기준 실제 결과가 어땠는지

---

## 3. 반드시 끝내야 하는 구현 범위

### 3.1 Intraday candidate sessionization
다음이 가능한 상태를 만든다.

- 전일 장후 생성된 `selection engine v2` 결과를 읽어 **다음 거래일 추적 후보군** 을 만든다.
- 후보군 생성 규칙은 최소한 다음을 포함한다.
  - `selection_date = T`
  - `session_date = next_trading_day(T)`
  - `symbol` 별 후보 생성
  - `horizon` 별 참조 가능
- 기본 후보 선정 규칙 예시:
  - `selected_flag = true`
  - `selection_grade in ('A', 'A-', 'B+')` 또는 동등한 상위 grade
  - 상위 `max_intraday_candidates_per_day` 제한 적용
- session candidate row 는 최소한 아래를 가져야 한다.
  - `selection_date`
  - `session_date`
  - `symbol`
  - `horizon`
  - `selection_engine_version`
  - `selection_rank`
  - `selection_grade`
  - `predicted_excess_return`
  - `uncertainty_score`
  - `disagreement_score`
  - `selected_flag`
  - `candidate_priority`
  - `candidate_reason_codes`
- 동일 `selection_date`, `session_date`, `symbol`, `horizon` 조합은 idempotent 해야 한다.

### 3.2 장중 데이터 수집 계층
다음이 가능한 상태를 만든다.

- 후보군만 대상으로 장중 데이터를 수집/요약/저장할 수 있어야 한다.
- 수집 계층은 provider 추상화를 유지해야 하며, 기본은 KIS provider 를 사용한다.
- 구현은 아래 세 레이어로 나눈다.
  1. **1분봉 레이어**
  2. **체결 요약 레이어**
  3. **호가 요약 레이어**

#### 3.2.1 1분봉 레이어
최소한 다음 필드를 materialize 할 수 있어야 한다.

- `session_date`
- `bar_minute_ts`
- `symbol`
- `open`, `high`, `low`, `close`
- `volume`
- `turnover`
- `vwap` (가능하면 bar-level 혹은 cumulative level)
- `trade_count` (provider 가능 시)
- `is_regular_session`

기본 저장 단위는 **1분봉** 이다.

#### 3.2.2 체결 요약 레이어
가능하면 아래를 저장한다.

- `execution_strength_raw` 또는 동등 지표
- `buy_volume_proxy`
- `sell_volume_proxy`
- `uptick_volume_proxy`
- `downtick_volume_proxy`
- `trade_count`
- `last_trade_price`
- `last_trade_ts`

provider 가 직접 `체결강도` 를 주지 않는 경우, 아래 순서로 fallback 한다.

1. provider native execution strength 사용
2. buy/sell aggressor proxy 기반 계산
3. uptick/downtick volume proxy 기반 계산
4. unavailable 로 표시하고 signal quality 를 낮춤

#### 3.2.3 호가 요약 레이어
원시 호가창 전체를 장기보관하지 말고, **요약 필드** 중심으로 저장한다.

최소한 아래 필드를 materialize 한다.

- `best_bid_price`
- `best_ask_price`
- `best_bid_size`
- `best_ask_size`
- `spread_abs`
- `spread_bps`
- `mid_price`
- `orderbook_imbalance`
- `topn_bid_size_sum` (가능 시)
- `topn_ask_size_sum` (가능 시)
- `book_pressure_score_raw`
- `quote_ts`

`orderbook_imbalance` 예시:

- `(best_bid_size - best_ask_size) / (best_bid_size + best_ask_size)`
- 또는 top-N aggregate 기반 imbalance

#### 3.2.4 시장 상태 플래그
가능한 범위에서 아래 상태도 저장한다.

- `market_session_state` (`PREOPEN`, `OPENING`, `REGULAR`, `CLOSE_AUCTION`, `AFTER_HOURS`)
- `is_trading_halt`
- `is_vi_active` 또는 equivalent
- `is_data_delayed`
- `provider_latency_ms` 또는 equivalent
- `signal_quality_flag`

### 3.3 Signal engineering for intraday entry timing
다음이 가능한 상태를 만든다.

- 장중 후보군에 대해 **checkpoint 시점별 신호 벡터** 를 생성할 수 있어야 한다.
- checkpoint 는 최소한 아래를 지원한다.
  - `09:05`
  - `09:15`
  - `09:30`
  - `10:00`
  - `11:00`
  - 필요 시 `14:00` optional
- 각 checkpoint 마다 아래 계열 신호를 계산한다.

#### 3.3.1 Gap / opening quality
- `open_gap_pct_vs_prev_close`
- `open_gap_pct_vs_expected_band_mid`
- `gap_zscore_vs_recent`
- `opening_range_high`
- `opening_range_low`
- `price_position_within_opening_range`
- `failed_gap_flag`
- `excessive_gap_flag`

#### 3.3.2 VWAP / micro-trend
- `distance_to_session_vwap_bps`
- `distance_to_5m_vwap_bps`
- `micro_trend_return_5m`
- `micro_trend_return_15m`
- `above_vwap_flag`
- `vwap_reclaim_flag`
- `vwap_reject_flag`

#### 3.3.3 Relative volume / activity
- `cum_volume_ratio_vs_recent`
- `minute_volume_zscore`
- `turnover_ratio_vs_recent`
- `trade_count_ratio_vs_recent`
- `active_participation_flag`

#### 3.3.4 Orderbook / execution strength
- `execution_strength_score`
- `execution_strength_delta`
- `orderbook_imbalance_score`
- `orderbook_imbalance_delta`
- `spread_penalty_score`
- `book_pressure_score`
- `quote_stability_score`

#### 3.3.5 Risk / friction / shock
- `intraday_realized_volatility`
- `range_expansion_score`
- `slippage_risk_proxy`
- `liquidity_thin_flag`
- `halt_or_vi_penalty_flag`
- `data_quality_penalty_flag`

### 3.4 Entry Timing Layer v1
다음이 가능한 상태를 만든다.

- 장중 신호를 기반으로 checkpoint 시점별 **진입 타이밍 판단** 을 만든다.
- 이 판단은 **rule-based + calibrated** 구조를 우선한다.
- 본 티켓에서는 정식 장중 ML 모델이 아니라 **투명한 규칙 기반 timing layer v1** 을 만든다.

최소한 아래 출력을 만든다.

- `entry_timing_score`
- `entry_action`
- `entry_confidence_band`
- `entry_reason_codes`
- `entry_blocker_codes`
- `recommended_recheck_ts`

`entry_action` 기본 값은 아래 중 하나여야 한다.

- `ENTER_NOW`
- `WAIT_RECHECK`
- `AVOID_TODAY`
- `DATA_INSUFFICIENT`

#### 3.4.1 Entry Timing Layer v1 의 기본 철학
- 과도한 시가 갭과 슬리피지 위험이 큰 경우 성급한 진입을 억제한다.
- selection engine v2 의 alpha 가 충분히 강하고 장중 흐름이 우호적일 때만 `ENTER_NOW` 로 승격한다.
- 장중 데이터가 불충분하거나 품질이 낮으면 보수적으로 `WAIT_RECHECK` 또는 `DATA_INSUFFICIENT` 로 둔다.
- selection engine v2 에서 이미 약한 종목은 장중 미세 신호가 좋아도 무리하게 승격하지 않는다.

#### 3.4.2 Entry Timing Layer v1 의 예시 구조
실제 구현은 아래와 완전히 동일할 필요는 없지만, 최소한 이 정도 수준의 요소는 반영해야 한다.

```text
entry_timing_score
= 0.20 * opening_quality_score
+ 0.20 * vwap_trend_score
+ 0.15 * relative_volume_score
+ 0.15 * execution_strength_score
+ 0.10 * orderbook_imbalance_score
+ 0.10 * selection_confidence_component
- 0.15 * spread_penalty_score
- 0.10 * shock_penalty_score
```

그리고 최종 판단은 예를 들어 아래와 같이 낼 수 있다.

- score 높고 blocker 없음 -> `ENTER_NOW`
- score 중간 / data quality 낮음 -> `WAIT_RECHECK`
- gap 과열 / VI / spread 악화 / selection_confidence 부족 -> `AVOID_TODAY`

구현 시 중요한 점:

- **selection engine v2 의 출력이 반드시 입력에 포함**되어야 한다.
- 장중 타이밍 점수는 **selection score 를 대체하는 것이 아니라 조정하는 것**이다.
- `entry_action` 은 deterministic 해야 한다.
- 동일 입력이면 동일 출력이 나와야 한다.

### 3.5 Checkpoint snapshot materialization
다음이 가능한 상태를 만든다.

- 각 후보군 종목에 대해 checkpoint 시점별 snapshot row 를 생성할 수 있어야 한다.
- snapshot 은 최소한 아래를 포함한다.
  - session candidate reference
  - 현재 장중 가격/거래량/VWAP/호가 요약
  - 계산된 intraday signal set
  - entry_timing_score
  - entry_action
  - reason / blocker / quality flags
  - 생성 시각
  - engine version
- 같은 checkpoint 를 재실행해도 idempotent 해야 하며, 필요한 경우 `latest` view 를 제공한다.

### 3.6 Intraday monitor / watchlist UI
다음이 가능한 상태를 만든다.

- Streamlit에 최소한 아래 기능을 가진 장중 전용 페이지가 있어야 한다.
  - 후보군 표
  - 현재 checkpoint 기준 action
  - gap / VWAP / RVOL / spread / imbalance / execution strength 표시
  - selection v2 요약값 표시
  - 최근 1분봉 차트 표시
  - reason code / blocker code 표시
- 페이지 이름 예시:
  - `Intraday Console`
  - `Entry Timing Monitor`
- 최소한 아래 필터가 가능해야 한다.
  - session date
  - action
  - horizon
  - grade
  - selected only

### 3.7 Ops / observability
다음이 가능한 상태를 만든다.

- 장중 엔진 health 를 확인할 수 있어야 한다.
- 최소한 아래를 Ops 에 보여준다.
  - 오늘 후보군 수
  - 수집 중인 심볼 수
  - 마지막 데이터 수신 시각
  - provider status
  - latency / delay flag
  - signal quality issue count
  - checkpoint materialization success/failure
  - 보관 정책 / 디스크 경고

### 3.8 기본 평가 레이어
다음이 가능한 상태를 만든다.

- 장중 엔진의 판단이 실제로 유용했는지 **기초 평가** 할 수 있어야 한다.
- 이번 티켓에서는 full-blown 실험 프레임워크까지는 아니더라도 아래는 가능해야 한다.
  - `naive_open_entry_return` 과 `timed_entry_return` 비교
  - checkpoint 별 이후 수익률 비교
  - `ENTER_NOW` / `WAIT_RECHECK` / `AVOID_TODAY` 별 outcome 분포 비교
  - 상위 후보군에서 timing layer 적용 전/후 평균 성과 비교
- 평가 타깃 예시:
  - `session_date open -> close`
  - `checkpoint price -> close`
  - `checkpoint price -> D+1 close` optional

---

## 4. 저장 계약 / 테이블 설계 요구

테이블명은 프로젝트 내부 네이밍 컨벤션에 맞춰 조정 가능하지만, 의미상 아래를 분명히 가져야 한다.

### 4.1 `fact_intraday_candidate_session`
역할: 전일 selection 결과에서 다음 거래일 장중 추적 후보군을 정의한다.

핵심 컬럼 예시:
- `selection_date`
- `session_date`
- `symbol`
- `horizon`
- `selection_run_id`
- `selection_engine_version`
- `selection_rank`
- `selection_grade`
- `selected_flag`
- `predicted_excess_return`
- `uncertainty_score`
- `disagreement_score`
- `candidate_priority`
- `candidate_reason_codes_json`
- `created_at`

### 4.2 `fact_intraday_bar_1m`
역할: 후보군 대상 1분봉 저장.

핵심 컬럼 예시:
- `session_date`
- `bar_minute_ts`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `turnover`
- `vwap`
- `trade_count`
- `source_provider`
- `ingested_at`

### 4.3 `fact_intraday_trade_summary`
역할: 체결강도/체결 요약 저장.

핵심 컬럼 예시:
- `session_date`
- `snapshot_ts`
- `symbol`
- `execution_strength_raw`
- `buy_volume_proxy`
- `sell_volume_proxy`
- `uptick_volume_proxy`
- `downtick_volume_proxy`
- `trade_count`
- `last_trade_price`
- `signal_quality_flag`
- `source_provider`
- `ingested_at`

### 4.4 `fact_intraday_quote_summary`
역할: 호가 요약 저장.

핵심 컬럼 예시:
- `session_date`
- `snapshot_ts`
- `symbol`
- `best_bid_price`
- `best_ask_price`
- `best_bid_size`
- `best_ask_size`
- `spread_abs`
- `spread_bps`
- `mid_price`
- `orderbook_imbalance`
- `topn_bid_size_sum`
- `topn_ask_size_sum`
- `book_pressure_score_raw`
- `signal_quality_flag`
- `source_provider`
- `ingested_at`

### 4.5 `fact_intraday_signal_snapshot`
역할: checkpoint 시점별 장중 신호 벡터 저장.

핵심 컬럼 예시:
- `session_date`
- `checkpoint_label`
- `checkpoint_ts`
- `symbol`
- `horizon`
- `selection_run_id`
- `open_gap_pct_vs_prev_close`
- `distance_to_session_vwap_bps`
- `cum_volume_ratio_vs_recent`
- `execution_strength_score`
- `orderbook_imbalance_score`
- `spread_penalty_score`
- `intraday_realized_volatility`
- `signal_quality_score`
- `signal_reason_codes_json`
- `engine_version`
- `created_at`

### 4.6 `fact_intraday_entry_decision`
역할: checkpoint 시점별 진입 판단 저장.

핵심 컬럼 예시:
- `session_date`
- `checkpoint_label`
- `checkpoint_ts`
- `symbol`
- `horizon`
- `entry_timing_score`
- `entry_action`
- `entry_confidence_band`
- `entry_reason_codes_json`
- `entry_blocker_codes_json`
- `recommended_recheck_ts`
- `selection_confidence_component`
- `timing_engine_version`
- `created_at`

### 4.7 `fact_intraday_timing_outcome`
역할: 장중 판단의 사후 성과 비교용 outcome 저장.

핵심 컬럼 예시:
- `session_date`
- `checkpoint_label`
- `symbol`
- `horizon`
- `entry_action`
- `decision_price`
- `close_price`
- `future_close_price`
- `return_to_close`
- `return_to_d1_close` (optional)
- `naive_open_return_to_close`
- `timing_edge_vs_naive_open`
- `outcome_bucket`
- `evaluated_at`

---

## 5. 파일 / 스크립트 산출물 요구

아래 파일명은 가능한 한 그대로 사용한다. 내부 패키지 구조에 맞게 모듈은 분리해도 된다.

### 5.1 Session candidate build
- `scripts/materialize_intraday_candidate_session.py`
  - 입력: `selection_date`, `session_date(optional)`, `horizons`, `max-candidates`
  - 역할: 전일 selection v2 결과에서 장중 후보군 생성

### 5.2 Intraday ingestion / backfill
- `scripts/backfill_intraday_candidate_bars.py`
  - 역할: 지정 후보군/날짜 범위의 1분봉 backfill
- `scripts/backfill_intraday_candidate_trade_summary.py`
  - 역할: 체결 요약 backfill 또는 summary materialization
- `scripts/backfill_intraday_candidate_quote_summary.py`
  - 역할: 호가 요약 backfill 또는 summary materialization

실시간/장중용 실행 entry point 도 필요하다.

예시:
- `scripts/run_intraday_candidate_collector.py`
  - 역할: session candidate 를 읽고 시장 시간 동안 데이터 수집 루프를 실행
  - dry-run / limited-symbol mode 지원

### 5.3 Signal / decision materialization
- `scripts/materialize_intraday_signal_snapshots.py`
  - 입력: `session-date`, `checkpoint`, `horizons`
  - 역할: checkpoint 시점 신호 스냅샷 생성
- `scripts/materialize_intraday_entry_decisions.py`
  - 입력: `session-date`, `checkpoint`, `horizons`
  - 역할: entry timing decision 생성

### 5.4 Evaluation / report
- `scripts/evaluate_intraday_timing_layer.py`
  - 역할: naive open 대비 timing layer 기초 평가
- `scripts/render_intraday_monitor_report.py`
  - 역할: 장중 후보군 모니터링용 HTML 또는 Markdown 리포트 생성

### 5.5 App / UI
- `app/ui/pages/` 아래 장중 전용 페이지 추가
  - 예시: `Intraday_Console.py`

### 5.6 Tests
- session candidate materialization 테스트
- 1분봉 / quote / trade summary normalization 테스트
- signal engineering deterministic 테스트
- entry timing action mapping 테스트
- fallback / missing data / low quality data 테스트
- checkpoint outcome evaluation 테스트

---

## 6. 세부 동작 규칙

### 6.1 후보군 기본 제한
기본값 예시:

- `max_intraday_candidates_per_day = 30`
- `candidate_grade_allowlist = ['A', 'A-', 'B+']`
- `candidate_horizons = [1, 5]`

이 값들은 설정 파일에서 조정 가능해야 한다.

### 6.2 장중 체크포인트
기본 checkpoint:

- `09:05`
- `09:15`
- `09:30`
- `10:00`
- `11:00`

선택적 추가 checkpoint:

- `13:00`
- `14:00`

체크포인트는 `config` 또는 settings 로 정의 가능해야 하며, 코드에 하드코딩되어 있어도 central setting 을 통해 바꿀 수 있어야 한다.

### 6.3 기본 액션 매핑 규칙
아래는 기본 예시이며, 구현 시 동등한 의미를 가지면 된다.

- `ENTER_NOW`
  - selection confidence 높음
  - gap 과열 아님
  - spread 허용 범위
  - VWAP 위 / reclaim 성공 / relative volume 양호 / execution strength 양호
- `WAIT_RECHECK`
  - selection 은 괜찮지만 opening noise / gap / book instability 존재
  - checkpoint 이후 재평가 가치 있음
- `AVOID_TODAY`
  - 과열 갭, VI, 극단적 스프레드, 데이터 품질 저하, liquidity thin, selection confidence 부족
- `DATA_INSUFFICIENT`
  - 필요한 신호 다수가 unavailable

### 6.4 fallback 정책
반드시 구현할 fallback:

- quote summary unavailable -> spread/imbalance 항목 null 허용 + `signal_quality_penalty`
- trade summary unavailable -> execution strength proxy fallback 또는 unavailable 표시
- 1분봉 일부 누락 -> 가능한 범위 계산 + `data_quality_penalty_flag`
- selection v2 row 없음 -> session candidate 생성 금지
- checkpoint 이전 데이터 부족 -> `WAIT_RECHECK` 또는 `DATA_INSUFFICIENT`
- 실시간 collector 실패 -> backfill path 로 복구 가능해야 함

fallback row 를 숨기지 말고, UI/Ops/리포트에 표시해야 한다.

### 6.5 저장 / TTL 정책
80GB 제약을 반영하여 아래를 기본 원칙으로 둔다.

- `fact_intraday_bar_1m`: 중기 보관
- `fact_intraday_trade_summary`: 단기~중기 보관
- `fact_intraday_quote_summary`: 단기 보관
- raw debug packet dump: 매우 짧은 TTL
- `fact_intraday_signal_snapshot`, `fact_intraday_entry_decision`, `fact_intraday_timing_outcome`: 영구 또는 장기 보관

TTL 은 설정으로 노출하되, 기본 정리 스크립트/문서가 있어야 한다.

---

## 7. UI / 리포트 요구사항

### 7.1 Intraday Console
최소한 아래 컬럼을 가진 테이블이 보여야 한다.

- symbol
- name
- selection grade
- selection rank
- horizon
- latest price
- latest return vs open
- gap
- RVOL
- VWAP distance
- spread bps
- orderbook imbalance
- execution strength score
- latest entry action
- latest timing score
- reason codes
- blocker codes
- last updated

### 7.2 종목 상세 카드
Stock Workbench 또는 장중 상세 카드에서 최소한 아래를 보여준다.

- 전일 selection v2 요약
- 오늘 장중 1분봉 mini chart
- VWAP line
- checkpoint timeline
- latest signal snapshot
- latest decision snapshot
- data quality / missing field 경고

### 7.3 장중 모니터 리포트
`render_intraday_monitor_report.py` 결과는 최소한 아래를 포함한다.

- 오늘 후보군 수
- action 별 종목 수
- `ENTER_NOW` 상위 후보
- `WAIT_RECHECK` 상위 후보
- `AVOID_TODAY` 사유 요약
- provider / data quality 경고
- checkpoint 기준 생성 시각

Discord 전송은 이번 티켓에서 **필수는 아님** 이지만, dry-run markdown 산출물 정도는 남길 수 있어야 한다.

---

## 8. 하지 말아야 할 것

다음은 이번 티켓에서 하지 않는다.

1. 자동매매 / 주문 API 연동
2. 포지션 관리 시스템
3. 실시간 계좌 잔고 추적
4. 전 종목 장중 전수 수집
5. 초고빈도 틱 raw 장기보관
6. 장중 엔진 단독 종목 발굴 로직
7. 뉴스 본문 전문 저장
8. selection v2 무시하는 공격적 intraday override
9. 장중 ML 대형 모델부터 먼저 붙이는 과도한 확장
10. UI 접속 시 실시간 collector 자동 실행

---

## 9. 완료 기준 (Definition of Done)

아래가 성립하면 이번 티켓 완료로 본다.

1. `selection_date` 기준으로 `session_date` 장중 후보군을 materialize 할 수 있다.
2. 후보군에 대해 1분봉 / trade summary / quote summary 를 수집 또는 backfill 할 수 있다.
3. 최소 5개 checkpoint 에 대해 signal snapshot 을 생성할 수 있다.
4. checkpoint 별 entry action (`ENTER_NOW`, `WAIT_RECHECK`, `AVOID_TODAY`, `DATA_INSUFFICIENT`) 을 생성할 수 있다.
5. Intraday Console 에서 후보군 상태와 최신 판단을 볼 수 있다.
6. Ops 에서 수집/지연/품질 상태를 볼 수 있다.
7. naive open 대비 timing layer outcome 을 기초 비교할 수 있다.
8. fallback / missing data / data quality issue 가 숨겨지지 않고 기록된다.
9. README 또는 ops 문서에 실행 방법이 정리된다.
10. 장중 엔진이 selection engine v2 를 깨뜨리지 않고, 연구 시스템의 read-only 성격을 유지한다.

---

## 10. 실행 예시

아래 명령은 예시이며, 내부 옵션명은 프로젝트 스타일에 맞춰 조정 가능하다.

```bash
python scripts/materialize_intraday_candidate_session.py --selection-date 2026-03-06 --horizons 1 5 --max-candidates 30
python scripts/backfill_intraday_candidate_bars.py --session-date 2026-03-09 --horizons 1 5
python scripts/backfill_intraday_candidate_trade_summary.py --session-date 2026-03-09 --horizons 1 5
python scripts/backfill_intraday_candidate_quote_summary.py --session-date 2026-03-09 --horizons 1 5
python scripts/materialize_intraday_signal_snapshots.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5
python scripts/materialize_intraday_entry_decisions.py --session-date 2026-03-09 --checkpoint 09:30 --horizons 1 5
python scripts/evaluate_intraday_timing_layer.py --start-session-date 2026-02-17 --end-session-date 2026-03-09 --horizons 1 5
python scripts/render_intraday_monitor_report.py --session-date 2026-03-09 --checkpoint 09:30 --dry-run
streamlit run app/ui/Home.py
```

실시간 collector 예시:

```bash
python scripts/run_intraday_candidate_collector.py --session-date 2026-03-09 --market-open 09:00 --market-close 15:30 --poll-seconds 15 --dry-run
```

---

## 11. README / 운영 문서에 반드시 적을 것

README 또는 동등 문서에 최소한 아래를 명시한다.

- intraday candidate session 개념
- selection v2 와 intraday timing layer 의 관계
- 자동매매가 아니라는 점
- 1분봉 / trade summary / quote summary 저장 원칙
- signal engineering 구성 요소
- action 정의 (`ENTER_NOW`, `WAIT_RECHECK`, `AVOID_TODAY`, `DATA_INSUFFICIENT`)
- checkpoint 정의
- fallback 정책
- TTL / storage policy
- collector 실행 방식
- known limitations

---

## 12. Codex가 작업 후 사용자에게 요약해야 하는 항목

작업 완료 후 아래를 간단히 정리해야 한다.

- 새로 추가된 파일 목록
- 생성/변경된 테이블 및 view 목록
- session candidate 생성 규칙
- intraday bar / trade / quote summary 수집 흐름
- checkpoint signal 계산 순서
- entry timing action 규칙 요약
- fallback / signal quality 정책 요약
- Intraday Console 에서 확인할 위치
- naive open 대비 timing evaluation 방식 요약
- 아직 남은 TODO
- TICKET-008 진입 전 주의사항

---

## 13. 다음 티켓(TICKET-008)으로 자연스럽게 이어질 수 있어야 하는 것

이번 티켓이 끝난 뒤 다음 티켓에서는 아래를 다루기 쉬워야 한다.

- intraday timing layer 사후평가 고도화
- selection v2 + intraday timing 조합 전략 비교
- regime-aware intraday adjustment
- 장중 알림/리포트 고도화
- 나스닥/미국주식 확장 시 intraday adapter 분리

즉, 이번 티켓은 **장중 데이터 수집과 진입 타이밍 보조를 처음으로 실전형 구조에 올리는 티켓**이지, 최종 자동매매 엔진을 만드는 티켓이 아니다.
