# TICKET-003 — Feature Store + D+1/D+5 라벨 + 설명용 점수/랭킹 엔진 v0

- 문서 목적: TICKET-002 이후, Codex가 바로 이어서 구현할 **첫 리서치 엔진 계층**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001의 symbol/calendar/provider activation 완료
  - TICKET-002의 일봉, 재무 스냅샷, 뉴스 메타데이터 적재가 동작해야 함
- 우선순위: 최상
- 기대 결과: “데이터를 쌓는 시스템”에서 한 단계 올라가, **피처 → 라벨 → 장세 스냅샷 → 설명용 점수 → 랭킹 스냅샷**까지 생성되는 상태

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **국내주식 리서치 플랫폼의 첫 연구 엔진을 구체화하는 작업**이다.

즉, 이번 티켓의 목표는 아래 네 가지를 안정적으로 만드는 것이다.

1. `as_of_date` 기준으로 재현 가능한 **feature snapshot** 이 생성된다.
2. `D+1`, `D+5` 기준의 **누수 없는 forward return label** 이 생성된다.
3. 장세를 반영하는 **market regime snapshot** 이 생성된다.
4. 실제 모델이 들어오기 전 단계로, 사람이 해석 가능한 **설명용 점수/랭킹 엔진 v0** 가 동작한다.

이 티켓이 끝나면 다음 티켓에서는 더 고도화된 수급/호가/체결/불확실성/예측모형 계층으로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 Feature Store v1
다음이 가능한 상태를 만든다.

- `fact_daily_ohlcv`, `fact_fundamentals_snapshot`, `fact_news_item` 을 입력으로 받아 `as_of_date` 기준 feature snapshot 을 생성한다.
- 최소한 **가격/추세/변동성/거래대금/재무/뉴스/데이터품질** 계열 피처가 생성된다.
- 피처는 **tall storage + wide access view/materialization** 이 가능한 구조여야 한다.
- 동일 날짜/동일 종목 재실행 시 중복 없이 idempotent 하게 갱신된다.
- run manifest 와 feature version 이 함께 남는다.

### 2.2 D+1 / D+5 Forward Label
다음이 가능한 상태를 만든다.

- `as_of_date` 장후 시점에서 **실제로 다음 거래일에 진입 가능하다는 전제**에 맞는 label 이 생성된다.
- 기본 forward label 은 **next open → future close** 기준으로 만든다.
- `D+1`, `D+5` 절대수익률과 시장대비 초과수익률 label 이 생성된다.
- 거래정지/데이터누락/미래 거래일 부족 시 label unavailable 처리가 되어야 한다.
- label 생성 규칙은 README와 코드에서 명시적으로 드러나야 한다.

### 2.3 Market Regime Snapshot
다음이 가능한 상태를 만든다.

- 시장 전체 또는 거래소별 기준으로 **breadth / realized volatility / turnover burst / median return** 등을 집계한다.
- `risk_off`, `neutral`, `risk_on`, `panic`, `euphoria` 같은 단순 상태가 산출된다.
- 이 상태가 설명용 점수 엔진의 `regime_fit_score` 에 반영된다.

### 2.4 설명용 점수/랭킹 엔진 v0
다음이 가능한 상태를 만든다.

- `D+1`, `D+5` 각각에 대해 별도 점수 로직이 존재한다.
- 점수는 **실제 predictive alpha 모델이 아니라 explanatory ranking layer** 임을 코드와 README에 분명히 적는다.
- 점수 구성요소는 사람이 이해 가능한 수준으로 노출되어야 한다.
- 종목별 `top_reason_tags_json`, `risk_flags_json`, `explanatory_score_json` 이 저장된다.
- `A`, `A-`, `B`, `C` 등급이 부여된다.
- Leaderboard 화면에서 최신 랭킹과 이유를 확인할 수 있다.

### 2.5 최소 검증/진단 도구
다음이 가능한 상태를 만든다.

- feature coverage 진단
- label coverage 진단
- score decile / grade bucket 별 forward return sanity check
- 최근 `N` 거래일 기준 top bucket 이 하위 bucket 보다 대체로 나은지 확인 가능한 간단한 리포트

### 2.6 스크립트/엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/build_feature_store.py`
- `scripts/build_forward_labels.py`
- `scripts/build_market_regime_snapshot.py`
- `scripts/materialize_explanatory_ranking.py`
- `scripts/validate_explanatory_ranking.py`

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- feature snapshot 생성
- wide/tall feature materialization
- D+1 / D+5 forward label 생성
- market regime snapshot 생성
- 설명용 점수 계산
- 설명용 랭킹/등급 부여
- score component / reason tag / risk flag 저장
- Leaderboard / Research / Ops 화면 확장
- 간단한 decile/bucket sanity validation
- README 및 실행 가이드 갱신
- 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- ML alpha model 학습/예측
- uncertainty/disagreement 기반 selection value 최종 엔진
- 투자자/기관/프로그램 매매 기반 정식 flow score
- 실시간 장중 체결/호가 기반 신호
- Discord 최종 보고서 디자인 완성
- D+1 / D+5 사후 평가 리포트 완성
- feature selection 자동화/하이퍼파라미터 서치
- 백테스트 엔진 전체 구축

즉, 이번 티켓은 **“데이터 축 위에 research-ready feature/label/ranking 층을 올리는 작업”** 까지다.

---

## 4. Codex가 작업 시작 전에 반드시 확인할 것

Codex는 작업 시작 전에 아래 순서를 따른다.

1. 루트 경로 `D:\MyApps\StockMaster` 를 기준으로 현재 저장소 상태를 확인한다.
2. 아래 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `TICKET_001_Universe_Calendar_Provider_Activation.md`
   - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
   - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
   - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
3. TICKET-002가 불완전하다면, 이번 티켓 수행에 직접 필요한 blocking issue 만 보완한다.
4. 기존 foundation / provider / curated schema 방향을 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 장후 연구 시스템 전제
이 플랫폼은 장후 리포트가 핵심이다.

- `as_of_date` 는 **장 마감 후 리서치 시점**을 의미한다.
- 따라서 기본 진입 기준은 `as_of_date` 당일 종가가 아니라 **다음 거래일 시가(next open)** 다.
- forward label 은 이 실행 가능성을 반영해야 한다.
- score/ranking 또한 “다음 날부터 참고 가능한 후보군”을 전제로 한다.

### 5.2 설명용 점수와 실제 예측 엔진의 분리
이번 티켓에서 만드는 점수는 **사용자에게 보여주는 explanatory score** 다.

- 이것은 최종 predictive alpha model 이 아니다.
- 실제 엔진은 이후 티켓에서 uncertainty, disagreement, implementation cost, flow, regime conditioning 등을 더 깊게 반영한다.
- 이번 티켓의 점수는 **UI/리서치/검증용 1차 랭킹 층**이다.
- 따라서 README와 코드 주석에서 이를 명시해야 한다.

### 5.3 누수 금지
- feature 는 오직 `as_of_date` 종료 시점까지 알 수 있는 정보로만 만든다.
- label 은 오직 `as_of_date` 이후 거래일 데이터로만 만든다.
- 점수 계산에 label 을 섞으면 안 된다.
- validation 도 과거 구간을 기준으로 하며 미래 정보를 섞지 않는다.

### 5.4 활성/비활성 score component 의 투명성
이전 논의에서 `추세/모멘텀`, `거래대금/회전율`, `수급`, `재무 퀄리티`, `밸류/안전성`, `뉴스 촉매`, `리스크 패널티` 같은 점수 구조를 합의했다.

다만 이번 시점에는 **정식 수급(flow) 데이터와 미시구조 데이터가 아직 완성되지 않았을 수 있다.**

따라서 이번 티켓에서는:

- 이미 확보된 데이터로 계산 가능한 component 는 **active** 로 처리한다.
- 아직 준비되지 않은 component 는 **reserved** 로 표기하고, 최종 점수 계산 시 가중치에서 제외하거나 neutral 처리한다.
- UI에는 사용자가 이 점수가 “partial v0” 임을 알 수 있어야 한다.

즉, **아직 없는 데이터를 있는 척하여 점수에 억지로 넣지 않는다.**

### 5.5 idempotent + backfill friendly
- 날짜 범위를 주어 과거 feature/label/ranking 을 생성할 수 있어야 한다.
- 동일 날짜/동일 종목 재실행 시 중복 적재가 되지 않아야 한다.
- 실행 범위, 적재 row 수, 실패 건수, version 정보는 run manifest 에 남긴다.

### 5.6 설명 가능성 확보
이번 티켓의 산출물은 이후 사용자가 “왜 이 종목이 A등급이었는가”를 확인할 수 있게 해야 한다.

따라서 최소한 아래는 남겨야 한다.

- component 별 점수
- top reason tags
- risk flags
- regime state
- feature version / ranking version
- used active component set

---

## 6. Feature Store v1 요구사항

## 6.1 목적
`fact_feature_snapshot` 은 향후 아래 모든 티켓의 기초가 된다.

- 설명용 점수/랭킹
- ML 학습용 입력 행렬
- 종목 카드 이유 설명
- 장세 해석
- 사후 평가 및 실패 원인 분석

즉, 이번 티켓의 feature store 는 **단순 파생값 저장소가 아니라 연구 재현성의 핵심**이다.

## 6.2 저장 전략
기본 원칙:

- **tall table** 을 정식 기록본으로 둔다.
- **wide parquet or wide view** 를 연구/점수 계산 편의용으로 둔다.
- tall 이 source of truth, wide 는 derived convenience artifact 다.

### 6.2.1 tall storage
기본 테이블: `fact_feature_snapshot`

권장 최소 컬럼:

- `run_id`
- `as_of_date`
- `symbol`
- `feature_name`
- `feature_value`
- `feature_group`
- `source_version`
- `created_at`

선택 추가 컬럼 허용:

- `feature_rank_pct`
- `feature_zscore`
- `is_imputed`
- `notes_json`

### 6.2.2 wide materialization
예시 경로:

```text
data/curated/features/as_of_date=2026-03-06/feature_matrix.parquet
```

wide access 용 최소 원칙:

- 한 행 = `as_of_date`, `symbol`
- 열 = 피처명
- score/ranking 스크립트가 직접 읽기 쉬워야 한다.
- 너무 aggressive 한 denormalization 은 피하되, 연구용 접근성은 확보한다.

### 6.2.3 DuckDB view / table 권장안
- `vw_feature_snapshot_latest`
- `vw_feature_matrix_latest`
- 필요 시 날짜별 wide table/materialized table

---

## 7. Feature Group 상세 정의

이번 티켓에서는 아래 그룹을 최소 구현 범위로 본다.

### 7.1 Price / Trend / Momentum Features
최소 구현 후보:

- `ret_1d`
- `ret_3d`
- `ret_5d`
- `ret_10d`
- `ret_20d`
- `ret_60d`
- `ma_5`
- `ma_20`
- `ma_60`
- `ma5_over_ma20`
- `ma20_over_ma60`
- `dist_from_20d_high`
- `dist_from_60d_high`
- `dist_from_20d_low`
- `close_pos_in_day_range`
- `up_day_count_5d`
- `up_day_count_20d`
- `drawdown_20d`
- `drawdown_60d`

원칙:
- roll window 부족 시 값은 null 또는 safe fallback 처리
- 분모 0/동일 high-low 같은 edge case 는 안정적으로 처리
- 계산식은 코드/README에서 추적 가능해야 한다

### 7.2 Volatility / Risk Features
최소 구현 후보:

- `realized_vol_5d`
- `realized_vol_10d`
- `realized_vol_20d`
- `hl_range_1d`
- `gap_open_1d`
- `gap_abs_avg_5d`
- `gap_abs_avg_20d`
- `max_loss_5d`
- `max_loss_20d`

원칙:
- 변동성은 수익률 표준편차 또는 명시적 단순 프록시로 계산 가능
- 너무 화려한 risk 모델을 넣지 말고, 해석 가능한 범위를 유지

### 7.3 Liquidity / Turnover / Participation Features
최소 구현 후보:

- `volume_ratio_1d_vs_20d`
- `turnover_value_1d`
- `turnover_value_ma_5`
- `turnover_value_ma_20`
- `turnover_z_5_20`
- `adv_20`
- `adv_60`
- `liquidity_rank_pct`

원칙:
- `turnover_value` 가 없는 경우 명시적 proxy 또는 null 허용
- proxy 를 썼다면 `notes_json` 또는 manifest notes 에 기록

### 7.4 Fundamentals / Quality Features
최소 구현 후보:

- `roe_latest`
- `debt_ratio_latest`
- `operating_margin_latest`
- `net_margin_latest` (가능 시)
- `revenue_latest`
- `operating_income_latest`
- `net_income_latest`
- `net_income_positive_flag`
- `operating_income_positive_flag`
- `days_since_latest_report`
- `fundamental_coverage_flag`

원칙:
- 무리하게 TTM 전체를 완벽히 구현하려고 티켓을 지연시키지 말 것
- 먼저 **latest available snapshot 기준 안정적인 품질 지표**를 만드는 데 집중
- TTM/YoY/QoQ 정교화는 후속 티켓으로 넘겨도 된다

### 7.5 Value / Safety Features
최소 구현 후보:

- `pb_proxy`
- `ps_proxy`
- `earnings_yield_proxy`
- `equity_positive_flag`
- `low_debt_preference_proxy`

원칙:
- market_cap, equity, revenue, earnings 가 모두 확보된 경우에만 계산
- 데이터가 부족하면 null 허용
- proxy 계산 여부는 명시

### 7.6 News / Catalyst Features
최소 구현 후보:

- `news_count_1d`
- `news_count_3d`
- `news_count_5d`
- `distinct_publishers_3d`
- `latest_news_age_hours`
- `fresh_news_flag`
- `positive_catalyst_count_3d` (가능 시)
- `negative_catalyst_count_3d` (가능 시)
- `news_link_confidence_score`
- `news_coverage_flag`

원칙:
- 정교한 LLM 뉴스 해석까지 가지 않는다
- 현재 저장된 메타데이터, snippet, tag, freshness 중심으로 간단하고 재현 가능한 feature 를 만든다
- 공격적 sentiment 분류보다 **freshness + coverage + catalyst tag presence** 를 우선한다

### 7.7 Data Quality / Availability Features
최소 구현 후보:

- `has_daily_ohlcv_flag`
- `has_fundamentals_flag`
- `has_news_flag`
- `stale_price_flag`
- `missing_key_feature_count`
- `data_confidence_score`

원칙:
- 이것은 ranking 의 eligibility 판단과 risk flag 생성에 사용된다.
- 데이터 공백을 숨기지 않는다.

---

## 8. Feature Normalization / Cross-sectional Transform

설명용 점수 엔진에서 직접 raw 값만 쓰면 시장 상태에 따라 해석이 흔들릴 수 있다.

따라서 이번 티켓에서는 **간단하고 일관된 cross-sectional normalization 층**을 함께 둔다.

권장 원칙:

1. 동일 `as_of_date` 기준 cross-section 에서 계산한다.
2. extreme outlier 는 winsorize 또는 clip 한다.
3. 주로 `rank_pct` 형태를 사용한다.
4. 일부 연속값은 z-score 도 함께 저장 가능하다.
5. null 은 명시적으로 처리하고, component 계산 시 neutral 또는 penalty 로 다룬다.

최소 구현 방향:

- `feature_rank_pct`
- 필요 시 `feature_zscore`
- score 엔진은 가능한 한 **rank 기반 조합**을 우선한다.

---

## 9. Market Regime Snapshot 요구사항

### 9.1 목적
시장 변동성이 비정상적으로 커진 구간에서는 동일한 모멘텀/뉴스/품질 신호라도 해석이 달라진다.

따라서 이번 티켓에서 **단순하지만 일관된 regime snapshot** 을 만들어 점수에 반영한다.

### 9.2 최소 테이블 권장안
권장 테이블: `fact_market_regime_snapshot`

권장 최소 컬럼:

- `run_id`
- `as_of_date`
- `market_scope` (`KR_ALL`, `KOSPI`, `KOSDAQ` 등)
- `breadth_up_ratio`
- `breadth_down_ratio`
- `median_symbol_return_1d`
- `median_symbol_return_5d`
- `market_realized_vol_20d`
- `turnover_burst_z`
- `new_high_ratio_20d`
- `new_low_ratio_20d`
- `regime_state`
- `regime_score`
- `created_at`

### 9.3 최소 regime state 분류
최소 상태:

- `panic`
- `risk_off`
- `neutral`
- `risk_on`
- `euphoria`

분류 로직은 완벽할 필요는 없지만, 명시적 규칙이 있어야 한다.

예시 방향:

- `panic`: breadth 급락 + 변동성 높음 + median return 악화 + new low ratio 상승
- `risk_off`: breadth 약세 + 변동성 높음
- `neutral`: 중간 영역
- `risk_on`: breadth 개선 + 변동성 낮거나 정상 + new high ratio 개선
- `euphoria`: breadth 과열 + turnover burst + new high ratio 급증

중요 원칙:
- 규칙은 설정 가능하게 두되 과도하게 복잡하게 만들지 않는다.
- 후속 티켓에서 개선 가능해야 한다.

### 9.4 ranking 과의 연결
`regime_fit_score` 는 종목 자체가 아니라 **해당 장세에서 어떤 특성이 상대적으로 선호될지**를 반영하는 보정점수다.

예시 방향:

- `panic`, `risk_off` 에서는
  - 높은 변동성/과열 종목 불리
  - 상대적으로 drawdown 이 적고 품질이 안정적인 종목 우대
- `risk_on`, `euphoria` 에서는
  - 최근 모멘텀 강하고 거래대금이 붙는 종목 우대

이번 티켓에서는 **단순 rule-based 보정**까지만 구현한다.

---

## 10. D+1 / D+5 Forward Label 요구사항

## 10.1 label 철학
이 플랫폼의 장후 리포트는 저녁에 생성된다.

따라서 label 은 아래 현실을 반영해야 한다.

- 추천은 `as_of_date` 장후에 확인한다.
- 실제 진입 가능 시점은 **다음 거래일 시가**다.
- 그러므로 기본 label 은 `as_of_date` 당일 종가 기준이 아니라 **next open 기준**이 더 적합하다.

## 10.2 기본 label 정의
기본 label horizon 은 `1`, `5` 두 개를 만든다.

### 10.2.1 D+1
- entry date = `next_trading_date(as_of_date)`
- entry price = `open[entry_date]`
- exit date = `entry_date`
- exit price = `close[entry_date]`
- gross forward return = `close[entry_date] / open[entry_date] - 1`

### 10.2.2 D+5
- entry date = `next_trading_date(as_of_date)`
- exit date = `trading_date_plus(entry_date, 4)`
- exit price = `close[exit_date]`
- gross forward return = `close[exit_date] / open[entry_date] - 1`

중요:
- 여기서 D+5 는 “다음 거래일 시가 진입 후 5거래일차 종가 청산”으로 정의한다.
- 날짜 계산은 반드시 `dim_trading_calendar` 를 따른다.

## 10.3 baseline / excess return 정의
이번 티켓의 기본 baseline 은 **same-market equal-weight proxy** 로 구현한다.

예시:
- KOSPI 종목이면 해당 거래일 KOSPI 유니버스 종목의 평균 forward return
- KOSDAQ 종목이면 해당 거래일 KOSDAQ 유니버스 종목의 평균 forward return

권장 컬럼:

- `baseline_type`
- `baseline_forward_return`
- `excess_forward_return`

추가 허용:
- 공식 benchmark index 수집이 이미 준비되어 있다면 병행 저장 가능
- 다만 이번 티켓의 blocking requirement 는 아니다

## 10.4 label unavailable rule
다음 경우는 label unavailable 로 처리한다.

- entry day OHLCV 가 없음
- exit day OHLCV 가 없음
- future trading day 수가 부족함
- entry open 이 비정상값/null
- 장기간 거래정지/데이터결손

unavailable 인 경우:
- row 를 만들되 `label_available_flag = false` 로 두거나
- 명시적 exclusion reason 을 남긴다

## 10.5 권장 테이블
권장 테이블: `fact_forward_return_label`

권장 최소 컬럼:

- `run_id`
- `as_of_date`
- `symbol`
- `horizon`
- `entry_date`
- `exit_date`
- `entry_basis`
- `exit_basis`
- `entry_price`
- `exit_price`
- `gross_forward_return`
- `baseline_type`
- `baseline_forward_return`
- `excess_forward_return`
- `label_available_flag`
- `exclusion_reason`
- `created_at`

추가 컬럼 허용:
- `market`
- `sector` (있는 경우)
- `limit_flag`
- `notes_json`

## 10.6 label 저장 경로 권장안
예시:

```text
data/curated/labels/as_of_date=2026-03-06/forward_return_labels.parquet
```

---

## 11. 설명용 점수/랭킹 엔진 v0 요구사항

## 11.1 목적
이번 티켓의 랭킹 엔진은 다음의 역할을 한다.

- 사람이 보기 쉬운 1차 매수 후보군 정리
- 왜 상위에 올라왔는지 설명
- 이후 predictive model / uncertainty engine 이 들어올 자리를 남겨 둠
- feature 와 label 이 최소한 방향성이 있는지 sanity check

즉, 이번 랭킹 엔진은 **최종 엔진이 아니라 초기 research UI 계층**이다.

## 11.2 active vs reserved component
현재 티켓 시점 기준으로 다음을 권장한다.

### Active component
- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `regime_fit_score`
- `risk_penalty_score`
- `data_confidence_score` (선택)

### Reserved component
- `flow_score`

`flow_score` 는 투자자/기관/프로그램/공매도/회원사 등 정식 수급 레이어가 충분히 붙은 뒤 활성화한다.
이번 티켓에서는 다음 중 하나로 처리한다.

- 아예 계산 대상에서 제외
- UI에 `reserved` 로 표시
- 내부적으로 neutral value 를 주되 최종 점수 분모에서는 제외

가장 중요한 점은 **사용자에게 이 컴포넌트가 아직 미구현이라는 사실이 분명히 보여야 한다는 것**이다.

## 11.3 component score 계산 원칙
- component score 는 0~100 범위로 만든다.
- 가능한 한 cross-sectional rank 기반으로 계산한다.
- 지나치게 복잡한 비선형 수식은 피한다.
- 같은 component 안에서 사용한 서브피처와 가중치는 코드/설정에서 읽히도록 한다.

### 11.3.1 Trend / Momentum Score
권장 D+1 구성 예시:
- `ret_5d_rank_pct`
- `ret_10d_rank_pct`
- `ma5_over_ma20_rank_pct`
- `dist_from_20d_high_inverse_rank`

권장 D+5 구성 예시:
- `ret_20d_rank_pct`
- `ret_60d_rank_pct`
- `ma20_over_ma60_rank_pct`
- `drawdown_20d_inverse_rank`

### 11.3.2 Turnover / Participation Score
권장 공통 구성 예시:
- `volume_ratio_1d_vs_20d_rank_pct`
- `turnover_z_5_20_rank_pct`
- `adv_20_rank_pct`
- `liquidity_rank_pct`

### 11.3.3 Quality Score
권장 구성 예시:
- `roe_latest_rank_pct`
- `operating_margin_latest_rank_pct`
- `net_income_positive_flag`
- `days_since_latest_report_inverse_rank`

### 11.3.4 Value / Safety Score
권장 구성 예시:
- `pb_proxy_inverse_rank`
- `ps_proxy_inverse_rank`
- `debt_ratio_latest_inverse_rank`
- `equity_positive_flag`

### 11.3.5 News Catalyst Score
권장 구성 예시:
- `news_count_1d_rank_pct`
- `news_count_3d_rank_pct`
- `latest_news_age_hours_inverse_rank`
- `distinct_publishers_3d_rank_pct`
- `positive_catalyst_count_3d_rank_pct`
- `negative_catalyst_count_3d` 는 감점 요소로 처리 가능

### 11.3.6 Regime Fit Score
권장 구성 예시:
- regime 이 `risk_on` / `euphoria` 인 경우: trend + turnover 가 강한 종목 보정 가점
- regime 이 `panic` / `risk_off` 인 경우: low drawdown + lower volatility + better quality 종목 보정 가점

복잡한 ML classification 을 하지 말고 **명시적 rule-based 보정**을 사용한다.

### 11.3.7 Risk Penalty Score
권장 구성 예시:
- `realized_vol_20d_rank_pct`
- `drawdown_20d_rank_pct`
- `gap_abs_avg_20d_rank_pct`
- `stale_price_flag`
- `missing_key_feature_count`

주의:
- risk penalty 는 “높을수록 나쁜 값”으로 다루기 쉽도록 정의를 맞춘다.
- 최종 합산 시 감점 항목으로 적용한다.

## 11.4 horizon 별 권장 가중치
이번 티켓은 **v0 explanatory score** 다.
실제 predictive alpha 가 아니므로, 사용 가능 데이터 중심으로 단순하고 투명하게 구현한다.

### 11.4.1 D+1 explanatory score v0
권장 active 가중치:

- `trend_momentum_score`: 25
- `turnover_participation_score`: 20
- `quality_score`: 5
- `value_safety_score`: 5
- `news_catalyst_score`: 20
- `regime_fit_score`: 15
- `risk_penalty_score`: -15
- `flow_score`: reserved

### 11.4.2 D+5 explanatory score v0
권장 active 가중치:

- `trend_momentum_score`: 30
- `turnover_participation_score`: 15
- `quality_score`: 15
- `value_safety_score`: 10
- `news_catalyst_score`: 10
- `regime_fit_score`: 10
- `risk_penalty_score`: -15
- `flow_score`: reserved

### 11.4.3 final score 계산 방식
권장 방식:

1. active positive component 의 가중평균을 계산한다.
2. risk penalty 를 감점한다.
3. missing component 는 neutral 혹은 제외 규칙에 따라 처리한다.
4. universe 내 percentile rank 를 계산한다.
5. percentile / hard rule 기반으로 등급을 부여한다.

중요:
- reserved component 때문에 denominator 가 흔들리면 안 된다.
- active component set 과 weight set 은 결과물에 저장되어야 한다.

## 11.5 eligibility / guardrail
추천 후보군을 만들 때 최소한 아래를 고려한다.

권장 예시:
- `has_daily_ohlcv_flag = true`
- 최근 20일 평균 거래대금이 너무 낮지 않을 것
- 핵심 feature 누락 수가 과도하지 않을 것
- 당일 가격 데이터가 stale 하지 않을 것

단, 이 조건을 **영구 exclude 규칙**으로 hardcode 하지 말고 설정 가능하게 둔다.

권장 결과:
- `eligible_flag`
- `eligibility_notes_json`

이 필드는 `fact_ranking` 에 비파괴적으로 추가해도 된다.

## 11.6 grade 부여 규칙
최소 등급:

- `A`
- `A-`
- `B`
- `C`

권장 기본 규칙:

- `A`: eligible universe 상위 5% + critical risk flag 없음
- `A-`: 다음 10%
- `B`: 다음 20%
- `C`: 나머지 또는 guardrail 경고가 있는 종목

또는 score 절대 임계값과 percentile 조건을 함께 사용할 수 있다.

핵심은:
- 규칙이 명시적이어야 한다.
- 재실행 시 재현 가능해야 한다.
- README에 기록되어야 한다.

## 11.7 이유 태그 / 리스크 플래그
각 종목별로 최소 아래를 저장한다.

### 11.7.1 top_reason_tags_json 예시
- `short_term_momentum_strong`
- `breakout_near_20d_high`
- `turnover_surge`
- `fresh_news_catalyst`
- `quality_metrics_supportive`
- `low_drawdown_relative`

### 11.7.2 risk_flags_json 예시
- `high_realized_volatility`
- `large_recent_drawdown`
- `weak_fundamental_coverage`
- `thin_liquidity`
- `news_link_low_confidence`
- `data_missingness_high`

## 11.8 ranking 저장 계약
정식 저장 대상: `fact_ranking`

권장 최소 컬럼:

- `run_id`
- `as_of_date`
- `symbol`
- `horizon`
- `final_selection_value`
- `grade`
- `explanatory_score_json`
- `top_reason_tags_json`
- `risk_flags_json`
- `created_at`

비파괴적 추가 허용:
- `eligible_flag`
- `eligibility_notes_json`
- `regime_state`
- `ranking_version`

`explanatory_score_json` 에는 최소 아래가 들어가야 한다.

- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `regime_fit_score`
- `risk_penalty_score`
- `flow_score_status`
- `active_weights`
- `score_version`

---

## 12. 최소 검증 / 진단 요구사항

이번 티켓은 research-ready 상태여야 하므로, 최소한 아래 sanity validation 은 포함한다.

### 12.1 feature coverage report
확인할 것:
- 날짜별 생성 종목 수
- 핵심 feature null 비율
- 재무/뉴스 coverage
- missing component 비율

### 12.2 label coverage report
확인할 것:
- D+1 label coverage
- D+5 label coverage
- unavailable reason 분포
- 시장/거래소별 coverage 차이

### 12.3 ranking sanity report
최소 확인:
- 최근 N거래일 기준 `A`, `A-`, `B`, `C` bucket 의 평균 gross/excess forward return
- top decile vs bottom decile 의 평균 차이
- extreme outlier 영향 여부

중요:
- 이것은 full backtest 가 아니다.
- 단지 feature/score 방향성이 완전히 뒤집혀 있지 않은지 확인하는 장치다.

### 12.4 출력 형식
권장 출력:
- markdown summary
- csv/parquet artifact
- Ops 또는 Research 페이지에 최근 validation 결과 일부 노출

---

## 13. UI / Ops 요구사항

### 13.1 Leaderboard 화면
최소한 아래를 볼 수 있어야 한다.

- 기준일(`as_of_date`) 선택
- horizon 선택 (`D+1`, `D+5`)
- 시장 선택 (전체 / KOSPI / KOSDAQ 가능 시)
- 상위 랭킹 테이블
- grade 별 건수
- 각 종목의 주요 component score 와 top reason tag
- regime state 배너 또는 요약

### 13.2 Research / Diagnostics 화면
최소한 아래를 볼 수 있어야 한다.

- 최근 feature build 상태
- 최근 label build 상태
- feature coverage 요약
- label coverage 요약
- 최근 validation decile 결과 일부

### 13.3 Stock Workbench 연결 준비
이번 티켓에서 완성할 필요는 없지만, 향후 개별 종목 카드로 연결되기 쉽도록 아래를 고려한다.

- 최신 feature snapshot 조회 가능
- latest ranking row 조회 가능
- reason tag / risk flag 를 카드에 붙일 수 있는 구조

### 13.4 Ops 화면
최소한 아래를 볼 수 있어야 한다.

- 최근 feature build 성공/실패
- 최근 label build 성공/실패
- 최근 ranking materialization 성공/실패
- 마지막 `run_id`
- 최신 `feature_version`, `ranking_version`

---

## 14. 추천 파일/모듈 구조

아래는 권장안이며, foundation 설계에 맞게 세부 경로는 조정 가능하다.

```text
app/
  features/
    builders/
      price_features.py
      liquidity_features.py
      fundamentals_features.py
      news_features.py
      quality_features.py
      market_regime_features.py
    feature_store.py
    normalization.py
  labels/
    forward_returns.py
  regime/
    snapshot.py
    classifier.py
  ranking/
    explanatory_score.py
    grade_assignment.py
    reason_tags.py
    validation.py
  ui/
    pages/
      2_Leaderboard.py
      3_Research.py
      4_Ops.py
scripts/
  build_feature_store.py
  build_forward_labels.py
  build_market_regime_snapshot.py
  materialize_explanatory_ranking.py
  validate_explanatory_ranking.py
tests/
  features/
  labels/
  ranking/
```

---

## 15. 테스트 요구사항

최소한 아래 테스트는 포함한다.

### 15.1 feature 계산 테스트
- synthetic OHLCV 입력으로 수익률/이동평균/변동성 계산이 기대값과 맞는지
- window 부족 시 null/안전처리가 맞는지
- 뉴스/재무 feature aggregation 이 날짜 기준으로 올바른지

### 15.2 label 누수 방지 테스트
- `as_of_date` 와 entry/exit date 계산이 trading calendar 기준으로 맞는지
- D+1, D+5 label 이 next open 기준으로 생성되는지
- 미래 데이터 부족 시 unavailable 처리되는지

### 15.3 ranking 테스트
- missing flow component 가 점수 계산을 깨지 않는지
- active component set 이 json 에 저장되는지
- grade assignment 가 재현 가능한지
- critical risk flag 가 있는 경우 `A` 로 올라가지 않는지

### 15.4 idempotency 테스트
- 동일 날짜 재실행 시 row 중복이 생기지 않는지
- run manifest 는 새로 남되 curated row 는 중복되지 않는지

---

## 16. 완료 기준 (Definition of Done)

아래를 모두 만족해야 이번 티켓이 완료된 것으로 본다.

1. `python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100`
   - 최신 feature snapshot 이 생성된다.
2. `python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100`
   - D+1 / D+5 label 이 생성된다.
3. `python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06`
   - regime snapshot 이 생성된다.
4. `python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
   - ranking table 이 생성된다.
5. `python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5`
   - 최소 sanity validation 결과가 나온다.
6. `streamlit run app/ui/Home.py`
   - Leaderboard / Research / Ops 에 최근 결과가 보인다.
7. README 에 아래가 추가된다.
   - feature 정의
   - label 정의
   - next open 기준 이유
   - regime state 규칙
   - score v0 가 explanatory layer 임을 명시
   - known limitations

---

## 17. 이번 티켓에서 하지 말아야 할 것

Codex는 아래를 하지 않는다.

- 실제 예측 모델까지 욕심내지 말 것
- 아직 없는 flow 데이터를 추정해서 있는 척 넣지 말 것
- uncertainty / disagreement 를 대충 임의 수치로 흉내내지 말 것
- intraday 미시구조까지 확장하지 말 것
- 점수 산출식을 지나치게 복잡하게 만들지 말 것
- label 정의를 종가 기준으로 모호하게 두지 말 것
- README 없이 코드만 남기지 말 것

이번 티켓의 핵심은 **정확한 정의 + 재현 가능한 스냅샷 + 설명 가능한 랭킹** 이다.

---

## 18. 실행 예시

```bash
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06
python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
streamlit run app/ui/Home.py
```

---

## 19. 작업 후 Codex가 정리해서 남겨야 할 것

작업 완료 후 아래를 간단히 정리한다.

1. 새로 추가된 파일 목록
2. 생성된/변경된 DuckDB 테이블 및 view 목록
3. feature group 목록과 핵심 계산식 요약
4. label 정의 요약
5. ranking version 및 active/reserved component 목록
6. 실행 순서
7. validation 결과를 확인하는 방법
8. 아직 남은 TODO
9. 다음 티켓(TICKET-004) 진입 전 주의사항

---

## 20. 다음 티켓을 위한 준비 상태

이번 티켓이 끝나면 다음 티켓에서는 자연스럽게 아래 중 하나로 이어질 수 있어야 한다.

- 수급/프로그램/공매도/기관/외국인 기반 **Flow Layer** 추가
- uncertainty/disagreement/implementation penalty 기반 **실제 selection engine** 추가
- Discord 장후 요약 리포트 초안 연결
- D+1 / D+5 사후 evaluation report 연결

즉, 이번 티켓 산출물은 다음 티켓의 발판이어야 한다.

