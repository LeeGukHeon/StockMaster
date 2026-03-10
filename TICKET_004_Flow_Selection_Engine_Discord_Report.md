# TICKET-004 — 수급/Flow Layer + Selection Engine v1 + Discord 장후 리포트 초안

- 문서 목적: TICKET-003 이후, Codex가 바로 이어서 구현할 **수급 활성화 계층 + 실사용에 가까운 선별 엔진 + Discord 장후 리포트 초안**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
- 우선순위: 최상
- 기대 결과: “설명용 랭킹 v0”에서 한 단계 올라가, **수급(flow) 신호가 활성화되고, 실행가능성과 불확실성 프록시를 반영한 selection engine v1이 동작하며, 장후 Discord 요약 리포트가 자동 생성/전송 가능한 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **리서치 플랫폼을 실제 매일 보는 도구로 바꾸는 첫 리포팅/선별 계층**이다.

즉, 이번 티켓의 목표는 아래 다섯 가지를 안정적으로 만드는 것이다.

1. 종목 단위 **수급(flow) 데이터**가 적재된다.
2. TICKET-003에서 reserved 였던 `flow_score` 가 **정식 활성화**된다.
3. 설명용 랭킹 v0 위에 **Selection Engine v1** 이 올라간다.
4. ML이 아직 없더라도 **경험적(calibrated) 기대밴드**를 생성할 수 있다.
5. 매일 장 마감 후 **Discord 장후 요약 리포트 초안**을 자동 생성하고 선택적으로 전송할 수 있다.

이 티켓이 끝나면 다음 티켓에서는 사후 평가 리포트, 예측 엔진 고도화, 후보군 장중 보조분석으로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 Investor Flow 적재 파이프라인
다음이 가능한 상태를 만든다.

- `vw_universe_active_common_stock` 기준 전 종목 또는 지정 종목군에 대해 **일자별 수급(flow)** 데이터를 적재한다.
- 최소한 **외국인, 기관, 프로그램** 계열의 순매수/순매도 관련 값을 저장한다.
- source field availability 가 종목/일자/주체별로 다를 수 있음을 고려하여, **nullable + explicit coverage flag** 구조로 저장한다.
- raw payload 보존, curated 적재, run manifest 기록이 동작한다.
- 동일 날짜/동일 종목 재실행 시 중복 없이 idempotent 하게 갱신된다.

### 2.2 Flow Feature + Flow Score 활성화
다음이 가능한 상태를 만든다.

- TICKET-003의 feature store 가 flow 데이터를 받아 **flow 관련 파생 피처**를 생성한다.
- `flow_score` 가 reserved 상태에서 **active component** 로 바뀐다.
- flow score 는 단순 raw 금액 비교가 아니라 **turnover/유동성 대비 정규화된 상대 강도**를 반영한다.
- flow coverage 가 부족한 종목은 억지 점수 계산 대신 **neutral / partial / unavailable** 처리가 가능해야 한다.
- UI와 README에 “flow data availability 수준” 이 드러나야 한다.

### 2.3 Selection Engine v1
다음이 가능한 상태를 만든다.

- TICKET-003의 explanatory ranking v0 와 별개로 **selection engine v1** 이 동작한다.
- selection engine v1 은 **ML alpha model 이 아니라 rule-based / calibration-based 연구 엔진** 임을 분명히 한다.
- 최소한 아래 요소를 반영한다.
  - explanatory score v0 또는 그 percentile
  - flow score
  - regime fit
  - uncertainty proxy
  - implementation penalty
- 결과로 `final_selection_value`, `grade`, `grade_detail`, `selection component summary` 가 생성된다.
- Leaderboard 는 기본적으로 selection engine v1 결과를 우선 노출한다.

### 2.4 Proxy Prediction Band / Calibration Layer
다음이 가능한 상태를 만든다.

- 아직 정식 ML 예측 모델이 없더라도, **과거 label history 기반 calibration** 으로 D+1 / D+5 기대밴드를 산출한다.
- 현재 점수 또는 selection percentile 과 과거 실제 초과수익률 관계를 이용해 **proxy expected excess return** 을 계산한다.
- `fact_prediction` 또는 동등한 저장 계약에 결과를 남긴다.
- `disagreement_score` 는 아직 없으면 null / unavailable 로 두고 절대로 가짜 값을 만들지 않는다.
- `uncertainty_score` 는 예측모형 불확실성이 아니라 **proxy uncertainty score** 임을 코드/README에 명시한다.

### 2.5 Discord 장후 리포트 초안
다음이 가능한 상태를 만든다.

- 최신 장세, 탑 랭킹 종목, 주요 뉴스, 리스크 메모를 담은 **Discord용 요약 리포트 payload** 가 생성된다.
- webhook 전송 전에 **dry-run/local preview** 가 가능해야 한다.
- Discord 전송은 `.env` 설정에 따라 on/off 가능해야 한다.
- payload 가 너무 길어질 경우를 대비해 **chunking 또는 multi-message strategy** 가 있어야 한다.
- 실패 시 앱 전체가 죽지 않고, Ops 에 전송 실패 기록만 남겨야 한다.

### 2.6 UI / Ops 가시성 강화
다음이 가능해야 한다.

- Leaderboard 에서 flow score, uncertainty proxy, implementation penalty, final selection value 를 확인할 수 있다.
- Stock Workbench 에서 개별 종목의 flow strip / reason tags / risk flags / 밴드 요약을 볼 수 있다.
- Market Pulse 에서 시장 전반 flow breadth 와 regime 요약을 볼 수 있다.
- Ops 페이지에서 마지막 selection run, calibration run, Discord 전송 성공/실패 여부를 볼 수 있다.

### 2.7 스크립트 / 엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/sync_investor_flow.py`
- `scripts/backfill_investor_flow.py`
- `scripts/materialize_selection_engine_v1.py`
- `scripts/calibrate_proxy_prediction_bands.py`
- `scripts/render_discord_eod_report.py`
- `scripts/publish_discord_eod_report.py`
- `scripts/validate_selection_engine_v1.py`

참고:
- `build_feature_store.py` 는 이번 티켓에서 flow feature 를 읽을 수 있도록 확장되어야 한다.
- `materialize_explanatory_ranking.py` 는 깨지지 않아야 하며, 필요 시 selection engine과 나란히 공존해야 한다.

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- 종목 단위 investor flow 적재
- flow feature 생성 및 score 활성화
- uncertainty proxy / implementation penalty 도입
- selection engine v1 구축
- calibration 기반 proxy prediction band 산출
- `fact_prediction` / `fact_ranking` 확장 또는 동등한 저장 계약 구현
- Discord 장후 리포트 payload 렌더러 및 webhook publisher
- Market Pulse / Leaderboard / Stock Workbench / Ops 확장
- README / 실행 가이드 갱신
- 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- 정식 ML alpha model 학습 및 예측
- transformer / boosting ensemble / uncertainty model 학습
- model disagreement 의 진짜 추정
- 장중 1분봉/체결/호가 기반 실시간 selection
- HTML/PDF 완성형 리포트 엔진
- D+1 / D+5 사후 평가 리포트 본편
- 자동매매 주문 실행 기능
- 뉴스 본문 전문 저장/배포

즉, 이번 티켓은 **실전형 선별과 장후 요약 리포트의 첫 운영형 버전** 까지다.

---

## 4. Codex가 작업 시작 전에 반드시 확인할 것

Codex는 작업 시작 전에 아래 순서를 따른다.

1. 루트 경로 `D:\MyApps\StockMaster` 기준으로 현재 저장소 상태를 확인한다.
2. 아래 문서를 먼저 읽는다.
   - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
   - `TICKET_000_Foundation_and_First_Work_Package.md`
   - `TICKET_001_Universe_Calendar_Provider_Activation.md`
   - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
   - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
   - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
   - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
   - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
3. TICKET-003이 불완전하다면, 이번 티켓 수행에 직접 필요한 blocking issue 만 보완한다.
4. 기존 foundation / curated schema / feature store / label 정의를 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 “가짜 ML” 금지
이번 티켓은 selection engine v1 이지만, 아직 정식 예측모델 티켓이 아니다.

따라서 아래를 반드시 지킨다.

- ML prediction 인 척하는 naming/README 설명을 쓰지 않는다.
- `expected_excess_return` 이 필요하다면 **calibrated proxy** 임을 명시한다.
- `disagreement_score` 는 없으면 null 로 둔다.
- uncertainty 는 **proxy uncertainty** 라고 적는다.

즉, 아직 없는 모델을 있는 것처럼 꾸미지 않는다.

### 5.2 Flow 활성화는 “금액 원시값”이 아니라 “상대 강도” 중심
외국인/기관/프로그램 raw 순매수 금액은 종목 크기 차이의 영향을 강하게 받는다.

따라서 flow score 는 다음 성질을 가져야 한다.

- turnover/ADV 대비 정규화
- cross-sectional rank 기반 비교
- 연속 순매수/순매도 지속성 고려
- 주체간 같은 방향 정렬 여부 고려

### 5.3 실행가능성(Implementation) 패널티는 필수
지금 장세처럼 변동성이 큰 구간에서는 “오를 것 같은 종목”보다 “실제로 들어갈 수 있는 종목”이 중요하다.

따라서 selection engine v1 은 최소한 아래를 penalty 로 고려해야 한다.

- 최근 평균 거래대금 부족
- 거래대금 burst 대비 평소 유동성 취약
- stale / missing data
- 지나친 변동성, 잦은 gap
- guardrail warning

### 5.4 calibration 은 과거 label history 를 사용한다
이번 티켓의 기대밴드는 점수에서 바로 임의 환산하는 방식보다, **최근 과거 구간의 score bin ↔ realized return 관계**를 쓰는 것이 더 낫다.

따라서 권장 방향은 아래와 같다.

- trailing window (예: 최근 60 거래일 또는 가능한 범위)
- horizon 별 분리
- score percentile/decile/bin 기준
- 해당 bin 의 realized excess return 분포에서 q25/q50/q75 추출
- 현재 종목의 bin 에 맞는 calibrated proxy band 제공

### 5.5 Discord 리포트는 “요약본”이어야 한다
Discord 는 전체 대시보드를 대체하지 않는다.

따라서 아래를 지킨다.

- 장황한 전문 보고서를 보내지 않는다.
- 헤더 + 시장요약 + 상위 종목 + 뉴스 + 리스크 메모 위주로 구성한다.
- 너무 긴 payload 는 나누어 보낸다.
- 링크/경로가 있으면 함께 제공할 수 있게 구조를 남긴다.

### 5.6 idempotent + dry-run first
- flow sync, selection materialization, Discord render/publish 모두 재실행 가능해야 한다.
- publisher 는 반드시 dry-run 모드를 지원해야 한다.
- 테스트/개발 단계에서 실제 webhook 전송 없이 payload 확인이 가능해야 한다.

---

## 6. Investor Flow 데이터 레이어 요구사항

## 6.1 목적
`fact_investor_flow` 는 TICKET-003에서 reserved 였던 `flow_score` 를 실질적으로 활성화하는 핵심 입력이다.

이 데이터는 이후 아래 티켓의 기반이 된다.

- flow score
- selection engine v1
- 장세 해석의 flow breadth
- 후보군 장중 보조분석의 prior signal
- 사후 평가에서 “왜 이 종목이 올라왔는가” 설명

## 6.2 최소 저장 계약
권장 테이블: `fact_investor_flow`

권장 최소 컬럼:

- `run_id`
- `trading_date`
- `symbol`
- `foreign_net_buy_value`
- `institution_net_buy_value`
- `program_net_buy_value`
- `flow_data_coverage_json`
- `source`
- `ingested_at`

추가 컬럼 허용:

- `foreign_net_buy_volume`
- `institution_net_buy_volume`
- `program_net_buy_volume`
- `retail_net_buy_value`
- `other_financial_net_buy_value`
- `notes_json`

중요:
- 소스에서 없는 값은 억지로 0으로 채우지 않는다.
- null 과 실제 0을 혼동하면 안 된다.
- `flow_data_coverage_json` 으로 어떤 주체가 실제로 존재하는지 기록한다.

## 6.3 수집 범위
기본 수집 범위:

- 대상: `vw_universe_active_common_stock`
- 시장: KOSPI, KOSDAQ
- 실행 모드:
  - 단일 거래일 적재
  - 날짜 범위 백필
  - 개발용 subset / limit-symbols

## 6.4 raw / curated 저장 경로 권장안
예시:

```text
data/raw/kis/investor_flow/trading_date=2026-03-06/symbol=005930/*.json
data/curated/market/investor_flow/trading_date=2026-03-06/*.parquet
```

원칙:
- raw 는 provider 원문 payload 중심
- curated 는 정규화된 종목 x 일자 row 중심
- run manifest 에 coverage / success / failure / missing participant 정보를 남긴다.

## 6.5 optional short activity
`fact_short_interest_or_short_activity` 는 이번 티켓의 **권장 추가 범위** 이지만 blocking requirement 는 아니다.

원칙:
- 공식 source availability 가 명확할 때만 붙인다.
- 불안정하거나 불완전하면 schema placeholder 만 두고 본격 활성화는 후속 티켓으로 넘긴다.
- short signal 이 없다고 해서 selection engine v1 의 완료를 막지 않는다.

---

## 7. Flow Feature / Flow Score 요구사항

### 7.1 목적
Flow score 는 이번 티켓에서 처음으로 정식 활성화되는 selection 핵심 축이다.

이 점수는 아래를 반영해야 한다.

- 누가 사고 있는가
- 얼마나 강하게 사고 있는가
- 거래대금 대비 의미 있는 강도인가
- 하루짜리 잡음인가, 며칠 지속되는가
- 주체들이 같은 방향인가, 엇갈리는가

### 7.2 최소 flow feature 후보
최소 구현 후보:

- `foreign_flow_ratio_1d`
- `institution_flow_ratio_1d`
- `program_flow_ratio_1d`
- `foreign_flow_ratio_5d`
- `institution_flow_ratio_5d`
- `program_flow_ratio_5d`
- `flow_total_ratio_1d`
- `flow_total_ratio_5d`
- `flow_alignment_score`
- `foreign_buy_streak_5d`
- `institution_buy_streak_5d`
- `flow_coverage_flag`
- `flow_confidence_score`

권장 계산 방향:

- `flow_ratio_*` = `net_buy_value / turnover_value` 또는 `net_buy_value / adv_20`
- `flow_alignment_score` = 외국인/기관/프로그램 주체의 방향 일치 정도
- `buy_streak_*` = 최근 N일 연속 순매수 여부
- `flow_confidence_score` = coverage + data freshness + null density 기반

### 7.3 normalization 원칙
- 동일 `as_of_date` 기준 cross-sectional rank 를 사용한다.
- extreme value 는 clip 또는 winsorize 가능하다.
- 주체별 coverage 가 없는 경우 해당 서브피처는 neutral 처리한다.
- flow score 전체를 null 로 버리는 대신, **coverage-aware partial score** 를 허용한다.

### 7.4 flow score 계산 원칙
최소 원칙:

- 최종 `flow_score` 는 0~100 범위로 산출한다.
- 높은 양(+)의 지속적 순매수는 가점 요인이다.
- 상충되는 주체 흐름은 점수 상한을 낮춘다.
- coverage 가 너무 낮으면 neutral 또는 낮은 confidence 로 처리한다.
- 점수 산식과 사용 feature 목록은 README에 명시한다.

### 7.5 TICKET-003 explanatory ranking 과의 연결
이번 티켓부터 `flow_score` 는 reserved 가 아니라 active component 다.

권장 처리:

- explanatory score 계산 시 `flow_score` 를 활성화한 `v1` 또는 equivalent version 을 도입한다.
- v0 결과는 재현성 확보를 위해 깨지지 않게 남긴다.
- UI에는 어떤 ranking/selection version 이 사용되었는지 표시한다.

---

## 8. Selection Engine v1 요구사항

## 8.1 목적
Selection engine v1 은 “보기 좋은 설명용 점수”에서 한 단계 올라가, **실전 매수 후보군 우선순위를 정하는 운영용 점수**를 만든다.

하지만 아직 정식 ML 모델은 없으므로, 이번 티켓의 selection engine v1 은 아래 성격을 가진다.

- rule-based / calibration-based
- 연구 재현 가능
- explanation friendly
- implementation aware
- uncertainty aware

## 8.2 권장 구성요소
최소 구성요소:

- `explanatory_score_v1_pct`
- `flow_score`
- `regime_fit_score`
- `uncertainty_proxy_score`
- `implementation_penalty_score`
- `data_confidence_score` (선택)

### 8.2.1 uncertainty proxy score
권장 프록시 입력:

- `realized_vol_20d`
- `gap_abs_avg_20d`
- `drawdown_20d`
- `news_burst_unstable_proxy`
- `missing_key_feature_count`
- `flow_confidence_score` 역방향

원칙:
- 이것은 prediction interval uncertainty 가 아니라 **selection risk proxy** 다.
- 값이 높을수록 더 불확실한 종목으로 해석한다.

### 8.2.2 implementation penalty score
권장 프록시 입력:

- `adv_20` 역방향
- `liquidity_rank_pct` 역방향
- `turnover_value_ma_20` 역방향
- `stale_price_flag`
- `flow_coverage_flag` 경고
- `eligible_flag` 경고

원칙:
- 값이 높을수록 실제 운용상 불리한 종목이다.
- 이 점수는 최종 합산에서 감점한다.

## 8.3 selection value 계산 원칙
권장 방향:

1. positive component 를 0~100 범위에서 합성한다.
2. penalty component 를 별도로 계산한다.
3. 최종 `final_selection_value` 는 양수 요인 - 페널티 구조로 만든다.
4. 결과를 percentile 로 재정렬하여 grade 를 부여한다.

권장 예시:

### D+1 selection v1
- `explanatory_score_v1_pct`: 35
- `flow_score`: 25
- `regime_fit_score`: 10
- `uncertainty_proxy_score`: -10
- `implementation_penalty_score`: -20

### D+5 selection v1
- `explanatory_score_v1_pct`: 45
- `flow_score`: 20
- `regime_fit_score`: 10
- `uncertainty_proxy_score`: -10
- `implementation_penalty_score`: -15

중요:
- 위 가중치는 starting point 이며 설정 가능해야 한다.
- code/config 에서 읽히도록 만들고 하드코딩을 최소화한다.
- active component set 을 결과물에 저장한다.

## 8.4 grade 와 grade detail
최소 등급:

- `A`
- `A-`
- `B`
- `C`

추가 상세 등급 권장:

- `A_stable`
- `A_catalyst`
- `B_watch`
- `C_exclude`

권장 기준:
- `A_stable`: 상위권 + implementation penalty 낮음 + uncertainty 낮음
- `A_catalyst`: 상위권 + 기대값은 높지만 뉴스/변동성 민감도 큼
- `B_watch`: 관찰 우선
- `C_exclude`: data/implementation/risk 이슈 또는 점수 미달

### 8.4.1 top reason tags / risk flags
최소한 아래를 남긴다.

- `top_reason_tags_json`
- `risk_flags_json`
- `selection_components_json`

예시 reason tags:
- `foreign_buying`
- `institution_buying`
- `trend_strong`
- `turnover_expanding`
- `fresh_catalyst`
- `quality_support`
- `risk_on_fit`

예시 risk flags:
- `high_volatility`
- `gap_risk`
- `thin_liquidity`
- `missing_flow`
- `stale_data`
- `news_burst`

## 8.5 권장 저장 계약
권장 테이블 사용 방향:

### `fact_prediction`
이번 티켓에서는 아래처럼 사용 가능하다.

- `model_name = selection_calibration_v1`
- `expected_excess_return` = calibrated proxy expected excess return
- `lower_band` = calibrated q25
- `median_band` = calibrated q50
- `upper_band` = calibrated q75
- `disagreement_score` = null 허용
- `uncertainty_score` = proxy uncertainty score
- `implementation_penalty` = implementation penalty score
- `regime_fit_score` = regime fit score

중요:
- 이것은 정식 ML forecast table 이 아니라, **calibrated proxy prediction contract** 로 먼저 사용한다.
- README에 반드시 이 사실을 명시한다.

### `fact_ranking`
최소한 아래가 반영되어야 한다.

- `final_selection_value`
- `grade`
- `top_reason_tags_json`
- `risk_flags_json`
- `explanatory_score_json`

비파괴적 추가 컬럼 허용:

- `ranking_version`
- `selection_engine_version`
- `grade_detail`
- `selection_components_json`
- `eligible_flag`
- `report_candidate_flag`
- `created_at`

## 8.6 calibration layer 요구사항
권장 방식:

- lookback window: 최근 40~120 거래일 중 가능한 범위
- binning: selection percentile 또는 decile
- target: `excess_forward_return`
- horizon 분리: D+1 / D+5
- market scope 분리 권장: KOSPI / KOSDAQ 또는 KR_ALL

최소 산출물:

- 각 horizon 의 bin 별
  - count
  - mean excess return
  - median excess return
  - q25
  - q75
  - std (가능 시)

현재 종목의 selection percentile 이 속한 bin 의 분포를 가져와 `fact_prediction` 을 채운다.

중요:
- calibration sample size 가 너무 작으면 fallback 규칙이 있어야 한다.
- 예: finer bin 부족 시 coarser bin, 그래도 부족하면 horizon 전체 중앙값.

## 8.7 guardrail / eligibility
selection engine v1 은 최소한 아래를 고려한다.

- 최근 20일 평균 거래대금 하한
- 핵심 feature 누락 수 상한
- label unavailable 은 현재 ranking 과 직접 관계 없지만 calibration validation 에서 제외
- flow coverage 가 극단적으로 부족하면 `A` 등급 제한 가능
- stale or suspicious data 는 report candidate 에서 제외 가능

guardrail 결과는 저장되어야 한다.

---

## 9. Discord 장후 리포트 초안 요구사항

## 9.1 목적
Discord 리포트는 매일 장후에 **“오늘 시장이 어땠고, 내일/향후 며칠 볼 후보군이 무엇인지”** 빠르게 파악하는 요약본이다.

따라서 긴 전문 문서를 그대로 보내는 것이 아니라, 핵심만 보이게 만들어야 한다.

## 9.2 기본 섹션
최소 섹션:

1. 헤더
   - 기준일
   - 시장 라벨 / regime
   - 간단한 한 줄 코멘트

2. 시장 요약
   - breadth up/down
   - median return
   - turnover burst 여부
   - 시장 전반 flow 요약

3. 상위 selection 후보
   - 상위 5~10 종목
   - grade / grade detail
   - 핵심 이유 태그
   - D+1 / D+5 밴드 요약

4. 주요 뉴스/이슈
   - 3~5개
   - 제목 + publisher + 링크
   - 너무 긴 본문 금지

5. 리스크 메모
   - 과열/공포/변동성/유동성 주의사항

## 9.3 메시지 생성 원칙
- 제목/헤더는 짧고 명확해야 한다.
- 종목 줄마다 정보량이 과도하지 않게 한다.
- D+1 / D+5 밴드는 “과도한 정밀도”를 피한다.
- 뉴스는 링크 중심으로 간결하게 넣는다.
- payload 가 길면 여러 메시지로 나눈다.

## 9.4 dry-run / preview 필수
최소 요구사항:

- 실제 webhook 전송 없이 payload preview 가능
- preview 결과를 파일 또는 콘솔로 저장/출력 가능
- 실패 시 어디서 깨졌는지 로그에 남아야 한다.

권장 산출물 예시:

```text
data/reports/discord/as_of_date=2026-03-06/payload_preview.json
data/reports/discord/as_of_date=2026-03-06/message_01.txt
```

## 9.5 publisher 동작 원칙
- `.env` 에 webhook URL 이 없으면 publish 를 skip 하고 warning 만 남긴다.
- `DISCORD_REPORT_ENABLED=false` 면 render 만 하고 전송하지 않는다.
- 네트워크 오류/HTTP 오류 시 run manifest 와 ops log 에 실패 상태를 남긴다.
- publisher 실패가 selection run 전체 실패로 번지지 않게 한다.

---

## 10. UI / Ops 요구사항

### 10.1 Market Pulse
최소 추가 항목:

- 시장 regime state
- flow breadth summary
- 외국인/기관/프로그램 총합 또는 요약
- risk_on / risk_off 해석 코멘트

### 10.2 Leaderboard
최소 추가 항목:

- ranking version / selection engine version
- `final_selection_value`
- `flow_score`
- `uncertainty_proxy_score`
- `implementation_penalty_score`
- `grade_detail`
- D+1 / D+5 밴드 요약

### 10.3 Stock Workbench
최소 추가 항목:

- flow strip / 최근 1D, 5D flow 요약
- selection component breakdown
- reason tags
- risk flags
- calibrated band summary

### 10.4 Ops
최소 추가 항목:

- 마지막 investor flow sync 상태
- selection engine v1 최근 실행 상태
- calibration run 상태
- Discord render/publish 상태
- 최근 실패 로그 요약

---

## 11. 추천 파일 / 모듈 구조

아래는 권장 구조이며, 기존 구조에 맞게 약간 조정 가능하다.

```text
app/
  domain/
    flow/
      models.py
      service.py
      scoring.py
    selection/
      engine_v1.py
      calibration.py
      bands.py
    report/
      discord_renderer.py
      discord_publisher.py
  providers/
    kis/
      investor_flow.py
  ui/
    pages/
      Leaderboard.py
      Market_Pulse.py
      Stock_Workbench.py
      Ops.py
scripts/
  sync_investor_flow.py
  backfill_investor_flow.py
  materialize_selection_engine_v1.py
  calibrate_proxy_prediction_bands.py
  render_discord_eod_report.py
  publish_discord_eod_report.py
  validate_selection_engine_v1.py
tests/
  test_investor_flow_sync.py
  test_flow_score.py
  test_selection_engine_v1.py
  test_calibration_proxy_bands.py
  test_discord_renderer.py
```

중요:
- 기존 이름/구조와 충돌하면 가장 덜 파괴적인 방식으로 맞춘다.
- provider/service/domain/ui 책임이 섞이지 않게 한다.

---

## 12. 테스트 요구사항

최소 테스트 범위:

1. investor flow provider parsing
   - null vs 0 구분
   - coverage flag 생성
   - idempotent 적재

2. flow score
   - 동일한 raw flow 에 대해 재현성 유지
   - coverage 부족 시 neutral/partial 처리
   - alignment/streak 계산 sanity

3. selection engine v1
   - final selection value 계산 일관성
   - penalty 증가 시 점수 하락 sanity
   - grade / grade detail 부여 로직

4. calibration proxy bands
   - sample 부족 fallback
   - horizon 별 분리
   - q25 <= q50 <= q75 관계 유지

5. Discord renderer/publisher
   - dry-run payload 생성
   - 긴 payload 분할 로직
   - webhook 설정 없음 처리

---

## 13. 완료 기준 (Definition of Done)

아래가 가능하면 이번 티켓은 완료로 본다.

1. `python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 100`
2. `python scripts/backfill_investor_flow.py --start 2026-02-17 --end 2026-03-06 --limit-symbols 100`
3. `python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100`
4. `python scripts/materialize_selection_engine_v1.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
5. `python scripts/calibrate_proxy_prediction_bands.py --start 2026-01-05 --end 2026-03-06 --horizons 1 5`
6. `python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run`
7. `python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run`
8. `python scripts/validate_selection_engine_v1.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5`
9. `streamlit run app/ui/Home.py`

그리고 아래가 확인되어야 한다.

- `fact_investor_flow` 또는 동등한 curated 저장소에 데이터가 적재된다.
- `flow_score` 가 active component 로 반영된다.
- selection engine v1 결과가 저장된다.
- calibrated proxy band 가 생성된다.
- Discord payload preview 가 생성된다.
- UI에서 selection 관련 필드가 보인다.
- README가 갱신된다.
- 테스트가 추가된다.

---

## 14. 이번 티켓에서 하지 말아야 할 것

- flow 데이터가 없는데 임의 0값/더미값으로 점수를 만드는 것
- disagreement score 를 가짜로 채우는 것
- calibrated proxy band 를 “정식 예측”처럼 설명하는 것
- Discord 전송 실패를 앱 전체 실패로 취급하는 것
- 뉴스 본문 전문을 대량 저장/전송하는 것
- 장중 실시간 시스템까지 한 번에 확장하는 것
- selection engine v1 과 explanatory ranking v0 의 차이를 문서화하지 않는 것

---

## 15. 실행 예시

```bash
python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 100
python scripts/backfill_investor_flow.py --start 2026-02-17 --end 2026-03-06 --limit-symbols 100
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/materialize_selection_engine_v1.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/calibrate_proxy_prediction_bands.py --start 2026-01-05 --end 2026-03-06 --horizons 1 5
python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/validate_selection_engine_v1.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
streamlit run app/ui/Home.py
```

---

## 16. 작업 후 Codex가 정리해서 남겨야 할 것

작업 완료 후 아래를 간단히 정리해 남긴다.

1. 새로 추가된 파일 목록
2. 생성/변경된 table / parquet / view 목록
3. investor flow source coverage 설명
4. flow score 계산 개요
5. selection engine v1 계산 개요
6. calibration band 생성 규칙
7. Discord payload 확인 방법
8. 남아 있는 known limitations
9. TICKET-005 진입 전 주의사항

---

## 17. 다음 티켓을 위한 준비 상태

이번 티켓이 끝나면 다음 티켓으로 아래 중 하나로 자연스럽게 이어질 수 있어야 한다.

### 후보 A — TICKET-005 Evaluation / After-the-Fact Review
- 전일 selection 결과 vs 실제 결과 비교
- 실패 원인 자동 분해
- 등급/밴드 calibration 체크
- D+1 / D+5 daily review report

### 후보 B — TICKET-005 Candidate Intraday Layer
- 상위 후보군 전용 1분봉/체결/호가 요약
- D+1 전술 진입 타이밍 보조
- intraday veto / re-rank layer

어느 쪽으로 가더라도, 이번 티켓에서 아래가 확보되어 있어야 한다.

- flow signal 이 정식 데이터로 저장됨
- selection 결과가 재현 가능하게 저장됨
- Discord 리포트가 daily routine 으로 연결 가능함
- proxy band 와 실제 결과를 다음 티켓에서 비교할 수 있음
