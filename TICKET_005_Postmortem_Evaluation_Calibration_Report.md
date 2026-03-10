# TICKET-005 — 사후 평가 리포트 + Selection 결과 vs 실제 결과 비교 + Calibration 검증

- 문서 목적: TICKET-004 이후, Codex가 바로 이어서 구현할 **사후 평가 파이프라인 + 예측/선별 결과의 실제 성과 검증 + calibration 품질 진단 + 장후 postmortem 리포트**의 범위와 완료 기준을 오해 없이 이해하도록 만드는 실행 문서
- 문서 버전: v1.0
- 기준 문서:
  - `KR_Stock_Research_Platform_v1_Implementation_Spec.md`
  - `TICKET_000_Foundation_and_First_Work_Package.md`
  - `TICKET_001_Universe_Calendar_Provider_Activation.md`
  - `TICKET_002_Daily_OHLCV_Fundamentals_News_Metadata.md`
  - `TICKET_003_Feature_Store_Labels_Explanatory_Ranking.md`
  - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
  - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
  - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
  - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
  - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
  - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
- 전제 상태:
  - TICKET-000 foundation 실행 가능
  - TICKET-001 universe/calendar/provider activation 완료
  - TICKET-002 core research data ingestion 완료
  - TICKET-003 feature store / labels / explanatory ranking v0 완료
  - TICKET-004 flow layer / selection engine v1 / Discord 장후 리포트 초안 완료
- 우선순위: 최상
- 기대 결과: **추천 당시의 판단과 실제 결과를 같은 기준으로 비교하고, D+1/D+5 성과를 매일 자동 평가하며, calibration 품질과 실패 원인을 보고서 형태로 남길 수 있는 상태**

---

## 1. 이번 티켓의 한 줄 정의

이번 티켓은 **“추천을 잘 만들었다”를 넘어서, 추천이 실제로 맞았는지·어디서 틀렸는지·다음에 무엇을 고쳐야 하는지”를 남기는 첫 검증 계층**이다.

즉, 이번 티켓의 목표는 아래 여섯 가지를 안정적으로 만드는 것이다.

1. selection/prediction 의 **만기 도달(realized outcome)** 을 일관된 규칙으로 적재한다.
2. 종목 단위와 코호트 단위에서 **실제 결과 vs 예측/밴드**를 비교한다.
3. selection engine v1 과 explanatory ranking v0 의 **증분 가치**를 비교한다.
4. calibrated proxy band 의 **coverage / bias / monotonicity** 를 검증한다.
5. 장 마감 후 **사후 평가(postmortem) 리포트**를 자동 생성하고 선택적으로 Discord 로 전송한다.
6. 실패 종목/실패 패턴/보완 포인트를 구조화해 **다음 티켓의 개선 backlog** 로 남긴다.

이 티켓이 끝나면 다음 티켓에서는 실제 ML alpha model, 장중 보조분석, 고도화된 uncertainty/disagreement, 전략 실험 관리로 넘어갈 수 있어야 한다.

---

## 2. 이번 티켓에서 반드시 끝내야 하는 것

### 2.1 Matured outcome materialization
다음이 가능한 상태를 만든다.

- `fact_ranking` / `fact_prediction` 의 과거 selection cohort 중, horizon 이 만기 도달한 건을 식별할 수 있어야 한다.
- TICKET-003 에서 정의한 라벨 규칙과 동일한 기준으로 realized outcome 을 계산해야 한다.
  - entry 기준: `selection_date` 다음 거래일 시가(next open)
  - exit 기준: horizon 에 해당하는 미래 거래일 종가(future close)
- `realized_return` 와 `realized_excess_return` 를 함께 저장한다.
- 아직 만기 전인 건은 평가 대상에 포함하지 말고 `pending` 상태로 구분할 수 있어야 한다.
- 동일 cohort 를 재실행해도 idempotent 하게 갱신된다.
- evaluation 에 사용된 source date / label snapshot / ranking snapshot 이 run manifest 와 연결되어야 한다.

### 2.2 Prediction vs realized 비교 계층
다음이 가능한 상태를 만든다.

- selection 시점에 저장된 calibrated proxy prediction band 와 실제 realized excess return 을 같은 row 에서 비교할 수 있어야 한다.
- 최소한 아래 비교값을 저장한다.
  - predicted expected excess return
  - lower / median / upper band
  - realized excess return
  - prediction error
  - band hit 여부
  - lower below / in-band / above upper 상태
  - 방향성(hit) 여부
- 중요한 점은 **평가 시점에 다시 예측을 새로 계산하지 않고**, selection 당시 스냅샷을 기준으로 비교해야 한다.
- selection 당시 band 가 없던 row 는 그 사실을 명시적으로 남기고, 조용히 제외하지 않는다.

### 2.3 Selection engine v1 성과 평가
다음이 가능한 상태를 만든다.

- selection engine v1 기준의 상위 후보군이 실제로 괜찮았는지 평가할 수 있어야 한다.
- 최소한 아래 집계를 만든다.
  - 전체 eligible 종목
  - report candidate
  - grade 별 (`A`, `A-`, `B`, `C` 또는 실제 grade 체계)
  - top 5 / top 10 / top 20 pseudo-portfolio
  - 시장 구분 (`KOSPI`, `KOSDAQ`, 전체)
- 각 집계에서 최소한 아래 메트릭을 계산한다.
  - mean realized return
  - mean realized excess return
  - median realized excess return
  - hit rate (`realized_excess_return > 0`)
  - positive raw return rate
  - realized band coverage rate
  - above-upper rate
  - below-lower rate
  - count / valid count / pending count
- 결과는 일별 cohort 와 rolling window 집계로 모두 확인 가능해야 한다.

### 2.4 Explanatory ranking v0 대비 증분 비교
다음이 가능한 상태를 만든다.

- selection engine v1 이 explanatory ranking v0 보다 나아졌는지 같은 날짜/같은 universe 기준으로 비교할 수 있어야 한다.
- 최소한 아래 baseline 을 지원한다.
  - explanatory ranking v0 top 10
  - explanatory ranking v0 top 20
  - selection engine v1 top 10
  - selection engine v1 top 20
- 가능하다면 `same-date overlap` 도 계산한다.
  - 겹치는 종목 수
  - 겹치지 않는 종목의 기여도
- 이 비교는 “selection engine 에 flow/penalty/regime 를 추가한 것이 실제로 의미 있었는가”를 보여주는 핵심이므로, README 와 Evaluation UI 에 드러나야 한다.

### 2.5 Calibration diagnostic layer
다음이 가능한 상태를 만든다.

- calibrated proxy band 가 현실과 얼마나 맞는지 점검할 수 있어야 한다.
- 최소한 아래 진단을 구현한다.
  - bin 별 sample count
  - bin 별 realized excess return 분포
  - bin 별 q25/q50/q75 vs 실제 realized 분포 비교
  - in-band coverage rate
  - median bias (`realized - predicted_median`)
  - monotonicity (selection percentile/score bucket 이 높을수록 성과가 개선되는지)
- horizon 별로 분리한다.
  - D+1
  - D+5
- calibration 부족 구간을 식별할 수 있어야 한다.
  - sample too small
  - coverage too low/high
  - optimistic bias
  - pessimistic bias
- calibration diagnostic 결과는 다음 티켓에서 예측 엔진 고도화의 입력이 되므로 별도 저장 계약을 갖는다.

### 2.6 Failure analysis / improvement note 생성
다음이 가능한 상태를 만든다.

- 사후 평가 리포트에서 **왜 틀렸는지** 에 대한 구조적 힌트를 보여줄 수 있어야 한다.
- 최소한 아래 관점의 실패 분석을 제공한다.
  - 고변동성 구간 실패 집중 여부
  - thin liquidity / implementation penalty 높은 종목 실패 집중 여부
  - missing flow / partial flow 종목 성과 저하 여부
  - 특정 regime 에서 과대낙관/과대비관 여부
  - 뉴스 burst / risk flags 동반 종목의 실패율
- 이 분석은 정교한 인과 추론이 아니라 **실무적인 개선 힌트** 여야 한다.
- 결과는 “추가 필요 정보” 또는 “다음 티켓 TODO” 형태의 bullet summary 로 남길 수 있어야 한다.

### 2.7 Postmortem report renderer
다음이 가능한 상태를 만든다.

- 장 마감 후 평가 가능한 cohort 를 기반으로 **postmortem 요약 리포트**를 렌더링할 수 있어야 한다.
- 이 리포트는 최소한 아래 내용을 포함한다.
  1. 오늘 평가된 cohort 범위
  2. D+1 / D+5 성과 요약
  3. top selection 결과
  4. grade 별 결과
  5. explanatory v0 대비 비교
  6. band calibration 요약
  7. 주요 실패 종목 / 주요 성공 종목
  8. 개선 포인트 / 운영 메모
- 리포트는 최소한 아래 형태 중 하나를 지원한다.
  - markdown
  - text payload
  - JSON payload + markdown preview
- 길이가 긴 경우 Discord 용은 축약 요약본, 로컬 저장용은 fuller version 으로 분리 가능해야 한다.

### 2.8 Discord postmortem 발송
다음이 가능한 상태를 만든다.

- Discord 로 postmortem 요약을 선택적으로 발송할 수 있어야 한다.
- `.env` 에 따라 on/off 가능해야 한다.
- `dry-run` 모드가 반드시 있어야 한다.
- publish 실패가 앱 전체 failure 로 이어지면 안 된다.
- Ops 에 전송 성공/실패 / 마지막 전송시각 / payload preview path 가 남아야 한다.

### 2.9 UI / Ops 가시성 강화
다음이 가능해야 한다.

- `Evaluation` 페이지가 실질적으로 usable 해야 한다.
- 최소한 아래 섹션이 있어야 한다.
  - Daily cohort summary
  - Rolling performance
  - Grade breakdown
  - Band coverage / calibration summary
  - Selection vs explanatory baseline comparison
  - Symbol-level outcome drilldown
- `Stock Workbench` 에서 선택일 기준 예측과 실제 결과를 함께 볼 수 있어야 한다.
- `Leaderboard` 에서 과거 날짜를 선택하면 이후 실제 outcome 으로 되돌아볼 수 있어야 한다.
- `Ops` 에서 마지막 evaluation run, postmortem render/publish 결과를 확인할 수 있어야 한다.

### 2.10 스크립트 / 엔트리포인트
다음 스크립트가 동작해야 한다.

- `scripts/materialize_selection_outcomes.py`
- `scripts/backfill_selection_outcomes.py`
- `scripts/materialize_prediction_evaluation.py`
- `scripts/materialize_calibration_diagnostics.py`
- `scripts/render_postmortem_report.py`
- `scripts/publish_discord_postmortem_report.py`
- `scripts/validate_evaluation_pipeline.py`

참고:
- 기존 `materialize_selection_engine_v1.py` 와 `calibrate_proxy_prediction_bands.py` 를 깨지 않는다.
- 필요 시 기존 스크립트에 evaluation-friendly metadata 를 소폭 추가하는 것은 허용한다.

---

## 3. 이번 티켓의 범위와 비범위

### 3.1 이번 티켓의 범위
- matured outcome 적재
- selection/prediction snapshot 대비 realized outcome 비교
- daily cohort / rolling evaluation 집계
- selection engine v1 vs explanatory ranking v0 baseline 비교
- calibration diagnostic 저장 및 UI 노출
- postmortem report renderer / Discord publisher
- Evaluation / Stock Workbench / Leaderboard / Ops 확장
- README / 실행 가이드 갱신
- 테스트 작성

### 3.2 이번 티켓의 비범위
이번 티켓에서는 아래를 완성하지 않는다.

- 정식 ML alpha model 학습/배포
- 진짜 predictive uncertainty / model disagreement 추정
- 장중 1분봉/체결/호가 기반 execution evaluation
- 거래비용을 정교하게 반영한 portfolio simulator
- 주문 실행 / 브로커 연동
- HTML/PDF 완성형 장문 리서치 리포트 엔진
- 실시간 웹 대시보드 스트리밍
- 뉴스 본문 전문 저장/재배포

즉, 이번 티켓은 **selection 결과의 사후 성과 검증과 postmortem 운영 루틴**까지다.

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
   - `TICKET_004_Flow_Selection_Engine_Discord_Report.md`
   - `CODEX_FIRST_INSTRUCTION_StockMaster.md`
   - `CODEX_SECOND_INSTRUCTION_StockMaster.md`
   - `CODEX_THIRD_INSTRUCTION_StockMaster.md`
   - `CODEX_FOURTH_INSTRUCTION_StockMaster.md`
   - `CODEX_FIFTH_INSTRUCTION_StockMaster.md`
3. TICKET-004가 불완전하다면, 이번 티켓 수행에 직접 필요한 blocking issue 만 보완한다.
4. selection label 정의, feature store 정의, ranking/prediction 저장 계약을 불필요하게 뒤집지 않는다.
5. 새로운 파일은 기존 `app/`, `config/`, `scripts/`, `tests/`, `docs/` 구조 안에 추가한다.

---

## 5. 이번 티켓의 설계 원칙

### 5.1 사후평가도 미래데이터 누수 없이 한다
이 티켓에서 가장 중요한 원칙이다.

- selection 당시 존재했던 `fact_ranking`, `fact_prediction` row 를 그대로 가져와서 비교한다.
- evaluation 시점에 새 calibration 결과를 과거 예측값처럼 덮어쓰지 않는다.
- 과거 cohort 의 예측 band 를 다시 계산해야 한다면, 그것은 별도의 backfill/debug artifact 로 다뤄야 한다.
- 실제 성과 비교는 **selection 시점 snapshot vs realized outcome** 이어야 한다.

### 5.2 라벨 규칙을 바꾸지 않는다
TICKET-003 에서 정한 forward label 정의를 그대로 사용한다.

- `selection_date` 의 종가 이후 판단
- 진입은 다음 거래일 시가
- 청산은 horizon 거래일 후 종가
- 같은 시장 scope 기준 excess return 사용

즉, 평가에서만 기준을 다르게 해서는 안 된다.

### 5.3 개별 종목 평가와 코호트 평가를 분리한다
하나의 종목이 맞았는지 틀렸는지와, 시스템 전체가 유의미했는지는 별개다.

따라서 다음 두 층을 모두 가져야 한다.

- symbol-level outcome
- cohort-level / rolling evaluation

### 5.4 selection engine v1 과 explanatory ranking v0 를 섞지 않는다
비교는 하되, 정체성은 분리해야 한다.

- selection engine v1 결과는 selection engine v1 로 평가한다.
- explanatory ranking v0 는 baseline 으로 별도 집계한다.
- README 와 UI 에서 둘의 차이를 분명히 적는다.

### 5.5 calibration 진단은 “멋있는 숫자”보다 “교정 가능성”을 보여줘야 한다
이번 티켓의 calibration 진단 목적은 논문용 fancy metric 이 아니라 다음 개선 근거를 만드는 것이다.

따라서 아래를 우선한다.

- sample count 충분한가
- band 가 너무 낙관적/비관적인가
- 상위 구간이 실제로 더 좋았는가
- 특정 구간에서 체계적 실패가 있는가

### 5.6 postmortem report 는 blame report 가 아니라 improvement report 여야 한다
성공/실패를 보여주되, **다음 개선 작업으로 바로 연결될 수 있는 메모**가 남아야 한다.

예:
- `thin_liquidity` 동반 종목의 D+1 성과 악화
- `partial_flow` 종목에서 A등급 과다 부여
- 특정 regime 에서 upper band 초과낙관 빈도 증가

---

## 6. 평가 대상의 정의

## 6.1 selection cohort
selection cohort 는 최소한 아래 key 로 식별 가능해야 한다.

- `selection_date`
- `symbol`
- `horizon`
- `selection_engine_version` 또는 ranking version
- `market_scope`

필요 시 추가 key:
- `prediction_model_name`
- `grade`
- `report_candidate_flag`

## 6.2 matured outcome eligibility
어떤 row 가 평가 가능한지는 아래를 만족해야 한다.

- 해당 `selection_date` 의 다음 거래일이 존재한다.
- 해당 horizon 의 exit 거래일이 존재한다.
- 필요한 일봉 데이터가 존재한다.
- required market baseline / excess return baseline 이 존재한다.

위 조건이 부족하면 상태를 명시한다.

권장 상태값 예시:
- `pending_horizon`
- `missing_price_data`
- `missing_market_baseline`
- `ready`
- `evaluated`

## 6.3 평가 단위
최소한 아래 단위를 지원한다.

- per symbol / per horizon
- per selection_date / per horizon
- rolling N-day summary / per horizon
- grade bucket / per horizon
- market bucket / per horizon

---

## 7. 권장 저장 계약

이번 티켓에서 반드시 아래 계약 중 하나를 구현한다.

### 7.1 `fact_selection_outcome`
종목 단위 realized outcome 저장용.

권장 필드:

- `selection_date`
- `evaluation_date`
- `symbol`
- `market`
- `horizon`
- `selection_engine_version`
- `ranking_version`
- `grade`
- `grade_detail`
- `report_candidate_flag`
- `eligible_flag`
- `final_selection_value`
- `selection_percentile`
- `expected_excess_return_at_selection`
- `lower_band_at_selection`
- `median_band_at_selection`
- `upper_band_at_selection`
- `uncertainty_score_at_selection`
- `implementation_penalty_at_selection`
- `regime_label_at_selection`
- `top_reason_tags_json`
- `risk_flags_json`
- `entry_trade_date`
- `exit_trade_date`
- `realized_return`
- `realized_excess_return`
- `prediction_error`
- `direction_hit_flag`
- `in_band_flag`
- `above_upper_flag`
- `below_lower_flag`
- `outcome_status`
- `evaluation_run_id`
- `created_at`
- `updated_at`

중요:
- `*_at_selection` 필드는 selection 당시 값을 freeze 한 것 이어야 한다.
- evaluation 때 새로 계산한 값으로 대체하지 않는다.

### 7.2 `fact_evaluation_summary`
코호트/구간/버킷 단위 집계 저장용.

권장 필드:

- `summary_date`
- `window_type` (`daily_cohort`, `rolling_20d`, `rolling_60d` 등)
- `window_start`
- `window_end`
- `horizon`
- `segment_type` (`all`, `grade`, `top_n`, `market`, `flow_coverage`, `regime`, `baseline_compare`)
- `segment_value`
- `count_total`
- `count_evaluated`
- `count_pending`
- `mean_realized_return`
- `mean_realized_excess_return`
- `median_realized_excess_return`
- `hit_rate`
- `positive_raw_return_rate`
- `band_coverage_rate`
- `above_upper_rate`
- `below_lower_rate`
- `avg_expected_excess_return`
- `avg_prediction_error`
- `score_monotonicity_hint`
- `created_at`

### 7.3 `fact_calibration_diagnostic`
band calibration / monotonicity / bias 진단 저장용.

권장 필드:

- `diagnostic_date`
- `window_start`
- `window_end`
- `horizon`
- `bin_type` (`selection_percentile_decile`, `selection_value_quantile`, `grade_bucket` 등)
- `bin_value`
- `sample_count`
- `expected_median`
- `expected_q25`
- `expected_q75`
- `observed_mean`
- `observed_median`
- `observed_q25`
- `observed_q75`
- `median_bias`
- `coverage_rate`
- `above_upper_rate`
- `below_lower_rate`
- `monotonicity_order`
- `quality_flag`
- `created_at`

### 7.4 `fact_report_snapshot` 또는 동등 계약
postmortem 리포트 산출물 저장용.

최소한 아래 정보는 남겨야 한다.

- `report_type` (`postmortem_daily` 등)
- `evaluation_date`
- `path_or_uri`
- `format`
- `dry_run_flag`
- `publish_target`
- `publish_status`
- `publish_error`
- `created_at`

주의:
- foundation 에 이미 유사 artifact 계약이 있으면 재사용해도 된다.
- 중복 구현은 피한다.

---

## 8. 평가 로직 요구사항

## 8.1 realized outcome 계산 규칙
최소 규칙:

- `realized_return = (future_close / next_open) - 1`
- `realized_excess_return = realized_return - same_market_baseline_return`
- baseline 정의는 TICKET-003 label 생성 때 쓴 규칙과 같아야 한다.

중요:
- adjusted price 를 이미 쓰고 있지 않다면, evaluation 과 label 양쪽이 동일한 가격 기준을 써야 한다.
- split / anomaly handling 이 이미 있으면 재사용한다.

## 8.2 band position 판정
최소한 아래 분류를 만든다.

- `below_lower`
- `between_lower_and_median`
- `between_median_and_upper`
- `above_upper`
- `unavailable_band`

그리고 boolean flag 로도 제공한다.

- `in_band_flag`
- `above_upper_flag`
- `below_lower_flag`

## 8.3 hit rule
최소한 두 종류의 hit 를 분리한다.

- `direction_hit_flag`: `realized_excess_return > 0`
- `raw_positive_flag`: `realized_return > 0`

선택적으로 아래도 허용한다.

- `band_center_hit_flag`: predicted median 과 같은 부호 방향인지

## 8.4 top-N pseudo-portfolio
최소한 아래를 평가한다.

- top 5
- top 10
- top 20

규칙:
- equal-weight 가 기본
- transaction cost 는 이번 티켓에서 정교하게 넣지 않아도 되지만, README 에 “pre-cost evaluation” 임을 적는다.
- report candidate 만 대상으로 한 top-N 과 전체 eligible 에서의 top-N 중 어떤 것을 기본으로 쓸지 문서화한다.

## 8.5 baseline 비교
최소 비교 대상:

- selection engine v1 top-N
- explanatory ranking v0 top-N
- broad market relative baseline (already embedded in excess return)

가능하면 아래도 보여준다.

- overlap ratio
- only-in-selection 성과
- only-in-explanatory 성과

## 8.6 rolling window
최소 rolling window:

- 최근 20 거래일
- 최근 60 거래일 (가능한 범위)

각 window 에 대해 아래를 본다.

- mean excess return
- hit rate
- band coverage
- optimistic/pessimistic bias
- grade 별 성과

## 8.7 failure analysis heuristic
다음 신호를 활용해 실패 패턴을 요약할 수 있어야 한다.

- `risk_flags_json`
- `implementation_penalty_at_selection`
- `uncertainty_score_at_selection`
- `flow coverage status`
- `market regime`
- `market` (`KOSPI` / `KOSDAQ`)

최소 출력 예시:
- `High implementation penalty rows underperformed in D+1.`
- `Partial flow coverage A-grade rows showed weak band coverage.`
- `Shock regime cohorts had optimistic upper bands.`

이것은 통계적으로 완벽한 설명이 아니라, 운영 메모를 만들기 위한 heuristic 이다.

---

## 9. UI 요구사항

## 9.1 Evaluation 페이지
최소 섹션:

1. Daily cohort summary
   - 오늘 평가된 D+1 / D+5 cohort 수
   - mean/median excess return
   - hit rate
   - top-N 성과

2. Rolling performance
   - 최근 20일 / 60일 성과
   - grade 별 / horizon 별 표

3. Calibration summary
   - band coverage rate
   - median bias
   - decile monotonicity summary

4. Baseline comparison
   - selection v1 vs explanatory v0
   - top 10 / top 20 비교

5. Symbol-level drilldown
   - 종목별 예측 band vs 실제 결과
   - reason tags / risk flags
   - failure note

## 9.2 Stock Workbench 확장
개별 종목에서 과거 selection_date 를 고르면 아래를 볼 수 있어야 한다.

- selection 당시 grade / selection value
- selection 당시 band
- 실제 realized return / excess return
- band position
- failure/success note

## 9.3 Leaderboard 과거 회고 모드
과거 날짜 선택 시 최소한 아래가 가능해야 한다.

- 당시 순위
- 현재가 아니라 당시 예측 band
- 이후 실제 결과
- evaluation status

## 9.4 Ops 페이지
최소한 아래가 보여야 한다.

- 마지막 selection outcome materialization 시각
- 마지막 evaluation summary 시각
- 마지막 calibration diagnostic 시각
- 마지막 postmortem render/publish 시각
- 실패 로그 요약

---

## 10. Postmortem report 요구사항

## 10.1 목적
이 리포트는 “전일/과거 추천이 실제로 어땠는지” 를 빠르게 보는 운영 문서다.

즉, 장후 selection 리포트와 역할이 다르다.
- 장후 selection 리포트: 앞으로 볼 후보군
- postmortem 리포트: 이미 추천한 후보군의 실제 결과와 개선 포인트

## 10.2 기본 섹션
최소 섹션:

1. 헤더
   - evaluation date
   - 포함된 matured cohort 범위
   - 한 줄 요약

2. D+1 요약
   - cohort count
   - mean/median excess return
   - hit rate
   - top-N 성과

3. D+5 요약
   - cohort count
   - mean/median excess return
   - hit rate
   - top-N 성과

4. Selection vs baseline 비교
   - selection v1 top 10 vs explanatory v0 top 10
   - selection v1 top 20 vs explanatory v0 top 20

5. 성공 종목 / 실패 종목
   - 3~5개씩
   - 실제 성과 + 당시 reason/risk 요약

6. Calibration / band 메모
   - coverage rate
   - median bias
   - too-wide / too-narrow 징후

7. 개선 포인트
   - 다음 티켓에 반영할 heuristic TODO

## 10.3 메시지 생성 원칙
- Discord 용은 짧고 명확해야 한다.
- 숫자는 과도한 소수점 정밀도를 피한다.
- 실패 분석은 blame tone 이 아니라 개선 tone 으로 쓴다.
- 뉴스 본문 전문, 장문 분석, 과도한 raw table dump 를 보내지 않는다.

---

## 11. 스크립트 요구사항 상세

### 11.1 `scripts/materialize_selection_outcomes.py`
역할:
- 특정 selection date 또는 평가 가능 범위에 대해 matured symbol-level outcome 계산

권장 CLI:
- `--selection-date YYYY-MM-DD` 또는 `--evaluation-date YYYY-MM-DD`
- `--horizons 1 5`
- `--limit-symbols N`
- `--force-recompute`

최소 동작:
- selection 당시 ranking/prediction snapshot 조회
- required future pricing/market baseline 조회
- `fact_selection_outcome` upsert
- run manifest 기록

### 11.2 `scripts/backfill_selection_outcomes.py`
역할:
- 과거 구간에 대해 outcome backfill

권장 CLI:
- `--start-selection-date YYYY-MM-DD`
- `--end-selection-date YYYY-MM-DD`
- `--horizons 1 5`
- `--limit-symbols N`

### 11.3 `scripts/materialize_prediction_evaluation.py`
역할:
- `fact_selection_outcome` 기반으로 cohort / rolling summary 집계 생성

권장 CLI:
- `--start-selection-date YYYY-MM-DD`
- `--end-selection-date YYYY-MM-DD`
- `--horizons 1 5`
- `--rolling-windows 20 60`

### 11.4 `scripts/materialize_calibration_diagnostics.py`
역할:
- band calibration / monotonicity / bias 진단 생성

권장 CLI:
- `--start-selection-date YYYY-MM-DD`
- `--end-selection-date YYYY-MM-DD`
- `--horizons 1 5`
- `--bin-count 10`

### 11.5 `scripts/render_postmortem_report.py`
역할:
- local preview 가능한 postmortem markdown/text/JSON 렌더

권장 CLI:
- `--evaluation-date YYYY-MM-DD`
- `--horizons 1 5`
- `--dry-run`
- `--output-format markdown`

### 11.6 `scripts/publish_discord_postmortem_report.py`
역할:
- webhook 으로 Discord 요약 전송

권장 CLI:
- `--evaluation-date YYYY-MM-DD`
- `--horizons 1 5`
- `--dry-run`

### 11.7 `scripts/validate_evaluation_pipeline.py`
역할:
- evaluation consistency / missing joins / band availability / cohort count sanity check

최소 검증:
- matured selection row count > evaluated row count 관계 확인
- pending / missing data 상태 확인
- selection snapshot field null rate 확인
- band coverage / bias 값의 sane range 확인
- top-N summary count consistency 확인

---

## 12. README 에 반드시 적어야 하는 것

README 또는 docs 에 최소한 아래를 명시한다.

- selection 당시 snapshot vs 평가 시점 재계산을 구분하는 원칙
- realized return / realized excess return 정의
- D+1 / D+5 평가 시점 정의
- band hit / above upper / below lower 정의
- selection engine v1 과 explanatory ranking v0 baseline 비교 방식
- rolling window 메트릭 정의
- calibration diagnostic 정의
- pre-cost 평가라는 점
- postmortem report dry-run / publish 사용법
- current known limitations

---

## 13. 테스트 요구사항

최소 테스트:

1. selection snapshot freeze test
   - evaluation 시 selection 당시 band/value 가 바뀌지 않는지 확인

2. matured eligibility test
   - horizon 미도달 row 는 evaluated 되지 않는지 확인

3. realized outcome formula test
   - next open / future close 규칙 검증

4. excess return baseline consistency test
   - label 생성 규칙과 evaluation 규칙이 같은지 확인

5. band classification test
   - below / in-band / above-upper 분류 검증

6. top-N cohort summary test
   - count, mean, hit rate 집계 일관성 검증

7. baseline comparison test
   - selection v1 vs explanatory v0 비교 집계 검증

8. calibration diagnostic sanity test
   - sample_count, bias, coverage 값 범위 검증

9. report rendering test
   - empty/partial/full case 에서 postmortem render 가 깨지지 않는지 확인

10. Discord dry-run test
   - publish 하지 않고 preview artifact 가 남는지 확인

---

## 14. 완료 기준 (Definition of Done)

아래가 최소한 재현 가능해야 한다.

1. `python scripts/materialize_selection_outcomes.py --selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
2. `python scripts/backfill_selection_outcomes.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --limit-symbols 100`
3. `python scripts/materialize_prediction_evaluation.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --rolling-windows 20 60`
4. `python scripts/materialize_calibration_diagnostics.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5 --bin-count 10`
5. `python scripts/render_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run`
6. `python scripts/publish_discord_postmortem_report.py --evaluation-date 2026-03-13 --horizons 1 5 --dry-run`
7. `python scripts/validate_evaluation_pipeline.py --start-selection-date 2026-02-17 --end-selection-date 2026-03-06 --horizons 1 5`
8. `streamlit run app/ui/Home.py`

그리고 아래가 확인되어야 한다.

- Evaluation 페이지에서 D+1 / D+5 cohort 결과 확인 가능
- Selection v1 vs explanatory v0 baseline 비교 가능
- 종목별 band vs realized outcome drilldown 가능
- postmortem preview artifact 생성 가능
- Discord publish dry-run 가능
- README 가 갱신되어 있음
- tests 가 통과함

---

## 15. 이번 티켓에서 하지 말아야 할 것

- 과거 예측 band 를 evaluation 시점에 새로 덮어쓰기
- selection engine v1 과 explanatory ranking v0 를 섞어버리기
- 아직 없는 거래비용 엔진을 있는 것처럼 서술하기
- 본문 전문 뉴스 저장/전송
- evaluation failure 때문에 전체 앱이 중단되게 만들기
- 지나친 over-engineering
- UI 예쁘게 만들겠다고 핵심 집계/저장 계약을 뒤로 미루기

---

## 16. 구현 후 Codex가 남겨야 하는 최종 정리

작업 완료 후 Codex는 아래를 간단히 정리해야 한다.

1. 새로 추가된 파일 목록
2. 생성/변경된 테이블 및 view 목록
3. matured outcome 계산 흐름
4. evaluation summary / calibration diagnostic 생성 흐름
5. selection v1 vs explanatory v0 비교 확인 방법
6. postmortem report preview / publish 확인 방법
7. known limitations
8. 다음 TICKET-006 진입 전 주의사항

