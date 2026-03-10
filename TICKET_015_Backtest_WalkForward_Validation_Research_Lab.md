# TICKET-015 — Research Backtest / Walk-Forward Validation / Experiment Lab

## 1. 목적

StockMaster에 이미 구현되었거나 구현 중인 아래 레이어를 **실전 이전에 검증**할 수 있도록, 프로젝트 고유의 데이터 계약과 실행 규칙을 따르는 **전용 백테스트/워크포워드 검증 프레임워크**를 추가한다.

대상 레이어:

- explanatory ranking v0
- selection engine v1 / v2
- intraday timing raw / adjusted / meta overlay
- integrated portfolio policy
- regime-aware adjustment layer

이 티켓의 목적은 단순히 “과거 수익률 그래프 하나”를 만드는 것이 아니라, 아래를 재현 가능하게 검증하는 것이다.

- 그 시점에 실제로 사용 가능했던 정보만으로 의사결정했는가
- 추천/랭킹/정책/포트폴리오가 미래 누수 없이 작동하는가
- 거래비용과 유동성 제약을 넣은 뒤에도 성과가 남는가
- 장세(regime)별로 어떤 계층이 유효/무효했는가
- open 진입, timing-assisted 진입, portfolio allocation까지 포함했을 때 결과가 어떻게 달라지는가

---

## 2. 이 티켓의 범위

### 포함

1. **워크포워드 백테스트 엔진**
   - 시간 순서 기반 fold
   - training window / validation window / test window 정의
   - horizon 별(D+1, D+5) 검증
   - as-of discipline 보장

2. **전략 단위별 비교 실험**
   - ranking only
   - selection v2 only
   - selection v2 + intraday raw
   - selection v2 + intraday adjusted
   - portfolio policy까지 포함한 end-to-end

3. **실행 모드 비교**
   - OPEN_ALL
   - TIMING_ASSISTED_RAW
   - TIMING_ASSISTED_ADJUSTED
   - META_OVERLAY_ASSISTED

4. **비용/제약 시뮬레이션**
   - commission
   - tax/fee placeholder
   - slippage proxy
   - turnover penalty
   - liquidity cap
   - max position size

5. **평가 산출물**
   - run/fold/trade/day/nav/summary tables
   - report bundle
   - UI research page
   - markdown/html summary

6. **실험 관리(Experiment Lab)**
   - scenario registry
   - parameter set registry
   - recommendation / freeze / rollback

### 제외

- 자동매매 연결
- 실시간 주문 시뮬레이터 수준의 초정밀 체결 엔진
- 옵션/선물/레버리지/공매도 포지션
- 초고빈도 틱 단위 백테스트
- 외부 백테스트 라이브러리에 종속된 핵심 로직 구성

---

## 3. 핵심 원칙

### 3.1 matured-only
백테스트는 반드시 **matured outcome** 만 사용한다.
즉, 특정 `as_of_date` 의 의사결정은 그 날짜 이후 실제로 닫힌(outcome matured) 결과로만 평가한다.

### 3.2 as-of discipline
모든 feature, score, label, policy, regime, allocation은 **해당 시점에 사용 가능했던 스냅샷**만 사용해야 한다.

금지:
- 재무 정정 이후 값으로 과거 판단 재계산
- 나중에 수집한 뉴스 요약을 과거 시점에 재투입
- future market breadth / realized vol 을 과거 의사결정에 사용
- intraday 종료 후 계산된 aggregate 를 장중 시점 판단에 사용

### 3.3 same-exit comparison
비교 실험은 가능한 한 **진입 로직만 다르고 exit 가 동일한 조건**으로 먼저 비교해야 한다.

예:
- selection v2 open 진입 vs timing raw first-enter
- timing raw vs timing adjusted
- timing adjusted vs meta overlay

### 3.4 scenario-first
백테스트는 “전략 코드 하나”가 아니라 **명시적 scenario 정의**로 운용한다.

예:
- `SEL_V2_OPEN_ALL_D1`
- `SEL_V2_TIMING_RAW_D1`
- `SEL_V2_TIMING_ADJ_D1`
- `PORT_BALANCED_OPEN_ALL_D5`
- `PORT_BALANCED_TIMING_ASSISTED_D5`

### 3.5 cost-aware
거래비용과 유동성 제약 없는 결과는 참고용일 뿐이며, 기본 summary 는 반드시 **비용 반영 기준**으로 제공한다.

---

## 4. 권장 아키텍처

### 4.1 내부 전용 백테스트 엔진을 기본으로 사용
StockMaster의 핵심 검증은 프로젝트 내부 데이터 계약에 맞춘 **custom backtest engine** 으로 구현한다.

이유:
- 현재 프로젝트는 단순 진입/청산 신호가 아니라
  - ranking
  - selection
  - intraday timing
  - meta overlay
  - portfolio allocation
  - risk budget
  - liquidity cap
  - regime adjustment
  를 함께 비교해야 한다.
- 따라서 일반적인 가격 시계열 중심 라이브러리보다, 현재의 `fact_*` 테이블과 `run manifest` 를 그대로 쓰는 엔진이 더 적합하다.

### 4.2 외부 라이브러리는 adapter 성격으로만 허용
다음은 선택적 adapter 로만 허용한다.

- `vectorbt`: 빠른 벡터화 연구 실험용
- `backtesting.py`: 단일 전략 규칙 검산용

하지만 이들 라이브러리가 프로젝트의 source of truth 가 되면 안 된다.

### 4.3 폴드 기반 검증
기본 검증 방식:

- expanding train + rolling test
- 또는 rolling train + rolling validation + rolling test

권장 기본값:

- 최소 학습 시작 구간: 18~24개월 이상 데이터 확보 후
- validation: 최근 3~6개월
- test: 1개월 또는 1개 리밸런스 구간
- horizon 분리: D+1 과 D+5 각각 별도 검증

---

## 5. 백테스트 레벨 정의

### Level A — Model / Score Validation
대상:
- explanatory score v0
- alpha model v1
- uncertainty/disagreement outputs

출력:
- cross-sectional rank IC
- top bucket vs bottom bucket excess return
- calibration by score decile
- regime slice diagnostics

### Level B — Selection Validation
대상:
- selection engine v1 / v2

출력:
- selection hit rate
- avg excess return
- q25/q50/q75 realized distribution
- missed winner / avoided loser analysis

### Level C — Timing Validation
대상:
- intraday raw / adjusted / meta overlay

출력:
- open 대비 timing edge
- saved loss by skip
- missed gain by wait/avoid
- action transition diagnostics

### Level D — Portfolio Validation
대상:
- portfolio policy

출력:
- daily NAV
- drawdown
- turnover
- capacity/liquidity breaches
- realized exposure path
- regime별 성과 분해

---

## 6. 시나리오 정의 계약

각 시나리오는 아래를 명시해야 한다.

- scenario_id
- scenario_name
- horizon (`D1` / `D5`)
- base_selection_source
- timing_mode
- portfolio_mode
- entry_rule
- exit_rule
- cost_profile
- liquidity_profile
- rebalance_frequency
- cash_buffer_rule
- position_cap_rule
- sector_cap_rule
- ksdq_cap_rule
- turnover_cap_rule
- regime_adjustment_enabled
- notes

### 예시 시나리오

1. `SCN_SEL_V2_OPEN_D1`
   - selection v2 top picks
   - next open entry
   - D+1 close exit
   - equal-weight candidate book

2. `SCN_SEL_V2_TIMING_RAW_D1`
   - selection v2 top picks
   - intraday raw first-enter
   - same day exit rule aligned with D+1 framework

3. `SCN_PORT_BALANCED_OPEN_D5`
   - portfolio policy balanced_long_only_v1
   - next open rebalance
   - D+5 evaluation horizon

4. `SCN_PORT_BALANCED_TIMING_ADJ_D5`
   - portfolio policy + timing adjusted gate
   - regime-aware adjustment enabled

---

## 7. 거래비용 / 실행 제약 기본 규칙

최소한 아래 파라미터를 scenario 별로 설정 가능해야 한다.

- `commission_bps`
- `tax_bps`
- `slippage_bps_open`
- `slippage_bps_intraday`
- `max_turnover_pct`
- `max_position_pct_of_adv`
- `min_price_filter`
- `min_adv_filter`
- `hard_exclude_flags`

### 기본 원칙

- open 진입은 open slippage 적용
- intraday first-enter 는 intraday slippage 적용
- liquidity cap 초과 시 체결 비율 축소 또는 skip
- turnover cap 초과 시 우선순위 낮은 신규 진입부터 제거
- hard exclude(관리/거래정지/데이터부족 등)는 무조건 제외

---

## 8. 데이터 계약

아래 테이블(또는 동등한 parquet curated dataset)을 추가한다.

### 8.1 `fact_backtest_run`
컬럼 예시:
- backtest_run_id
- created_at_kst
- scenario_id
- scenario_version
- horizon
- date_from
- date_to
- train_policy
- test_policy
- cost_profile_name
- liquidity_profile_name
- data_cutoff_policy
- status
- root_run_id
- notes

### 8.2 `fact_backtest_fold`
- backtest_run_id
- fold_id
- train_start_date
- train_end_date
- validation_start_date
- validation_end_date
- test_start_date
- test_end_date
- model_bundle_run_id
- selection_run_id
- portfolio_policy_id
- status
- summary_json

### 8.3 `fact_backtest_position_day`
- backtest_run_id
- fold_id
- date
- symbol
- target_weight
- realized_weight
- entry_mode
- entry_price_assumed
- close_price
- mtm_return_gross
- mtm_return_net
- turnover_contrib
- liquidity_flag
- regime_family

### 8.4 `fact_backtest_trade`
- backtest_run_id
- fold_id
- symbol
- decision_as_of_date
- entry_datetime_assumed
- entry_mode
- exit_datetime_assumed
- exit_mode
- gross_return
- net_return
- excess_return_vs_market
- cost_bps_total
- slippage_bps
- capacity_scaled
- action_trace_ref

### 8.5 `fact_backtest_nav_day`
- backtest_run_id
- fold_id
- date
- nav_gross
- nav_net
- daily_return_gross
- daily_return_net
- benchmark_return
- active_return
- drawdown
- turnover
- gross_exposure
- cash_weight

### 8.6 `fact_backtest_summary`
- backtest_run_id
- scenario_id
- horizon
- annualized_return_net
- annualized_vol
- sharpe_like
- max_drawdown
- win_rate
- avg_trade_return
- avg_active_return
- hit_rate_positive
- turnover_avg
- capacity_breach_rate
- skipped_trade_rate
- coverage_days
- regime_breakdown_json
- created_at_kst

### 8.7 `fact_backtest_diagnostic`
- backtest_run_id
- fold_id
- diagnostic_type
- slice_key
- metric_name
- metric_value
- metric_json

### 8.8 `dim_backtest_scenario`
- scenario_id
- scenario_name
- scenario_family
- description
- is_active
- config_json
- created_at_kst
- retired_at_kst

---

## 9. 평가 지표

### 필수 지표

- cumulative return (gross / net)
- avg active return
- hit rate
- turnover
- drawdown
- capacity breach rate
- skip rate
- net-of-cost top-k performance
- regime별 성과

### 강력 권장 지표

- decile spread
- rank IC / Spearman IC
- calibration by score band
- uncertainty bucket monotonicity
- disagreement bucket spread
- sector-neutralized active return
- KOSPI / KOSDAQ split performance

### timing 전용 지표

- open 대비 edge
- saved loss by WAIT/AVOID
- missed winner by WAIT/AVOID
- first-enter delay cost
- action override confusion matrix

### portfolio 전용 지표

- realized cash buffer path
- single-name cap hit count
- sector cap hit count
- KOSDAQ cap hit count
- max concurrent positions
- realized participation vs ADV

---

## 10. UI 요구사항

### 10.1 Research Lab > Backtest
화면 요소:
- scenario selector
- date range selector
- horizon selector
- execution mode selector
- cost profile selector
- compare runs multi-select
- summary KPI cards
- cumulative NAV chart
- drawdown chart
- turnover chart
- regime slice table
- top/bottom period analysis
- export buttons

### 10.2 Research Lab > Fold Diagnostics
- fold list
- fold-by-fold KPIs
- fold OOS performance chart
- fold calibration diagnostics
- fold universe coverage diagnostics

### 10.3 Evaluation > Strategy Comparison
- open vs timing raw vs timing adjusted vs meta overlay
- equal exit comparison table
- saved loss / missed winner decomposition

### 10.4 Portfolio > Backtest Replay
- date slider
- holdings snapshot
- target vs realized weights
- cap constraint hits
- sector exposure
- cash buffer usage

### 10.5 Ops > Backtest Queue / Artifacts
- running/finished/failed backtests
- artifact paths
- manifest link
- disk usage impact
- cleanup eligibility

---

## 11. 산출물(bundle) 요구사항

백테스트 run 완료 시 다음을 남긴다.

- manifest json
- scenario config snapshot
- fold summary parquet/csv
- summary markdown
- summary html
- comparison plots png/html
- diagnostics parquet
- optional notebook-friendly export

권장 폴더 구조:

```text
artifacts/
  backtests/
    <backtest_run_id>/
      manifest.json
      scenario.json
      summary.md
      summary.html
      nav_daily.parquet
      trades.parquet
      diagnostics.parquet
      charts/
```

---

## 12. 스크립트/CLI 요구사항

Codex는 최소한 아래 CLI를 추가해야 한다.

1. `python -m app.jobs.backtest.run_scenario --scenario-id SCN_SEL_V2_OPEN_D1 --date-from 2024-01-01 --date-to 2025-12-31`
2. `python -m app.jobs.backtest.run_compare --scenario-ids SCN_SEL_V2_OPEN_D1,SCN_SEL_V2_TIMING_ADJ_D1 --date-from ... --date-to ...`
3. `python -m app.jobs.backtest.recompute_summary --backtest-run-id ...`
4. `python -m app.jobs.backtest.export_bundle --backtest-run-id ...`
5. `python -m app.jobs.backtest.validate_integrity --backtest-run-id ...`

선택 사항:

6. `python -m app.jobs.backtest.adapter_vectorbt --scenario-id ...`
7. `python -m app.jobs.backtest.adapter_backtesting_py --scenario-id ...`

---

## 13. 구현 우선순위

### Phase A — 기본 검증선
- selection v2 open D+1 / D+5
- timing raw / adjusted 비교
- summary tables
- NAV / trade / summary output

### Phase B — portfolio 포함
- portfolio policy replay
- risk budget / cap / turnover 반영
- portfolio backtest replay UI

### Phase C — experiment lab
- scenario registry
- compare runs
- fold diagnostics
- recommendation / freeze

### Phase D — optional adapter
- vectorbt adapter
- backtesting.py adapter

---

## 14. 테스트 요구사항

### 단위 테스트
- future leakage guard
- same-exit comparison correctness
- cost application correctness
- turnover cap logic
- liquidity cap scaling
- date alignment correctness

### 통합 테스트
- sample period single scenario run
- compare run generation
- summary export generation
- UI dataset loading

### 회귀 테스트
- known fixture scenario에서 KPI drift 없는지 검증
- manifest reproducibility 검증

---

## 15. 완료 기준 (Definition of Done)

다음을 만족해야 완료로 본다.

1. 최소 3개 이상의 scenario 가 end-to-end로 실행된다.
2. D+1 / D+5 각각에 대해 net-of-cost summary 가 생성된다.
3. backtest run / fold / trade / nav / summary 데이터가 저장된다.
4. UI에서 compare view 가 동작한다.
5. future leakage 방지 테스트가 통과한다.
6. scenario config snapshot 과 manifest 가 재현 가능하다.
7. README 또는 docs 에 사용법이 반영된다.

---

## 16. Codex 구현 지시 핵심

- 기존 문서들을 source of truth 로 읽고 구현하라.
- generic toy backtester 를 만들지 말고, 현재 StockMaster의 selection/timing/portfolio 데이터 계약에 맞춘 엔진을 만들라.
- `as_of_date` 와 matured-only 규칙을 절대 깨지 말라.
- 초기 버전은 **정확성 우선, 속도는 그 다음** 이다.
- 외부 라이브러리는 optional adapter 로만 넣고, 핵심 엔진은 내부 구현으로 유지하라.
- 백테스트 결과는 UI와 markdown/html bundle 양쪽에서 확인 가능해야 한다.

---

## 17. 참고 방향

- 시간 순서 검증은 임의 셔플이 아니라 time-aware split 을 사용해야 한다.
- hyperparameter tuning 은 walk-forward 바깥이 아니라 fold 규칙 안에서 이뤄져야 한다.
- tuning / experiment artifact 는 별도 저장소와 manifest 로 관리하는 편이 좋다.
- vectorbt 는 빠른 벡터화 연구에, backtesting.py 는 규칙 전략 검산에 유용할 수 있으나 본 프로젝트의 진실 원천은 내부 엔진이어야 한다.

