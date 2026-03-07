# StockMaster

StockMaster is a Korea-focused personal stock research platform for post-market analysis, explanatory ranking, reporting, and retrospective evaluation.

Implemented through TICKET-004:

- foundation, settings, logging, bootstrap, and disk guard
- provider activation for KIS, DART, and Naver News
- reference data sync for `dim_symbol` and `dim_trading_calendar`
- core research ingestion for `fact_daily_ohlcv`, `fact_fundamentals_snapshot`, and `fact_news_item`
- feature store, forward return labels, market regime snapshot, and explanatory ranking v0
- investor flow ingestion, selection engine v1, calibrated proxy prediction bands, and Discord EOD report draft
- Streamlit Home, Ops, Research, Leaderboard, Market Pulse, and Stock Workbench pages

Out of scope:

- auto-trading, order routing, and execution
- full news article storage
- ML alpha model training or prediction
- fake uncertainty or fake model disagreement scores

The current ranking is an explanatory layer. It is not a predictive engine.

## Repository layout

```text
app/                application packages
config/             yaml configuration and local examples
data/               runtime storage roots (.gitkeep only)
docs/               architecture notes and ticket docs
scripts/            cli entrypoints
tests/              unit and integration tests
```

## Local setup

1. Create a virtual environment and install dependencies.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

2. Prepare environment variables.

```powershell
Copy-Item .env.example .env
```

3. Fill the provider credentials you actually have.

Required for live ingestion:

- `KIS_APP_KEY`
- `KIS_APP_SECRET`
- `DART_API_KEY`
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

Used only when the related capability is enabled:

- `KIS_ACCOUNT_NO`
- `KIS_PRODUCT_CODE`
- `KIS_USE_MOCK`
- `KRX_API_KEY`
- `DISCORD_REPORT_ENABLED`
- `DISCORD_WEBHOOK_URL`

Notes:

- `KIS_USE_MOCK=false` uses the production KIS host.
- `KRX_API_KEY` is still optional because KRX is not yet a live ingestion dependency.
- `DISCORD_REPORT_ENABLED=false` keeps Discord in render-only mode. Live publish requires both `DISCORD_REPORT_ENABLED=true` and `DISCORD_WEBHOOK_URL`.
- `DISCORD_WEBHOOK_URL` is optional for preview and dry-run workflows.

## Initial bootstrap and reference data

Run these first on a new machine:

```powershell
python scripts/bootstrap.py
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
python scripts/sync_universe.py
python scripts/provider_smoke_check.py --symbol 005930
```

## Core ingestion flow

TICKET-002 research ingestion:

```powershell
python scripts/sync_daily_ohlcv.py --date 2026-03-06 --limit-symbols 50
python scripts/sync_fundamentals_snapshot.py --as-of-date 2026-03-06 --limit-symbols 50
python scripts/sync_news_metadata.py --date 2026-03-06 --mode market_and_focus --limit-symbols 50
python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 50
python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50
```

`python scripts/run_daily_pipeline.py` now orchestrates:

- OHLCV sync
- fundamentals snapshot sync
- news metadata sync
- investor flow sync
- feature store build
- market regime snapshot build
- explanatory ranking materialization
- selection engine v1 materialization
- proxy prediction calibration when enough labeled history exists
- Discord preview render on every run
- Discord publish only when `DISCORD_REPORT_ENABLED=true`

Forward labels and validation remain separate because they depend on future trading days. Proxy calibration is attempted inside the daily pipeline, but it is skipped gracefully until enough labeled history exists.

## TICKET-003 execution flow

Single-date build flow:

```powershell
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06
python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
```

## TICKET-004 execution flow

Investor flow ingestion:

```powershell
python scripts/sync_investor_flow.py --trading-date 2026-03-06 --limit-symbols 100
python scripts/backfill_investor_flow.py --start 2026-02-17 --end 2026-03-06 --limit-symbols 100
```

Selection engine, proxy bands, and Discord:

```powershell
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/materialize_selection_engine_v1.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/calibrate_proxy_prediction_bands.py --start 2026-01-05 --end 2026-03-06 --horizons 1 5
python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/validate_selection_engine_v1.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
```

Backfill note:

- `backfill_investor_flow.py` is date-loop orchestration over `sync_investor_flow.py`
- selection validation is meaningful only on dates where `fact_forward_return_label.label_available_flag = TRUE`
- calibrated proxy bands are attached to the latest available selection snapshot at or before the calibration end date
- `publish_discord_eod_report.py` always renders preview artifacts first, then optionally sends one or more Discord messages depending on payload length

UI:

```powershell
streamlit run app/ui/Home.py
```

Historical validation note:

- validation only works on dates where both `fact_ranking` and `fact_forward_return_label` exist
- the latest as-of date often has no available label yet because future closes are not known
- to validate a window, materialize historical feature/regime/ranking snapshots first

PowerShell example for historical ranking snapshots:

```powershell
foreach ($d in "2026-03-02","2026-03-03","2026-03-04","2026-03-05","2026-03-06") {
  python scripts/build_feature_store.py --as-of-date $d --limit-symbols 100
  python scripts/build_market_regime_snapshot.py --as-of-date $d
  python scripts/materialize_explanatory_ranking.py --as-of-date $d --horizons 1 5 --limit-symbols 100
}
python scripts/validate_explanatory_ranking.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5
```

## Feature groups

Current `fact_feature_snapshot` groups:

- `price_trend`
- `volatility_risk`
- `liquidity_turnover`
- `fundamentals_quality`
- `value_safety`
- `news_catalyst`
- `investor_flow`
- `data_quality`

Representative features:

- price/trend: `ret_3d`, `ret_5d`, `ret_20d`, `ma5_over_ma20`, `ma20_over_ma60`, `drawdown_20d`
- volatility/risk: `realized_vol_20d`, `gap_abs_avg_20d`, `hl_range_1d`, `max_loss_20d`
- turnover/liquidity: `volume_ratio_1d_vs_20d`, `turnover_z_5_20`, `adv_20`, `adv_60`
- fundamentals/quality: `roe_latest`, `debt_ratio_latest`, `operating_margin_latest`, `days_since_latest_report`
- value/safety: `earnings_yield_proxy`, `low_debt_preference_proxy`, `profitability_support_proxy`
- news: `news_count_3d`, `distinct_publishers_3d`, `latest_news_age_hours`, `positive_catalyst_count_3d`
- investor flow: `foreign_net_value_ratio_1d`, `foreign_net_value_ratio_5d`, `institution_net_value_ratio_5d`, `smart_money_flow_ratio_20d`, `flow_alignment_score`, `flow_coverage_flag`
- data quality: `stale_price_flag`, `missing_key_feature_count`, `data_confidence_score`

## Feature calculation overview

- price and trend features come from `fact_daily_ohlcv`
- turnover features use `turnover_value` and fall back to `close * volume`
- fundamentals features come from the latest eligible DART snapshot at or before `as_of_date`
- news features use metadata only and conservative symbol linking
- investor flow features use KIS daily investor trend payloads normalized into `fact_investor_flow`
- if investor flow coverage is missing, raw flow features stay `NULL`; the pipeline does not zero-fill missing source coverage
- data quality features score whether price, fundamentals, and news are present and current
- all feature rows are materialized into `fact_feature_snapshot`
- cross-sectional `rank_pct` and `zscore` are computed within market (`KOSPI`, `KOSDAQ`)

## Investor flow coverage and null handling

- source: KIS `investor-trade-by-stock-daily`
- storage contract: `fact_investor_flow`
- coverage rule: if the requested trading date is missing or KIS does not return a usable flow field, the curated row is skipped and downstream flow features remain `NULL`
- the only explicit fill is `flow_coverage_flag`, which is `1.0` when same-day flow exists and `0.0` when it does not
- missing coverage is handled as neutral/unknown in scoring, then reflected through uncertainty and implementation penalties rather than fake zero flow

## Label definition

`fact_forward_return_label` uses explicit next-open logic:

- D+1: next trading day open -> same day close
- D+5: next trading day open -> close of the fifth trading session in the holding window
- baseline: same-market equal-weight average forward return
- excess label: symbol forward return minus same-market baseline

This is intentionally next-open based because a post-market research decision cannot assume same-day close execution without look-ahead bias.

## Regime state rules

`fact_market_regime_snapshot` is built for:

- `KR_ALL`
- `KOSPI`
- `KOSDAQ`

Current v0 regime states:

- `panic`
- `risk_off`
- `neutral`
- `risk_on`
- `euphoria`

Inputs used by the rule engine:

- breadth up ratio
- median symbol return 1d
- median symbol return 5d
- 20d market realized volatility
- turnover burst z-score
- 20d new high ratio
- 20d new low ratio

## Explanatory score v0

Active components:

- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `regime_fit_score`
- `risk_penalty_score`

Reserved components:

- `flow_score`

`flow_score` is explicitly marked as reserved, not faked.

Weights differ by horizon:

- D+1 favors short-term trend, turnover, and news
- D+5 shifts more weight toward medium-term trend, quality, and value/safety

Stored outputs:

- `final_selection_value`
- `top_reason_tags_json`
- `risk_flags_json`
- `eligibility_notes_json`
- `explanatory_score_json`

## Selection engine v1

Selection engine v1 is a separate layer from explanatory ranking v0.

Active components:

- `trend_momentum_score`
- `turnover_participation_score`
- `quality_score`
- `value_safety_score`
- `news_catalyst_score`
- `flow_score`
- `regime_fit_score`
- `risk_penalty_score`
- `uncertainty_proxy_score`
- `implementation_penalty_score`

Reserved / nullable components:

- `disagreement_score = null` when no model-disagreement source exists

Definitions:

- `flow_score`: uses investor flow ratios, smart-money flow, and alignment between foreign/institution vs individual flow
- `uncertainty_proxy_score`: higher when realized volatility, gap behavior, or missing flow/data coverage suggests unstable outcomes
- `implementation_penalty_score`: higher when liquidity is thin, turnover is weak, price staleness exists, or flow coverage is missing

Difference from explanatory ranking v0:

- v0 is a human-readable inspection score with reserved `flow_score`
- v1 activates `flow_score` and adds uncertainty / implementation frictions to form a pragmatic selection layer
- neither v0 nor v1 is an ML alpha model

## Calibrated proxy prediction bands

`fact_prediction` stores:

- `expected_excess_return`
- `lower_band`
- `median_band`
- `upper_band`

These are calibrated historical proxies, not model forecasts.

Current rule:

- bucket latest selection rows by score decile
- look up historical excess-return distribution for the same horizon and score bucket
- prefer same-market calibration where available, otherwise fall back to `KR_ALL`
- store quartile-like proxy bands and sample size in `fact_prediction`

## Grade rules

Current grade assignment:

- `A`: eligible and top 5%
- `A-`: eligible and top 15%
- `B`: eligible and top 35%, or critical-risk names that still clear the minimum threshold
- `C`: everything else, including ineligible names

Grades are presentation buckets, not probability estimates.

## Tables and views added by TICKET-003

Tables:

- `fact_investor_flow`
- `fact_feature_snapshot`
- `fact_forward_return_label`
- `fact_market_regime_snapshot`
- `fact_ranking`
- `fact_prediction`
- `ops_ranking_validation_summary`
- `ops_selection_validation_summary`

Views:

- `vw_latest_investor_flow`
- `vw_feature_snapshot_latest`
- `vw_feature_matrix_latest`
- `vw_latest_forward_return_label`
- `vw_market_regime_latest`
- `vw_ranking_latest`
- `vw_prediction_latest`
- `vw_latest_ranking_validation_summary`
- `vw_latest_selection_validation_summary`

## Raw, curated, and artifact outputs

Representative paths:

```text
data/curated/features/as_of_date=YYYY-MM-DD/feature_snapshot.parquet
data/curated/features/as_of_date=YYYY-MM-DD/feature_matrix.parquet
data/curated/market/investor_flow/trading_date=YYYY-MM-DD/investor_flow.parquet
data/curated/labels/as_of_date=YYYY-MM-DD/forward_return_labels.parquet
data/curated/regime/as_of_date=YYYY-MM-DD/market_regime_snapshot.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/explanatory_ranking.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/ranking_version=selection_engine_v1/selection_engine_v1.parquet
data/curated/prediction/as_of_date=YYYY-MM-DD/proxy_prediction_band.parquet
data/artifacts/validation/ranking/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/ranking_validation_summary.parquet
data/artifacts/validation/selection_engine_v1/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/selection_validation_summary.parquet
data/artifacts/discord/as_of_date=YYYY-MM-DD/<run_id>/discord_preview.md
```

## News query pack adjustment

The default query pack lives in [config/news_queries.yaml](/d:/MyApps/StockMaster/config/news_queries.yaml).

How to adjust:

1. Edit `packs.default.market`
2. Add or remove `bucket` / `keyword` pairs
3. Rerun `scripts/sync_news_metadata.py`

Operational note:

- `market_and_focus` without explicit symbols caps the focus subset to avoid unbounded query fan-out

## Fundamentals availability rule

Current rule is conservative and date-only:

- a DART filing is eligible only if `rcept_dt <= as_of_date`
- the latest eligible regular filing wins
- future filings are excluded from historical snapshots

## Discord dry-run and publish

Use dry-run first:

```powershell
python scripts/render_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06 --dry-run
```

Live publish:

```powershell
python scripts/publish_discord_eod_report.py --as-of-date 2026-03-06
```

Operational behavior:

- render always writes preview artifacts under `data/artifacts/discord/...`
- publish always renders preview artifacts first and then sends one or more Discord messages if needed
- live publish requires `DISCORD_REPORT_ENABLED=true`
- publish failure is recorded as a warning note and does not fail the broader daily pipeline
- only metadata and selection summary are sent; no full article body is stored or transmitted

## Known limitations

- explanatory ranking v0 still keeps `flow_score` reserved by design
- selection engine v1 is not an ML alpha model; its prediction bands are calibrated historical proxies only
- `disagreement_score` remains `null` until a real multi-model layer exists
- `market_cap` depends on upstream availability and is not yet independently validated
- many 20d and 60d features will be null on shallow history windows
- symbol linking for news is intentionally conservative and may leave `symbol_candidates = []`
- investor flow coverage can be partial and stays null instead of being zero-filled
- no full article body storage
- validation is sparse unless historical ranking snapshots and forward labels have already been materialized

## Validation and lint

Run tests:

```powershell
python -m pytest
```

Run lint:

```powershell
python -m ruff check .
```

## UI checkpoints

After `streamlit run app/ui/Home.py`, verify:

- Home shows reference data, research freshness, feature/ranking snapshot, and validation summary
- Ops shows version tracking, flow/prediction coverage, regime snapshot, Discord preview, and failures
- Research shows feature store, flow, regime, labels, and selection validation
- Leaderboard compares explanatory ranking v0 and selection engine v1
- Market Pulse shows regime + flow breadth + latest top selections
- Stock Workbench shows one symbol across features, flow history, prices, and news metadata

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.
