# StockMaster

StockMaster is a Korea-focused personal stock research platform for post-market analysis, explanatory ranking, reporting, and retrospective evaluation.

Implemented through TICKET-003:

- foundation, settings, logging, bootstrap, and disk guard
- provider activation for KIS, DART, and Naver News
- reference data sync for `dim_symbol` and `dim_trading_calendar`
- core research ingestion for `fact_daily_ohlcv`, `fact_fundamentals_snapshot`, and `fact_news_item`
- feature store, forward return labels, market regime snapshot, and explanatory ranking v0
- Streamlit Home, Ops, Research, and Leaderboard pages

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
- `DISCORD_WEBHOOK_URL`

Notes:

- `KIS_USE_MOCK=false` uses the production KIS host.
- `KRX_API_KEY` is still optional because KRX is not yet a live ingestion dependency.
- `DISCORD_WEBHOOK_URL` is still reserved for later delivery/report tickets.

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
python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50
```

`python scripts/run_daily_pipeline.py` now orchestrates:

- OHLCV sync
- fundamentals snapshot sync
- news metadata sync
- feature store build
- market regime snapshot build
- explanatory ranking materialization

Forward labels and ranking validation remain separate because they depend on future trading days.

## TICKET-003 execution flow

Single-date build flow:

```powershell
python scripts/build_feature_store.py --as-of-date 2026-03-06 --limit-symbols 100
python scripts/build_forward_labels.py --start 2026-03-02 --end 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/build_market_regime_snapshot.py --as-of-date 2026-03-06
python scripts/materialize_explanatory_ranking.py --as-of-date 2026-03-06 --horizons 1 5 --limit-symbols 100
python scripts/validate_explanatory_ranking.py --start 2026-02-17 --end 2026-03-06 --horizons 1 5
```

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
- `data_quality`

Representative features:

- price/trend: `ret_3d`, `ret_5d`, `ret_20d`, `ma5_over_ma20`, `ma20_over_ma60`, `drawdown_20d`
- volatility/risk: `realized_vol_20d`, `gap_abs_avg_20d`, `hl_range_1d`, `max_loss_20d`
- turnover/liquidity: `volume_ratio_1d_vs_20d`, `turnover_z_5_20`, `adv_20`, `adv_60`
- fundamentals/quality: `roe_latest`, `debt_ratio_latest`, `operating_margin_latest`, `days_since_latest_report`
- value/safety: `earnings_yield_proxy`, `low_debt_preference_proxy`, `profitability_support_proxy`
- news: `news_count_3d`, `distinct_publishers_3d`, `latest_news_age_hours`, `positive_catalyst_count_3d`
- data quality: `stale_price_flag`, `missing_key_feature_count`, `data_confidence_score`

## Feature calculation overview

- price and trend features come from `fact_daily_ohlcv`
- turnover features use `turnover_value` and fall back to `close * volume`
- fundamentals features come from the latest eligible DART snapshot at or before `as_of_date`
- news features use metadata only and conservative symbol linking
- data quality features score whether price, fundamentals, and news are present and current
- all feature rows are materialized into `fact_feature_snapshot`
- cross-sectional `rank_pct` and `zscore` are computed within market (`KOSPI`, `KOSDAQ`)

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

## Grade rules

Current grade assignment:

- `A`: eligible and top 5%
- `A-`: eligible and top 15%
- `B`: eligible and top 35%, or critical-risk names that still clear the minimum threshold
- `C`: everything else, including ineligible names

Grades are presentation buckets, not probability estimates.

## Tables and views added by TICKET-003

Tables:

- `fact_feature_snapshot`
- `fact_forward_return_label`
- `fact_market_regime_snapshot`
- `fact_ranking`
- `ops_ranking_validation_summary`

Views:

- `vw_feature_snapshot_latest`
- `vw_feature_matrix_latest`
- `vw_latest_forward_return_label`
- `vw_market_regime_latest`
- `vw_ranking_latest`
- `vw_latest_ranking_validation_summary`

## Raw, curated, and artifact outputs

Representative paths:

```text
data/curated/features/as_of_date=YYYY-MM-DD/feature_snapshot.parquet
data/curated/features/as_of_date=YYYY-MM-DD/feature_matrix.parquet
data/curated/labels/as_of_date=YYYY-MM-DD/forward_return_labels.parquet
data/curated/regime/as_of_date=YYYY-MM-DD/market_regime_snapshot.parquet
data/curated/ranking/as_of_date=YYYY-MM-DD/horizon=N/explanatory_ranking.parquet
data/artifacts/validation/ranking/start_date=YYYY-MM-DD/end_date=YYYY-MM-DD/ranking_validation_summary.parquet
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

## Known limitations

- ranking is explanatory only and does not include ML alpha, uncertainty, or disagreement
- `flow_score` is reserved
- `market_cap` depends on upstream availability and is not yet independently validated
- many 20d and 60d features will be null on shallow history windows
- symbol linking for news is intentionally conservative and may leave `symbol_candidates = []`
- no full article body storage
- validation is sparse unless historical ranking snapshots have already been materialized

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
- Ops shows version tracking, feature coverage, label coverage, regime snapshot, and failures
- Research shows feature store, regime, labels, and sample upstream tables
- Leaderboard shows rank table, grade mix, and validation summary

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.
