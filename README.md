# StockMaster

StockMaster is a Korea-focused personal stock research platform for post-market analysis, ranking, reporting, and evaluation. The repository now covers:

- foundation and storage bootstrap
- provider activation for KIS, DART, and Naver News
- reference data sync for `dim_symbol` and `dim_trading_calendar`
- core research data sync for OHLCV, fundamentals snapshots, and news metadata

Auto-trading, order routing, and tick/orderbook warehousing remain out of scope.

## Scope in this state

Implemented now:

- YAML + `.env` typed settings
- structured logging with `run_id` and `run_type`
- DuckDB bootstrap, run manifest, and disk watermark tracking
- KIS token cache, symbol master download, quote probe, and daily OHLCV ingestion
- DART corpCode cache, company overview probe, regular disclosure scan, and financial snapshot materialization
- Naver News metadata fetch, dedupe, and conservative symbol linking
- `dim_symbol`, `dim_trading_calendar`, `fact_daily_ohlcv`, `fact_fundamentals_snapshot`, `fact_news_item`
- Streamlit Home, Ops, and Research inspection pages
- unit and integration tests for the current ingestion layers

Still follow-up work:

- full feature store and ranking models
- report rendering and Discord delivery
- richer news scoring and more advanced entity linking
- sector/industry enrichment beyond the current seed fallback

## Repository layout

```text
app/                Application packages
config/             YAML configuration and local examples
data/               Runtime storage roots (.gitkeep only)
docs/               Architecture notes and ticket docs
scripts/            CLI entrypoints
tests/              Unit and integration tests
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

Live data collection requires:

- `KIS_APP_KEY`
- `KIS_APP_SECRET`
- `DART_API_KEY`
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

Needed when you use the related provider features:

- `KIS_ACCOUNT_NO`
- `KIS_PRODUCT_CODE`
- `KIS_USE_MOCK`
- `KRX_API_KEY`
- `DISCORD_WEBHOOK_URL`

Notes:

- `KIS_USE_MOCK=false` uses the production KIS host.
- `KRX_API_KEY` is still optional in TICKET-002 because KRX is not yet used as a live ingestion source.
- `DISCORD_WEBHOOK_URL` is still unused by the current pipelines.

## Bootstrap and reference data

TICKET-002 assumes the TICKET-001 reference data already exists. Run these first on a new machine:

```powershell
python scripts/bootstrap.py
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
python scripts/sync_universe.py
python scripts/provider_smoke_check.py --symbol 005930
```

## TICKET-002 pipeline flow

Run the core research data syncs in this order.

1. Daily OHLCV

```powershell
python scripts/sync_daily_ohlcv.py --date 2026-03-06 --limit-symbols 50
```

2. Fundamentals snapshot

```powershell
python scripts/sync_fundamentals_snapshot.py --as-of-date 2026-03-06 --limit-symbols 50
```

3. News metadata

```powershell
python scripts/sync_news_metadata.py --date 2026-03-06 --mode market_and_focus --limit-symbols 50
```

4. Backfill example

```powershell
python scripts/backfill_core_research_data.py --start 2026-03-02 --end 2026-03-06 --limit-symbols 50
```

5. Scheduled-style daily orchestration

```powershell
python scripts/run_daily_pipeline.py
```

This entrypoint resolves the latest available trading day from `dim_trading_calendar` and runs the OHLCV, fundamentals, and news metadata syncs as one manifest-tracked daily job.

6. Start the UI

```powershell
streamlit run app/ui/Home.py
```

## Command reference

### `scripts/sync_daily_ohlcv.py`

Supported options:

- `--date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--market KOSPI|KOSDAQ|ALL`
- `--force`
- `--dry-run`

Behavior:

- requires `dim_trading_calendar`
- skips non-trading days with a success note
- validates `high >= max(open, close)`, `low <= min(open, close)`, positive prices, and non-negative volume
- uses `(trading_date, symbol)` as the deterministic upsert key

### `scripts/sync_fundamentals_snapshot.py`

Supported options:

- `--as-of-date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--force`
- `--dry-run`

Behavior:

- reads `dim_symbol.dart_corp_code`
- scans DART regular disclosures up to the requested `as_of_date`
- prefers consolidated statements (`CFS`), then falls back to separate statements (`OFS`)
- materializes one row per `(as_of_date, symbol)`

### `scripts/sync_news_metadata.py`

Supported options:

- `--date YYYY-MM-DD`
- `--start YYYY-MM-DD --end YYYY-MM-DD`
- `--symbols 005930,000660`
- `--limit-symbols 100`
- `--mode market_only|market_and_focus|symbol_list`
- `--query-pack default`
- `--max-items-per-query 50`
- `--force`
- `--dry-run`

Behavior:

- stores metadata only, never full article body
- filters Naver search results by `signal_date`
- dedupes by canonical link or stable article identity
- uses conservative symbol linking and allows empty `symbol_candidates`

## Raw and curated storage

Representative outputs created by TICKET-002:

```text
data/raw/kis/daily_ohlcv/trading_date=YYYY-MM-DD/symbol=XXXXXX/*.json
data/raw/dart/financials/disclosed_date=YYYY-MM-DD/symbol=XXXXXX/*.json
data/raw/naver_news/fetch_date=YYYY-MM-DD/query_bucket=.../*.json

data/curated/market/daily_ohlcv/trading_date=YYYY-MM-DD/*.parquet
data/curated/fundamentals/snapshot/as_of_date=YYYY-MM-DD/*.parquet
data/curated/news/items/signal_date=YYYY-MM-DD/*.parquet
```

DuckDB helper views available after bootstrap:

- `vw_universe_active_common_stock`
- `vw_latest_daily_ohlcv`
- `vw_latest_fundamentals_snapshot`
- `vw_news_recent_market`
- `vw_news_recent_by_symbol`

## News query pack adjustment

The default query pack lives in [config/news_queries.yaml](/d:/MyApps/StockMaster/config/news_queries.yaml).

Current structure:

- `packs.default.market`: market-wide and thematic queries
- focus queries: generated from `dim_symbol.company_name` for the selected subset

How to adjust:

1. Edit `packs.default.market`
2. Add or remove `bucket` / `keyword` pairs
3. Rerun `scripts/sync_news_metadata.py`

Operational note:

- `market_and_focus` without explicit symbols defaults the focus subset to 25 names to avoid an unbounded query fan-out.

## Fundamentals availability rule

Current rule is the conservative date-only rule:

- a DART filing is eligible only if `rcept_dt <= as_of_date`
- the latest eligible regular filing wins
- the code is structured so a later ticket can add a time-of-day cutoff rule

This avoids mixing future disclosures into historical snapshots.

## Known limitations

- `market_cap` is still null in `fact_daily_ohlcv`
- DART ratio calculations are simple snapshot ratios, not yet TTM or sector-adjusted
- news publisher is derived from the article link domain
- symbol linking is intentionally conservative and may leave `symbol_candidates = []`
- Naver Search can rate limit bursty runs; the client retries on `429`, but very large focus sets will still slow down
- no full article text storage
- no feature store, model inference, leaderboard, or evaluation reports yet

## Validation

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

- Home shows reference data summary and research data freshness
- Ops shows latest sync runs and recent failed runs
- Research page shows sample OHLCV, fundamentals, and news rows

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.
