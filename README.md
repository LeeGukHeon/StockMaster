# StockMaster

StockMaster is a Korea-focused personal stock research platform. The repository now covers the foundation plus the first live reference-data ticket:

- KIS minimal provider activation
- DART minimal provider activation
- `dim_symbol` population
- `dim_trading_calendar` population
- Streamlit Home/Ops summaries for universe, calendar, and provider health

The product scope remains post-market research, ranking, reporting, and evaluation. Order routing and auto-trading are intentionally out of scope.

## Scope in this state

Implemented now:

- Typed settings loader using YAML + `.env`
- Structured logging with `run_id` and `run_type`
- Data directory bootstrap and DuckDB initialization
- KIS token cache, symbol master download, current quote probe, daily OHLCV probe
- DART corpCode download/cache/parse and company overview probe
- `dim_symbol`, `vw_universe_active_common_stock`, and `dim_trading_calendar`
- Streamlit Home, Ops, and Research placeholder pages
- Unit and integration tests for normalization, calendar sync, bootstrap, universe sync, and provider smoke checks

Still skeleton / follow-up tickets:

- Full OHLCV bulk ingestion across the universe
- Financial statement bulk ingestion
- News ingestion and ranking features
- D+1 / D+5 feature store, modeling, evaluation, and report delivery

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

Required for live provider activation:

- `KIS_APP_KEY`
- `KIS_APP_SECRET`
- `DART_API_KEY`

Optional in this ticket:

- `KIS_ACCOUNT_NO`
- `KIS_PRODUCT_CODE`
- `KRX_API_KEY`
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`
- `DISCORD_WEBHOOK_URL`

Notes:

- `KIS_USE_MOCK=false` uses the production KIS host.
- `KRX_API_KEY` can stay blank in TICKET-001.
- `DISCORD_WEBHOOK_URL` is not used yet by the current scripts.

## Reference-data flow

Run the ticket flow in this order.

1. Bootstrap storage and DuckDB.

```powershell
python scripts/bootstrap.py
```

2. Build the trading calendar.

```powershell
python scripts/sync_trading_calendar.py --start 2025-01-01 --end 2026-12-31
```

3. Sync the KOSPI/KOSDAQ symbol universe.

```powershell
python scripts/sync_universe.py
```

4. Run the provider smoke check.

```powershell
python scripts/provider_smoke_check.py --symbol 005930
```

5. Start the Streamlit UI.

```powershell
streamlit run app/ui/Home.py
```

## What each command does

`python scripts/bootstrap.py`

- creates `data/raw`, `data/curated`, `data/marts`, `data/cache`, `data/logs`, `data/artifacts`
- creates or migrates DuckDB
- records a `bootstrap` run in `ops_run_manifest`

`python scripts/sync_trading_calendar.py --start ... --end ...`

- generates `dim_trading_calendar`
- uses weekend rules + Korea public holidays + optional override CSV
- records a `sync_trading_calendar` run
- writes a snapshot parquet under `data/artifacts/trading_calendar/`

`python scripts/sync_universe.py`

- downloads official KIS KOSPI/KOSDAQ symbol master files
- downloads or loads cached DART corpCode mapping
- normalizes and upserts `dim_symbol`
- recreates `vw_universe_active_common_stock`
- records a `sync_universe` run

`python scripts/provider_smoke_check.py --symbol 005930`

- runs a KIS current-quote probe
- resolves DART `corp_code` by symbol when possible
- runs a DART company overview probe when mapping exists
- records a `provider_smoke_check` run

## Raw storage rules

Representative outputs created by this ticket:

```text
data/raw/kis/current_quote_probe/date=YYYY-MM-DD/*.json
data/raw/kis/daily_ohlcv_probe/date=YYYY-MM-DD/*.json
data/raw/kis/daily_ohlcv_probe/date=YYYY-MM-DD/*.parquet
data/raw/kis/symbol_master_zip/date=YYYY-MM-DD/*.zip
data/raw/dart/corp_codes/date=YYYY-MM-DD/*.zip
data/raw/dart/company_overview/date=YYYY-MM-DD/*.json
data/raw/reference/symbol_master/date=YYYY-MM-DD/*.parquet
data/raw/reference/symbol_master_snapshot/date=YYYY-MM-DD/*.parquet
```

## Override and fallback files

Optional files:

- `config/trading_calendar_overrides.csv`
- `config/seeds/symbol_master_seed.csv`
- `config/universe_filters.yaml`

Examples are included here:

- `config/trading_calendar_overrides.example.csv`
- `config/seeds/symbol_master_seed.example.csv`

Usage:

- copy the example file to the non-example name
- keep only the rows you want to override or enrich
- rerun the matching sync script

Current fallback behavior:

- calendar overrides are applied last and win over weekend/holiday rules
- symbol seed fallback only enriches missing `sector`, `industry`, and `market_segment`
- KRX seed fallback is optional and does not replace KIS as the primary source

## Validation

Run the current test suite:

```powershell
python -m pytest
```

Run lint:

```powershell
python -m ruff check .
```

## UI checkpoints

After `streamlit run app/ui/Home.py`, verify:

- Home shows data root and DuckDB path
- Home shows symbol counts, DART mapped count, and calendar min/max range
- Ops shows recent run manifest rows
- Provider health shows KIS/DART status summaries

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.
