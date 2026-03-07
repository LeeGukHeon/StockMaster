# StockMaster

StockMaster is the foundation repository for a Korea-focused stock research platform. This first work package sets up the runtime skeleton only: settings, logging, DuckDB bootstrap, provider stubs, batch entrypoints, and a Streamlit UI shell.

## Scope of this foundation

- Domestic equities only (`KOSPI` / `KOSDAQ`) for v1
- Post-market research, ranking, reporting, and evaluation
- No order routing or auto-trading
- DuckDB + Parquet storage foundation with run manifest tracking
- Streamlit dashboard shell for Home, Ops, and Research placeholders

## Repository layout

```text
app/                Application packages
config/             YAML configuration
data/               Runtime storage roots (.gitkeep only)
docs/               Architecture notes and ADRs
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

3. Bootstrap storage and DuckDB.

```powershell
python scripts/bootstrap.py
```

4. Start the Streamlit UI.

```powershell
python -m streamlit run app/ui/Home.py
```

5. Run the current skeleton jobs.

```powershell
python scripts/run_daily_pipeline.py
python scripts/run_evaluation.py
python scripts/prune_storage.py
```

6. Run tests.

```powershell
pytest
```

## Docker

Copy `.env.example` to `.env`, then run:

```powershell
docker compose up --build
```

The container starts the Streamlit dashboard on `http://localhost:8501`.

## Current foundation status

Implemented now:

- Typed settings loader using YAML + `.env`
- Structured logging with `run_id` and `run_type`
- Data directory bootstrap
- DuckDB initialization for `dim_symbol`, `dim_trading_calendar`, `ops_run_manifest`, and `ops_disk_usage_log`
- Provider skeletons for KIS, DART, KRX, and Naver News
- Streamlit Home, Ops, and Research placeholder pages
- Unit/integration tests for settings, run context, disk guard, and bootstrap

Still skeleton / placeholder:

- Real provider API integrations
- Market data ingestion pipelines
- Feature engineering and ranking logic
- Evaluation engine details
- HTML report generation and Discord delivery

## Notes

- Runtime outputs under `data/` are intentionally ignored by Git.
- The root planning documents remain in place and are treated as the source of truth.
- Provider methods currently return stub payloads and health-check placeholders until follow-up tickets implement the real integrations.
