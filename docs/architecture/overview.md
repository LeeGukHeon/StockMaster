# Architecture Overview

StockMaster v1 starts with a split between runtime concerns:

- `app/settings.py` centralizes YAML + `.env` loading into typed settings.
- `app/storage/` owns directory bootstrap, DuckDB initialization, manifest logging, and parquet helpers.
- `app/providers/` defines provider-specific skeletons behind a shared base class.
- `app/scheduler/jobs.py` records every batch entrypoint in `ops_run_manifest`.
- `app/ui/` is read-only presentation for current runtime state.

## Foundation flow

1. `scripts/bootstrap.py` creates the runtime directory layout and core DuckDB tables.
2. Every batch entrypoint allocates a `run_id`, writes `ops_run_manifest`, and records disk usage.
3. Streamlit pages read the manifest and storage status without owning ingestion logic.
4. Follow-up tickets can fill in providers, curated storage, ranking, reporting, and evaluation without changing the foundation contract.
