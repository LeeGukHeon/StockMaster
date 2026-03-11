# Metadata / Host Worker Validation

## Purpose

This document records the validation sequence after introducing:

- postgres metadata store
- metadata dual-write
- host scheduler worker

## Server Preconditions

- `stockmaster-app-1` is healthy
- `stockmaster-nginx-1` is healthy
- `stockmaster-metadata_db-1` is healthy
- server code is at the latest `main`
- `deploy/env/.env.server` contains:
  - `METADATA_DB_ENABLED=true`
  - `METADATA_DB_BACKEND=postgres`
  - `METADATA_DB_HOST_PORT=5433`

## Validation Order

### 1. metadata schema bootstrap

```bash
cd /opt/stockmaster/app
python3 scripts/bootstrap_metadata_store.py
```

Expected:

- schema exists
- no connection error to `metadata_db`

### 2. metadata migration

```bash
cd /opt/stockmaster/app
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  run --rm app python scripts/migrate_duckdb_metadata_to_postgres.py --truncate-first
```

Expected:

- table row counts printed
- no psycopg or schema errors

### 3. latest metadata rebuild

```bash
cd /opt/stockmaster/app
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  run --rm app python scripts/build_report_index.py
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  run --rm app python scripts/build_ui_freshness_snapshot.py
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  run --rm app python scripts/build_latest_app_snapshot.py --as-of-date 2026-03-11
docker compose --profile metadata --env-file deploy/env/.env.server -f deploy/docker-compose.server.yml \
  run --rm app python scripts/validate_release_candidate.py --as-of-date 2026-03-11
```

Expected:

- latest metadata timestamps move forward in postgres
- release validation finishes without connection errors

### 4. host worker dry-run

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --as-of-date 2026-03-11 --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --as-of-date 2026-03-10 --dry-run --force --skip-discord
sudo -E bash scripts/server/run_scheduler_job_host.sh evaluation --as-of-date 2026-03-10 --dry-run --force
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-audit-lite --as-of-date 2026-03-11 --dry-run --force
```

Expected:

- command runs through `/opt/stockmaster/worker-venv`
- scheduler path no longer depends on `docker compose exec -T app ...`
- if another bundle currently holds the single-writer lock, result may be `SKIPPED_LOCKED` rather than failure

## Operational Interpretation

- `SKIPPED_LOCKED` during validation is acceptable if there is a real active writer
- stale `RUNNING` rows or stale active locks must be cleaned before treating the path as broken
- a psycopg syntax error or metadata connection error is a real regression
