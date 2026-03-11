# Metadata / Host Worker Validation

이 문서는 metadata Postgres + host scheduler worker 전환 후 검증 순서를 남기는 체크리스트입니다.
운영 절차 전체는 [RUNBOOK_SERVER_OPERATIONS.md](d:/MyApps/StockMaster/docs/RUNBOOK_SERVER_OPERATIONS.md)를 우선합니다.

## Server Preconditions

- `stockmaster-app-1` healthy
- `stockmaster-nginx-1` healthy
- `stockmaster-metadata_db-1` healthy
- server code is on intended revision
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

- row counts are printed
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

- latest metadata timestamps move forward in Postgres
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

- commands run through `/opt/stockmaster/worker-venv`
- scheduler path no longer depends on `docker compose exec -T app ...`
- if another writer currently holds the single-writer lock, `SKIPPED_LOCKED` is acceptable

## Operational Interpretation

- `SKIPPED_LOCKED` during validation is acceptable when a real writer already exists
- stale `RUNNING` rows or stale active locks should be cleaned before calling the new path broken
- psycopg connection errors or schema errors are real regressions
