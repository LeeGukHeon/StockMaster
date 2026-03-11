# Scheduler Host Worker Update

## Purpose

This note supplements the existing scheduler runbook with the 2026-03 host-worker transition.

## What Changed

- scheduler services no longer rely on `docker compose exec -T app ...`
- scheduler services now call `scripts/server/run_scheduler_job_host.sh`
- the host worker uses a dedicated Python 3.11 virtual environment:
  - default path: `/opt/stockmaster/worker-venv`
- metadata split mode is now supported in server automation:
  - `metadata_db` container is started first
  - the worker talks to metadata postgres through loopback

## Required Server Env

These keys must exist in `deploy/env/.env.server` when metadata split is enabled:

- `METADATA_DB_ENABLED=true`
- `METADATA_DB_BACKEND=postgres`
- `METADATA_DB_SCHEMA=stockmaster_meta`
- `METADATA_DB_POSTGRES_DB=stockmaster_meta`
- `METADATA_DB_POSTGRES_USER=stockmaster`
- `METADATA_DB_POSTGRES_PASSWORD=...`
- `METADATA_DB_HOST_PORT=5433`
- `METADATA_DB_URL=postgresql://stockmaster:...@metadata_db:5432/stockmaster_meta`

## Worker Setup

Install or refresh the scheduler worker environment:

```bash
cd /opt/stockmaster/app
bash scripts/server/ensure_scheduler_worker_venv.sh
```

The script prefers `python3.11` and recreates the venv if an older Python version is detected.

## systemd Unit

The scheduler unit now points to:

```text
/opt/stockmaster/app/scripts/server/run_scheduler_job_host.sh
```

Reinstall units after updating code:

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/install_scheduler_units.sh
bash scripts/server/status_scheduler_units.sh
```

## Dry-run Validation

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --as-of-date 2026-03-11 --dry-run
```

Expected result:

- the command runs through the host worker venv
- `metadata_db` is reachable
- scheduler serial lock and DB active lock logic still applies

## Current Limitation

Even with the host worker path in place, a currently active batch can still legitimately hold
`scheduler_global_write`. In that case the new worker should skip or block cleanly rather than
crash. This is expected behavior, not a regression in the new worker path.
