# Scheduler Server Runbook Update

## Scope

This note supplements `docs/SCHEDULER_SERVER_RUNBOOK.md` after the host worker transition.

## Current Scheduler Path

systemd timer -> `stockmaster-scheduler@.service` -> `scripts/server/run_scheduler_job_host.sh`

The host worker then runs:

- `/opt/stockmaster/worker-venv/bin/python`
- `/opt/stockmaster/app/scripts/run_scheduled_bundle.py`

## Key Differences From The Old Path

Old path:

- `docker compose exec -T app python ...`

Current path:

- host Python 3.11 worker venv
- metadata DB available through loopback
- app container no longer needs to be the scheduler execution process

## Provisioning

Install or refresh the worker venv:

```bash
cd /opt/stockmaster/app
bash scripts/server/ensure_scheduler_worker_venv.sh
```

Install or refresh units:

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/install_scheduler_units.sh
bash scripts/server/status_scheduler_units.sh
```

## Validation

Dry-run examples:

```bash
cd /opt/stockmaster/app
sudo -E bash scripts/server/run_scheduler_job_host.sh news-morning --as-of-date 2026-03-11 --dry-run
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-close --as-of-date 2026-03-10 --dry-run --force --skip-discord
sudo -E bash scripts/server/run_scheduler_job_host.sh evaluation --as-of-date 2026-03-10 --dry-run --force
sudo -E bash scripts/server/run_scheduler_job_host.sh daily-audit-lite --as-of-date 2026-03-11 --dry-run
```

Interpretation:

- `SUCCESS` or `SKIPPED` is acceptable in dry-run mode
- `SKIPPED_LOCKED` means another real writer is currently active
- stale `RUNNING` rows and stale active locks should be cleaned before calling the new path broken

## Compatibility Wrapper

`scripts/server/run_scheduler_job.sh` now acts as a deprecated wrapper and delegates to the host worker runner.

## Current Limitation

The platform still uses DuckDB as a single-writer analytics store.
The host worker path reduces process overlap, but legitimate writer serialization still applies.
