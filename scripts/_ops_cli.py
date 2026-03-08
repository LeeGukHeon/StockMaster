# ruff: noqa: E402

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.ops.runtime import JobRunContext
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def load_cli_settings():
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    return settings


def log_and_print(message: str, *, extra: dict[str, object] | None = None) -> None:
    get_logger(__name__).info(message, extra=extra or {})
    print(message)


def run_standalone_job(
    settings,
    *,
    job_name: str,
    as_of_date,
    dry_run: bool,
    policy_config_path: str | None,
    runner,
):
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name=job_name,
            as_of_date=as_of_date,
            dry_run=dry_run,
            policy_config_path=policy_config_path,
            notes=f"{job_name} executed from CLI.",
        ) as job:
            return runner(connection, job)
