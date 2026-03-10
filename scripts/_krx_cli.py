# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging
from app.providers.krx.monitoring import resolve_default_as_of_date
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--as-of-date", type=parse_date)
    return parser


def load_cli_settings():
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    return settings


def resolve_cli_as_of_date(settings, requested_date: date | None) -> date:
    if requested_date is not None:
        return requested_date
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        return resolve_default_as_of_date(settings, connection=connection)
