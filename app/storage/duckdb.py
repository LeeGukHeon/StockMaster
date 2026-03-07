from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from app.common.paths import ensure_directory

CORE_TABLE_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS dim_symbol (
        symbol VARCHAR PRIMARY KEY,
        company_name VARCHAR,
        market VARCHAR,
        sector VARCHAR,
        industry VARCHAR,
        listing_date DATE,
        is_common_stock BOOLEAN,
        is_etf BOOLEAN,
        is_etn BOOLEAN,
        is_spac BOOLEAN,
        is_delisted BOOLEAN,
        status_flags VARCHAR,
        updated_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_trading_calendar (
        trading_date DATE PRIMARY KEY,
        is_trading_day BOOLEAN NOT NULL,
        market_session_type VARCHAR,
        prev_trading_date DATE,
        next_trading_date DATE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops_run_manifest (
        run_id VARCHAR PRIMARY KEY,
        run_type VARCHAR NOT NULL,
        as_of_date DATE,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status VARCHAR NOT NULL,
        input_sources_json VARCHAR,
        output_artifacts_json VARCHAR,
        model_version VARCHAR,
        feature_version VARCHAR,
        git_commit VARCHAR,
        notes VARCHAR,
        error_message VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops_disk_usage_log (
        measured_at TIMESTAMPTZ NOT NULL,
        mount_point VARCHAR NOT NULL,
        used_gb DOUBLE NOT NULL,
        available_gb DOUBLE NOT NULL,
        usage_ratio DOUBLE NOT NULL,
        action_taken VARCHAR
    )
    """,
)


def connect_duckdb(
    db_path: Path,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    ensure_directory(db_path.parent)
    return duckdb.connect(str(db_path), read_only=read_only)


@contextmanager
def duckdb_connection(
    db_path: Path,
    *,
    read_only: bool = False,
) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = connect_duckdb(db_path, read_only=read_only)
    try:
        yield connection
    finally:
        connection.close()


def bootstrap_core_tables(connection: duckdb.DuckDBPyConnection) -> None:
    for ddl in CORE_TABLE_DDL:
        connection.execute(ddl)


def fetch_dataframe(connection: duckdb.DuckDBPyConnection, query: str):
    return connection.execute(query).fetchdf()
