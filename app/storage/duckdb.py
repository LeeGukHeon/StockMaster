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
        market_segment VARCHAR,
        sector VARCHAR,
        industry VARCHAR,
        listing_date DATE,
        security_type VARCHAR,
        is_common_stock BOOLEAN,
        is_preferred_stock BOOLEAN,
        is_etf BOOLEAN,
        is_etn BOOLEAN,
        is_spac BOOLEAN,
        is_reit BOOLEAN,
        is_delisted BOOLEAN,
        is_trading_halt BOOLEAN,
        is_management_issue BOOLEAN,
        status_flags VARCHAR,
        dart_corp_code VARCHAR,
        dart_corp_name VARCHAR,
        source VARCHAR,
        as_of_date DATE,
        updated_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_trading_calendar (
        trading_date DATE PRIMARY KEY,
        is_trading_day BOOLEAN NOT NULL,
        market_session_type VARCHAR,
        weekday INTEGER,
        is_weekend BOOLEAN,
        is_public_holiday BOOLEAN,
        holiday_name VARCHAR,
        source VARCHAR,
        source_confidence VARCHAR,
        is_override BOOLEAN,
        prev_trading_date DATE,
        next_trading_date DATE,
        updated_at TIMESTAMPTZ
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

SYMBOL_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS market_segment VARCHAR",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS security_type VARCHAR",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS is_preferred_stock BOOLEAN",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS is_reit BOOLEAN",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS is_trading_halt BOOLEAN",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS is_management_issue BOOLEAN",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS dart_corp_code VARCHAR",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS dart_corp_name VARCHAR",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS source VARCHAR",
    "ALTER TABLE dim_symbol ADD COLUMN IF NOT EXISTS as_of_date DATE",
)

CALENDAR_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS weekday INTEGER",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS is_public_holiday BOOLEAN",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS holiday_name VARCHAR",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS source VARCHAR",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS source_confidence VARCHAR",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS is_override BOOLEAN",
    "ALTER TABLE dim_trading_calendar ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
)

ACTIVE_COMMON_STOCK_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_universe_active_common_stock AS
SELECT *
FROM dim_symbol
WHERE market IN ('KOSPI', 'KOSDAQ')
  AND COALESCE(is_common_stock, FALSE)
  AND NOT COALESCE(is_etf, FALSE)
  AND NOT COALESCE(is_etn, FALSE)
  AND NOT COALESCE(is_spac, FALSE)
  AND NOT COALESCE(is_reit, FALSE)
  AND NOT COALESCE(is_delisted, FALSE)
"""


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

    for ddl in SYMBOL_COLUMN_MIGRATIONS:
        connection.execute(ddl)

    for ddl in CALENDAR_COLUMN_MIGRATIONS:
        connection.execute(ddl)

    connection.execute(ACTIVE_COMMON_STOCK_VIEW_DDL)


def fetch_dataframe(connection: duckdb.DuckDBPyConnection, query: str):
    return connection.execute(query).fetchdf()
