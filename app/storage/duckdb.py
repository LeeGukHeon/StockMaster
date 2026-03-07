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
    CREATE TABLE IF NOT EXISTS fact_daily_ohlcv (
        trading_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        turnover_value DOUBLE,
        market_cap DOUBLE,
        source VARCHAR,
        source_notes_json VARCHAR,
        ingested_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (trading_date, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_fundamentals_snapshot (
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        fiscal_year INTEGER,
        report_code VARCHAR,
        revenue DOUBLE,
        operating_income DOUBLE,
        net_income DOUBLE,
        roe DOUBLE,
        debt_ratio DOUBLE,
        operating_margin DOUBLE,
        source_doc_id VARCHAR,
        source VARCHAR,
        disclosed_at TIMESTAMPTZ,
        statement_basis VARCHAR,
        report_name VARCHAR,
        currency VARCHAR,
        accounting_standard VARCHAR,
        source_notes_json VARCHAR,
        ingested_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_news_item (
        news_id VARCHAR PRIMARY KEY,
        signal_date DATE,
        published_at TIMESTAMPTZ,
        symbol_candidates VARCHAR,
        query_keyword VARCHAR,
        title VARCHAR,
        publisher VARCHAR,
        link VARCHAR,
        snippet VARCHAR,
        tags_json VARCHAR,
        catalyst_score DOUBLE,
        sentiment_score DOUBLE,
        freshness_score DOUBLE,
        source VARCHAR,
        canonical_link VARCHAR,
        match_method_json VARCHAR,
        query_bucket VARCHAR,
        is_market_wide BOOLEAN,
        source_notes_json VARCHAR,
        ingested_at TIMESTAMPTZ NOT NULL
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

CORE_VIEW_DDL: tuple[str, ...] = (
    """
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
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_daily_ohlcv AS
    SELECT *
    FROM fact_daily_ohlcv
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol
        ORDER BY trading_date DESC, ingested_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_fundamentals_snapshot AS
    SELECT *
    FROM fact_fundamentals_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol
        ORDER BY as_of_date DESC, disclosed_at DESC NULLS LAST, ingested_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_news_recent_market AS
    SELECT
        signal_date,
        published_at,
        title,
        publisher,
        query_bucket,
        tags_json,
        link,
        freshness_score
    FROM fact_news_item
    WHERE COALESCE(is_market_wide, FALSE)
      AND signal_date >= CURRENT_DATE - INTERVAL 7 DAY
    ORDER BY signal_date DESC, published_at DESC
    """,
    """
    CREATE OR REPLACE VIEW vw_news_recent_by_symbol AS
    SELECT
        signal_date,
        published_at,
        title,
        publisher,
        symbol_candidates,
        query_bucket,
        link,
        freshness_score
    FROM fact_news_item
    WHERE COALESCE(symbol_candidates, '[]') <> '[]'
      AND signal_date >= CURRENT_DATE - INTERVAL 7 DAY
    ORDER BY signal_date DESC, published_at DESC
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

    for ddl in SYMBOL_COLUMN_MIGRATIONS:
        connection.execute(ddl)

    for ddl in CALENDAR_COLUMN_MIGRATIONS:
        connection.execute(ddl)

    for ddl in CORE_VIEW_DDL:
        connection.execute(ddl)


def fetch_dataframe(connection: duckdb.DuckDBPyConnection, query: str):
    return connection.execute(query).fetchdf()
