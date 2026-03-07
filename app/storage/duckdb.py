from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from app.common.paths import ensure_directory
from app.features.constants import FEATURE_NAMES


def _build_feature_matrix_latest_view() -> str:
    select_columns = ",\n        ".join(
        [
            "MAX(CASE WHEN feature_name = "
            f"'{feature_name}' THEN feature_value END) AS {feature_name}"
            for feature_name in FEATURE_NAMES
        ]
    )
    return f"""
    CREATE OR REPLACE VIEW vw_feature_matrix_latest AS
    WITH latest_date AS (
        SELECT MAX(as_of_date) AS as_of_date
        FROM fact_feature_snapshot
    )
    SELECT
        snapshot.as_of_date,
        snapshot.symbol,
        {select_columns}
    FROM fact_feature_snapshot AS snapshot
    JOIN latest_date
      ON snapshot.as_of_date = latest_date.as_of_date
    GROUP BY snapshot.as_of_date, snapshot.symbol
    """


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
    CREATE TABLE IF NOT EXISTS fact_investor_flow (
        run_id VARCHAR NOT NULL,
        trading_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        market VARCHAR,
        foreign_net_volume DOUBLE,
        institution_net_volume DOUBLE,
        individual_net_volume DOUBLE,
        foreign_net_value DOUBLE,
        institution_net_value DOUBLE,
        individual_net_value DOUBLE,
        source VARCHAR,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (trading_date, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_feature_snapshot (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        feature_name VARCHAR NOT NULL,
        feature_value DOUBLE,
        feature_group VARCHAR,
        source_version VARCHAR,
        feature_rank_pct DOUBLE,
        feature_zscore DOUBLE,
        is_imputed BOOLEAN,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol, feature_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_forward_return_label (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        market VARCHAR,
        entry_date DATE,
        exit_date DATE,
        entry_basis VARCHAR,
        exit_basis VARCHAR,
        entry_price DOUBLE,
        exit_price DOUBLE,
        gross_forward_return DOUBLE,
        baseline_type VARCHAR,
        baseline_forward_return DOUBLE,
        excess_forward_return DOUBLE,
        label_available_flag BOOLEAN NOT NULL,
        exclusion_reason VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol, horizon)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_market_regime_snapshot (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        market_scope VARCHAR NOT NULL,
        breadth_up_ratio DOUBLE,
        breadth_down_ratio DOUBLE,
        median_symbol_return_1d DOUBLE,
        median_symbol_return_5d DOUBLE,
        market_realized_vol_20d DOUBLE,
        turnover_burst_z DOUBLE,
        new_high_ratio_20d DOUBLE,
        new_low_ratio_20d DOUBLE,
        regime_state VARCHAR,
        regime_score DOUBLE,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, market_scope)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_ranking (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        final_selection_value DOUBLE,
        final_selection_rank_pct DOUBLE,
        grade VARCHAR,
        explanatory_score_json VARCHAR,
        top_reason_tags_json VARCHAR,
        risk_flags_json VARCHAR,
        eligible_flag BOOLEAN,
        eligibility_notes_json VARCHAR,
        regime_state VARCHAR,
        ranking_version VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol, horizon, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_prediction (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        market VARCHAR,
        ranking_version VARCHAR NOT NULL,
        prediction_version VARCHAR NOT NULL,
        expected_excess_return DOUBLE,
        lower_band DOUBLE,
        median_band DOUBLE,
        upper_band DOUBLE,
        calibration_start_date DATE,
        calibration_end_date DATE,
        calibration_bucket VARCHAR,
        calibration_sample_size BIGINT,
        disagreement_score DOUBLE,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol, horizon, prediction_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_selection_outcome (
        selection_date DATE NOT NULL,
        evaluation_date DATE,
        symbol VARCHAR NOT NULL,
        market VARCHAR,
        horizon INTEGER NOT NULL,
        ranking_version VARCHAR NOT NULL,
        selection_engine_version VARCHAR,
        grade VARCHAR,
        grade_detail VARCHAR,
        report_candidate_flag BOOLEAN,
        eligible_flag BOOLEAN,
        final_selection_value DOUBLE,
        selection_percentile DOUBLE,
        expected_excess_return_at_selection DOUBLE,
        lower_band_at_selection DOUBLE,
        median_band_at_selection DOUBLE,
        upper_band_at_selection DOUBLE,
        uncertainty_score_at_selection DOUBLE,
        implementation_penalty_at_selection DOUBLE,
        regime_label_at_selection VARCHAR,
        top_reason_tags_json VARCHAR,
        risk_flags_json VARCHAR,
        entry_trade_date DATE,
        exit_trade_date DATE,
        realized_return DOUBLE,
        realized_excess_return DOUBLE,
        prediction_error DOUBLE,
        direction_hit_flag BOOLEAN,
        raw_positive_flag BOOLEAN,
        band_available_flag BOOLEAN,
        band_status VARCHAR,
        in_band_flag BOOLEAN,
        above_upper_flag BOOLEAN,
        below_lower_flag BOOLEAN,
        outcome_status VARCHAR,
        source_label_version VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (selection_date, symbol, horizon, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_evaluation_summary (
        summary_date DATE NOT NULL,
        window_type VARCHAR NOT NULL,
        window_start DATE NOT NULL,
        window_end DATE NOT NULL,
        horizon INTEGER NOT NULL,
        ranking_version VARCHAR NOT NULL,
        segment_type VARCHAR NOT NULL,
        segment_value VARCHAR NOT NULL,
        count_total BIGINT NOT NULL,
        count_evaluated BIGINT NOT NULL,
        count_pending BIGINT NOT NULL,
        mean_realized_return DOUBLE,
        mean_realized_excess_return DOUBLE,
        median_realized_excess_return DOUBLE,
        hit_rate DOUBLE,
        positive_raw_return_rate DOUBLE,
        band_coverage_rate DOUBLE,
        above_upper_rate DOUBLE,
        below_lower_rate DOUBLE,
        avg_expected_excess_return DOUBLE,
        avg_prediction_error DOUBLE,
        overlap_count BIGINT,
        score_monotonicity_hint DOUBLE,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            summary_date,
            window_type,
            horizon,
            ranking_version,
            segment_type,
            segment_value
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_calibration_diagnostic (
        diagnostic_date DATE NOT NULL,
        window_start DATE NOT NULL,
        window_end DATE NOT NULL,
        horizon INTEGER NOT NULL,
        ranking_version VARCHAR NOT NULL,
        bin_type VARCHAR NOT NULL,
        bin_value VARCHAR NOT NULL,
        sample_count BIGINT NOT NULL,
        expected_median DOUBLE,
        expected_q25 DOUBLE,
        expected_q75 DOUBLE,
        observed_mean DOUBLE,
        observed_median DOUBLE,
        observed_q25 DOUBLE,
        observed_q75 DOUBLE,
        median_bias DOUBLE,
        coverage_rate DOUBLE,
        above_upper_rate DOUBLE,
        below_lower_rate DOUBLE,
        monotonicity_order DOUBLE,
        quality_flag VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            diagnostic_date,
            horizon,
            ranking_version,
            bin_type,
            bin_value
        )
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
        ranking_version VARCHAR,
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
    """
    CREATE TABLE IF NOT EXISTS ops_ranking_validation_summary (
        run_id VARCHAR NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        bucket_type VARCHAR NOT NULL,
        bucket_name VARCHAR NOT NULL,
        symbol_count BIGINT NOT NULL,
        avg_gross_forward_return DOUBLE,
        avg_excess_forward_return DOUBLE,
        median_excess_forward_return DOUBLE,
        top_decile_gap DOUBLE,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops_selection_validation_summary (
        run_id VARCHAR NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        bucket_type VARCHAR NOT NULL,
        bucket_name VARCHAR NOT NULL,
        symbol_count BIGINT NOT NULL,
        avg_excess_forward_return DOUBLE,
        median_excess_forward_return DOUBLE,
        hit_rate DOUBLE,
        avg_expected_excess_return DOUBLE,
        avg_prediction_error DOUBLE,
        top_decile_gap DOUBLE,
        ranking_version VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
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

MANIFEST_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE ops_run_manifest ADD COLUMN IF NOT EXISTS ranking_version VARCHAR",
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
    CREATE OR REPLACE VIEW vw_latest_investor_flow AS
    SELECT *
    FROM fact_investor_flow
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol
        ORDER BY trading_date DESC, created_at DESC
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
    """
    CREATE OR REPLACE VIEW vw_feature_snapshot_latest AS
    SELECT *
    FROM fact_feature_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, feature_name
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    _build_feature_matrix_latest_view(),
    """
    CREATE OR REPLACE VIEW vw_latest_forward_return_label AS
    SELECT *
    FROM fact_forward_return_label
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_market_regime_latest AS
    SELECT *
    FROM fact_market_regime_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY market_scope
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_ranking_latest AS
    SELECT *
    FROM fact_ranking
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, ranking_version
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_prediction_latest AS
    SELECT *
    FROM fact_prediction
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, prediction_version
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_selection_outcome_latest AS
    SELECT *
    FROM fact_selection_outcome
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, ranking_version
        ORDER BY selection_date DESC, updated_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_evaluation_summary AS
    SELECT *
    FROM fact_evaluation_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY window_type, horizon, ranking_version, segment_type, segment_value
        ORDER BY summary_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_calibration_diagnostic AS
    SELECT *
    FROM fact_calibration_diagnostic
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, ranking_version, bin_type, bin_value
        ORDER BY diagnostic_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_ranking_validation_summary AS
    SELECT *
    FROM ops_ranking_validation_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, bucket_type, bucket_name
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_selection_validation_summary AS
    SELECT *
    FROM ops_selection_validation_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, bucket_type, bucket_name, ranking_version
        ORDER BY created_at DESC
    ) = 1
    """,
)


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _migrate_fact_ranking_table(connection: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(connection, "fact_ranking"):
        return

    table_info = connection.execute("PRAGMA table_info('fact_ranking')").fetchdf()
    if table_info.empty:
        return

    ranking_row = table_info.loc[table_info["name"] == "ranking_version"]
    ranking_pk = int(ranking_row["pk"].iloc[0]) if not ranking_row.empty else 0
    if ranking_pk > 0:
        return

    connection.execute("ALTER TABLE fact_ranking RENAME TO fact_ranking_legacy")
    connection.execute(
        """
        CREATE TABLE fact_ranking (
            run_id VARCHAR NOT NULL,
            as_of_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            horizon INTEGER NOT NULL,
            final_selection_value DOUBLE,
            final_selection_rank_pct DOUBLE,
            grade VARCHAR,
            explanatory_score_json VARCHAR,
            top_reason_tags_json VARCHAR,
            risk_flags_json VARCHAR,
            eligible_flag BOOLEAN,
            eligibility_notes_json VARCHAR,
            regime_state VARCHAR,
            ranking_version VARCHAR,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (as_of_date, symbol, horizon, ranking_version)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_ranking (
            run_id,
            as_of_date,
            symbol,
            horizon,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            explanatory_score_json,
            top_reason_tags_json,
            risk_flags_json,
            eligible_flag,
            eligibility_notes_json,
            regime_state,
            ranking_version,
            created_at
        )
        SELECT
            run_id,
            as_of_date,
            symbol,
            horizon,
            final_selection_value,
            final_selection_rank_pct,
            grade,
            explanatory_score_json,
            top_reason_tags_json,
            risk_flags_json,
            eligible_flag,
            eligibility_notes_json,
            regime_state,
            COALESCE(ranking_version, 'explanatory_ranking_v0'),
            created_at
        FROM fact_ranking_legacy
        """
    )
    connection.execute("DROP TABLE fact_ranking_legacy")


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
    try:
        _migrate_fact_ranking_table(connection)

        for ddl in CORE_TABLE_DDL:
            connection.execute(ddl)

        for ddl in SYMBOL_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in CALENDAR_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in MANIFEST_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in CORE_VIEW_DDL:
            connection.execute(ddl)
    except duckdb.InvalidInputException as exc:
        if "read-only mode" not in str(exc).lower():
            raise


def fetch_dataframe(connection: duckdb.DuckDBPyConnection, query: str):
    return connection.execute(query).fetchdf()
