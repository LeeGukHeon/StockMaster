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
        model_version VARCHAR,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        fallback_flag BOOLEAN,
        fallback_reason VARCHAR,
        member_count BIGINT,
        ensemble_weight_json VARCHAR,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, symbol, horizon, prediction_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_model_training_run (
        training_run_id VARCHAR PRIMARY KEY,
        run_id VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        train_end_date DATE NOT NULL,
        training_window_start DATE,
        training_window_end DATE,
        validation_window_start DATE,
        validation_window_end DATE,
        train_row_count BIGINT NOT NULL,
        validation_row_count BIGINT NOT NULL,
        feature_count BIGINT NOT NULL,
        ensemble_weight_json VARCHAR,
        model_family_json VARCHAR,
        fallback_flag BOOLEAN NOT NULL,
        fallback_reason VARCHAR,
        artifact_uri VARCHAR,
        notes VARCHAR,
        status VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_model_member_prediction (
        training_run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        model_version VARCHAR NOT NULL,
        prediction_role VARCHAR NOT NULL,
        member_name VARCHAR NOT NULL,
        predicted_excess_return DOUBLE,
        actual_excess_return DOUBLE,
        residual DOUBLE,
        fallback_flag BOOLEAN,
        fallback_reason VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            training_run_id,
            as_of_date,
            symbol,
            horizon,
            prediction_role,
            member_name
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_model_metric_summary (
        training_run_id VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        member_name VARCHAR NOT NULL,
        split_name VARCHAR NOT NULL,
        metric_name VARCHAR NOT NULL,
        metric_value DOUBLE,
        sample_count BIGINT,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            training_run_id,
            member_name,
            split_name,
            metric_name
        )
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
        disagreement_score_at_selection DOUBLE,
        implementation_penalty_at_selection DOUBLE,
        fallback_flag_at_selection BOOLEAN,
        fallback_reason_at_selection VARCHAR,
        prediction_version_at_selection VARCHAR,
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
    CREATE TABLE IF NOT EXISTS fact_intraday_candidate_session (
        run_id VARCHAR NOT NULL,
        selection_date DATE NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        market VARCHAR,
        company_name VARCHAR,
        horizon INTEGER NOT NULL,
        ranking_version VARCHAR NOT NULL,
        candidate_rank BIGINT,
        final_selection_value DOUBLE,
        final_selection_rank_pct DOUBLE,
        grade VARCHAR,
        eligible_flag BOOLEAN,
        expected_excess_return DOUBLE,
        lower_band DOUBLE,
        upper_band DOUBLE,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        fallback_flag BOOLEAN,
        top_reason_tags_json VARCHAR,
        risk_flags_json VARCHAR,
        session_status VARCHAR,
        checkpoint_plan_json VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_bar_1m (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        bar_ts TIMESTAMPTZ NOT NULL,
        bar_time VARCHAR NOT NULL,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        turnover_value DOUBLE,
        vwap DOUBLE,
        source VARCHAR,
        data_quality VARCHAR,
        fetch_latency_ms DOUBLE,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, bar_ts)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_trade_summary (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        cumulative_volume BIGINT,
        cumulative_turnover DOUBLE,
        execution_strength DOUBLE,
        buy_pressure_proxy DOUBLE,
        sell_pressure_proxy DOUBLE,
        activity_ratio DOUBLE,
        trade_count_estimate BIGINT,
        trade_summary_status VARCHAR,
        source VARCHAR,
        fetch_latency_ms DOUBLE,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, checkpoint_time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_quote_summary (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        best_bid DOUBLE,
        best_ask DOUBLE,
        mid_price DOUBLE,
        spread_bps DOUBLE,
        total_bid_quantity DOUBLE,
        total_ask_quantity DOUBLE,
        imbalance_ratio DOUBLE,
        quote_status VARCHAR,
        source VARCHAR,
        fetch_latency_ms DOUBLE,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, checkpoint_time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_signal_snapshot (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        gap_opening_quality_score DOUBLE,
        micro_trend_score DOUBLE,
        relative_activity_score DOUBLE,
        orderbook_score DOUBLE,
        execution_strength_score DOUBLE,
        risk_friction_score DOUBLE,
        signal_quality_score DOUBLE,
        timing_adjustment_score DOUBLE,
        signal_notes_json VARCHAR,
        fallback_flags_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_entry_decision (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        action VARCHAR NOT NULL,
        action_score DOUBLE,
        timing_adjustment_score DOUBLE,
        signal_quality_score DOUBLE,
        entry_reference_price DOUBLE,
        fallback_flag BOOLEAN,
        action_reason_json VARCHAR,
        risk_flags_json VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_timing_outcome (
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        ranking_version VARCHAR NOT NULL,
        selection_date DATE,
        selected_checkpoint_time VARCHAR,
        selected_action VARCHAR,
        execution_flag BOOLEAN,
        naive_open_price DOUBLE,
        decision_entry_price DOUBLE,
        exit_trade_date DATE,
        future_exit_price DOUBLE,
        realized_return_from_open DOUBLE,
        realized_return_from_decision DOUBLE,
        timing_edge_return DOUBLE,
        timing_edge_bps DOUBLE,
        outcome_status VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_market_context_snapshot (
        run_id VARCHAR NOT NULL,
        selection_date DATE,
        session_date DATE NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        context_scope VARCHAR NOT NULL,
        market_session_state VARCHAR,
        prior_daily_regime_state VARCHAR,
        prior_daily_regime_score DOUBLE,
        candidate_count BIGINT,
        advancers_count BIGINT,
        decliners_count BIGINT,
        market_breadth_ratio DOUBLE,
        kospi_return_from_open DOUBLE,
        kosdaq_return_from_open DOUBLE,
        candidate_mean_return_from_open DOUBLE,
        candidate_median_return_from_open DOUBLE,
        candidate_hit_ratio_from_open DOUBLE,
        candidate_mean_relative_volume DOUBLE,
        candidate_mean_spread_bps DOUBLE,
        candidate_mean_execution_strength DOUBLE,
        candidate_mean_orderbook_imbalance DOUBLE,
        candidate_mean_gap_score DOUBLE,
        candidate_mean_signal_quality DOUBLE,
        market_shock_proxy DOUBLE,
        intraday_volatility_proxy DOUBLE,
        dispersion_proxy DOUBLE,
        bar_coverage_ratio DOUBLE,
        trade_coverage_ratio DOUBLE,
        quote_coverage_ratio DOUBLE,
        provider_latency_ms DOUBLE,
        data_quality_flag VARCHAR,
        context_reason_codes_json VARCHAR,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, checkpoint_time, context_scope)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_regime_adjustment (
        run_id VARCHAR NOT NULL,
        selection_date DATE,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        market_regime_family VARCHAR NOT NULL,
        adjustment_profile VARCHAR NOT NULL,
        selection_confidence_bucket VARCHAR,
        signal_quality_flag VARCHAR,
        raw_action VARCHAR,
        raw_timing_score DOUBLE,
        adjusted_timing_score DOUBLE,
        regime_support_delta DOUBLE,
        regime_risk_penalty DOUBLE,
        gap_chase_penalty DOUBLE,
        data_quality_penalty DOUBLE,
        friction_penalty_delta DOUBLE,
        regime_adjustment_delta DOUBLE,
        eligible_to_execute_flag BOOLEAN,
        context_reason_codes_json VARCHAR,
        adjustment_reason_codes_json VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_adjusted_entry_decision (
        run_id VARCHAR NOT NULL,
        selection_date DATE,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        market_regime_family VARCHAR NOT NULL,
        adjustment_profile VARCHAR NOT NULL,
        raw_action VARCHAR NOT NULL,
        adjusted_action VARCHAR NOT NULL,
        raw_timing_score DOUBLE,
        adjusted_timing_score DOUBLE,
        selection_confidence_bucket VARCHAR,
        signal_quality_flag VARCHAR,
        eligible_to_execute_flag BOOLEAN,
        fallback_flag BOOLEAN,
        adjustment_reason_codes_json VARCHAR,
        risk_flags_json VARCHAR,
        decision_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_strategy_result (
        selection_date DATE,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        market VARCHAR,
        company_name VARCHAR,
        horizon INTEGER NOT NULL,
        strategy_id VARCHAR NOT NULL,
        strategy_family VARCHAR,
        cutoff_checkpoint_time VARCHAR,
        entry_checkpoint_time VARCHAR,
        entry_action_source VARCHAR,
        raw_action VARCHAR,
        adjusted_action VARCHAR,
        market_regime_family VARCHAR,
        adjustment_profile VARCHAR,
        executed_flag BOOLEAN,
        no_entry_flag BOOLEAN,
        eligible_to_execute_flag BOOLEAN,
        entry_timestamp TIMESTAMPTZ,
        entry_price DOUBLE,
        exit_trade_date DATE,
        exit_price DOUBLE,
        baseline_open_price DOUBLE,
        baseline_open_return DOUBLE,
        baseline_open_excess_return DOUBLE,
        realized_return DOUBLE,
        realized_excess_return DOUBLE,
        timing_edge_vs_open_return DOUBLE,
        timing_edge_vs_open_bps DOUBLE,
        skip_reason_code VARCHAR,
        skip_saved_loss_flag BOOLEAN,
        missed_winner_flag BOOLEAN,
        outcome_status VARCHAR,
        source_decision_run_id VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, strategy_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_strategy_comparison (
        start_session_date DATE NOT NULL,
        end_session_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        strategy_id VARCHAR NOT NULL,
        comparison_scope VARCHAR NOT NULL,
        comparison_value VARCHAR NOT NULL,
        cutoff_checkpoint_time VARCHAR NOT NULL,
        sample_count BIGINT NOT NULL,
        matured_count BIGINT NOT NULL,
        executed_count BIGINT NOT NULL,
        no_entry_count BIGINT NOT NULL,
        execution_rate DOUBLE,
        mean_realized_excess_return DOUBLE,
        median_realized_excess_return DOUBLE,
        hit_rate DOUBLE,
        mean_timing_edge_vs_open_bps DOUBLE,
        median_timing_edge_vs_open_bps DOUBLE,
        positive_timing_edge_rate DOUBLE,
        skip_saved_loss_rate DOUBLE,
        missed_winner_rate DOUBLE,
        coverage_ok_rate DOUBLE,
        evaluation_run_id VARCHAR NOT NULL,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            start_session_date,
            end_session_date,
            horizon,
            strategy_id,
            comparison_scope,
            comparison_value,
            cutoff_checkpoint_time
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_timing_calibration (
        window_start_date DATE NOT NULL,
        window_end_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        grouping_key VARCHAR NOT NULL,
        grouping_value VARCHAR NOT NULL,
        sample_count BIGINT NOT NULL,
        executed_count BIGINT NOT NULL,
        execution_rate DOUBLE,
        mean_realized_excess_return DOUBLE,
        median_realized_excess_return DOUBLE,
        hit_rate DOUBLE,
        mean_timing_edge_vs_open_bps DOUBLE,
        positive_timing_edge_rate DOUBLE,
        skip_saved_loss_rate DOUBLE,
        missed_winner_rate DOUBLE,
        quality_flag VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            window_start_date,
            window_end_date,
            horizon,
            grouping_key,
            grouping_value
        )
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

PREDICTION_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS model_version VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS uncertainty_score DOUBLE",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS fallback_flag BOOLEAN",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS fallback_reason VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS member_count BIGINT",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS ensemble_weight_json VARCHAR",
)

SELECTION_OUTCOME_COLUMN_MIGRATIONS: tuple[str, ...] = (
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS disagreement_score_at_selection DOUBLE"
    ),
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS fallback_flag_at_selection BOOLEAN"
    ),
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS fallback_reason_at_selection VARCHAR"
    ),
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS prediction_version_at_selection VARCHAR"
    ),
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
    CREATE OR REPLACE VIEW vw_latest_model_training_run AS
    SELECT *
    FROM fact_model_training_run
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, model_version
        ORDER BY train_end_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_model_member_prediction AS
    SELECT *
    FROM fact_model_member_prediction
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY as_of_date, symbol, horizon, prediction_role, member_name
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_model_metric_summary AS
    SELECT *
    FROM fact_model_metric_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, model_version, member_name, split_name, metric_name
        ORDER BY created_at DESC
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
    CREATE OR REPLACE VIEW vw_latest_intraday_candidate_session AS
    SELECT *
    FROM fact_intraday_candidate_session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, ranking_version
        ORDER BY session_date DESC, updated_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_bar_1m AS
    SELECT *
    FROM fact_intraday_bar_1m
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, bar_time
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_trade_summary AS
    SELECT *
    FROM fact_intraday_trade_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, checkpoint_time
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_quote_summary AS
    SELECT *
    FROM fact_intraday_quote_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, checkpoint_time
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_signal_snapshot AS
    SELECT *
    FROM fact_intraday_signal_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_entry_decision AS
    SELECT *
    FROM fact_intraday_entry_decision
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_timing_outcome AS
    SELECT *
    FROM fact_intraday_timing_outcome
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, ranking_version
        ORDER BY session_date DESC, updated_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_market_context_snapshot AS
    SELECT *
    FROM fact_intraday_market_context_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY checkpoint_time, context_scope
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_regime_adjustment AS
    SELECT *
    FROM fact_intraday_regime_adjustment
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_adjusted_entry_decision AS
    SELECT *
    FROM fact_intraday_adjusted_entry_decision
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_strategy_result AS
    SELECT *
    FROM fact_intraday_strategy_result
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, strategy_id
        ORDER BY session_date DESC, updated_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_strategy_comparison AS
    SELECT *
    FROM fact_intraday_strategy_comparison
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            horizon,
            strategy_id,
            comparison_scope,
            comparison_value,
            cutoff_checkpoint_time
        ORDER BY end_session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_timing_calibration AS
    SELECT *
    FROM fact_intraday_timing_calibration
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, grouping_key, grouping_value
        ORDER BY window_end_date DESC, created_at DESC
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

        for ddl in PREDICTION_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in SELECTION_OUTCOME_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in CORE_VIEW_DDL:
            connection.execute(ddl)
    except duckdb.InvalidInputException as exc:
        if "read-only mode" not in str(exc).lower():
            raise


def fetch_dataframe(connection: duckdb.DuckDBPyConnection, query: str):
    return connection.execute(query).fetchdf()
