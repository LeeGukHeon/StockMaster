from __future__ import annotations

import shutil
import tempfile
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
        training_run_id VARCHAR,
        model_spec_id VARCHAR,
        active_alpha_model_id VARCHAR,
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
    CREATE TABLE IF NOT EXISTS dim_alpha_model_spec (
        model_spec_id VARCHAR PRIMARY KEY,
        model_domain VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        estimation_scheme VARCHAR NOT NULL,
        rolling_window_days INTEGER,
        feature_version VARCHAR,
        label_version VARCHAR,
        selection_engine_version VARCHAR,
        spec_payload_json VARCHAR,
        active_candidate_flag BOOLEAN NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_model_training_run (
        training_run_id VARCHAR PRIMARY KEY,
        run_id VARCHAR NOT NULL,
        model_domain VARCHAR,
        model_version VARCHAR NOT NULL,
        model_spec_id VARCHAR,
        estimation_scheme VARCHAR,
        rolling_window_days INTEGER,
        horizon INTEGER NOT NULL,
        panel_name VARCHAR,
        train_end_date DATE NOT NULL,
        training_window_start DATE,
        training_window_end DATE,
        validation_window_start DATE,
        validation_window_end DATE,
        train_row_count BIGINT NOT NULL,
        validation_row_count BIGINT NOT NULL,
        train_session_count BIGINT,
        validation_session_count BIGINT,
        feature_count BIGINT NOT NULL,
        ensemble_weight_json VARCHAR,
        model_family_json VARCHAR,
        threshold_payload_json VARCHAR,
        diagnostic_artifact_uri VARCHAR,
        metadata_json VARCHAR,
        fallback_flag BOOLEAN NOT NULL,
        fallback_reason VARCHAR,
        artifact_uri VARCHAR,
        notes VARCHAR,
        status VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_active_model (
        active_alpha_model_id VARCHAR PRIMARY KEY,
        horizon INTEGER NOT NULL,
        model_spec_id VARCHAR NOT NULL,
        training_run_id VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL,
        promotion_type VARCHAR NOT NULL,
        promotion_report_json VARCHAR,
        effective_from_date DATE NOT NULL,
        effective_to_date DATE,
        active_flag BOOLEAN NOT NULL,
        rollback_of_active_alpha_model_id VARCHAR,
        note VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_shadow_prediction (
        run_id VARCHAR NOT NULL,
        selection_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        model_spec_id VARCHAR NOT NULL,
        training_run_id VARCHAR NOT NULL,
        expected_excess_return DOUBLE,
        lower_band DOUBLE,
        median_band DOUBLE,
        upper_band DOUBLE,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        fallback_flag BOOLEAN,
        fallback_reason VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (selection_date, symbol, horizon, model_spec_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_shadow_ranking (
        run_id VARCHAR NOT NULL,
        selection_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        model_spec_id VARCHAR NOT NULL,
        training_run_id VARCHAR NOT NULL,
        final_selection_value DOUBLE,
        selection_percentile DOUBLE,
        grade VARCHAR,
        report_candidate_flag BOOLEAN,
        eligible_flag BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (selection_date, symbol, horizon, model_spec_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_shadow_selection_outcome (
        selection_date DATE NOT NULL,
        evaluation_date DATE,
        symbol VARCHAR NOT NULL,
        market VARCHAR,
        horizon INTEGER NOT NULL,
        model_spec_id VARCHAR NOT NULL,
        training_run_id VARCHAR NOT NULL,
        selection_percentile DOUBLE,
        report_candidate_flag BOOLEAN,
        grade VARCHAR,
        eligible_flag BOOLEAN,
        final_selection_value DOUBLE,
        expected_excess_return_at_selection DOUBLE,
        lower_band_at_selection DOUBLE,
        median_band_at_selection DOUBLE,
        upper_band_at_selection DOUBLE,
        uncertainty_score_at_selection DOUBLE,
        disagreement_score_at_selection DOUBLE,
        realized_excess_return DOUBLE,
        prediction_error DOUBLE,
        outcome_status VARCHAR,
        source_label_version VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (selection_date, symbol, horizon, model_spec_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_shadow_evaluation_summary (
        summary_date DATE NOT NULL,
        window_type VARCHAR NOT NULL,
        window_start DATE NOT NULL,
        window_end DATE NOT NULL,
        horizon INTEGER NOT NULL,
        model_spec_id VARCHAR NOT NULL,
        segment_value VARCHAR NOT NULL,
        count_evaluated BIGINT NOT NULL,
        mean_realized_excess_return DOUBLE,
        mean_point_loss DOUBLE,
        rank_ic DOUBLE,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (summary_date, window_type, horizon, model_spec_id, segment_value)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alpha_promotion_test (
        promotion_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        incumbent_model_spec_id VARCHAR NOT NULL,
        challenger_model_spec_id VARCHAR NOT NULL,
        loss_name VARCHAR NOT NULL,
        window_start DATE,
        window_end DATE,
        sample_count BIGINT NOT NULL,
        mcs_member_flag BOOLEAN NOT NULL,
        incumbent_mcs_member_flag BOOLEAN NOT NULL,
        p_value DOUBLE,
        decision VARCHAR NOT NULL,
        detail_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            promotion_date,
            horizon,
            incumbent_model_spec_id,
            challenger_model_spec_id,
            loss_name
        )
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
        model_domain VARCHAR NOT NULL DEFAULT 'default',
        model_version VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        panel_name VARCHAR NOT NULL DEFAULT 'all',
        member_name VARCHAR NOT NULL,
        split_name VARCHAR NOT NULL,
        metric_scope VARCHAR NOT NULL DEFAULT 'all',
        class_label VARCHAR NOT NULL DEFAULT 'all',
        comparison_key VARCHAR NOT NULL DEFAULT 'all',
        metric_name VARCHAR NOT NULL,
        metric_value DOUBLE,
        sample_count BIGINT,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            training_run_id,
            member_name,
            split_name,
            metric_scope,
            class_label,
            comparison_key,
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
        training_run_id_at_selection VARCHAR,
        model_spec_id_at_selection VARCHAR,
        active_alpha_model_id_at_selection VARCHAR,
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
    CREATE TABLE IF NOT EXISTS fact_intraday_policy_experiment_run (
        experiment_run_id VARCHAR PRIMARY KEY,
        experiment_name VARCHAR NOT NULL,
        experiment_type VARCHAR NOT NULL,
        search_space_version VARCHAR,
        objective_version VARCHAR,
        split_version VARCHAR,
        split_mode VARCHAR,
        as_of_date DATE,
        start_session_date DATE,
        end_session_date DATE,
        train_start_date DATE,
        train_end_date DATE,
        validation_start_date DATE,
        validation_end_date DATE,
        test_start_date DATE,
        test_end_date DATE,
        horizon INTEGER,
        checkpoint_scope VARCHAR,
        regime_scope VARCHAR,
        candidate_count BIGINT,
        selected_policy_candidate_id VARCHAR,
        fallback_used_flag BOOLEAN,
        status VARCHAR NOT NULL,
        artifact_path VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_policy_candidate (
        policy_candidate_id VARCHAR PRIMARY KEY,
        search_space_version VARCHAR NOT NULL,
        template_id VARCHAR NOT NULL,
        scope_type VARCHAR NOT NULL,
        scope_key VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR,
        regime_cluster VARCHAR,
        regime_family VARCHAR,
        candidate_label VARCHAR,
        parameter_hash VARCHAR NOT NULL,
        enter_threshold_delta DOUBLE,
        wait_threshold_delta DOUBLE,
        avoid_threshold_delta DOUBLE,
        min_selection_confidence_gate DOUBLE,
        min_signal_quality_gate DOUBLE,
        uncertainty_penalty_weight DOUBLE,
        spread_penalty_weight DOUBLE,
        friction_penalty_weight DOUBLE,
        gap_chase_penalty_weight DOUBLE,
        cohort_weakness_penalty_weight DOUBLE,
        market_shock_penalty_weight DOUBLE,
        data_weak_guard_strength DOUBLE,
        max_gap_up_allowance_pct DOUBLE,
        min_execution_strength_gate DOUBLE,
        min_orderbook_imbalance_gate DOUBLE,
        allow_enter_under_data_weak BOOLEAN,
        allow_wait_override BOOLEAN,
        selection_rank_cap INTEGER,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_policy_evaluation (
        experiment_run_id VARCHAR NOT NULL,
        experiment_type VARCHAR NOT NULL,
        search_space_version VARCHAR,
        objective_version VARCHAR,
        split_version VARCHAR,
        split_mode VARCHAR,
        split_name VARCHAR NOT NULL,
        split_index INTEGER NOT NULL,
        window_start_date DATE,
        window_end_date DATE,
        horizon INTEGER NOT NULL,
        policy_candidate_id VARCHAR NOT NULL,
        template_id VARCHAR NOT NULL,
        scope_type VARCHAR NOT NULL,
        scope_key VARCHAR NOT NULL,
        checkpoint_time VARCHAR,
        regime_cluster VARCHAR,
        regime_family VARCHAR,
        window_session_count BIGINT NOT NULL,
        sample_count BIGINT NOT NULL,
        matured_count BIGINT NOT NULL,
        executed_count BIGINT NOT NULL,
        no_entry_count BIGINT NOT NULL,
        execution_rate DOUBLE,
        mean_realized_excess_return DOUBLE,
        median_realized_excess_return DOUBLE,
        hit_rate DOUBLE,
        mean_timing_edge_vs_open_bps DOUBLE,
        positive_timing_edge_rate DOUBLE,
        skip_saved_loss_rate DOUBLE,
        missed_winner_rate DOUBLE,
        left_tail_proxy DOUBLE,
        stability_score DOUBLE,
        objective_score DOUBLE,
        manual_review_required_flag BOOLEAN,
        fallback_scope_type VARCHAR,
        fallback_scope_key VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            experiment_run_id,
            split_name,
            split_index,
            horizon,
            policy_candidate_id
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_policy_ablation_result (
        experiment_run_id VARCHAR NOT NULL,
        ablation_date DATE NOT NULL,
        start_session_date DATE NOT NULL,
        end_session_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        base_policy_source VARCHAR NOT NULL,
        base_policy_candidate_id VARCHAR NOT NULL,
        ablation_name VARCHAR NOT NULL,
        sample_count BIGINT NOT NULL,
        mean_realized_excess_return_delta DOUBLE,
        median_realized_excess_return_delta DOUBLE,
        hit_rate_delta DOUBLE,
        mean_timing_edge_vs_open_bps_delta DOUBLE,
        execution_rate_delta DOUBLE,
        skip_saved_loss_rate_delta DOUBLE,
        missed_winner_rate_delta DOUBLE,
        left_tail_proxy_delta DOUBLE,
        stability_score_delta DOUBLE,
        objective_score_delta DOUBLE,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (experiment_run_id, horizon, ablation_name, base_policy_candidate_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_policy_selection_recommendation (
        recommendation_date DATE NOT NULL,
        horizon INTEGER NOT NULL,
        scope_type VARCHAR NOT NULL,
        scope_key VARCHAR NOT NULL,
        recommendation_rank INTEGER NOT NULL,
        policy_candidate_id VARCHAR NOT NULL,
        template_id VARCHAR NOT NULL,
        source_experiment_run_id VARCHAR,
        search_space_version VARCHAR,
        objective_version VARCHAR,
        split_version VARCHAR,
        sample_count BIGINT NOT NULL,
        test_session_count BIGINT NOT NULL,
        executed_count BIGINT NOT NULL,
        execution_rate DOUBLE,
        mean_realized_excess_return DOUBLE,
        median_realized_excess_return DOUBLE,
        hit_rate DOUBLE,
        mean_timing_edge_vs_open_bps DOUBLE,
        positive_timing_edge_rate DOUBLE,
        skip_saved_loss_rate DOUBLE,
        missed_winner_rate DOUBLE,
        left_tail_proxy DOUBLE,
        stability_score DOUBLE,
        objective_score DOUBLE,
        manual_review_required_flag BOOLEAN,
        fallback_scope_type VARCHAR,
        fallback_scope_key VARCHAR,
        recommendation_reason_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            recommendation_date,
            horizon,
            scope_type,
            scope_key,
            recommendation_rank
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_active_policy (
        active_policy_id VARCHAR PRIMARY KEY,
        horizon INTEGER NOT NULL,
        scope_type VARCHAR NOT NULL,
        scope_key VARCHAR NOT NULL,
        checkpoint_time VARCHAR,
        regime_cluster VARCHAR,
        regime_family VARCHAR,
        policy_candidate_id VARCHAR NOT NULL,
        source_recommendation_date DATE,
        promotion_type VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL,
        effective_from_date DATE NOT NULL,
        effective_to_date DATE,
        active_flag BOOLEAN NOT NULL,
        fallback_scope_type VARCHAR,
        fallback_scope_key VARCHAR,
        rollback_of_active_policy_id VARCHAR,
        note VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_meta_prediction (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        panel_name VARCHAR,
        tuned_action VARCHAR NOT NULL,
        active_policy_candidate_id VARCHAR,
        active_meta_model_id VARCHAR,
        training_run_id VARCHAR,
        model_version VARCHAR,
        predicted_class VARCHAR,
        predicted_class_probability DOUBLE,
        confidence_margin DOUBLE,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        class_probability_json VARCHAR,
        fallback_flag BOOLEAN,
        fallback_reason VARCHAR,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_meta_decision (
        run_id VARCHAR NOT NULL,
        session_date DATE NOT NULL,
        symbol VARCHAR NOT NULL,
        horizon INTEGER NOT NULL,
        checkpoint_time VARCHAR NOT NULL,
        ranking_version VARCHAR NOT NULL,
        raw_action VARCHAR,
        adjusted_action VARCHAR,
        tuned_action VARCHAR NOT NULL,
        final_action VARCHAR NOT NULL,
        panel_name VARCHAR,
        predicted_class VARCHAR,
        predicted_class_probability DOUBLE,
        confidence_margin DOUBLE,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        active_policy_candidate_id VARCHAR,
        active_meta_model_id VARCHAR,
        active_meta_training_run_id VARCHAR,
        hard_guard_block_flag BOOLEAN,
        override_applied_flag BOOLEAN,
        override_type VARCHAR,
        fallback_flag BOOLEAN,
        fallback_reason VARCHAR,
        decision_reason_codes_json VARCHAR,
        risk_flags_json VARCHAR,
        source_notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_date, symbol, horizon, checkpoint_time, ranking_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_research_capability (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        feature_slug VARCHAR NOT NULL,
        enabled_flag BOOLEAN NOT NULL,
        rollout_mode VARCHAR NOT NULL,
        blocking_dependency VARCHAR,
        dependency_ready_flag BOOLEAN NOT NULL,
        active_policy_ids_json VARCHAR,
        active_meta_model_ids_json VARCHAR,
        report_available_flag BOOLEAN NOT NULL,
        latest_report_type VARCHAR,
        last_successful_run_id VARCHAR,
        last_successful_run_at TIMESTAMPTZ,
        last_degraded_run_id VARCHAR,
        last_degraded_run_at TIMESTAMPTZ,
        last_skip_reason VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, feature_slug)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_intraday_active_meta_model (
        active_meta_model_id VARCHAR PRIMARY KEY,
        horizon INTEGER NOT NULL,
        panel_name VARCHAR NOT NULL,
        training_run_id VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL,
        promotion_type VARCHAR NOT NULL,
        threshold_payload_json VARCHAR,
        calibration_summary_json VARCHAR,
        effective_from_date DATE NOT NULL,
        effective_to_date DATE,
        active_flag BOOLEAN NOT NULL,
        rollback_of_active_meta_model_id VARCHAR,
        note VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_policy_registry (
        active_portfolio_policy_id VARCHAR PRIMARY KEY,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        display_name VARCHAR,
        config_path VARCHAR,
        config_hash VARCHAR,
        policy_payload_json VARCHAR,
        source_type VARCHAR,
        promotion_type VARCHAR,
        effective_from_date DATE NOT NULL,
        effective_to_date DATE,
        active_flag BOOLEAN NOT NULL,
        rollback_of_active_portfolio_policy_id VARCHAR,
        note VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_candidate (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        session_date DATE,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        active_portfolio_policy_id VARCHAR,
        symbol VARCHAR NOT NULL,
        company_name VARCHAR,
        market VARCHAR,
        sector VARCHAR,
        ranking_version VARCHAR,
        primary_horizon INTEGER,
        tactical_horizon INTEGER,
        candidate_rank BIGINT,
        current_holding_flag BOOLEAN,
        current_shares BIGINT,
        current_weight DOUBLE,
        final_selection_value DOUBLE,
        effective_alpha_long DOUBLE,
        tactical_alpha DOUBLE,
        lower_band DOUBLE,
        flow_score DOUBLE,
        regime_score DOUBLE,
        uncertainty_score DOUBLE,
        disagreement_score DOUBLE,
        implementation_penalty_score DOUBLE,
        volatility_proxy DOUBLE,
        adv20_krw DOUBLE,
        risk_scaled_conviction DOUBLE,
        candidate_state VARCHAR,
        timing_action VARCHAR,
        timing_gate_status VARCHAR,
        entry_eligible_flag BOOLEAN,
        hold_eligible_flag BOOLEAN,
        hard_exit_flag BOOLEAN,
        blocked_reason VARCHAR,
        tie_breaker_json VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, execution_mode, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_target_book (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        session_date DATE,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        active_portfolio_policy_id VARCHAR,
        symbol VARCHAR NOT NULL,
        company_name VARCHAR,
        market VARCHAR,
        sector VARCHAR,
        candidate_state VARCHAR,
        target_rank BIGINT,
        target_weight DOUBLE,
        target_notional DOUBLE,
        target_shares BIGINT,
        target_price DOUBLE,
        current_shares BIGINT,
        current_weight DOUBLE,
        score_value DOUBLE,
        gate_status VARCHAR,
        included_flag BOOLEAN,
        blocked_flag BOOLEAN,
        waitlist_flag BOOLEAN,
        waitlist_rank BIGINT,
        constraint_flags_json VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, execution_mode, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_rebalance_plan (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        session_date DATE,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        active_portfolio_policy_id VARCHAR,
        symbol VARCHAR NOT NULL,
        company_name VARCHAR,
        market VARCHAR,
        sector VARCHAR,
        rebalance_action VARCHAR,
        action_sequence BIGINT,
        gate_status VARCHAR,
        candidate_state VARCHAR,
        current_shares BIGINT,
        target_shares BIGINT,
        delta_shares BIGINT,
        reference_price DOUBLE,
        current_notional DOUBLE,
        target_notional DOUBLE,
        notional_delta DOUBLE,
        turnover_contribution DOUBLE,
        cash_delta DOUBLE,
        waitlist_flag BOOLEAN,
        blocked_reason VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (as_of_date, execution_mode, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_position_snapshot (
        run_id VARCHAR NOT NULL,
        snapshot_date DATE NOT NULL,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        active_portfolio_policy_id VARCHAR,
        symbol VARCHAR NOT NULL,
        company_name VARCHAR,
        market VARCHAR,
        sector VARCHAR,
        shares BIGINT,
        average_cost DOUBLE,
        close_price DOUBLE,
        market_value DOUBLE,
        target_weight DOUBLE,
        actual_weight DOUBLE,
        cash_like_flag BOOLEAN,
        source_rebalance_run_id VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            snapshot_date,
            execution_mode,
            portfolio_policy_id,
            portfolio_policy_version,
            symbol
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_nav_snapshot (
        run_id VARCHAR NOT NULL,
        snapshot_date DATE NOT NULL,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        active_portfolio_policy_id VARCHAR,
        nav_value DOUBLE,
        invested_value DOUBLE,
        cash_value DOUBLE,
        gross_exposure DOUBLE,
        net_exposure DOUBLE,
        daily_return DOUBLE,
        cumulative_return DOUBLE,
        drawdown DOUBLE,
        turnover_ratio DOUBLE,
        cash_weight DOUBLE,
        holding_count BIGINT,
        max_single_weight DOUBLE,
        top3_weight DOUBLE,
        source_position_run_id VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            snapshot_date,
            execution_mode,
            portfolio_policy_id,
            portfolio_policy_version
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_constraint_event (
        run_id VARCHAR NOT NULL,
        as_of_date DATE NOT NULL,
        session_date DATE,
        execution_mode VARCHAR NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        constraint_type VARCHAR NOT NULL,
        severity VARCHAR,
        event_code VARCHAR NOT NULL,
        requested_value DOUBLE,
        applied_value DOUBLE,
        limit_value DOUBLE,
        message VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            as_of_date,
            execution_mode,
            symbol,
            constraint_type,
            event_code
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_portfolio_evaluation_summary (
        evaluation_date DATE NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        portfolio_policy_id VARCHAR NOT NULL,
        portfolio_policy_version VARCHAR NOT NULL,
        execution_mode VARCHAR NOT NULL,
        comparison_key VARCHAR NOT NULL,
        metric_name VARCHAR NOT NULL,
        metric_value DOUBLE,
        sample_count BIGINT,
        notes_json VARCHAR,
        evaluation_run_id VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            evaluation_date,
            start_date,
            end_date,
            portfolio_policy_id,
            portfolio_policy_version,
            execution_mode,
            comparison_key,
            metric_name
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
    """
    CREATE TABLE IF NOT EXISTS fact_job_run (
        run_id VARCHAR PRIMARY KEY,
        job_name VARCHAR NOT NULL,
        trigger_type VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        as_of_date DATE,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        root_run_id VARCHAR NOT NULL,
        parent_run_id VARCHAR,
        recovery_of_run_id VARCHAR,
        lock_name VARCHAR,
        policy_id VARCHAR,
        policy_version VARCHAR,
        dry_run BOOLEAN NOT NULL,
        step_count INTEGER NOT NULL,
        failed_step_count INTEGER NOT NULL,
        artifact_count INTEGER NOT NULL,
        notes VARCHAR,
        error_message VARCHAR,
        details_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_job_step_run (
        step_run_id VARCHAR PRIMARY KEY,
        job_run_id VARCHAR NOT NULL,
        step_name VARCHAR NOT NULL,
        step_order INTEGER NOT NULL,
        status VARCHAR NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        critical_flag BOOLEAN NOT NULL,
        notes VARCHAR,
        error_message VARCHAR,
        details_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_pipeline_dependency_state (
        checked_at TIMESTAMPTZ NOT NULL,
        pipeline_name VARCHAR NOT NULL,
        dependency_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        ready_flag BOOLEAN NOT NULL,
        required_state VARCHAR,
        observed_state VARCHAR,
        as_of_date DATE,
        details_json VARCHAR,
        job_run_id VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (checked_at, pipeline_name, dependency_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_health_snapshot (
        snapshot_at TIMESTAMPTZ NOT NULL,
        health_scope VARCHAR NOT NULL,
        component_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        metric_name VARCHAR NOT NULL,
        metric_value_double DOUBLE,
        metric_value_text VARCHAR,
        as_of_date DATE,
        details_json VARCHAR,
        job_run_id VARCHAR,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (snapshot_at, health_scope, component_name, metric_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_disk_watermark_event (
        event_id VARCHAR PRIMARY KEY,
        measured_at TIMESTAMPTZ NOT NULL,
        disk_status VARCHAR NOT NULL,
        usage_ratio DOUBLE NOT NULL,
        used_gb DOUBLE NOT NULL,
        available_gb DOUBLE NOT NULL,
        total_gb DOUBLE NOT NULL,
        policy_id VARCHAR,
        policy_version VARCHAR,
        cleanup_required_flag BOOLEAN NOT NULL,
        emergency_block_flag BOOLEAN NOT NULL,
        notes VARCHAR,
        details_json VARCHAR,
        job_run_id VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_retention_cleanup_run (
        cleanup_run_id VARCHAR PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        status VARCHAR NOT NULL,
        dry_run BOOLEAN NOT NULL,
        cleanup_scope VARCHAR NOT NULL,
        removed_file_count BIGINT NOT NULL,
        reclaimed_bytes BIGINT NOT NULL,
        target_paths_json VARCHAR,
        notes VARCHAR,
        details_json VARCHAR,
        job_run_id VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_alert_event (
        alert_id VARCHAR PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL,
        alert_type VARCHAR NOT NULL,
        severity VARCHAR NOT NULL,
        component_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        message VARCHAR NOT NULL,
        details_json VARCHAR,
        job_run_id VARCHAR,
        resolved_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_recovery_action (
        recovery_action_id VARCHAR PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL,
        action_type VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        target_job_run_id VARCHAR,
        triggered_by_run_id VARCHAR,
        recovery_run_id VARCHAR,
        lock_name VARCHAR,
        notes VARCHAR,
        details_json VARCHAR,
        finished_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_active_ops_policy (
        ops_policy_registry_id VARCHAR PRIMARY KEY,
        policy_id VARCHAR NOT NULL,
        policy_version VARCHAR NOT NULL,
        policy_name VARCHAR NOT NULL,
        policy_path VARCHAR NOT NULL,
        effective_from_at TIMESTAMPTZ NOT NULL,
        effective_to_at TIMESTAMPTZ,
        active_flag BOOLEAN NOT NULL,
        promotion_type VARCHAR NOT NULL,
        note VARCHAR,
        rollback_of_registry_id VARCHAR,
        config_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_active_lock (
        lock_name VARCHAR PRIMARY KEY,
        job_name VARCHAR NOT NULL,
        owner_run_id VARCHAR NOT NULL,
        acquired_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        released_at TIMESTAMPTZ,
        release_reason VARCHAR,
        status VARCHAR NOT NULL,
        details_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_external_api_request_log (
        request_id VARCHAR PRIMARY KEY,
        provider_name VARCHAR NOT NULL,
        service_slug VARCHAR NOT NULL,
        run_id VARCHAR,
        as_of_date DATE,
        request_ts TIMESTAMPTZ NOT NULL,
        http_status INTEGER,
        status VARCHAR NOT NULL,
        latency_ms INTEGER,
        rows_received BIGINT NOT NULL DEFAULT 0,
        used_fallback BOOLEAN NOT NULL DEFAULT FALSE,
        error_code VARCHAR,
        error_message VARCHAR,
        endpoint_url VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_external_api_budget_snapshot (
        budget_snapshot_id VARCHAR PRIMARY KEY,
        provider_name VARCHAR NOT NULL,
        snapshot_ts TIMESTAMPTZ NOT NULL,
        date_kst DATE NOT NULL,
        request_budget BIGINT NOT NULL,
        requests_used BIGINT NOT NULL,
        usage_ratio DOUBLE NOT NULL,
        throttle_state VARCHAR NOT NULL,
        details_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_krx_service_status (
        service_status_id VARCHAR PRIMARY KEY,
        service_slug VARCHAR NOT NULL,
        display_name_ko VARCHAR NOT NULL,
        approval_expected BOOLEAN NOT NULL,
        enabled_by_env BOOLEAN NOT NULL,
        last_smoke_status VARCHAR NOT NULL,
        last_smoke_ts TIMESTAMPTZ NOT NULL,
        last_success_ts TIMESTAMPTZ,
        last_http_status INTEGER,
        last_error_class VARCHAR,
        fallback_mode VARCHAR,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_source_attribution_snapshot (
        attribution_snapshot_id VARCHAR PRIMARY KEY,
        snapshot_ts TIMESTAMPTZ NOT NULL,
        as_of_date DATE,
        page_slug VARCHAR NOT NULL,
        component_slug VARCHAR NOT NULL,
        source_label VARCHAR NOT NULL,
        provider_name VARCHAR NOT NULL,
        active_flag BOOLEAN NOT NULL,
        notes_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_latest_app_snapshot (
        snapshot_id VARCHAR PRIMARY KEY,
        snapshot_ts TIMESTAMPTZ NOT NULL,
        as_of_date DATE,
        latest_daily_bundle_run_id VARCHAR,
        latest_daily_bundle_status VARCHAR,
        latest_evaluation_date DATE,
        latest_evaluation_run_id VARCHAR,
        latest_intraday_session_date DATE,
        latest_intraday_run_id VARCHAR,
        latest_portfolio_as_of_date DATE,
        latest_portfolio_run_id VARCHAR,
        active_intraday_policy_id VARCHAR,
        active_meta_model_ids_json VARCHAR,
        active_portfolio_policy_id VARCHAR,
        active_ops_policy_id VARCHAR,
        health_status VARCHAR,
        market_regime_family VARCHAR,
        top_actionable_symbol_list_json VARCHAR,
        latest_report_bundle_id VARCHAR,
        critical_alert_count BIGINT NOT NULL DEFAULT 0,
        warning_alert_count BIGINT NOT NULL DEFAULT 0,
        notes VARCHAR,
        details_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_latest_report_index (
        report_index_id VARCHAR PRIMARY KEY,
        report_type VARCHAR NOT NULL,
        report_key VARCHAR NOT NULL,
        as_of_date DATE,
        generated_ts TIMESTAMPTZ NOT NULL,
        status VARCHAR NOT NULL,
        run_id VARCHAR,
        artifact_path VARCHAR NOT NULL,
        artifact_format VARCHAR NOT NULL,
        published_flag BOOLEAN NOT NULL DEFAULT FALSE,
        dry_run_flag BOOLEAN NOT NULL DEFAULT FALSE,
        summary_json VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_release_candidate_check (
        release_candidate_check_id VARCHAR PRIMARY KEY,
        check_ts TIMESTAMPTZ NOT NULL,
        environment VARCHAR NOT NULL,
        check_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        severity VARCHAR NOT NULL,
        detail_json VARCHAR,
        recommended_action VARCHAR,
        created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_ui_data_freshness_snapshot (
        freshness_snapshot_id VARCHAR PRIMARY KEY,
        snapshot_ts TIMESTAMPTZ NOT NULL,
        page_name VARCHAR NOT NULL,
        dataset_name VARCHAR NOT NULL,
        latest_available_ts TIMESTAMPTZ,
        freshness_seconds DOUBLE,
        stale_flag BOOLEAN NOT NULL,
        warning_level VARCHAR NOT NULL,
        notes VARCHAR,
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
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS training_run_id VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS model_spec_id VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS active_alpha_model_id VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS uncertainty_score DOUBLE",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS fallback_flag BOOLEAN",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS fallback_reason VARCHAR",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS member_count BIGINT",
    "ALTER TABLE fact_prediction ADD COLUMN IF NOT EXISTS ensemble_weight_json VARCHAR",
)

MODEL_TRAINING_RUN_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS model_domain VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS model_spec_id VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS estimation_scheme VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS rolling_window_days INTEGER",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS panel_name VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS train_session_count BIGINT",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS validation_session_count BIGINT",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS threshold_payload_json VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS diagnostic_artifact_uri VARCHAR",
    "ALTER TABLE fact_model_training_run ADD COLUMN IF NOT EXISTS metadata_json VARCHAR",
)

MODEL_METRIC_SUMMARY_COLUMN_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE fact_model_metric_summary ADD COLUMN IF NOT EXISTS model_domain VARCHAR",
    "ALTER TABLE fact_model_metric_summary ADD COLUMN IF NOT EXISTS panel_name VARCHAR",
    "ALTER TABLE fact_model_metric_summary ADD COLUMN IF NOT EXISTS metric_scope VARCHAR",
    "ALTER TABLE fact_model_metric_summary ADD COLUMN IF NOT EXISTS class_label VARCHAR",
    "ALTER TABLE fact_model_metric_summary ADD COLUMN IF NOT EXISTS comparison_key VARCHAR",
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
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS training_run_id_at_selection VARCHAR"
    ),
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS model_spec_id_at_selection VARCHAR"
    ),
    (
        "ALTER TABLE fact_selection_outcome "
        "ADD COLUMN IF NOT EXISTS active_alpha_model_id_at_selection VARCHAR"
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
        PARTITION BY
            horizon,
            model_version,
            COALESCE(model_domain, 'default'),
            COALESCE(model_spec_id, 'default'),
            COALESCE(panel_name, 'all')
        ORDER BY train_end_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_alpha_active_model AS
    SELECT *
    FROM fact_alpha_active_model
    WHERE active_flag
      AND effective_from_date <= CURRENT_DATE
      AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon
        ORDER BY effective_from_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_alpha_shadow_evaluation_summary AS
    SELECT *
    FROM fact_alpha_shadow_evaluation_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, model_spec_id, segment_value, window_type
        ORDER BY summary_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_alpha_promotion_test AS
    SELECT *
    FROM fact_alpha_promotion_test
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, incumbent_model_spec_id, challenger_model_spec_id, loss_name
        ORDER BY promotion_date DESC, created_at DESC
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
        PARTITION BY
            horizon,
            model_version,
            COALESCE(model_domain, 'default'),
            COALESCE(panel_name, 'all'),
            member_name,
            split_name,
            COALESCE(metric_scope, 'all'),
            COALESCE(class_label, 'all'),
            COALESCE(comparison_key, 'all'),
            metric_name
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
    CREATE OR REPLACE VIEW vw_latest_intraday_policy_experiment_run AS
    SELECT *
    FROM fact_intraday_policy_experiment_run
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            experiment_type,
            COALESCE(horizon, -1),
            COALESCE(checkpoint_scope, 'all'),
            COALESCE(regime_scope, 'all')
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_policy_candidate AS
    SELECT *
    FROM fact_intraday_policy_candidate
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY policy_candidate_id
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_policy_evaluation AS
    SELECT *
    FROM fact_intraday_policy_evaluation
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY split_name, split_index, horizon, policy_candidate_id
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_policy_ablation_result AS
    SELECT *
    FROM fact_intraday_policy_ablation_result
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, base_policy_candidate_id, ablation_name
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_policy_selection_recommendation AS
    SELECT *
    FROM fact_intraday_policy_selection_recommendation
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, scope_type, scope_key, recommendation_rank
        ORDER BY recommendation_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_active_policy AS
    SELECT *
    FROM fact_intraday_active_policy
    WHERE active_flag
      AND effective_from_date <= CURRENT_DATE
      AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, scope_type, scope_key
        ORDER BY effective_from_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_meta_prediction AS
    SELECT *
    FROM fact_intraday_meta_prediction
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_meta_decision AS
    SELECT *
    FROM fact_intraday_meta_decision
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_research_capability AS
    SELECT *
    FROM fact_intraday_research_capability
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY feature_slug
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_intraday_decision_lineage AS
    WITH prediction_pref AS (
        SELECT *
        FROM fact_prediction
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY as_of_date, symbol, horizon
            ORDER BY
                CASE prediction_version
                    WHEN 'alpha_prediction_v1' THEN 0
                    ELSE 1
                END,
                created_at DESC
        ) = 1
    ),
    portfolio_pref AS (
        SELECT *
        FROM fact_portfolio_target_book
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY as_of_date, symbol
            ORDER BY
                CASE execution_mode
                    WHEN 'TIMING_ASSISTED' THEN 0
                    WHEN 'OPEN_ALL' THEN 1
                    ELSE 2
                END,
                created_at DESC
        ) = 1
    ),
    regime_pref AS (
        SELECT *
        FROM fact_market_regime_snapshot
        WHERE market_scope = 'KR_EQUITY'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY as_of_date
            ORDER BY created_at DESC
        ) = 1
    )
    SELECT
        decision.session_date,
        decision.symbol,
        decision.horizon,
        decision.checkpoint_time,
        decision.ranking_version,
        candidate.selection_date,
        candidate.company_name,
        candidate.market,
        candidate.run_id AS candidate_session_run_id,
        candidate.candidate_rank,
        candidate.final_selection_value,
        candidate.final_selection_value AS candidate_selection_value,
        candidate.grade AS candidate_grade,
        ranking.run_id AS ranking_run_id,
        ranking.final_selection_value AS ranking_selection_value,
        ranking.grade AS ranking_grade,
        raw.run_id AS raw_decision_run_id,
        raw.action AS raw_policy_action,
        adjusted.run_id AS adjusted_decision_run_id,
        adjusted.adjusted_action,
        adjusted.market_regime_family,
        adjusted.adjustment_profile,
        decision.run_id AS meta_decision_run_id,
        decision.panel_name,
        decision.predicted_class,
        decision.predicted_class_probability,
        decision.confidence_margin,
        decision.uncertainty_score,
        decision.disagreement_score,
        decision.final_action,
        decision.raw_action,
        decision.active_policy_candidate_id,
        decision.active_meta_model_id,
        prediction.run_id AS prediction_run_id,
        prediction.prediction_version,
        prediction.expected_excess_return,
        prediction.lower_band,
        prediction.upper_band,
        prediction.uncertainty_score AS prediction_uncertainty_score,
        prediction.disagreement_score AS prediction_disagreement_score,
        portfolio.run_id AS portfolio_target_run_id,
        portfolio.execution_mode AS portfolio_execution_mode,
        portfolio.portfolio_policy_id,
        portfolio.portfolio_policy_version,
        portfolio.active_portfolio_policy_id,
        portfolio.target_weight,
        portfolio.target_notional,
        portfolio.target_shares,
        portfolio.gate_status,
        portfolio.included_flag,
        portfolio.blocked_flag,
        portfolio.waitlist_flag,
        regime.run_id AS market_regime_run_id,
        regime.regime_state AS market_regime_state,
        regime.regime_score
    FROM fact_intraday_meta_decision AS decision
    LEFT JOIN fact_intraday_candidate_session AS candidate
      ON decision.session_date = candidate.session_date
     AND decision.symbol = candidate.symbol
     AND decision.horizon = candidate.horizon
     AND decision.ranking_version = candidate.ranking_version
    LEFT JOIN fact_intraday_entry_decision AS raw
      ON decision.session_date = raw.session_date
     AND decision.symbol = raw.symbol
     AND decision.horizon = raw.horizon
     AND decision.checkpoint_time = raw.checkpoint_time
     AND decision.ranking_version = raw.ranking_version
    LEFT JOIN fact_intraday_adjusted_entry_decision AS adjusted
      ON decision.session_date = adjusted.session_date
     AND decision.symbol = adjusted.symbol
     AND decision.horizon = adjusted.horizon
     AND decision.checkpoint_time = adjusted.checkpoint_time
     AND decision.ranking_version = adjusted.ranking_version
    LEFT JOIN fact_ranking AS ranking
      ON candidate.selection_date = ranking.as_of_date
     AND decision.symbol = ranking.symbol
     AND decision.horizon = ranking.horizon
     AND decision.ranking_version = ranking.ranking_version
    LEFT JOIN prediction_pref AS prediction
      ON candidate.selection_date = prediction.as_of_date
     AND decision.symbol = prediction.symbol
     AND decision.horizon = prediction.horizon
    LEFT JOIN portfolio_pref AS portfolio
      ON candidate.selection_date = portfolio.as_of_date
     AND decision.symbol = portfolio.symbol
    LEFT JOIN regime_pref AS regime
      ON candidate.selection_date = regime.as_of_date
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_decision_lineage AS
    SELECT *
    FROM vw_intraday_decision_lineage
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol, horizon, checkpoint_time, ranking_version
        ORDER BY session_date DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW fact_intraday_final_action AS
    SELECT
        run_id,
        session_date,
        symbol,
        horizon,
        checkpoint_time,
        ranking_version,
        raw_action,
        adjusted_action,
        tuned_action,
        final_action,
        panel_name,
        predicted_class,
        predicted_class_probability,
        confidence_margin,
        uncertainty_score,
        disagreement_score,
        active_policy_candidate_id,
        active_meta_model_id,
        active_meta_training_run_id,
        hard_guard_block_flag,
        override_applied_flag,
        override_type,
        fallback_flag,
        fallback_reason,
        decision_reason_codes_json,
        risk_flags_json,
        source_notes_json,
        created_at
    FROM fact_intraday_meta_decision
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_intraday_active_meta_model AS
    SELECT *
    FROM fact_intraday_active_meta_model
    WHERE active_flag
      AND effective_from_date <= CURRENT_DATE
      AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY horizon, panel_name
        ORDER BY effective_from_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_policy_registry AS
    SELECT *
    FROM fact_portfolio_policy_registry
    WHERE active_flag
      AND effective_from_date <= CURRENT_DATE
      AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY portfolio_policy_id, portfolio_policy_version
        ORDER BY effective_from_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_candidate AS
    SELECT *
    FROM fact_portfolio_candidate
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, symbol
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_target_book AS
    SELECT *
    FROM fact_portfolio_target_book
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, symbol
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_rebalance_plan AS
    SELECT *
    FROM fact_portfolio_rebalance_plan
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, symbol
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_position_snapshot AS
    SELECT *
    FROM fact_portfolio_position_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, symbol
        ORDER BY snapshot_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_nav_snapshot AS
    SELECT *
    FROM fact_portfolio_nav_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, portfolio_policy_id, portfolio_policy_version
        ORDER BY snapshot_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_constraint_event AS
    SELECT *
    FROM fact_portfolio_constraint_event
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY execution_mode, symbol, constraint_type, event_code
        ORDER BY as_of_date DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_portfolio_evaluation_summary AS
    SELECT *
    FROM fact_portfolio_evaluation_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            portfolio_policy_id,
            portfolio_policy_version,
            execution_mode,
            comparison_key,
            metric_name
        ORDER BY evaluation_date DESC, created_at DESC
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
    """
    CREATE OR REPLACE VIEW vw_latest_job_run AS
    SELECT *
    FROM fact_job_run
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY job_name, COALESCE(as_of_date, DATE '1900-01-01'), trigger_type
        ORDER BY started_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_job_step_run AS
    SELECT *
    FROM fact_job_step_run
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY job_run_id, step_name
        ORDER BY started_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_pipeline_dependency_state AS
    SELECT *
    FROM fact_pipeline_dependency_state
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pipeline_name, dependency_name
        ORDER BY checked_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_health_snapshot AS
    SELECT *
    FROM fact_health_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY health_scope, component_name, metric_name
        ORDER BY snapshot_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_disk_watermark_event AS
    SELECT *
    FROM fact_disk_watermark_event
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY disk_status
        ORDER BY measured_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_retention_cleanup_run AS
    SELECT *
    FROM fact_retention_cleanup_run
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cleanup_scope, dry_run
        ORDER BY started_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_alert_event AS
    SELECT *
    FROM fact_alert_event
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY alert_type, component_name, message
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_recovery_action AS
    SELECT *
    FROM fact_recovery_action
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY target_job_run_id, action_type
        ORDER BY created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_active_ops_policy AS
    SELECT *
    FROM fact_active_ops_policy
    WHERE active_flag
      AND effective_from_at <= CURRENT_TIMESTAMP
      AND (effective_to_at IS NULL OR effective_to_at >= CURRENT_TIMESTAMP)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY policy_id, policy_version
        ORDER BY effective_from_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_active_lock AS
    SELECT *
    FROM fact_active_lock
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lock_name
        ORDER BY acquired_at DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_external_api_request_log AS
    SELECT *
    FROM fact_external_api_request_log
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY provider_name, service_slug, COALESCE(as_of_date, DATE '1970-01-01')
        ORDER BY request_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_external_api_budget_snapshot AS
    SELECT *
    FROM fact_external_api_budget_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY provider_name, date_kst
        ORDER BY snapshot_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_krx_service_status AS
    SELECT *
    FROM fact_krx_service_status
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY service_slug
        ORDER BY last_smoke_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_source_attribution_snapshot AS
    SELECT *
    FROM fact_source_attribution_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY page_slug, component_slug
        ORDER BY snapshot_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_app_snapshot AS
    SELECT *
    FROM fact_latest_app_snapshot
    QUALIFY ROW_NUMBER() OVER (
        ORDER BY snapshot_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_report_index AS
    SELECT *
    FROM fact_latest_report_index
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY report_type
        ORDER BY generated_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_release_candidate_check AS
    SELECT *
    FROM fact_release_candidate_check
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY check_name
        ORDER BY check_ts DESC, created_at DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW vw_latest_ui_data_freshness_snapshot AS
    SELECT *
    FROM fact_ui_data_freshness_snapshot
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY page_name, dataset_name
        ORDER BY snapshot_ts DESC, created_at DESC
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


def _migrate_fact_model_metric_summary_table(connection: duckdb.DuckDBPyConnection) -> None:
    exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = 'fact_model_metric_summary'
        """
    ).fetchone()[0]
    if not exists:
        return
    info = connection.execute("PRAGMA table_info('fact_model_metric_summary')").fetchdf()
    legacy_columns = {str(name) for name in info["name"].astype(str).tolist()}
    pk_columns = info.loc[info["pk"].fillna(0).astype(int) > 0, "name"].astype(str).tolist()
    expected_pk = [
        "training_run_id",
        "member_name",
        "split_name",
        "metric_scope",
        "class_label",
        "comparison_key",
        "metric_name",
    ]
    if pk_columns == expected_pk:
        return
    connection.execute(
        "ALTER TABLE fact_model_metric_summary "
        "RENAME TO fact_model_metric_summary_legacy"
    )
    connection.execute(
        """
        CREATE TABLE fact_model_metric_summary (
            training_run_id VARCHAR NOT NULL,
            model_domain VARCHAR NOT NULL DEFAULT 'default',
            model_version VARCHAR NOT NULL,
            horizon INTEGER NOT NULL,
            panel_name VARCHAR NOT NULL DEFAULT 'all',
            member_name VARCHAR NOT NULL,
            split_name VARCHAR NOT NULL,
            metric_scope VARCHAR NOT NULL DEFAULT 'all',
            class_label VARCHAR NOT NULL DEFAULT 'all',
            comparison_key VARCHAR NOT NULL DEFAULT 'all',
            metric_name VARCHAR NOT NULL,
            metric_value DOUBLE,
            sample_count BIGINT,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (
                training_run_id,
                member_name,
                split_name,
                metric_scope,
                class_label,
                comparison_key,
                metric_name
            )
        )
        """
    )
    def _legacy_expr(column_name: str, default_sql: str) -> str:
        if column_name in legacy_columns:
            return f"COALESCE({column_name}, {default_sql})"
        return default_sql

    connection.execute(
        f"""
        INSERT INTO fact_model_metric_summary (
            training_run_id,
            model_domain,
            model_version,
            horizon,
            panel_name,
            member_name,
            split_name,
            metric_scope,
            class_label,
            comparison_key,
            metric_name,
            metric_value,
            sample_count,
            created_at
        )
        SELECT
            training_run_id,
            {_legacy_expr("model_domain", "'default'")},
            model_version,
            horizon,
            {_legacy_expr("panel_name", "'all'")},
            member_name,
            split_name,
            {_legacy_expr("metric_scope", "'all'")},
            {_legacy_expr("class_label", "'all'")},
            {_legacy_expr("comparison_key", "'all'")},
            metric_name,
            metric_value,
            sample_count,
            created_at
        FROM fact_model_metric_summary_legacy
        """
    )
    connection.execute("DROP TABLE fact_model_metric_summary_legacy")


def connect_duckdb(
    db_path: Path,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    ensure_directory(db_path.parent)
    try:
        return duckdb.connect(str(db_path), read_only=read_only)
    except duckdb.ConnectionException as exc:
        # Within a single process, bundle runners can hold an existing read/write
        # connection and then call helper paths that attempt a read-only attach.
        # DuckDB rejects mixing configs for the same database file, so fall back
        # to a regular connection for that nested case only.
        if read_only and "different configuration" in str(exc).lower():
            return duckdb.connect(str(db_path), read_only=False)
        raise


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


@contextmanager
def duckdb_snapshot_connection(db_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    ensure_directory(db_path.parent)
    fd, snapshot_path_raw = tempfile.mkstemp(prefix="stockmaster-duckdb-", suffix=".duckdb")
    snapshot_path = Path(snapshot_path_raw)
    import os

    os.close(fd)
    try:
        shutil.copy2(db_path, snapshot_path)
        connection = duckdb.connect(str(snapshot_path), read_only=True)
        try:
            yield connection
        finally:
            connection.close()
    finally:
        try:
            Path(snapshot_path).unlink(missing_ok=True)
        except PermissionError:
            pass


def bootstrap_core_tables(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        _migrate_fact_ranking_table(connection)
        _migrate_fact_model_metric_summary_table(connection)

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

        for ddl in MODEL_TRAINING_RUN_COLUMN_MIGRATIONS:
            connection.execute(ddl)

        for ddl in MODEL_METRIC_SUMMARY_COLUMN_MIGRATIONS:
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
