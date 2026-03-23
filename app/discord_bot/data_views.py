from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION


def preferred_ranking_versions() -> list[str]:
    return [
        SELECTION_ENGINE_V2_VERSION,
        SELECTION_ENGINE_VERSION,
        EXPLANATORY_RANKING_VERSION,
    ]


def prediction_version_for_ranking(ranking_version: str | None) -> str | None:
    if ranking_version == SELECTION_ENGINE_V2_VERSION:
        return ALPHA_PREDICTION_VERSION
    if ranking_version == SELECTION_ENGINE_VERSION:
        return PREDICTION_VERSION
    return None


def resolve_latest_ranking_version(
    connection: duckdb.DuckDBPyConnection,
    ranking_version: str | None = None,
) -> str | None:
    if ranking_version:
        return ranking_version
    preferred_versions = preferred_ranking_versions()
    order_clause = " ".join(
        f"WHEN ranking_version = '{value}' THEN {index}"
        for index, value in enumerate(preferred_versions)
    )
    row = connection.execute(
        f"""
        SELECT ranking_version
        FROM fact_ranking
        ORDER BY
            CASE {order_clause} ELSE {len(preferred_versions)} END,
            as_of_date DESC,
            created_at DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row[0])


def resolve_latest_ranking_date(
    connection: duckdb.DuckDBPyConnection,
    ranking_version: str | None = None,
):
    effective_version = resolve_latest_ranking_version(connection, ranking_version)
    if effective_version is None:
        return None
    row = connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [effective_version],
    ).fetchone()
    return None if row is None else row[0]


def leaderboard_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date,
    ranking_version: str,
) -> pd.DataFrame:
    prediction_version = prediction_version_for_ranking(ranking_version)
    if prediction_version is None:
        return pd.DataFrame()
    frame = connection.execute(
        """
        SELECT
            ranking.as_of_date,
            ranking.as_of_date AS selection_date,
            (
                SELECT MIN(calendar.trading_date)
                FROM dim_trading_calendar AS calendar
                WHERE calendar.trading_date > ranking.as_of_date
                  AND calendar.is_trading_day
            ) AS next_entry_trade_date,
            ranking.symbol,
            symbol.company_name,
            symbol.market,
            symbol.sector,
            symbol.industry,
            ranking.horizon,
            ranking.final_selection_value,
            ranking.final_selection_rank_pct,
            ranking.grade,
            ranking.regime_state,
            ranking.ranking_version,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json,
            ranking.explanatory_score_json,
            prediction.expected_excess_return,
            prediction.lower_band,
            prediction.median_band,
            prediction.upper_band,
            prediction.model_spec_id,
            prediction.active_alpha_model_id,
            prediction.uncertainty_score,
            prediction.disagreement_score,
            prediction.fallback_flag,
            prediction.fallback_reason,
            daily.close AS selection_close_price,
            outcome.outcome_status,
            outcome.realized_excess_return,
            outcome.band_status
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
         AND prediction.prediction_version = ?
         AND prediction.ranking_version = ranking.ranking_version
        LEFT JOIN fact_daily_ohlcv AS daily
          ON ranking.symbol = daily.symbol
         AND ranking.as_of_date = daily.trading_date
        LEFT JOIN fact_selection_outcome AS outcome
          ON ranking.as_of_date = outcome.selection_date
         AND ranking.symbol = outcome.symbol
         AND ranking.horizon = outcome.horizon
         AND ranking.ranking_version = outcome.ranking_version
        WHERE ranking.as_of_date = ?
          AND ranking.ranking_version = ?
        ORDER BY ranking.horizon, ranking.final_selection_value DESC, ranking.symbol
        """,
        [prediction_version, as_of_date, ranking_version],
    ).fetchdf()
    if frame.empty:
        return frame
    base_price = pd.to_numeric(frame.get("selection_close_price"), errors="coerce")
    expected = pd.to_numeric(frame.get("expected_excess_return"), errors="coerce")
    upper = pd.to_numeric(frame.get("upper_band"), errors="coerce")
    lower = pd.to_numeric(frame.get("lower_band"), errors="coerce")
    frame["flat_target_price"] = base_price * (1.0 + expected)
    frame["flat_upper_target_price"] = base_price * (1.0 + upper)
    frame["flat_stop_price"] = base_price * (1.0 + lower)
    frame["reasons"] = frame["top_reason_tags_json"].fillna("[]")
    frame["risks"] = frame["risk_flags_json"].fillna("[]")
    return frame


def stock_workbench_summary_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            feature.symbol,
            symbol_meta.company_name,
            symbol_meta.market,
            feature.as_of_date,
            feature.ret_5d,
            feature.ret_20d,
            feature.adv_20,
            feature.news_count_3d,
            feature.foreign_net_value_ratio_5d,
            feature.smart_money_flow_ratio_20d,
            feature.flow_coverage_flag,
            selection_v2_1.final_selection_value AS d1_selection_v2_value,
            selection_v2_1.grade AS d1_selection_v2_grade,
            selection_1.final_selection_value AS d1_selection_value,
            selection_1.grade AS d1_grade,
            selection_v2_5.final_selection_value AS d5_selection_v2_value,
            selection_v2_5.grade AS d5_selection_v2_grade,
            selection_5.final_selection_value AS d5_selection_value,
            selection_5.grade AS d5_grade,
            prediction_alpha_5.expected_excess_return AS d5_alpha_expected_excess_return,
            prediction_alpha_5.lower_band AS d5_alpha_lower_band,
            prediction_alpha_5.upper_band AS d5_alpha_upper_band,
            prediction_alpha_5.uncertainty_score AS d5_alpha_uncertainty_score,
            prediction_alpha_5.disagreement_score AS d5_alpha_disagreement_score,
            prediction_alpha_5.fallback_flag AS d5_alpha_fallback_flag,
            prediction_5.expected_excess_return AS d5_expected_excess_return,
            prediction_5.lower_band AS d5_lower_band,
            prediction_5.upper_band AS d5_upper_band,
            outcome_1.realized_excess_return AS d1_realized_excess_return,
            outcome_1.band_status AS d1_band_status,
            outcome_v2_5.realized_excess_return AS d5_selection_v2_realized_excess_return,
            outcome_v2_5.band_status AS d5_selection_v2_band_status,
            outcome_5.realized_excess_return AS d5_realized_excess_return,
            outcome_5.band_status AS d5_band_status
        FROM vw_feature_matrix_latest AS feature
        JOIN dim_symbol AS symbol_meta
          ON feature.symbol = symbol_meta.symbol
        LEFT JOIN vw_ranking_latest AS selection_v2_1
          ON feature.symbol = selection_v2_1.symbol
         AND selection_v2_1.horizon = 1
         AND selection_v2_1.ranking_version = ?
        LEFT JOIN vw_ranking_latest AS selection_1
          ON feature.symbol = selection_1.symbol
         AND selection_1.horizon = 1
         AND selection_1.ranking_version = ?
        LEFT JOIN vw_ranking_latest AS selection_v2_5
          ON feature.symbol = selection_v2_5.symbol
         AND selection_v2_5.horizon = 5
         AND selection_v2_5.ranking_version = ?
        LEFT JOIN vw_ranking_latest AS selection_5
          ON feature.symbol = selection_5.symbol
         AND selection_5.horizon = 5
         AND selection_5.ranking_version = ?
        LEFT JOIN vw_prediction_latest AS prediction_alpha_5
          ON feature.symbol = prediction_alpha_5.symbol
         AND prediction_alpha_5.horizon = 5
         AND prediction_alpha_5.prediction_version = ?
        LEFT JOIN vw_prediction_latest AS prediction_5
          ON feature.symbol = prediction_5.symbol
         AND prediction_5.horizon = 5
         AND prediction_5.prediction_version = ?
        LEFT JOIN vw_selection_outcome_latest AS outcome_1
          ON feature.symbol = outcome_1.symbol
         AND outcome_1.horizon = 1
         AND outcome_1.ranking_version = ?
        LEFT JOIN vw_selection_outcome_latest AS outcome_v2_5
          ON feature.symbol = outcome_v2_5.symbol
         AND outcome_v2_5.horizon = 5
         AND outcome_v2_5.ranking_version = ?
        LEFT JOIN vw_selection_outcome_latest AS outcome_5
          ON feature.symbol = outcome_5.symbol
         AND outcome_5.horizon = 5
         AND outcome_5.ranking_version = ?
        WHERE symbol_meta.market IN ('KOSPI', 'KOSDAQ')
        ORDER BY feature.symbol
        """,
        [
            SELECTION_ENGINE_V2_VERSION,
            SELECTION_ENGINE_VERSION,
            SELECTION_ENGINE_V2_VERSION,
            SELECTION_ENGINE_VERSION,
            ALPHA_PREDICTION_VERSION,
            PREDICTION_VERSION,
            SELECTION_ENGINE_VERSION,
            SELECTION_ENGINE_V2_VERSION,
            SELECTION_ENGINE_VERSION,
        ],
    ).fetchdf()


def stock_workbench_live_snapshot_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    ranking_as_of_date,
) -> pd.DataFrame:
    if ranking_as_of_date is None:
        return pd.DataFrame()
    return connection.execute(
        """
        WITH d1 AS (
            SELECT
                symbol,
                final_selection_value AS live_d1_selection_v2_value,
                grade AS live_d1_selection_v2_grade,
                eligible_flag AS live_d1_eligible_flag,
                (COALESCE(eligible_flag, FALSE) AND COALESCE(final_selection_rank_pct, 0.0) >= 0.85)
                    AS live_d1_report_candidate_flag
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = 1
              AND ranking_version = ?
        ),
        d5 AS (
            SELECT
                symbol,
                final_selection_value AS live_d5_selection_v2_value,
                grade AS live_d5_selection_v2_grade,
                eligible_flag AS live_d5_eligible_flag,
                (COALESCE(eligible_flag, FALSE) AND COALESCE(final_selection_rank_pct, 0.0) >= 0.85)
                    AS live_d5_report_candidate_flag
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = 5
              AND ranking_version = ?
        ),
        prediction AS (
            SELECT
                symbol,
                expected_excess_return AS live_d5_expected_excess_return,
                lower_band,
                upper_band
            FROM fact_prediction
            WHERE as_of_date = ?
              AND horizon = 5
              AND prediction_version = ?
              AND ranking_version = ?
        ),
        reference_price AS (
            SELECT
                symbol,
                trading_date AS live_reference_date,
                close AS live_reference_price
            FROM fact_daily_ohlcv
            WHERE trading_date = ?
        )
        SELECT
            symbol_meta.symbol,
            symbol_meta.company_name,
            symbol_meta.market,
            ? AS live_as_of_date,
            reference_price.live_reference_date,
            reference_price.live_reference_price,
            d1.live_d1_selection_v2_value,
            d1.live_d1_selection_v2_grade,
            d1.live_d1_eligible_flag,
            d1.live_d1_report_candidate_flag,
            d5.live_d5_selection_v2_value,
            d5.live_d5_selection_v2_grade,
            d5.live_d5_eligible_flag,
            d5.live_d5_report_candidate_flag,
            prediction.live_d5_expected_excess_return,
            CASE
                WHEN reference_price.live_reference_price IS NULL
                  OR prediction.live_d5_expected_excess_return IS NULL THEN NULL
                ELSE reference_price.live_reference_price * (1.0 + prediction.live_d5_expected_excess_return)
            END AS live_d5_target_price,
            CASE
                WHEN reference_price.live_reference_price IS NULL
                  OR prediction.upper_band IS NULL THEN NULL
                ELSE reference_price.live_reference_price * (1.0 + prediction.upper_band)
            END AS live_d5_upper_target_price,
            CASE
                WHEN reference_price.live_reference_price IS NULL
                  OR prediction.lower_band IS NULL THEN NULL
                ELSE reference_price.live_reference_price * (1.0 + prediction.lower_band)
            END AS live_d5_stop_price
        FROM dim_symbol AS symbol_meta
        LEFT JOIN d1 ON symbol_meta.symbol = d1.symbol
        LEFT JOIN d5 ON symbol_meta.symbol = d5.symbol
        LEFT JOIN prediction ON symbol_meta.symbol = prediction.symbol
        LEFT JOIN reference_price ON symbol_meta.symbol = reference_price.symbol
        WHERE symbol_meta.market IN ('KOSPI', 'KOSDAQ')
        ORDER BY symbol_meta.symbol
        """,
        [
            ranking_as_of_date,
            SELECTION_ENGINE_V2_VERSION,
            ranking_as_of_date,
            SELECTION_ENGINE_V2_VERSION,
            ranking_as_of_date,
            ALPHA_PREDICTION_VERSION,
            SELECTION_ENGINE_V2_VERSION,
            ranking_as_of_date,
            ranking_as_of_date,
        ],
    ).fetchdf()


def latest_evaluation_summary_frame(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            summary_date,
            window_type,
            horizon,
            ranking_version,
            segment_value,
            count_evaluated,
            mean_realized_excess_return,
            hit_rate,
            avg_expected_excess_return
        FROM vw_latest_evaluation_summary
        WHERE segment_type = 'coverage'
          AND segment_value = 'all'
        ORDER BY window_type, horizon, ranking_version
        LIMIT 200
        """
    ).fetchdf()


def latest_intraday_policy_evaluation_frame(
    connection: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    for split_name in ("test", "validation", "all"):
        frame = connection.execute(
            """
            SELECT
                experiment_run_id,
                split_name,
                split_index,
                horizon,
                template_id,
                scope_type,
                scope_key,
                checkpoint_time,
                regime_cluster,
                regime_family,
                window_session_count,
                sample_count,
                matured_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                skip_saved_loss_rate,
                missed_winner_rate,
                left_tail_proxy,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key
            FROM vw_latest_intraday_policy_evaluation
            WHERE split_name = ?
            ORDER BY window_end_date DESC, horizon, objective_score DESC NULLS LAST
            LIMIT 200
            """,
            [split_name],
        ).fetchdf()
        if not frame.empty:
            return frame
    return pd.DataFrame()
