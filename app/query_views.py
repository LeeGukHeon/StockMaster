from __future__ import annotations

import json
from datetime import date

import pandas as pd

from app.intraday.policy import apply_active_intraday_policy_frame
from app.ml.constants import MODEL_DOMAIN as ALPHA_MODEL_DOMAIN
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def _latest_intraday_session_date(settings: Settings):
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            "SELECT MAX(session_date) FROM fact_intraday_candidate_session"
        ).fetchone()
        return None if row is None or row[0] is None else pd.Timestamp(row[0]).date()


def latest_alpha_active_model_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 20,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        target_date = as_of_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(effective_from_date) FROM fact_alpha_active_model"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        if active_only:
            return connection.execute(
                """
                SELECT
                    active.horizon,
                    active.model_spec_id,
                    active.training_run_id,
                    train.train_end_date,
                    active.model_version,
                    active.source_type,
                    active.promotion_type,
                    active.effective_from_date,
                    active.effective_to_date,
                    active.note
                FROM fact_alpha_active_model AS active
                LEFT JOIN fact_model_training_run AS train
                  ON active.training_run_id = train.training_run_id
                WHERE active.effective_from_date <= ?
                  AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY active.horizon
                    ORDER BY active.effective_from_date DESC, active.created_at DESC
                ) = 1
                ORDER BY active.horizon
                LIMIT ?
                """,
                [target_date, target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                horizon,
                model_spec_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_alpha_model_id,
                note,
                updated_at
            FROM fact_alpha_active_model
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_alpha_training_candidate_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            WITH latest AS (
                SELECT
                    model_spec_id,
                    horizon,
                    estimation_scheme,
                    rolling_window_days,
                    train_end_date,
                    training_run_id,
                    model_version,
                    fallback_flag,
                    fallback_reason,
                    created_at
                FROM fact_model_training_run
                WHERE model_domain = ?
                  AND status = 'success'
                  AND artifact_uri IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY model_spec_id, horizon
                    ORDER BY train_end_date DESC, created_at DESC
                ) = 1
            )
            SELECT
                model_spec_id,
                horizon,
                estimation_scheme,
                rolling_window_days,
                train_end_date,
                training_run_id,
                model_version,
                fallback_flag,
                fallback_reason
            FROM latest
            ORDER BY model_spec_id, horizon
            LIMIT ?
            """,
            [ALPHA_MODEL_DOMAIN, limit],
        ).fetchdf()


def latest_alpha_model_spec_frame(
    settings: Settings,
    *,
    limit: int = 20,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    query = """
        SELECT
            model_spec_id,
            model_domain,
            model_version,
            estimation_scheme,
            rolling_window_days,
            feature_version,
            label_version,
            selection_engine_version,
            spec_payload_json,
            active_candidate_flag,
            updated_at
        FROM dim_alpha_model_spec
    """
    parameters: list[object] = []
    if active_only:
        query += " WHERE active_candidate_flag = TRUE"
    query += " ORDER BY model_spec_id LIMIT ?"
    parameters.append(limit)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        frame = connection.execute(query, parameters).fetchdf()
    if frame.empty:
        return frame
    lifecycle_roles: list[str | None] = []
    lifecycle_fallback_flags: list[bool] = []
    for payload in frame["spec_payload_json"].tolist():
        parsed: dict[str, object] = {}
        if isinstance(payload, str) and payload.strip():
            try:
                loaded = json.loads(payload)
            except json.JSONDecodeError:
                loaded = {}
            if isinstance(loaded, dict):
                parsed = loaded
        lifecycle_roles.append(
            None if parsed.get("lifecycle_role") in (None, "") else str(parsed.get("lifecycle_role"))
        )
        lifecycle_fallback_flags.append(bool(parsed.get("lifecycle_fallback_flag", False)))
    frame["lifecycle_role"] = lifecycle_roles
    frame["lifecycle_fallback_flag"] = lifecycle_fallback_flags
    return frame


def latest_alpha_selection_gap_scorecard_frame(
    settings: Settings,
    *,
    summary_date=None,
    window_name: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        target_summary_date = summary_date
        if target_summary_date is None:
            row = connection.execute(
                "SELECT MAX(summary_date) FROM fact_alpha_shadow_selection_gap_scorecard"
            ).fetchone()
            target_summary_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_summary_date is None:
            return pd.DataFrame()
        query = """
            SELECT
                summary_date,
                window_name,
                window_start,
                window_end,
                horizon,
                model_spec_id,
                segment_name,
                matured_selection_date_count,
                required_selection_date_count,
                insufficient_history_flag,
                raw_top5_source,
                hit_rate_formula,
                raw_top5_mean_realized_excess_return,
                selected_top5_mean_realized_excess_return,
                report_candidates_mean_realized_excess_return,
                raw_top5_hit_rate,
                selected_top5_hit_rate,
                report_candidates_hit_rate,
                top5_overlap,
                pred_only_top5_mean_realized_excess_return,
                sel_only_top5_mean_realized_excess_return,
                raw_top5_worst_realized_excess_return,
                selected_top5_worst_realized_excess_return,
                raw_top5_top1_expected_return_share,
                selected_top5_top1_expected_return_share,
                raw_top5_top1_minus_median_expected_return,
                selected_top5_top1_minus_median_expected_return,
                extreme_expected_return_threshold,
                raw_top5_extreme_expected_return_count,
                selected_top5_extreme_expected_return_count,
                drag_vs_raw_top5,
                evaluation_run_id
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date = ?
        """
        params: list[object] = [target_summary_date]
        if window_name is not None:
            query += " AND window_name = ?"
            params.append(window_name)
        query += """
            ORDER BY window_name, horizon, model_spec_id, segment_name
            LIMIT ?
        """
        params.append(limit)
        return connection.execute(query, params).fetchdf()


def latest_alpha_rollback_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            SELECT
                horizon,
                model_spec_id,
                training_run_id,
                promotion_type,
                rollback_of_active_alpha_model_id,
                effective_from_date,
                note,
                updated_at
            FROM fact_alpha_active_model
            WHERE promotion_type = 'ROLLBACK'
               OR rollback_of_active_alpha_model_id IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_market_context_frame(
    settings: Settings,
    *,
    session_date=None,
    limit: int = 20,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            SELECT
                session_date,
                checkpoint_time,
                context_scope,
                market_session_state,
                prior_daily_regime_state,
                market_breadth_ratio,
                candidate_mean_return_from_open,
                candidate_mean_relative_volume,
                candidate_mean_signal_quality,
                bar_coverage_ratio,
                trade_coverage_ratio,
                quote_coverage_ratio,
                data_quality_flag
            FROM fact_intraday_market_context_snapshot
            WHERE session_date = ?
            ORDER BY checkpoint_time, context_scope
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_intraday_strategy_comparison_frame(
    settings: Settings,
    *,
    end_session_date=None,
    comparison_scope: str = "all",
    limit: int = 30,
) -> pd.DataFrame:
    target_date = end_session_date or _latest_intraday_session_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            SELECT
                end_session_date,
                horizon,
                strategy_id,
                comparison_scope,
                comparison_value,
                cutoff_checkpoint_time,
                sample_count,
                matured_count,
                executed_count,
                no_entry_count,
                execution_rate,
                mean_realized_excess_return,
                median_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                positive_timing_edge_rate,
                skip_saved_loss_rate,
                missed_winner_rate,
                coverage_ok_rate
            FROM fact_intraday_strategy_comparison
            WHERE end_session_date = ?
              AND comparison_scope = ?
            ORDER BY horizon, comparison_value, strategy_id
            LIMIT ?
            """,
            [target_date, comparison_scope, limit],
        ).fetchdf()


def latest_intraday_timing_calibration_frame(
    settings: Settings,
    *,
    window_end_date=None,
    grouping_key: str | None = None,
    limit: int = 30,
) -> pd.DataFrame:
    target_date = window_end_date or _latest_intraday_session_date(settings)
    if target_date is None or not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        if grouping_key is None:
            return connection.execute(
                """
                SELECT
                    window_end_date,
                    horizon,
                    grouping_key,
                    grouping_value,
                    sample_count,
                    executed_count,
                    execution_rate,
                    mean_realized_excess_return,
                    hit_rate,
                    mean_timing_edge_vs_open_bps,
                    skip_saved_loss_rate,
                    missed_winner_rate,
                    quality_flag
                FROM fact_intraday_timing_calibration
                WHERE window_end_date = ?
                  AND grouping_key IN ('overall', 'strategy_id', 'regime_family')
                ORDER BY horizon, grouping_key, grouping_value
                LIMIT ?
                """,
                [target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                window_end_date,
                horizon,
                grouping_key,
                grouping_value,
                sample_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                skip_saved_loss_rate,
                missed_winner_rate,
                quality_flag
            FROM fact_intraday_timing_calibration
            WHERE window_end_date = ?
              AND grouping_key = ?
            ORDER BY horizon, grouping_value
            LIMIT ?
            """,
            [target_date, grouping_key, limit],
        ).fetchdf()


def stock_workbench_intraday_decision_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            SELECT
                raw.session_date,
                raw.checkpoint_time,
                raw.horizon,
                raw.action AS raw_action,
                adjusted.adjusted_action,
                adjusted.market_regime_family,
                adjusted.adjustment_profile,
                raw.action_score AS raw_timing_score,
                adjusted.adjusted_timing_score,
                adjusted.signal_quality_flag,
                adjusted.fallback_flag
            FROM fact_intraday_entry_decision AS raw
            LEFT JOIN fact_intraday_adjusted_entry_decision AS adjusted
              ON raw.session_date = adjusted.session_date
             AND raw.symbol = adjusted.symbol
             AND raw.horizon = adjusted.horizon
             AND raw.checkpoint_time = adjusted.checkpoint_time
             AND raw.ranking_version = adjusted.ranking_version
            WHERE raw.symbol = ?
            ORDER BY raw.session_date DESC, raw.checkpoint_time DESC, raw.horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def latest_intraday_policy_experiment_frame(
    settings: Settings,
    *,
    limit: int = 30,
    experiment_type: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        if experiment_type is None:
            return connection.execute(
                """
                SELECT
                    experiment_name,
                    experiment_type,
                    search_space_version,
                    objective_version,
                    split_version,
                    split_mode,
                    horizon,
                    candidate_count,
                    selected_policy_candidate_id,
                    fallback_used_flag,
                    status,
                    created_at
                FROM vw_latest_intraday_policy_experiment_run
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                experiment_name,
                experiment_type,
                search_space_version,
                objective_version,
                split_version,
                split_mode,
                horizon,
                candidate_count,
                selected_policy_candidate_id,
                fallback_used_flag,
                status,
                created_at
            FROM vw_latest_intraday_policy_experiment_run
            WHERE experiment_type = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [experiment_type, limit],
        ).fetchdf()


def latest_intraday_policy_evaluation_frame(
    settings: Settings,
    *,
    split_name: str = "test",
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        base_query = """
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
            LIMIT ?
        """
        split_order = [split_name]
        if split_name == "test":
            split_order.extend(["validation", "all"])
        for target_split in split_order:
            frame = connection.execute(base_query, [target_split, limit]).fetchdf()
            if not frame.empty:
                return frame
    return pd.DataFrame()


def latest_intraday_policy_ablation_frame(
    settings: Settings,
    *,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            """
            SELECT
                ablation_date,
                horizon,
                base_policy_source,
                ablation_name,
                sample_count,
                mean_realized_excess_return_delta,
                hit_rate_delta,
                mean_timing_edge_vs_open_bps_delta,
                execution_rate_delta,
                skip_saved_loss_rate_delta,
                missed_winner_rate_delta,
                left_tail_proxy_delta,
                stability_score_delta,
                objective_score_delta
            FROM vw_latest_intraday_policy_ablation_result
            ORDER BY ablation_date DESC, horizon, ablation_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_intraday_policy_recommendation_frame(
    settings: Settings,
    *,
    recommendation_date=None,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        target_date = recommendation_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(recommendation_date) FROM fact_intraday_policy_selection_recommendation"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank,
                policy_candidate_id,
                template_id,
                test_session_count,
                executed_count,
                execution_rate,
                mean_realized_excess_return,
                hit_rate,
                mean_timing_edge_vs_open_bps,
                stability_score,
                objective_score,
                manual_review_required_flag,
                fallback_scope_type,
                fallback_scope_key
            FROM fact_intraday_policy_selection_recommendation
            WHERE recommendation_date = ?
            ORDER BY horizon, recommendation_rank, scope_type, scope_key
            LIMIT ?
            """,
            [target_date, limit],
        ).fetchdf()


def latest_intraday_active_policy_frame(
    settings: Settings,
    *,
    as_of_date=None,
    limit: int = 30,
    active_only: bool = True,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        target_date = as_of_date
        if target_date is None:
            row = connection.execute(
                "SELECT MAX(effective_from_date) FROM fact_intraday_active_policy"
            ).fetchone()
            target_date = None if row is None or row[0] is None else pd.Timestamp(row[0]).date()
        if target_date is None:
            return pd.DataFrame()
        if active_only:
            return connection.execute(
                """
                SELECT
                    active.horizon,
                    active.scope_type,
                    active.scope_key,
                    active.checkpoint_time,
                    active.regime_cluster,
                    active.regime_family,
                    active.policy_candidate_id,
                    candidate.template_id,
                    active.source_recommendation_date,
                    active.promotion_type,
                    active.effective_from_date,
                    active.effective_to_date,
                    active.fallback_scope_type,
                    active.fallback_scope_key,
                    active.note
                FROM fact_intraday_active_policy AS active
                JOIN fact_intraday_policy_candidate AS candidate
                  ON active.policy_candidate_id = candidate.policy_candidate_id
                WHERE active.effective_from_date <= ?
                  AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                ORDER BY active.horizon, active.scope_type, active.scope_key
                LIMIT ?
                """,
                [target_date, target_date, limit],
            ).fetchdf()
        return connection.execute(
            """
            SELECT
                horizon,
                scope_type,
                scope_key,
                policy_candidate_id,
                promotion_type,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_policy_id,
                note,
                updated_at
            FROM fact_intraday_active_policy
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def intraday_console_tuned_action_frame(
    settings: Settings,
    *,
    session_date=None,
    symbol: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    target_date = session_date or _latest_intraday_session_date(settings)
    if target_date is None:
        return pd.DataFrame()
    frame = apply_active_intraday_policy_frame(
        settings,
        session_date=target_date,
        symbol=symbol,
        limit=limit,
    )
    if frame.empty:
        return frame
    columns = [
        "session_date",
        "checkpoint_time",
        "symbol",
        "company_name",
        "horizon",
        "market_regime_family",
        "adjusted_action",
        "tuned_action",
        "adjusted_timing_score",
        "tuned_score",
        "active_policy_candidate_id",
        "active_policy_template_id",
        "active_policy_scope_type",
        "active_policy_scope_key",
        "policy_trace",
        "fallback_used_flag",
    ]
    available = [column for column in columns if column in frame.columns]
    return frame.loc[:, available].copy()
