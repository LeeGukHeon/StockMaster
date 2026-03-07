from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from app.common.disk import DiskUsageReport, measure_disk_usage
from app.providers.base import ProviderHealth
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.client import KrxProvider
from app.providers.naver_news.client import NaverNewsProvider
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings, load_settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import fetch_recent_runs


def load_ui_settings(project_root: Path) -> Settings:
    settings = load_settings(project_root=project_root)
    ensure_storage_layout(settings)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
    return settings


def recent_runs_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        frame = fetch_recent_runs(connection, limit=limit)
    return frame


def disk_report(settings: Settings) -> DiskUsageReport:
    return measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=settings.storage.warning_ratio,
        prune_ratio=settings.storage.prune_ratio,
        limit_ratio=settings.storage.limit_ratio,
    )


def provider_health_frame(settings: Settings) -> pd.DataFrame:
    providers = [
        KISProvider(settings),
        DartProvider(settings),
        KrxProvider(settings),
        NaverNewsProvider(settings),
    ]
    rows: list[ProviderHealth] = []
    try:
        for provider in providers:
            try:
                rows.append(provider.health_check())
            except Exception as exc:
                rows.append(
                    ProviderHealth(
                        provider=provider.provider_name,
                        configured=provider.is_configured(),
                        status="error",
                        detail=str(exc),
                    )
                )
    finally:
        for provider in providers:
            provider.close()
    return pd.DataFrame([asdict(row) for row in rows])


def watermark_frame(settings: Settings) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"threshold": "warning", "ratio": settings.storage.warning_ratio},
            {"threshold": "prune", "ratio": settings.storage.prune_ratio},
            {"threshold": "limit", "ratio": settings.storage.limit_ratio},
        ]
    )


def _resolve_latest_ranking_version(connection, ranking_version: str | None) -> str | None:
    if ranking_version:
        return ranking_version
    row = connection.execute(
        """
        SELECT ranking_version
        FROM fact_ranking
        ORDER BY
            CASE WHEN ranking_version = ? THEN 0 ELSE 1 END,
            as_of_date DESC,
            created_at DESC
        LIMIT 1
        """,
        [SELECTION_ENGINE_VERSION],
    ).fetchone()
    return None if row is None else str(row[0])


def _resolve_latest_ranking_date(connection, ranking_version: str | None) -> object:
    effective_version = _resolve_latest_ranking_version(connection, ranking_version)
    if effective_version is None:
        return None
    return connection.execute(
        """
        SELECT MAX(as_of_date)
        FROM fact_ranking
        WHERE ranking_version = ?
        """,
        [effective_version],
    ).fetchone()[0]


def universe_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                COUNT(*) AS total_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSPI') AS kospi_symbols,
                COUNT(*) FILTER (WHERE market = 'KOSDAQ') AS kosdaq_symbols,
                COUNT(*) FILTER (WHERE dart_corp_code IS NOT NULL) AS dart_mapped_symbols,
                COUNT(*) FILTER (
                    WHERE market IN ('KOSPI', 'KOSDAQ')
                      AND COALESCE(is_common_stock, FALSE)
                      AND NOT COALESCE(is_etf, FALSE)
                      AND NOT COALESCE(is_etn, FALSE)
                      AND NOT COALESCE(is_spac, FALSE)
                      AND NOT COALESCE(is_reit, FALSE)
                      AND NOT COALESCE(is_delisted, FALSE)
                ) AS active_common_stock_count
            FROM dim_symbol
            """
        ).fetchdf()


def calendar_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                MIN(trading_date) AS min_trading_date,
                MAX(trading_date) AS max_trading_date,
                COUNT(*) AS total_days,
                COUNT(*) FILTER (WHERE is_trading_day) AS trading_days,
                COUNT(*) FILTER (WHERE is_override) AS override_days
            FROM dim_trading_calendar
            """
        ).fetchdf()


def latest_sync_runs_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                run_type,
                started_at,
                finished_at,
                status,
                notes
            FROM ops_run_manifest
            WHERE run_type IN (
                'sync_universe',
                'sync_trading_calendar',
                'sync_daily_ohlcv',
                'sync_fundamentals_snapshot',
                'sync_news_metadata',
                'sync_investor_flow',
                'build_feature_store',
                'build_forward_labels',
                'build_market_regime_snapshot',
                'materialize_explanatory_ranking',
                'materialize_selection_engine_v1',
                'calibrate_proxy_prediction_bands',
                'materialize_selection_outcomes',
                'materialize_prediction_evaluation',
                'materialize_calibration_diagnostics',
                'validate_explanatory_ranking'
                ,
                'validate_selection_engine_v1',
                'validate_evaluation_pipeline',
                'render_discord_eod_report',
                'publish_discord_eod_report',
                'render_postmortem_report',
                'publish_discord_postmortem_report'
            )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY run_type ORDER BY started_at DESC) = 1
            ORDER BY run_type
            """
        ).fetchdf()


def research_data_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                (SELECT MAX(trading_date) FROM fact_daily_ohlcv) AS latest_ohlcv_date,
                (SELECT COUNT(*) FROM fact_daily_ohlcv WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_daily_ohlcv
                )) AS latest_ohlcv_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_fundamentals_snapshot
                ) AS latest_fundamentals_date,
                (SELECT COUNT(*) FROM fact_fundamentals_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_fundamentals_snapshot
                )) AS latest_fundamentals_rows,
                (SELECT MAX(signal_date) FROM fact_news_item) AS latest_news_date,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                )) AS latest_news_rows,
                (SELECT COUNT(*) FROM fact_news_item WHERE signal_date = (
                    SELECT MAX(signal_date) FROM fact_news_item
                ) AND COALESCE(symbol_candidates, '[]') = '[]') AS latest_news_unmatched,
                (SELECT MAX(trading_date) FROM fact_investor_flow) AS latest_flow_date,
                (SELECT COUNT(*) FROM fact_investor_flow WHERE trading_date = (
                    SELECT MAX(trading_date) FROM fact_investor_flow
                )) AS latest_flow_rows,
                (SELECT MAX(as_of_date) FROM fact_feature_snapshot) AS latest_feature_date,
                (SELECT COUNT(*) FROM fact_feature_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_feature_snapshot
                )) AS latest_feature_rows,
                (SELECT MAX(as_of_date) FROM fact_forward_return_label) AS latest_label_date,
                (SELECT COUNT(*) FROM fact_forward_return_label WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_forward_return_label
                ) AND label_available_flag) AS latest_available_label_rows,
                (SELECT MAX(as_of_date) FROM fact_market_regime_snapshot) AS latest_regime_date,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AS latest_explanatory_ranking_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = 'explanatory_ranking_v0'
                ) AND ranking_version = 'explanatory_ranking_v0')
                    AS latest_explanatory_ranking_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_ranking
                    WHERE ranking_version = ?
                ) AS latest_selection_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking WHERE ranking_version = ?
                ) AND ranking_version = ?) AS latest_selection_rows,
                (
                    SELECT MAX(as_of_date)
                    FROM fact_prediction
                    WHERE prediction_version = ?
                ) AS latest_prediction_date,
                (SELECT COUNT(*) FROM fact_prediction WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_prediction WHERE prediction_version = ?
                ) AND prediction_version = ?) AS latest_prediction_rows,
                (SELECT MAX(evaluation_date) FROM fact_selection_outcome) AS latest_outcome_date,
                (SELECT COUNT(*) FROM fact_selection_outcome WHERE evaluation_date = (
                    SELECT MAX(evaluation_date) FROM fact_selection_outcome
                )) AS latest_outcome_rows,
                (
                    SELECT MAX(summary_date)
                    FROM fact_evaluation_summary
                ) AS latest_evaluation_summary_date,
                (SELECT COUNT(*) FROM fact_evaluation_summary WHERE summary_date = (
                    SELECT MAX(summary_date) FROM fact_evaluation_summary
                )) AS latest_evaluation_summary_rows,
                (
                    SELECT MAX(diagnostic_date)
                    FROM fact_calibration_diagnostic
                ) AS latest_calibration_date,
                (SELECT COUNT(*) FROM fact_calibration_diagnostic WHERE diagnostic_date = (
                    SELECT MAX(diagnostic_date) FROM fact_calibration_diagnostic
                )) AS latest_calibration_rows
            """,
            [
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
                PREDICTION_VERSION,
            ],
        ).fetchdf()


def recent_failure_runs_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                run_type,
                as_of_date,
                started_at,
                finished_at,
                error_message
            FROM ops_run_manifest
            WHERE status = 'failed'
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_ohlcv_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                symbol,
                open,
                high,
                low,
                close,
                volume
            FROM fact_daily_ohlcv
            ORDER BY trading_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_fundamentals_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                symbol,
                revenue,
                operating_income,
                net_income,
                roe,
                debt_ratio
            FROM fact_fundamentals_snapshot
            ORDER BY as_of_date DESC, symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_news_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                signal_date,
                published_at,
                title,
                publisher,
                symbol_candidates,
                query_bucket
            FROM fact_news_item
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_feature_sample_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT *
            FROM vw_feature_matrix_latest
            ORDER BY symbol
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_label_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_forward_return_label
            )
            SELECT
                label.horizon,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE label_available_flag) AS available_rows,
                AVG(CASE WHEN label_available_flag THEN 1.0 ELSE 0.0 END) AS coverage_ratio
            FROM fact_forward_return_label AS label
            JOIN latest_date
              ON label.as_of_date = latest_date.as_of_date
            GROUP BY label.horizon
            ORDER BY label.horizon
            """
        ).fetchdf()


def latest_feature_coverage_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_feature_snapshot
            )
            SELECT
                feature_name,
                COUNT(*) AS symbol_rows,
                AVG(CASE WHEN feature_value IS NULL THEN 1.0 ELSE 0.0 END) AS null_ratio
            FROM fact_feature_snapshot
            WHERE as_of_date = (SELECT as_of_date FROM latest_date)
              AND feature_name IN (
                'ret_5d',
                'ret_20d',
                'adv_20',
                'roe_latest',
                'debt_ratio_latest',
                'news_count_3d',
                'foreign_net_value_ratio_5d',
                'smart_money_flow_ratio_20d',
                'flow_coverage_flag',
                'data_confidence_score'
              )
            GROUP BY feature_name
            ORDER BY feature_name
            """
        ).fetchdf()


def latest_regime_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                as_of_date,
                market_scope,
                regime_state,
                regime_score,
                breadth_up_ratio,
                median_symbol_return_1d,
                market_realized_vol_20d,
                turnover_burst_z
            FROM vw_market_regime_latest
            ORDER BY market_scope
            """
        ).fetchdf()


def latest_version_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                (
                    SELECT feature_version
                    FROM ops_run_manifest
                    WHERE run_type = 'build_feature_store'
                      AND status = 'success'
                      AND feature_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_feature_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_explanatory_ranking'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_explanatory_ranking_version,
                (
                    SELECT ranking_version
                    FROM ops_run_manifest
                    WHERE run_type = 'materialize_selection_engine_v1'
                      AND status = 'success'
                      AND ranking_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_selection_ranking_version,
                (
                    SELECT model_version
                    FROM ops_run_manifest
                    WHERE run_type = 'calibrate_proxy_prediction_bands'
                      AND status = 'success'
                      AND model_version IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_prediction_version
            """
        ).fetchdf()


def latest_validation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_gross_forward_return,
                avg_excess_forward_return,
                median_excess_forward_return,
                top_decile_gap
            FROM vw_latest_ranking_validation_summary
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_ranking_versions(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT ranking_version
            FROM fact_ranking
            ORDER BY
                CASE WHEN ranking_version = ? THEN 0 ELSE 1 END,
                ranking_version
            """,
            [SELECTION_ENGINE_VERSION],
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_ranking_dates(settings: Settings, *, ranking_version: str | None = None) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return []
        rows = connection.execute(
            """
            SELECT DISTINCT as_of_date
            FROM fact_ranking
            WHERE ranking_version = ?
            ORDER BY as_of_date DESC
            """,
            [effective_version],
        ).fetchall()
    return [str(row[0]) for row in rows]


def available_evaluation_dates(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT evaluation_date
            FROM fact_selection_outcome
            WHERE evaluation_date IS NOT NULL
            ORDER BY evaluation_date DESC
            """
        ).fetchall()
    return [str(row[0]) for row in rows]


def leaderboard_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    market: str = "ALL",
    limit: int = 20,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        frame = connection.execute(
            """
            SELECT
                ranking.as_of_date,
                ranking.symbol,
                symbol.company_name,
                symbol.market,
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
            LEFT JOIN fact_selection_outcome AS outcome
              ON ranking.as_of_date = outcome.selection_date
             AND ranking.symbol = outcome.symbol
             AND ranking.horizon = outcome.horizon
             AND ranking.ranking_version = outcome.ranking_version
            WHERE ranking.as_of_date = ?
              AND ranking.horizon = ?
              AND ranking.ranking_version = ?
            ORDER BY ranking.final_selection_value DESC, ranking.symbol
            """,
            [PREDICTION_VERSION, selected_date, horizon, effective_version],
        ).fetchdf()
    if frame.empty:
        return frame
    if market.upper() != "ALL":
        frame = frame.loc[frame["market"].str.upper() == market.upper()].copy()
    frame["reasons"] = frame["top_reason_tags_json"].fillna("[]")
    frame["risks"] = frame["risk_flags_json"].fillna("[]")
    return frame.head(limit).reset_index(drop=True)


def leaderboard_grade_count_frame(
    settings: Settings,
    *,
    as_of_date: str | None = None,
    horizon: int = 5,
    ranking_version: str | None = None,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        effective_version = _resolve_latest_ranking_version(connection, ranking_version)
        if effective_version is None:
            return pd.DataFrame()
        selected_date = as_of_date or _resolve_latest_ranking_date(connection, effective_version)
        if selected_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT grade, COUNT(*) AS row_count
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = ?
              AND ranking_version = ?
            GROUP BY grade
            ORDER BY grade
            """,
            [selected_date, horizon, effective_version],
        ).fetchdf()


def latest_flow_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(trading_date) AS trading_date
                FROM fact_investor_flow
            )
            SELECT
                flow.trading_date,
                COUNT(*) AS row_count,
                AVG(
                    CASE WHEN foreign_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS foreign_value_coverage,
                AVG(
                    CASE WHEN institution_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS institution_value_coverage,
                AVG(
                    CASE WHEN individual_net_value IS NOT NULL THEN 1.0 ELSE 0.0 END
                ) AS individual_value_coverage
            FROM fact_investor_flow AS flow
            JOIN latest_date
              ON flow.trading_date = latest_date.trading_date
            GROUP BY flow.trading_date
            """
        ).fetchdf()


def latest_prediction_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM fact_prediction
                WHERE prediction_version = ?
            )
            SELECT
                horizon,
                COUNT(*) AS row_count,
                AVG(expected_excess_return) AS avg_expected_excess_return,
                AVG(upper_band - lower_band) AS avg_band_width
            FROM fact_prediction
            WHERE prediction_version = ?
              AND as_of_date = (SELECT as_of_date FROM latest_date)
            GROUP BY horizon
            ORDER BY horizon
            """,
            [PREDICTION_VERSION, PREDICTION_VERSION],
        ).fetchdf()


def latest_selection_validation_summary_frame(
    settings: Settings,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                start_date,
                end_date,
                horizon,
                bucket_type,
                bucket_name,
                symbol_count,
                avg_excess_forward_return,
                median_excess_forward_return,
                hit_rate,
                avg_expected_excess_return,
                avg_prediction_error,
                top_decile_gap
            FROM vw_latest_selection_validation_summary
            WHERE ranking_version = ?
            ORDER BY bucket_type, horizon, bucket_name
            LIMIT ?
            """,
            [SELECTION_ENGINE_VERSION, limit],
        ).fetchdf()


def latest_outcome_summary_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_date AS (
                SELECT MAX(evaluation_date) AS evaluation_date
                FROM fact_selection_outcome
            )
            SELECT
                evaluation_date,
                horizon,
                ranking_version,
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE outcome_status = 'matured') AS matured_rows,
                AVG(realized_excess_return) AS avg_realized_excess_return,
                AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate
            FROM fact_selection_outcome
            WHERE evaluation_date = (SELECT evaluation_date FROM latest_date)
            GROUP BY evaluation_date, horizon, ranking_version
            ORDER BY horizon, ranking_version
            """
        ).fetchdf()


def latest_evaluation_summary_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
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
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def latest_evaluation_comparison_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            WITH latest_summary AS (
                SELECT *
                FROM vw_latest_evaluation_summary
                WHERE segment_type = 'coverage'
                  AND segment_value = 'all'
            )
            SELECT
                selection.summary_date,
                selection.window_type,
                selection.horizon,
                selection.mean_realized_excess_return AS selection_avg_excess,
                explanatory.mean_realized_excess_return AS explanatory_avg_excess,
                selection.mean_realized_excess_return
                    - explanatory.mean_realized_excess_return AS avg_excess_gap,
                selection.hit_rate - explanatory.hit_rate AS hit_rate_gap
            FROM latest_summary AS selection
            JOIN latest_summary AS explanatory
              ON selection.summary_date = explanatory.summary_date
             AND selection.window_type = explanatory.window_type
             AND selection.horizon = explanatory.horizon
             AND selection.ranking_version = ?
             AND explanatory.ranking_version = ?
            ORDER BY selection.window_type, selection.horizon
            """,
            [SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
        ).fetchdf()


def latest_calibration_diagnostic_frame(settings: Settings, *, limit: int = 20) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                diagnostic_date,
                horizon,
                bin_type,
                bin_value,
                sample_count,
                expected_median,
                observed_mean,
                coverage_rate,
                median_bias,
                quality_flag
            FROM vw_latest_calibration_diagnostic
            ORDER BY horizon, bin_type, bin_value
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def evaluation_outcomes_frame(
    settings: Settings,
    *,
    evaluation_date: str | None = None,
    horizon: int = 5,
    ranking_version: str = SELECTION_ENGINE_VERSION,
    limit: int = 50,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        if evaluation_date is None:
            row = connection.execute(
                """
                SELECT MAX(evaluation_date)
                FROM fact_selection_outcome
                """
            ).fetchone()
            if row is None or row[0] is None:
                return pd.DataFrame()
            evaluation_date = str(row[0])
        return connection.execute(
            """
            SELECT
                outcome.evaluation_date,
                outcome.selection_date,
                outcome.symbol,
                meta.company_name,
                meta.market,
                outcome.horizon,
                outcome.ranking_version,
                outcome.final_selection_value,
                outcome.expected_excess_return_at_selection,
                outcome.realized_excess_return,
                outcome.band_status,
                outcome.outcome_status
            FROM fact_selection_outcome AS outcome
            JOIN dim_symbol AS meta
              ON outcome.symbol = meta.symbol
            WHERE outcome.evaluation_date = ?
              AND outcome.horizon = ?
              AND outcome.ranking_version = ?
            ORDER BY outcome.realized_excess_return DESC, outcome.symbol
            LIMIT ?
            """,
            [evaluation_date, horizon, ranking_version, limit],
        ).fetchdf()


def market_pulse_frame(settings: Settings) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                regime.as_of_date,
                regime.regime_state,
                regime.regime_score,
                regime.breadth_up_ratio,
                regime.market_realized_vol_20d,
                flow.row_count AS investor_flow_rows,
                flow.foreign_positive_ratio,
                flow.institution_positive_ratio,
                selection.selection_rows,
                prediction.prediction_rows
            FROM (
                SELECT *
                FROM vw_market_regime_latest
                WHERE market_scope = 'KR_ALL'
            ) AS regime
            LEFT JOIN (
                SELECT
                    trading_date,
                    COUNT(*) AS row_count,
                    AVG(
                        CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS foreign_positive_ratio,
                    AVG(
                        CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END
                    ) AS institution_positive_ratio
                FROM fact_investor_flow
                WHERE trading_date = (SELECT MAX(trading_date) FROM fact_investor_flow)
                GROUP BY trading_date
            ) AS flow
              ON regime.as_of_date = flow.trading_date
            LEFT JOIN (
                SELECT as_of_date, COUNT(*) AS selection_rows
                FROM fact_ranking
                WHERE ranking_version = ?
                GROUP BY as_of_date
                QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
            ) AS selection
              ON regime.as_of_date = selection.as_of_date
            LEFT JOIN (
                SELECT as_of_date, COUNT(*) AS prediction_rows
                FROM fact_prediction
                WHERE prediction_version = ?
                GROUP BY as_of_date
                QUALIFY ROW_NUMBER() OVER (ORDER BY as_of_date DESC) = 1
            ) AS prediction
              ON regime.as_of_date = prediction.as_of_date
            """,
            [SELECTION_ENGINE_VERSION, PREDICTION_VERSION],
        ).fetchdf()


def latest_market_news_frame(settings: Settings, *, limit: int = 5) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, title, publisher, link
            FROM fact_news_item
            WHERE signal_date = (SELECT MAX(signal_date) FROM fact_news_item)
              AND COALESCE(is_market_wide, FALSE)
            ORDER BY published_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()


def available_symbols(settings: Settings, *, limit: int = 200) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT symbol
            FROM dim_symbol
            WHERE market IN ('KOSPI', 'KOSDAQ')
            ORDER BY symbol
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [str(row[0]).zfill(6) for row in rows]


def stock_workbench_summary_frame(settings: Settings, *, symbol: str) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
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
                selection_1.final_selection_value AS d1_selection_value,
                selection_1.grade AS d1_grade,
                selection_5.final_selection_value AS d5_selection_value,
                selection_5.grade AS d5_grade,
                prediction_5.expected_excess_return AS d5_expected_excess_return,
                prediction_5.lower_band AS d5_lower_band,
                prediction_5.upper_band AS d5_upper_band,
                outcome_1.realized_excess_return AS d1_realized_excess_return,
                outcome_1.band_status AS d1_band_status,
                outcome_5.realized_excess_return AS d5_realized_excess_return,
                outcome_5.band_status AS d5_band_status
            FROM vw_feature_matrix_latest AS feature
            JOIN dim_symbol AS symbol_meta
              ON feature.symbol = symbol_meta.symbol
            LEFT JOIN vw_ranking_latest AS selection_1
              ON feature.symbol = selection_1.symbol
             AND selection_1.horizon = 1
             AND selection_1.ranking_version = ?
            LEFT JOIN vw_ranking_latest AS selection_5
              ON feature.symbol = selection_5.symbol
             AND selection_5.horizon = 5
             AND selection_5.ranking_version = ?
            LEFT JOIN vw_prediction_latest AS prediction_5
              ON feature.symbol = prediction_5.symbol
             AND prediction_5.horizon = 5
             AND prediction_5.prediction_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_1
              ON feature.symbol = outcome_1.symbol
             AND outcome_1.horizon = 1
             AND outcome_1.ranking_version = ?
            LEFT JOIN vw_selection_outcome_latest AS outcome_5
              ON feature.symbol = outcome_5.symbol
             AND outcome_5.horizon = 5
             AND outcome_5.ranking_version = ?
            WHERE feature.symbol = ?
            """,
            [
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                PREDICTION_VERSION,
                SELECTION_ENGINE_VERSION,
                SELECTION_ENGINE_VERSION,
                symbol,
            ],
        ).fetchdf()


def stock_workbench_price_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 30,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT trading_date, open, high, low, close, volume, turnover_value
            FROM fact_daily_ohlcv
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_flow_frame(settings: Settings, *, symbol: str, limit: int = 30) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                trading_date,
                foreign_net_value,
                institution_net_value,
                individual_net_value,
                foreign_net_volume,
                institution_net_volume,
                individual_net_volume
            FROM fact_investor_flow
            WHERE symbol = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def stock_workbench_news_frame(settings: Settings, *, symbol: str, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT signal_date, published_at, title, publisher, query_bucket, link
            FROM fact_news_item
            WHERE symbol_candidates LIKE ?
            ORDER BY signal_date DESC, published_at DESC
            LIMIT ?
            """,
            [f"%{symbol}%", limit],
        ).fetchdf()


def stock_workbench_outcome_frame(
    settings: Settings,
    *,
    symbol: str,
    limit: int = 20,
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                selection_date,
                evaluation_date,
                horizon,
                ranking_version,
                final_selection_value,
                expected_excess_return_at_selection,
                realized_excess_return,
                band_status,
                outcome_status
            FROM fact_selection_outcome
            WHERE symbol = ?
            ORDER BY selection_date DESC, ranking_version, horizon
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()


def latest_discord_preview(settings: Settings) -> str | None:
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT output_artifacts_json
            FROM ops_run_manifest
            WHERE run_type = 'render_discord_eod_report'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None or not row[0]:
        return None
    artifacts = json.loads(row[0])
    preview_candidates = [Path(item) for item in artifacts if str(item).endswith(".md")]
    if not preview_candidates:
        return None
    preview_path = preview_candidates[-1]
    if not preview_path.exists():
        return None
    return preview_path.read_text(encoding="utf-8")


def latest_postmortem_preview(settings: Settings) -> str | None:
    if not settings.paths.duckdb_path.exists():
        return None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT output_artifacts_json
            FROM ops_run_manifest
            WHERE run_type = 'render_postmortem_report'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None or not row[0]:
        return None
    artifacts = json.loads(row[0])
    preview_candidates = [Path(item) for item in artifacts if str(item).endswith(".md")]
    if not preview_candidates:
        return None
    preview_path = preview_candidates[-1]
    if not preview_path.exists():
        return None
    return preview_path.read_text(encoding="utf-8")
