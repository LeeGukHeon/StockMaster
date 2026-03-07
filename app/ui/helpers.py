from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd

from app.common.disk import DiskUsageReport, measure_disk_usage
from app.providers.base import ProviderHealth
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.client import KrxProvider
from app.providers.naver_news.client import NaverNewsProvider
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
                'build_feature_store',
                'build_forward_labels',
                'build_market_regime_snapshot',
                'materialize_explanatory_ranking',
                'validate_explanatory_ranking'
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
                (SELECT MAX(as_of_date) FROM fact_feature_snapshot) AS latest_feature_date,
                (SELECT COUNT(*) FROM fact_feature_snapshot WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_feature_snapshot
                )) AS latest_feature_rows,
                (SELECT MAX(as_of_date) FROM fact_forward_return_label) AS latest_label_date,
                (SELECT COUNT(*) FROM fact_forward_return_label WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_forward_return_label
                ) AND label_available_flag) AS latest_available_label_rows,
                (SELECT MAX(as_of_date) FROM fact_market_regime_snapshot) AS latest_regime_date,
                (SELECT MAX(as_of_date) FROM fact_ranking) AS latest_ranking_date,
                (SELECT COUNT(*) FROM fact_ranking WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM fact_ranking
                )) AS latest_ranking_rows
            """
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
                ) AS latest_ranking_version
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


def available_ranking_dates(settings: Settings) -> list[str]:
    if not settings.paths.duckdb_path.exists():
        return []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT as_of_date
            FROM fact_ranking
            ORDER BY as_of_date DESC
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
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        selected_date = as_of_date or connection.execute(
            "SELECT MAX(as_of_date) FROM fact_ranking"
        ).fetchone()[0]
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
                ranking.top_reason_tags_json,
                ranking.risk_flags_json,
                ranking.explanatory_score_json
            FROM fact_ranking AS ranking
            JOIN dim_symbol AS symbol
              ON ranking.symbol = symbol.symbol
            WHERE ranking.as_of_date = ?
              AND ranking.horizon = ?
            ORDER BY ranking.final_selection_value DESC, ranking.symbol
            """,
            [selected_date, horizon],
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
) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        selected_date = as_of_date or connection.execute(
            "SELECT MAX(as_of_date) FROM fact_ranking"
        ).fetchone()[0]
        if selected_date is None:
            return pd.DataFrame()
        return connection.execute(
            """
            SELECT grade, COUNT(*) AS row_count
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = ?
            GROUP BY grade
            ORDER BY grade
            """,
            [selected_date, horizon],
        ).fetchdf()
