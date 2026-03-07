from __future__ import annotations

from datetime import date

from app.common.paths import project_root
from app.features.feature_store import FeatureStoreBuildResult
from app.pipelines.daily_ohlcv import DailyOhlcvSyncResult
from app.pipelines.fundamentals_snapshot import FundamentalsSnapshotSyncResult
from app.pipelines.news_metadata import NewsMetadataSyncResult
from app.ranking.explanatory_score import RankingMaterializationResult
from app.regime.snapshot import MarketRegimeBuildResult
from app.scheduler.jobs import run_daily_pipeline_job
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


def test_run_daily_pipeline_job_orchestrates_core_syncs(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "integration.duckdb"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"APP_DATA_DIR={data_dir.as_posix()}",
                f"APP_DUCKDB_PATH={duckdb_path.as_posix()}",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)
    bootstrap_storage(settings)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_trading_calendar (
                trading_date,
                is_trading_day,
                market_session_type,
                weekday,
                is_weekend,
                is_public_holiday,
                source,
                source_confidence,
                is_override,
                updated_at
            )
            VALUES (?, TRUE, 'regular', 4, FALSE, FALSE, 'test', 'high', FALSE, now())
            """,
            [date(2026, 3, 6)],
        )

    observed_dates: list[date] = []

    def fake_sync_daily_ohlcv(settings_arg, *, trading_date, **kwargs):
        observed_dates.append(trading_date)
        return DailyOhlcvSyncResult(
            run_id="ohlcv-run",
            trading_date=trading_date,
            requested_symbol_count=10,
            row_count=8,
            skipped_symbol_count=2,
            failed_symbol_count=0,
            artifact_paths=["raw/ohlcv.json", "curated/ohlcv.parquet"],
            notes="ok",
        )

    def fake_sync_fundamentals_snapshot(settings_arg, *, as_of_date, **kwargs):
        observed_dates.append(as_of_date)
        return FundamentalsSnapshotSyncResult(
            run_id="fund-run",
            as_of_date=as_of_date,
            requested_symbol_count=10,
            row_count=6,
            skipped_symbol_count=3,
            unmatched_corp_code_count=1,
            failed_symbol_count=0,
            artifact_paths=["raw/fund.json", "curated/fund.parquet"],
            notes="ok",
        )

    def fake_sync_news_metadata(settings_arg, *, signal_date, mode, **kwargs):
        observed_dates.append(signal_date)
        assert mode == "market_and_focus"
        return NewsMetadataSyncResult(
            run_id="news-run",
            signal_date=signal_date,
            query_count=4,
            row_count=9,
            deduped_row_count=7,
            unmatched_symbol_count=2,
            artifact_paths=["raw/news.json", "curated/news.parquet"],
            notes="ok",
        )

    def fake_build_feature_store(settings_arg, *, as_of_date, **kwargs):
        observed_dates.append(as_of_date)
        return FeatureStoreBuildResult(
            run_id="feature-run",
            as_of_date=as_of_date,
            symbol_count=10,
            feature_row_count=640,
            artifact_paths=["curated/features.parquet"],
            notes="ok",
            feature_version="feature_store_v1",
        )

    def fake_build_market_regime_snapshot(settings_arg, *, as_of_date, **kwargs):
        observed_dates.append(as_of_date)
        return MarketRegimeBuildResult(
            run_id="regime-run",
            as_of_date=as_of_date,
            row_count=3,
            artifact_paths=["curated/regime.parquet"],
            notes="ok",
            regime_version="market_regime_v1",
        )

    def fake_materialize_explanatory_ranking(settings_arg, *, as_of_date, horizons, **kwargs):
        observed_dates.append(as_of_date)
        assert horizons == [1, 5]
        return RankingMaterializationResult(
            run_id="ranking-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/ranking.parquet"],
            notes="ok",
            ranking_version="explanatory_ranking_v0",
        )

    monkeypatch.setattr("app.scheduler.jobs.sync_daily_ohlcv", fake_sync_daily_ohlcv)
    monkeypatch.setattr(
        "app.scheduler.jobs.sync_fundamentals_snapshot",
        fake_sync_fundamentals_snapshot,
    )
    monkeypatch.setattr("app.scheduler.jobs.sync_news_metadata", fake_sync_news_metadata)
    monkeypatch.setattr("app.scheduler.jobs.build_feature_store", fake_build_feature_store)
    monkeypatch.setattr(
        "app.scheduler.jobs.build_market_regime_snapshot",
        fake_build_market_regime_snapshot,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_explanatory_ranking",
        fake_materialize_explanatory_ranking,
    )

    result = run_daily_pipeline_job(settings)

    assert result.status == "success"
    assert observed_dates == [date(2026, 3, 6)] * 6
    assert "ohlcv_rows=8" in result.notes
    assert "fundamentals_rows=6" in result.notes
    assert "news_rows=7" in result.notes
    assert "feature_rows=640" in result.notes
    assert "regime_rows=3" in result.notes
    assert "ranking_rows=20" in result.notes

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        manifest_row = connection.execute(
            """
            SELECT run_type, status, notes, feature_version, ranking_version
            FROM ops_run_manifest
            WHERE run_type = 'daily_pipeline'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert manifest_row == (
            "daily_pipeline",
            "success",
            result.notes,
            "feature_store_v1",
            "explanatory_ranking_v0",
        )
