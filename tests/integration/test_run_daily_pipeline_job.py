from __future__ import annotations

from datetime import date

from app.common.paths import project_root
from app.features.feature_store import FeatureStoreBuildResult
from app.ml.inference import AlphaPredictionMaterializationResult
from app.ml.shadow import AlphaShadowMaterializationResult
from app.ml.training import AlphaTrainingResult
from app.pipelines.daily_ohlcv import DailyOhlcvSyncResult
from app.pipelines.fundamentals_snapshot import FundamentalsSnapshotSyncResult
from app.pipelines.investor_flow import InvestorFlowSyncResult
from app.pipelines.news_metadata import NewsMetadataSyncResult
from app.ranking.explanatory_score import RankingMaterializationResult
from app.regime.snapshot import MarketRegimeBuildResult
from app.reports.discord_eod import DiscordPublishResult
from app.scheduler.jobs import run_daily_pipeline_job
from app.selection.calibration import ProxyPredictionCalibrationResult
from app.selection.engine_v1 import SelectionEngineMaterializationResult
from app.selection.engine_v2 import SelectionEngineV2Result
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

    def fake_sync_investor_flow(settings_arg, *, trading_date, **kwargs):
        observed_dates.append(trading_date)
        return InvestorFlowSyncResult(
            run_id="flow-run",
            trading_date=trading_date,
            requested_symbol_count=10,
            row_count=8,
            skipped_symbol_count=1,
            failed_symbol_count=0,
            artifact_paths=["raw/flow.json", "curated/flow.parquet"],
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

    def fake_materialize_selection_engine_v1(settings_arg, *, as_of_date, horizons, **kwargs):
        observed_dates.append(as_of_date)
        assert horizons == [1, 5]
        return SelectionEngineMaterializationResult(
            run_id="selection-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/selection.parquet"],
            notes="ok",
            ranking_version="selection_engine_v1",
        )

    def fake_train_alpha_model_v1(
        settings_arg,
        *,
        train_end_date,
        horizons,
        min_train_days,
        validation_days,
        **kwargs,
    ):
        observed_dates.append(train_end_date)
        assert horizons == [1, 5]
        assert min_train_days == 120
        assert validation_days == 20
        return AlphaTrainingResult(
            run_id="alpha-train-run",
            train_end_date=train_end_date,
            row_count=200,
            training_run_count=2,
            artifact_paths=["artifacts/alpha_training.parquet"],
            notes="ok",
            model_version="alpha_model_v1",
        )

    def fake_materialize_alpha_predictions_v1(settings_arg, *, as_of_date, horizons, **kwargs):
        observed_dates.append(as_of_date)
        assert horizons == [1, 5]
        return AlphaPredictionMaterializationResult(
            run_id="alpha-pred-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/alpha_prediction.parquet"],
            notes="ok",
            prediction_version="alpha_prediction_v1",
        )

    def fake_train_alpha_candidate_models(
        settings_arg,
        *,
        train_end_date,
        horizons,
        min_train_days,
        validation_days,
        **kwargs,
    ):
        observed_dates.append(train_end_date)
        assert horizons == [1, 5]
        assert min_train_days == 120
        assert validation_days == 20
        return AlphaTrainingResult(
            run_id="alpha-candidate-train-run",
            train_end_date=train_end_date,
            row_count=200,
            training_run_count=4,
            artifact_paths=["artifacts/alpha_candidate_training.parquet"],
            notes="ok",
            model_version="alpha_model_v1",
        )

    def fake_materialize_alpha_shadow_candidates(
        settings_arg,
        *,
        as_of_date,
        horizons,
        **kwargs,
    ):
        observed_dates.append(as_of_date)
        assert horizons == [1, 5]
        return AlphaShadowMaterializationResult(
            run_id="alpha-shadow-run",
            as_of_date=as_of_date,
            prediction_row_count=60,
            ranking_row_count=60,
            artifact_paths=["curated/alpha_shadow.parquet"],
            notes="ok",
        )

    def fake_materialize_selection_engine_v2(settings_arg, *, as_of_date, horizons, **kwargs):
        observed_dates.append(as_of_date)
        assert horizons == [1, 5]
        return SelectionEngineV2Result(
            run_id="selection-v2-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/selection_v2.parquet"],
            notes="ok",
            ranking_version="selection_engine_v2",
        )

    def fake_calibrate_proxy_prediction_bands(settings_arg, *, start_date, end_date, horizons):
        observed_dates.append(end_date)
        assert start_date <= end_date
        assert horizons == [1, 5]
        return ProxyPredictionCalibrationResult(
            run_id="prediction-run",
            as_of_date=end_date,
            row_count=20,
            calibration_row_count=6,
            artifact_paths=["artifacts/prediction.parquet"],
            notes="ok",
            prediction_version="proxy_prediction_band_v1",
        )

    def fake_publish_discord_eod_report(settings_arg, *, as_of_date, dry_run):
        observed_dates.append(as_of_date)
        assert dry_run is True
        return DiscordPublishResult(
            run_id="discord-run",
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=False,
            artifact_paths=["artifacts/discord_preview.md"],
            notes="ok",
        )

    monkeypatch.setattr("app.scheduler.jobs.sync_daily_ohlcv", fake_sync_daily_ohlcv)
    monkeypatch.setattr(
        "app.scheduler.jobs.sync_fundamentals_snapshot",
        fake_sync_fundamentals_snapshot,
    )
    monkeypatch.setattr("app.scheduler.jobs.sync_news_metadata", fake_sync_news_metadata)
    monkeypatch.setattr("app.scheduler.jobs.sync_investor_flow", fake_sync_investor_flow)
    monkeypatch.setattr("app.scheduler.jobs.build_feature_store", fake_build_feature_store)
    monkeypatch.setattr(
        "app.scheduler.jobs.build_market_regime_snapshot",
        fake_build_market_regime_snapshot,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_explanatory_ranking",
        fake_materialize_explanatory_ranking,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_engine_v1",
        fake_materialize_selection_engine_v1,
    )
    monkeypatch.setattr("app.scheduler.jobs.train_alpha_model_v1", fake_train_alpha_model_v1)
    monkeypatch.setattr(
        "app.scheduler.jobs.train_alpha_candidate_models",
        fake_train_alpha_candidate_models,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_predictions_v1",
        fake_materialize_alpha_predictions_v1,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_candidates",
        fake_materialize_alpha_shadow_candidates,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_engine_v2",
        fake_materialize_selection_engine_v2,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.calibrate_proxy_prediction_bands",
        fake_calibrate_proxy_prediction_bands,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.publish_discord_eod_report",
        fake_publish_discord_eod_report,
    )

    result = run_daily_pipeline_job(settings)

    assert result.status == "success"
    assert observed_dates == [date(2026, 3, 6)] * 15
    assert "ohlcv_rows=8" in result.notes
    assert "fundamentals_rows=6" in result.notes
    assert "news_rows=7" in result.notes
    assert "flow_rows=8" in result.notes
    assert "feature_rows=640" in result.notes
    assert "regime_rows=3" in result.notes
    assert "ranking_rows=20" in result.notes
    assert "selection_rows=20" in result.notes
    assert "alpha_training_runs=2" in result.notes
    assert "alpha_candidate_training_runs=4" in result.notes
    assert "alpha_prediction_rows=20" in result.notes
    assert "alpha_shadow_prediction_rows=60" in result.notes
    assert "alpha_shadow_ranking_rows=60" in result.notes
    assert "selection_v2_rows=20" in result.notes
    assert "prediction_rows=20" in result.notes
    assert "discord_published=False" in result.notes

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        manifest_row = connection.execute(
            """
            SELECT run_type, status, notes, model_version, feature_version, ranking_version
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
            "alpha_model_v1",
            "feature_store_v1",
            "selection_engine_v2",
        )


def test_run_daily_pipeline_job_allows_empty_calibration_history(tmp_path, monkeypatch):
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

    def fake_sync_daily_ohlcv(settings_arg, *, trading_date, **kwargs):
        return DailyOhlcvSyncResult(
            run_id="ohlcv-run",
            trading_date=trading_date,
            requested_symbol_count=10,
            row_count=8,
            skipped_symbol_count=2,
            failed_symbol_count=0,
            artifact_paths=["raw/ohlcv.json"],
            notes="ok",
        )

    def fake_sync_fundamentals_snapshot(settings_arg, *, as_of_date, **kwargs):
        return FundamentalsSnapshotSyncResult(
            run_id="fund-run",
            as_of_date=as_of_date,
            requested_symbol_count=10,
            row_count=6,
            skipped_symbol_count=3,
            unmatched_corp_code_count=1,
            failed_symbol_count=0,
            artifact_paths=["raw/fund.json"],
            notes="ok",
        )

    def fake_sync_news_metadata(settings_arg, *, signal_date, mode, **kwargs):
        return NewsMetadataSyncResult(
            run_id="news-run",
            signal_date=signal_date,
            query_count=4,
            row_count=9,
            deduped_row_count=7,
            unmatched_symbol_count=2,
            artifact_paths=["raw/news.json"],
            notes="ok",
        )

    def fake_sync_investor_flow(settings_arg, *, trading_date, **kwargs):
        return InvestorFlowSyncResult(
            run_id="flow-run",
            trading_date=trading_date,
            requested_symbol_count=10,
            row_count=8,
            skipped_symbol_count=1,
            failed_symbol_count=0,
            artifact_paths=["raw/flow.json"],
            notes="ok",
        )

    def fake_build_feature_store(settings_arg, *, as_of_date, **kwargs):
        return FeatureStoreBuildResult(
            run_id="feature-run",
            as_of_date=as_of_date,
            symbol_count=10,
            feature_row_count=640,
            artifact_paths=["curated/features.parquet"],
            notes="ok",
            feature_version="feature_store_v2",
        )

    def fake_build_market_regime_snapshot(settings_arg, *, as_of_date, **kwargs):
        return MarketRegimeBuildResult(
            run_id="regime-run",
            as_of_date=as_of_date,
            row_count=3,
            artifact_paths=["curated/regime.parquet"],
            notes="ok",
            regime_version="market_regime_v1",
        )

    def fake_materialize_explanatory_ranking(settings_arg, *, as_of_date, horizons, **kwargs):
        return RankingMaterializationResult(
            run_id="ranking-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/ranking.parquet"],
            notes="ok",
            ranking_version="explanatory_ranking_v0",
        )

    def fake_materialize_selection_engine_v1(settings_arg, *, as_of_date, horizons, **kwargs):
        return SelectionEngineMaterializationResult(
            run_id="selection-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/selection.parquet"],
            notes="ok",
            ranking_version="selection_engine_v1",
        )

    def fake_train_alpha_model_v1(
        settings_arg,
        *,
        train_end_date,
        horizons,
        min_train_days,
        validation_days,
        **kwargs,
    ):
        assert horizons == [1, 5]
        assert min_train_days == 120
        assert validation_days == 20
        return AlphaTrainingResult(
            run_id="alpha-train-run",
            train_end_date=train_end_date,
            row_count=200,
            training_run_count=2,
            artifact_paths=["artifacts/alpha_training.parquet"],
            notes="ok",
            model_version="alpha_model_v1",
        )

    def fake_materialize_alpha_predictions_v1(settings_arg, *, as_of_date, horizons, **kwargs):
        assert horizons == [1, 5]
        return AlphaPredictionMaterializationResult(
            run_id="alpha-pred-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/alpha_prediction.parquet"],
            notes="ok",
            prediction_version="alpha_prediction_v1",
        )

    def fake_train_alpha_candidate_models(
        settings_arg,
        *,
        train_end_date,
        horizons,
        min_train_days,
        validation_days,
        **kwargs,
    ):
        assert horizons == [1, 5]
        assert min_train_days == 120
        assert validation_days == 20
        return AlphaTrainingResult(
            run_id="alpha-candidate-train-run",
            train_end_date=train_end_date,
            row_count=200,
            training_run_count=4,
            artifact_paths=["artifacts/alpha_candidate_training.parquet"],
            notes="ok",
            model_version="alpha_model_v1",
        )

    def fake_materialize_alpha_shadow_candidates(
        settings_arg,
        *,
        as_of_date,
        horizons,
        **kwargs,
    ):
        assert horizons == [1, 5]
        return AlphaShadowMaterializationResult(
            run_id="alpha-shadow-run",
            as_of_date=as_of_date,
            prediction_row_count=60,
            ranking_row_count=60,
            artifact_paths=["curated/alpha_shadow.parquet"],
            notes="ok",
        )

    def fake_materialize_selection_engine_v2(settings_arg, *, as_of_date, horizons, **kwargs):
        assert horizons == [1, 5]
        return SelectionEngineV2Result(
            run_id="selection-v2-run",
            as_of_date=as_of_date,
            row_count=20,
            artifact_paths=["curated/selection_v2.parquet"],
            notes="ok",
            ranking_version="selection_engine_v2",
        )

    def fake_calibrate_proxy_prediction_bands(settings_arg, *, start_date, end_date, horizons):
        raise RuntimeError(
            "No overlapping selection-engine rows and forward labels were available "
            "for proxy calibration."
        )

    def fake_publish_discord_eod_report(settings_arg, *, as_of_date, dry_run):
        return DiscordPublishResult(
            run_id="discord-run",
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=False,
            artifact_paths=["artifacts/discord_preview.md"],
            notes="skipped",
        )

    monkeypatch.setattr("app.scheduler.jobs.sync_daily_ohlcv", fake_sync_daily_ohlcv)
    monkeypatch.setattr(
        "app.scheduler.jobs.sync_fundamentals_snapshot",
        fake_sync_fundamentals_snapshot,
    )
    monkeypatch.setattr("app.scheduler.jobs.sync_news_metadata", fake_sync_news_metadata)
    monkeypatch.setattr("app.scheduler.jobs.sync_investor_flow", fake_sync_investor_flow)
    monkeypatch.setattr("app.scheduler.jobs.build_feature_store", fake_build_feature_store)
    monkeypatch.setattr(
        "app.scheduler.jobs.build_market_regime_snapshot",
        fake_build_market_regime_snapshot,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_explanatory_ranking",
        fake_materialize_explanatory_ranking,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_engine_v1",
        fake_materialize_selection_engine_v1,
    )
    monkeypatch.setattr("app.scheduler.jobs.train_alpha_model_v1", fake_train_alpha_model_v1)
    monkeypatch.setattr(
        "app.scheduler.jobs.train_alpha_candidate_models",
        fake_train_alpha_candidate_models,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_predictions_v1",
        fake_materialize_alpha_predictions_v1,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_candidates",
        fake_materialize_alpha_shadow_candidates,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_engine_v2",
        fake_materialize_selection_engine_v2,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.calibrate_proxy_prediction_bands",
        fake_calibrate_proxy_prediction_bands,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.publish_discord_eod_report",
        fake_publish_discord_eod_report,
    )

    result = run_daily_pipeline_job(settings)

    assert result.status == "success"
    assert "alpha_training_runs=2" in result.notes
    assert "alpha_candidate_training_runs=4" in result.notes
    assert "alpha_prediction_rows=20" in result.notes
    assert "alpha_shadow_prediction_rows=60" in result.notes
    assert "alpha_shadow_ranking_rows=60" in result.notes
    assert "selection_v2_rows=20" in result.notes
    assert "prediction_rows=0" in result.notes
    assert "calibration_skipped=" in result.notes
