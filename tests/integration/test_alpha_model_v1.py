from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from app.evaluation.outcomes import materialize_selection_outcomes
from app.features.feature_store import build_feature_store, load_feature_matrix
from app.ml.active import freeze_alpha_active_model
from app.ml.comparison import compare_selection_engines
from app.ml.constants import (
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PREDICTION_VERSION,
    SELECTION_ENGINE_VERSION,
    get_alpha_model_spec,
)
from app.ml.dataset import _ensure_feature_snapshots
from app.ml.diagnostics import render_model_diagnostic_report
from app.ml.indicator_product import run_alpha_indicator_product_bundle
from app.ml.inference import (
    _apply_d1_lead_prediction_shape_control,
    _bucket_from_calibration,
    build_prediction_frame_from_training_run,
    materialize_alpha_predictions_v1,
    upsert_predictions,
)
from app.ml.registry import load_latest_training_run, load_model_artifact
from app.ml.training import (
    backfill_alpha_oof_predictions,
    build_model_training_dataset,
    load_training_dataset,
    train_alpha_candidate_models,
    train_alpha_model_v1,
)
from app.ml.validation import _validation_reference_runs_sql, validate_alpha_model_v1
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.engine_v2 import (
    _score_selection_engine_v2_frame,
    materialize_selection_engine_v2,
)
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _prepare_ticket006_data(tmp_path) -> object:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings, limit_symbols=4)
    return settings


def test_train_alpha_model_v1_persists_registry_and_metrics(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    dataset_result = build_model_training_dataset(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        limit_symbols=4,
    )
    training_result = train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )

    assert dataset_result.row_count > 0
    assert training_result.training_run_count == 2
    assert training_result.model_version == MODEL_VERSION

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        registry_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_training_run
            WHERE train_end_date = ?
              AND model_version = ?
            """,
            [date(2026, 3, 6), MODEL_VERSION],
        ).fetchone()[0]
        metric_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_metric_summary
            WHERE model_version = ?
              AND split_name = 'validation'
            """,
            [MODEL_VERSION],
        ).fetchone()[0]
        validation_prediction_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_member_prediction
            WHERE model_version = ?
              AND prediction_role = 'validation'
            """,
            [MODEL_VERSION],
        ).fetchone()[0]

    assert registry_count == 2
    assert metric_count >= 8
    assert validation_prediction_count > 0


    with duckdb_connection(settings.paths.duckdb_path) as connection:
        dataset_frame = load_training_dataset(
            connection,
            train_end_date=date(2026, 3, 6),
            horizons=[1, 5],
            limit_symbols=4,
        )

    assert dataset_frame["target_rank_h1"].dropna().between(0.0, 1.0, inclusive="both").all()
    assert dataset_frame["target_rank_h5"].dropna().between(0.0, 1.0, inclusive="both").all()
    assert dataset_frame["target_rank_h1"].notna().any()
    assert dataset_frame["target_rank_h5"].notna().any()
    assert dataset_frame["target_top5_h1"].dropna().isin([0.0, 1.0]).all()
    assert dataset_frame["target_top5_h5"].dropna().isin([0.0, 1.0]).all()
    assert dataset_frame["target_top5_h1"].notna().any()
    assert dataset_frame["target_top5_h5"].notna().any()
    assert dataset_frame["target_topbucket_h1"].dropna().between(0.0, 1.0, inclusive="both").all()
    assert set(dataset_frame["target_topbucket_h1"].dropna().unique()).issubset(
        {0.0, 0.25, 0.5, 1.0}
    )


def test_train_alpha_model_v1_excludes_labels_not_available_by_train_end_date(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        overlap_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_training_run
            WHERE model_version = ?
              AND train_end_date = ?
              AND COALESCE(validation_window_end, training_window_end) >= train_end_date
            """,
            [MODEL_VERSION, date(2026, 3, 6)],
        ).fetchone()[0]

    assert overlap_count == 0


def test_ensure_feature_snapshots_rebuilds_invalid_quality_features(tmp_path, monkeypatch):
    settings = _prepare_ticket006_data(tmp_path)
    rebuild_calls: list[tuple[date, bool]] = []

    from app.features.feature_store import build_feature_store as real_build_feature_store

    real_build_feature_store(
        settings,
        as_of_date=date(2026, 3, 6),
        limit_symbols=4,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            UPDATE fact_feature_snapshot
            SET feature_value = NULL
            WHERE as_of_date = ?
              AND feature_name IN (
                  'has_daily_ohlcv_flag',
                  'stale_price_flag',
                  'missing_key_feature_count'
              )
            """,
            [date(2026, 3, 6)],
        )

    def _wrapped_build_feature_store(settings_arg, *, as_of_date, **kwargs):
        rebuild_calls.append((as_of_date, bool(kwargs.get('force'))))
        return real_build_feature_store(settings_arg, as_of_date=as_of_date, **kwargs)

    monkeypatch.setattr('app.ml.dataset.build_feature_store', _wrapped_build_feature_store)

    rebuilt_dates = _ensure_feature_snapshots(
        settings,
        candidate_dates=[date(2026, 3, 6)],
        symbols=None,
        limit_symbols=4,
        market='ALL',
    )

    assert rebuilt_dates == [date(2026, 3, 6)]
    assert rebuild_calls == [(date(2026, 3, 6), True)]

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        feature_frame = load_feature_matrix(
            connection,
            as_of_date=date(2026, 3, 6),
            limit_symbols=4,
        )

    assert feature_frame['has_daily_ohlcv_flag'].notna().all()
    assert feature_frame['stale_price_flag'].notna().all()
    assert feature_frame['missing_key_feature_count'].notna().all()


def test_freeze_active_alpha_model_controls_inference_and_outcome_lineage(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 5),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    freeze_result = freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_suite",
        note="freeze prior run",
        horizons=[1, 5],
        train_end_date=date(2026, 3, 5),
    )
    prediction_result = materialize_alpha_predictions_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    materialize_selection_outcomes(
        settings,
        selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
        ranking_versions=[SELECTION_ENGINE_VERSION],
    )

    assert freeze_result.row_count == 2
    assert prediction_result.row_count == 8

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        expected_runs = dict(
            connection.execute(
                """
                SELECT horizon, training_run_id
                FROM fact_model_training_run
                WHERE train_end_date = ?
                  AND model_version = ?
                ORDER BY horizon
                """,
                [date(2026, 3, 5), MODEL_VERSION],
            ).fetchall()
        )
        prediction_lineage = connection.execute(
            """
            SELECT
                horizon,
                COUNT(DISTINCT training_run_id) AS training_run_count,
                MIN(training_run_id) AS training_run_id,
                COUNT(DISTINCT active_alpha_model_id) AS active_model_count,
                MIN(model_spec_id) AS model_spec_id
            FROM fact_prediction
            WHERE as_of_date = ?
              AND prediction_version = ?
            GROUP BY horizon
            ORDER BY horizon
            """,
            [date(2026, 3, 6), PREDICTION_VERSION],
        ).fetchall()
        outcome_lineage_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_selection_outcome
            WHERE selection_date = ?
              AND ranking_version = ?
              AND training_run_id_at_selection IS NOT NULL
              AND model_spec_id_at_selection IS NOT NULL
              AND active_alpha_model_id_at_selection IS NOT NULL
            """,
            [date(2026, 3, 6), SELECTION_ENGINE_VERSION],
        ).fetchone()[0]

    assert len(expected_runs) == 2
    for horizon, training_run_count, training_run_id, active_model_count, model_spec_id in (
        prediction_lineage
    ):
        assert int(training_run_count) == 1
        assert training_run_id == expected_runs[int(horizon)]
        assert int(active_model_count) == 1
        assert model_spec_id == MODEL_SPEC_ID
    assert int(outcome_lineage_count or 0) > 0


def test_materialize_alpha_predictions_and_selection_engine_v2(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )

    prediction_result = materialize_alpha_predictions_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    selection_result = materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    assert prediction_result.row_count == 8
    assert prediction_result.prediction_version == PREDICTION_VERSION
    assert selection_result.row_count == 8
    assert selection_result.ranking_version == SELECTION_ENGINE_VERSION

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        prediction_row = connection.execute(
            """
            SELECT
                COUNT(*),
                MAX(member_count),
                SUM(CASE WHEN disagreement_score IS NOT NULL THEN 1 ELSE 0 END)
            FROM fact_prediction
            WHERE as_of_date = ?
              AND prediction_version = ?
            """,
            [date(2026, 3, 6), PREDICTION_VERSION],
        ).fetchone()
        ranking_row = connection.execute(
            """
            SELECT explanatory_score_json
            FROM fact_ranking
            WHERE as_of_date = ?
              AND ranking_version = ?
            ORDER BY final_selection_value DESC, symbol
            LIMIT 1
            """,
            [date(2026, 3, 6), SELECTION_ENGINE_VERSION],
        ).fetchone()

    assert prediction_row[0] == 8
    assert int(prediction_row[1] or 0) >= 1
    assert int(prediction_row[2] or 0) >= 1
    assert '"alpha_core_score"' in ranking_row[0]


def test_d5_primary_selection_scoring_preserves_safe_raw_leaders(tmp_path):
    settings = build_test_settings(tmp_path)
    base = pd.DataFrame(
        {
            "symbol": list("ABCDEFG"),
            "eligible_flag": [True, True, True, True, True, True, True],
            "flow_score": [65.0, 64.0, 62.0, 61.0, 60.0, 59.0, 58.0],
            "trend_momentum_score": [55.0, 54.0, 70.0, 69.0, 68.0, 67.0, 66.0],
            "news_catalyst_score": [40.0, 40.0, 38.0, 38.0, 38.0, 38.0, 38.0],
            "quality_score": [72.0, 71.0, 65.0, 64.0, 63.0, 62.0, 61.0],
            "value_safety_score": [73.0, 72.0, 64.0, 63.0, 62.0, 61.0, 60.0],
            "regime_fit_score": [60.0, 60.0, 55.0, 55.0, 55.0, 55.0, 55.0],
            "risk_penalty_score": [10.0, 10.0, 18.0, 18.0, 18.0, 18.0, 18.0],
            "implementation_penalty_score": [6.0, 6.0, 8.0, 8.0, 8.0, 8.0, 8.0],
            "residual_ret_5d_rank_pct": [0.96, 0.93, 0.62, 0.58, 0.54, 0.50, 0.46],
            "residual_ret_10d_rank_pct": [0.95, 0.92, 0.60, 0.57, 0.53, 0.49, 0.45],
            "drawdown_20d_rank_pct": [0.80, 0.78, 0.62, 0.60, 0.58, 0.56, 0.54],
            "foreign_flow_persistence_5d_rank_pct": [0.86, 0.84, 0.44, 0.42, 0.40, 0.38, 0.36],
            "institution_flow_persistence_5d_rank_pct": [0.87, 0.85, 0.45, 0.43, 0.41, 0.39, 0.37],
            "flow_disagreement_score_rank_pct": [0.20, 0.22, 0.52, 0.54, 0.56, 0.58, 0.60],
            "news_drift_persistence_score_rank_pct": [0.82, 0.80, 0.36, 0.34, 0.32, 0.30, 0.28],
            "news_burst_share_1d_rank_pct": [0.25, 0.27, 0.88, 0.86, 0.84, 0.82, 0.80],
            "distinct_publishers_3d_rank_pct": [0.76, 0.74, 0.35, 0.34, 0.33, 0.32, 0.31],
            "ret_10d_rank_pct": [0.66, 0.64, 0.96, 0.94, 0.92, 0.90, 0.88],
            "dist_from_20d_high_rank_pct": [0.28, 0.30, 0.95, 0.93, 0.91, 0.89, 0.87],
            "turnover_burst_persistence_5d_rank_pct": [0.34, 0.36, 0.94, 0.92, 0.90, 0.88, 0.86],
            "uncertainty_proxy_score": [15.0, 15.0, 18.0, 18.0, 18.0, 18.0, 18.0],
            "market": ["KOSPI"] * 7,
        }
    )
    prediction_frame = pd.DataFrame(
        {
            "symbol": list("ABCDEFG"),
            "model_spec_id": ["alpha_swing_d5_v2"] * 7,
            "expected_excess_return": [0.17, 0.15, 0.11, 0.10, 0.09, 0.08, 0.07],
            "prediction_version": [PREDICTION_VERSION] * 7,
            "uncertainty_score": [12.0, 12.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [10.0, 10.0, 18.0, 18.0, 18.0, 18.0, 18.0],
            "fallback_flag": [False, False, False, False, False, False, False],
            "fallback_reason": [None, None, None, None, None, None, None],
        }
    )

    scored = _score_selection_engine_v2_frame(
        base,
        prediction_frame,
        horizon=5,
        settings=settings,
    )

    top_symbols = (
        scored.sort_values(["final_selection_value", "symbol"], ascending=[False, True])
        .head(5)["symbol"]
        .tolist()
    )
    row_a = scored.loc[scored["symbol"] == "A"].iloc[0]
    row_b = scored.loc[scored["symbol"] == "B"].iloc[0]
    payload_a = json.loads(str(row_a["explanatory_score_json"]))

    assert "A" in top_symbols
    assert "B" in top_symbols
    assert bool(row_a["raw_top5_candidate_flag"]) is True
    assert bool(row_a["raw_preservation_guardrail_applied"]) is True
    assert bool(row_b["raw_top5_candidate_flag"]) is True
    assert payload_a["raw_preservation_guardrail_applied"] is True
    assert payload_a["alpha_core_magnitude_component_score"] is not None


def test_materialize_alpha_predictions_handles_mixed_model_and_proxy_frames(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO fact_prediction (
                run_id,
                as_of_date,
                symbol,
                horizon,
                market,
                ranking_version,
                prediction_version,
                expected_excess_return,
                lower_band,
                median_band,
                upper_band,
                calibration_start_date,
                calibration_end_date,
                calibration_bucket,
                calibration_sample_size,
                model_version,
                training_run_id,
                model_spec_id,
                active_alpha_model_id,
                uncertainty_score,
                disagreement_score,
                fallback_flag,
                fallback_reason,
                member_count,
                ensemble_weight_json,
                source_notes_json,
                created_at
            )
            WITH selected_symbols AS (
                SELECT DISTINCT symbol
                FROM fact_feature_snapshot
                WHERE as_of_date = ?
                ORDER BY symbol
                LIMIT 4
            )
            SELECT
                'proxy-seed',
                ?,
                selected.symbol,
                5,
                symbol.market,
                'selection_engine_v1',
                'proxy_prediction_band_v1',
                0.001,
                -0.002,
                0.001,
                0.003,
                DATE '2026-03-02',
                DATE '2026-03-06',
                'decile_01',
                10,
                'selection_engine_v1_proxy',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                TRUE,
                'seeded_proxy_for_test',
                0,
                '{}',
                '{}',
                now()
            FROM selected_symbols AS selected
            JOIN dim_symbol AS symbol
              ON selected.symbol = symbol.symbol
            """,
            [date(2026, 3, 6), date(2026, 3, 6)],
        )
        proxy_seed_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_prediction
            WHERE as_of_date = ?
              AND horizon = 5
              AND prediction_version = 'proxy_prediction_band_v1'
              AND ranking_version = 'selection_engine_v1'
            """,
            [date(2026, 3, 6)],
        ).fetchone()[0]

    assert int(proxy_seed_count or 0) == 4

    prediction_result = materialize_alpha_predictions_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    assert prediction_result.row_count == 8
    assert prediction_result.artifact_paths
    assert all(Path(path).exists() for path in prediction_result.artifact_paths)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        fallback_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_prediction
            WHERE as_of_date = ?
              AND prediction_version = ?
              AND horizon = 5
              AND fallback_reason = 'use_proxy_prediction_band_v1'
            """,
            [date(2026, 3, 6), PREDICTION_VERSION],
        ).fetchone()[0]

    assert int(fallback_rows or 0) == 4


def test_backfill_validate_compare_and_render_diagnostic(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    backfill_result = backfill_alpha_oof_predictions(
        settings,
        start_train_end_date=date(2026, 3, 2),
        end_train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_models=3,
        limit_symbols=4,
    )
    materialize_alpha_predictions_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    validation_result = validate_alpha_model_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
    )
    comparison_result = compare_selection_engines(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )
    diagnostic_result = render_model_diagnostic_report(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        dry_run=True,
    )

    assert backfill_result.run_count == 3
    assert validation_result.row_count >= 16
    assert comparison_result.row_count > 0
    assert diagnostic_result.artifact_paths
    assert all(Path(path).exists() for path in validation_result.artifact_paths)
    assert all(Path(path).exists() for path in diagnostic_result.artifact_paths)
    validation_markdown = next(
        path for path in validation_result.artifact_paths if path.endswith(".md")
    )
    validation_text = Path(validation_markdown).read_text(encoding="utf-8")
    assert "top10_mean_excess_return_h1" in validation_text
    assert "top20_mean_excess_return_h5" in validation_text
    assert "rank_ic_h1" in validation_text
    assert "selection_gap_top5_drag_alpha_lead_d1_v1_h1_rolling_20" in validation_text
    assert "selection_gap_top5_drag_alpha_swing_d5_v1_h5_rolling_60" in validation_text
    assert "d1_concentration_roll20" in validation_text

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        comparison_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_selection_outcome
            WHERE selection_date BETWEEN ? AND ?
              AND ranking_version = ?
            """,
            [date(2026, 3, 2), date(2026, 3, 6), SELECTION_ENGINE_VERSION],
        ).fetchone()[0]
        latest_training_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_model_training_run
            WHERE model_version = ?
            """,
            [MODEL_VERSION],
        ).fetchone()[0]

    assert comparison_rows > 0
    assert latest_training_rows >= 2


def test_validate_alpha_model_prefers_active_model_lineage_over_newer_challengers(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 5),
        horizons=[1],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test_suite",
        note="freeze active default before challenger run",
        horizons=[1],
        train_end_date=date(2026, 3, 5),
    )
    train_alpha_candidate_models(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )

    reference_runs_sql = _validation_reference_runs_sql("1")
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_training_run_id = connection.execute(
            """
            SELECT training_run_id
            FROM fact_alpha_active_model
            WHERE horizon = 1
              AND effective_from_date <= ?
              AND (effective_to_date IS NULL OR effective_to_date >= ?)
              AND active_flag = TRUE
            ORDER BY effective_from_date DESC, created_at DESC
            LIMIT 1
            """,
            [date(2026, 3, 6), date(2026, 3, 6)],
        ).fetchone()[0]
        latest_challenger_training_run_id = connection.execute(
            """
            SELECT training_run_id
            FROM fact_model_training_run
            WHERE horizon = 1
              AND train_end_date <= ?
              AND model_spec_id <> ?
              AND status = 'success'
            ORDER BY train_end_date DESC, created_at DESC, training_run_id DESC
            LIMIT 1
            """,
            [date(2026, 3, 6), MODEL_SPEC_ID],
        ).fetchone()[0]
        reference_training_run_id = connection.execute(
            reference_runs_sql + " SELECT training_run_id FROM reference_runs WHERE horizon = 1",
            [
                MODEL_VERSION,
                date(2026, 3, 6),
                date(2026, 3, 6),
                MODEL_VERSION,
                MODEL_SPEC_ID,
                date(2026, 3, 6),
            ],
        ).fetchone()[0]

    assert reference_training_run_id == active_training_run_id
    assert reference_training_run_id != latest_challenger_training_run_id


def test_run_alpha_indicator_product_bundle_smoke(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 6),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
        freeze_horizons=[1, 5],
    )

    assert result.training_run_count == 2
    assert result.freeze_horizons == [1, 5]
    assert result.freeze_row_count == 2
    assert result.frozen_model_spec_ids == ["alpha_swing_d5_v1"]
    assert result.blocked_freeze_model_spec_ids == ["alpha_lead_d1_v1"]
    assert result.freeze_block_reasons["alpha_lead_d1_v1"][0].startswith(
        "insufficient_matured_shadow_dates="
    )
    assert result.missing_training_model_spec_ids == []
    assert result.active_model_spec_ids_by_horizon[1] == MODEL_SPEC_ID
    assert result.active_model_spec_ids_by_horizon[5] == "alpha_swing_d5_v1"
    assert result.prediction_row_count == 8
    assert result.ranking_row_count == 8
    assert result.shadow_prediction_row_count > 0
    assert result.shadow_ranking_row_count > 0
    assert result.gap_scorecard_row_count > 0
    assert result.validation_check_count >= 15

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_h1 = connection.execute(
            """
            SELECT model_spec_id
            FROM fact_alpha_active_model
            WHERE horizon = 1
              AND active_flag = TRUE
            ORDER BY effective_from_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        active_h5 = connection.execute(
            """
            SELECT model_spec_id
            FROM fact_alpha_active_model
            WHERE horizon = 5
              AND active_flag = TRUE
            ORDER BY effective_from_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        gap_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date = ?
              AND model_spec_id IN ('alpha_lead_d1_v1', 'alpha_swing_d5_v1')
            """,
            [date(2026, 3, 6)],
        ).fetchone()[0]

    assert active_h1[0] == MODEL_SPEC_ID
    assert active_h5[0] == "alpha_swing_d5_v1"
    assert int(gap_rows or 0) > 0


def test_run_alpha_indicator_product_bundle_preserves_h5_when_freeze_horizon_is_d1_only(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test-seed",
        horizons=[5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
        promotion_type="MANUAL_FREEZE",
    )

    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 6),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
        freeze_horizons=[1],
    )

    assert result.freeze_horizons == [1]
    assert result.frozen_model_spec_ids == []
    assert result.blocked_freeze_model_spec_ids == ["alpha_lead_d1_v1"]
    assert result.active_model_spec_ids_by_horizon[1] == MODEL_SPEC_ID
    assert result.active_model_spec_ids_by_horizon[5] == MODEL_SPEC_ID

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        active_h5 = connection.execute(
            """
            SELECT model_spec_id
            FROM fact_alpha_active_model
            WHERE horizon = 5
              AND active_flag = TRUE
            ORDER BY effective_from_date DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()

    assert active_h5[0] == MODEL_SPEC_ID


def test_run_alpha_indicator_product_bundle_defaults_to_d1_only_freeze_when_h1_is_present(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    freeze_alpha_active_model(
        settings,
        as_of_date=date(2026, 3, 6),
        source="test-seed",
        horizons=[5],
        model_spec_id=MODEL_SPEC_ID,
        train_end_date=date(2026, 3, 6),
        promotion_type="MANUAL_FREEZE",
    )

    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 6),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
    )

    assert result.freeze_horizons == [1]
    assert result.frozen_model_spec_ids == []
    assert result.blocked_freeze_model_spec_ids == ["alpha_lead_d1_v1"]
    assert result.active_model_spec_ids_by_horizon[1] == MODEL_SPEC_ID
    assert result.active_model_spec_ids_by_horizon[5] == MODEL_SPEC_ID


def test_materialize_alpha_predictions_v1_applies_d1_shape_control_only(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    build_feature_store(settings, as_of_date=date(2026, 3, 6), limit_symbols=4)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        feature_frame = load_feature_matrix(
            connection,
            as_of_date=date(2026, 3, 6),
            limit_symbols=4,
            market="ALL",
        )

    train_alpha_candidate_models(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        model_specs=(
            get_alpha_model_spec("alpha_lead_d1_v1"),
            get_alpha_model_spec("alpha_swing_d5_v1"),
        ),
    )
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        lead_training_run = load_latest_training_run(
            connection,
            horizon=1,
            model_version=MODEL_VERSION,
            train_end_date=date(2026, 3, 6),
            model_domain=MODEL_DOMAIN,
            model_spec_id="alpha_lead_d1_v1",
        )
        swing_training_run = load_latest_training_run(
            connection,
            horizon=5,
            model_version=MODEL_VERSION,
            train_end_date=date(2026, 3, 6),
            model_domain=MODEL_DOMAIN,
            model_spec_id="alpha_swing_d5_v1",
        )

    lead_result, _ = build_prediction_frame_from_training_run(
        run_id="test-lead-shape",
        as_of_date=date(2026, 3, 6),
        horizon=1,
        feature_frame=feature_frame,
        training_run=lead_training_run,
        training_run_source="test",
        active_alpha_model_id="test-active-lead",
        persist_member_predictions=False,
    )
    swing_result, _ = build_prediction_frame_from_training_run(
        run_id="test-swing-shape",
        as_of_date=date(2026, 3, 6),
        horizon=5,
        feature_frame=feature_frame,
        training_run=swing_training_run,
        training_run_source="test",
        active_alpha_model_id="test-active-swing",
        persist_member_predictions=False,
    )
    lead_artifact = load_model_artifact(Path(str(lead_training_run["artifact_uri"])))
    feature_columns = list(lead_artifact.get("feature_columns", []))
    inference_features = feature_frame.reindex(columns=feature_columns).apply(
        pd.to_numeric,
        errors="coerce",
    )
    member_predictions = {}
    for member_name in lead_artifact.get("member_order", []):
        model = lead_artifact["members"].get(member_name)
        if model is None:
            continue
        member_predictions[member_name] = pd.Series(
            model.predict(inference_features),
            index=feature_frame.index,
            dtype="float64",
        )
    ensemble_weights = {
        str(key): float(value)
        for key, value in lead_artifact.get("ensemble_weights", {}).items()
        if key in member_predictions
    }
    raw_ensemble = sum(
        member_predictions[member_name] * weight
        for member_name, weight in ensemble_weights.items()
    )
    expected_transformed = _apply_d1_lead_prediction_shape_control(raw_ensemble)
    calibration_rows = lead_artifact.get("calibration", [])
    assert not lead_result.empty
    assert not swing_result.empty
    assert lead_result["expected_excess_return"].abs().max() <= 0.05 + 1e-9
    pd.testing.assert_series_equal(
        lead_result["expected_excess_return"].reset_index(drop=True),
        expected_transformed.reset_index(drop=True),
        check_names=False,
        atol=1e-12,
        rtol=0.0,
    )
    assert lead_result["calibration_bucket"].nunique() >= 1
    expected_buckets = [
        _bucket_from_calibration(calibration_rows, float(value))["bucket"]
        for value in raw_ensemble
    ]
    assert lead_result["calibration_bucket"].tolist() == expected_buckets
    calibration_band_frame = pd.DataFrame(
        [
            _bucket_from_calibration(calibration_rows, float(value))
            for value in raw_ensemble
        ]
    )
    pd.testing.assert_series_equal(
        lead_result["median_band"].reset_index(drop=True),
        (
            expected_transformed.reset_index(drop=True)
            + pd.to_numeric(calibration_band_frame["residual_median"], errors="coerce")
            .fillna(0.0)
            .reset_index(drop=True)
        ),
        check_names=False,
        atol=1e-12,
        rtol=0.0,
    )
    assert swing_result["expected_excess_return"].abs().max() > 0.0


def test_run_alpha_indicator_product_bundle_backfills_shadow_history_across_range(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    legacy_h1_spec = get_alpha_model_spec("alpha_topbucket_h1_rolling_120_v1")

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
            market="ALL",
            model_specs=(legacy_h1_spec,),
        )

    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 4),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_lead_d1_v1", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
        freeze_horizons=[1],
        backfill_shadow_history=True,
    )

    assert result.shadow_backfill_enabled is True
    assert result.shadow_backfill_selection_date_count == 3
    assert result.shadow_backfill_processed_selection_date_count == 3
    assert result.shadow_backfill_skipped_selection_date_count == 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        lead_h1_dates = connection.execute(
            """
            SELECT COUNT(DISTINCT selection_date)
            FROM fact_alpha_shadow_prediction
            WHERE model_spec_id = 'alpha_lead_d1_v1'
              AND horizon = 1
              AND selection_date BETWEEN ? AND ?
            """,
            [date(2026, 3, 4), date(2026, 3, 6)],
        ).fetchone()[0]
        swing_h5_dates = connection.execute(
            """
            SELECT COUNT(DISTINCT selection_date)
            FROM fact_alpha_shadow_prediction
            WHERE model_spec_id = 'alpha_swing_d5_v1'
              AND horizon = 5
              AND selection_date BETWEEN ? AND ?
            """,
            [date(2026, 3, 4), date(2026, 3, 6)],
        ).fetchone()[0]
        gap_row = connection.execute(
            """
            SELECT matured_selection_date_count, insufficient_history_flag
            FROM fact_alpha_shadow_selection_gap_scorecard
            WHERE summary_date = ?
              AND model_spec_id = 'alpha_lead_d1_v1'
              AND horizon = 1
              AND window_name = 'cohort'
            """,
            [date(2026, 3, 6)],
        ).fetchone()
        comparator_rows = connection.execute(
            """
            SELECT model_spec_id, horizon
            FROM fact_alpha_shadow_evaluation_summary
            WHERE summary_date = ?
              AND segment_value = 'top5'
              AND model_spec_id IN (
                'alpha_lead_d1_v1',
                'alpha_recursive_expanding_v1',
                'alpha_topbucket_h1_rolling_120_v1'
              )
            GROUP BY model_spec_id, horizon
            ORDER BY model_spec_id, horizon
            """,
            [date(2026, 3, 6)],
        ).fetchall()

    assert int(lead_h1_dates or 0) == 3
    assert int(swing_h5_dates or 0) >= 2
    assert int(gap_row[0] or 0) == 3
    assert bool(gap_row[1]) is False
    assert comparator_rows == [
        ("alpha_lead_d1_v1", 1),
        ("alpha_recursive_expanding_v1", 1),
        ("alpha_topbucket_h1_rolling_120_v1", 1),
    ]


def test_run_alpha_indicator_product_bundle_d5_focus_enforces_comparator_lock(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    swing_v1_spec = get_alpha_model_spec("alpha_swing_d5_v1")
    swing_v2_spec = get_alpha_model_spec("alpha_swing_d5_v2")
    legacy_h1_spec = get_alpha_model_spec("alpha_topbucket_h1_rolling_120_v1")

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
            market="ALL",
            model_specs=(swing_v1_spec, swing_v2_spec, legacy_h1_spec),
        )

    result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 4),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_swing_d5_v2", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
        freeze_horizons=[5],
        backfill_shadow_history=True,
    )

    assert result.freeze_horizons == [5]
    assert result.frozen_model_spec_ids == ["alpha_swing_d5_v1"]
    assert "alpha_swing_d5_v2" in result.blocked_freeze_model_spec_ids
    assert result.freeze_block_reasons["alpha_swing_d5_v2"] == ["challenger_only_no_auto_freeze"]
    assert result.active_model_spec_ids_by_horizon[5] == "alpha_swing_d5_v1"

    allowed_result = run_alpha_indicator_product_bundle(
        settings,
        train_end_date=date(2026, 3, 6),
        as_of_date=date(2026, 3, 6),
        shadow_start_selection_date=date(2026, 3, 4),
        shadow_end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        model_spec_ids=["alpha_swing_d5_v2", "alpha_swing_d5_v1"],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
        market="ALL",
        rolling_windows=[20, 60],
        freeze_horizons=[5],
        backfill_shadow_history=True,
        allow_d5_active_freeze=True,
    )

    assert allowed_result.freeze_horizons == [5]
    assert allowed_result.frozen_model_spec_ids == ["alpha_swing_d5_v2"]
    assert "alpha_swing_d5_v1" in allowed_result.blocked_freeze_model_spec_ids
    assert allowed_result.freeze_block_reasons["alpha_swing_d5_v1"] == [
        "d5_focus_active_freeze_preferred"
    ]
    assert allowed_result.active_model_spec_ids_by_horizon[5] == "alpha_swing_d5_v2"

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        comparator_rows = connection.execute(
            """
            SELECT model_spec_id, horizon
            FROM fact_alpha_shadow_evaluation_summary
            WHERE summary_date = ?
              AND segment_value = 'top5'
              AND model_spec_id IN (
                'alpha_swing_d5_v2',
                'alpha_swing_d5_v1',
                'alpha_recursive_expanding_v1',
                'alpha_topbucket_h1_rolling_120_v1'
              )
            GROUP BY model_spec_id, horizon
            ORDER BY horizon, model_spec_id
            """,
            [date(2026, 3, 6)],
        ).fetchall()

    assert comparator_rows == [
        ("alpha_recursive_expanding_v1", 1),
        ("alpha_topbucket_h1_rolling_120_v1", 1),
        ("alpha_recursive_expanding_v1", 5),
        ("alpha_swing_d5_v1", 5),
        ("alpha_swing_d5_v2", 5),
    ]

    validation_markdown = max(
        (settings.paths.artifacts_dir / "model_validation").glob("*.md"),
        key=lambda path: path.stat().st_mtime,
    )
    validation_text = validation_markdown.read_text(encoding="utf-8")
    assert "d5_primary_top5_vs_swing_v1_cohort" in validation_text
    assert "d5_primary_top5_vs_swing_v1_rolling20" in validation_text
    assert "d5_primary_drag_improvement_cohort" in validation_text
    assert "d5_primary_selected_top5_floor_cohort" in validation_text
    assert "d5_bucket_continuation_vs_swing_v1" in validation_text
    assert "d5_bucket_win_count_vs_swing_v1" in validation_text


def test_validate_alpha_model_h5_only_omits_d1_concentration_checks(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)

    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )
    materialize_alpha_predictions_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[5],
        limit_symbols=4,
        market="ALL",
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[5],
        limit_symbols=4,
    )
    validation_result = validate_alpha_model_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[5],
    )

    validation_markdown = next(
        path for path in validation_result.artifact_paths if path.endswith(".md")
    )
    validation_text = Path(validation_markdown).read_text(encoding="utf-8")

    assert "d1_concentration_roll20" not in validation_text
    assert "d1_raw_top1_expected_return_share_roll20" not in validation_text


def test_selection_engine_v2_filters_to_active_model_predictions(tmp_path):
    settings = _prepare_ticket006_data(tmp_path)
    build_feature_store(settings, as_of_date=date(2026, 3, 6), limit_symbols=4)
    build_market_regime_snapshot(settings, as_of_date=date(2026, 3, 6))

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        symbols = (
            connection.execute(
                """
                SELECT DISTINCT symbol
                FROM fact_daily_ohlcv
                WHERE trading_date = ?
                ORDER BY symbol
                LIMIT 4
                """,
                [date(2026, 3, 6)],
            )
            .fetchdf()["symbol"]
            .astype(str)
            .tolist()
        )
        connection.execute(
            """
            INSERT INTO fact_alpha_active_model (
                active_alpha_model_id,
                horizon,
                model_spec_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                promotion_report_json,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_alpha_model_id,
                note,
                created_at,
                updated_at
            ) VALUES (
                'active-lead-h1', 1, 'alpha_lead_d1_v1', 'train-lead-h1', 'alpha_model_v1',
                'test', 'MANUAL_FREEZE', NULL, ?, NULL, TRUE, NULL, 'seed', now(), now()
            )
            """,
            [date(2026, 3, 6)],
        )
        rows = []
        for index, symbol in enumerate(symbols):
            if index < 2:
                model_spec_id = "alpha_lead_d1_v1"
                active_alpha_model_id = "active-lead-h1"
            else:
                model_spec_id = MODEL_SPEC_ID
                active_alpha_model_id = "legacy-default-h1"
            rows.append(
                {
                    "run_id": "seed-selection-v2",
                    "as_of_date": date(2026, 3, 6),
                    "symbol": symbol,
                    "horizon": 1,
                    "market": "KOSPI",
                    "ranking_version": SELECTION_ENGINE_VERSION,
                    "prediction_version": PREDICTION_VERSION,
                    "expected_excess_return": 0.01 + (0.01 * index),
                    "lower_band": -0.01,
                    "median_band": 0.0,
                    "upper_band": 0.02,
                    "calibration_start_date": date(2026, 3, 1),
                    "calibration_end_date": date(2026, 3, 5),
                    "calibration_bucket": "bucket_01",
                    "calibration_sample_size": 20,
                    "model_version": MODEL_VERSION,
                    "training_run_id": f"seed-train-{model_spec_id}",
                    "model_spec_id": model_spec_id,
                    "active_alpha_model_id": active_alpha_model_id,
                    "uncertainty_score": 10.0,
                    "disagreement_score": 5.0,
                    "fallback_flag": False,
                    "fallback_reason": None,
                    "member_count": 3,
                    "ensemble_weight_json": "{}",
                    "source_notes_json": "{}",
                    "created_at": pd.Timestamp.utcnow(),
                }
            )
        upsert_predictions(connection, pd.DataFrame(rows))

    selection_result = materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1],
        limit_symbols=4,
        ensure_predictions=False,
    )

    assert selection_result.row_count == 4

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        ranking_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_ranking
            WHERE as_of_date = ?
              AND horizon = 1
              AND ranking_version = ?
            """,
            [date(2026, 3, 6), SELECTION_ENGINE_VERSION],
        ).fetchone()[0]

    assert int(ranking_count or 0) == 4
