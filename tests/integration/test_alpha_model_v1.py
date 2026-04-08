from __future__ import annotations

from datetime import date
from pathlib import Path

from app.evaluation.outcomes import materialize_selection_outcomes
from app.ml.active import freeze_alpha_active_model
from app.ml.comparison import compare_selection_engines
from app.ml.constants import (
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PREDICTION_VERSION,
    SELECTION_ENGINE_VERSION,
)
from app.ml.diagnostics import render_model_diagnostic_report
from app.ml.inference import materialize_alpha_predictions_v1
from app.ml.training import (
    backfill_alpha_oof_predictions,
    build_model_training_dataset,
    train_alpha_model_v1,
)
from app.ml.validation import validate_alpha_model_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
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
    assert validation_result.row_count == 11
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
