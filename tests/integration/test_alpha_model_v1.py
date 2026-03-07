from __future__ import annotations

from datetime import date
from pathlib import Path

from app.ml.comparison import compare_selection_engines
from app.ml.constants import MODEL_VERSION, PREDICTION_VERSION, SELECTION_ENGINE_VERSION
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
    assert validation_result.row_count == 5
    assert comparison_result.row_count > 0
    assert diagnostic_result.artifact_paths
    assert all(Path(path).exists() for path in validation_result.artifact_paths)
    assert all(Path(path).exists() for path in diagnostic_result.artifact_paths)

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
