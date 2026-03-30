from __future__ import annotations

from datetime import date

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ml.training import train_alpha_model_v1
from app.evaluation.summary import materialize_prediction_evaluation
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_materialize_prediction_evaluation_builds_cohort_and_rolling_rows(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings)
    train_alpha_model_v1(
        settings,
        train_end_date=date(2026, 3, 6),
        horizons=[1, 5],
        min_train_days=5,
        validation_days=2,
        limit_symbols=4,
    )

    result = materialize_prediction_evaluation(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[20, 60],
        limit_symbols=4,
    )

    assert result.row_count > 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        counts = connection.execute(
            """
            SELECT window_type, COUNT(*)
            FROM fact_evaluation_summary
            GROUP BY window_type
            ORDER BY window_type
            """
        ).fetchall()
        v2_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_evaluation_summary
            WHERE ranking_version = ?
            """,
            [SELECTION_ENGINE_V2_VERSION],
        ).fetchone()[0]
        assert dict(counts)["cohort"] > 0
        assert dict(counts)["rolling_20d"] > 0
        assert dict(counts)["rolling_60d"] > 0
        assert int(v2_count or 0) > 0
