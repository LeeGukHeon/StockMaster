from __future__ import annotations

from datetime import date

from app.evaluation.calibration_diagnostics import materialize_calibration_diagnostics
from app.evaluation.summary import materialize_prediction_evaluation
from app.evaluation.validation import validate_evaluation_pipeline
from app.ml.training import train_alpha_model_v1
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_validate_evaluation_pipeline_passes_on_consistent_data(tmp_path):
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

    materialize_prediction_evaluation(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[20, 60],
        limit_symbols=4,
    )
    materialize_calibration_diagnostics(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        bin_count=4,
        limit_symbols=4,
    )

    result = validate_evaluation_pipeline(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )

    assert result.row_count >= 5
    assert any(path.endswith(".json") for path in result.artifact_paths)
