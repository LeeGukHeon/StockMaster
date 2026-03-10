from __future__ import annotations

from datetime import date

from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_outcomes,
)
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.training import train_alpha_candidate_models, train_alpha_model_v1
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def _prepare_shadow_settings(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings, limit_symbols=4)
    return settings


def test_materialize_alpha_shadow_candidates_and_self_backtest(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)

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
        )
        materialize_alpha_shadow_candidates(
            settings,
            as_of_date=train_end_date,
            horizons=[1, 5],
            limit_symbols=4,
        )

    outcome_result = materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )
    summary_result = materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[2],
    )

    assert outcome_result.row_count > 0
    assert outcome_result.matured_row_count > 0
    assert summary_result.row_count > 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT
                (SELECT COUNT(DISTINCT model_spec_id)
                 FROM fact_alpha_shadow_prediction
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_prediction
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_ranking
                 WHERE selection_date = ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_selection_outcome
                 WHERE selection_date BETWEEN ? AND ?),
                (SELECT COUNT(*)
                 FROM fact_alpha_shadow_evaluation_summary
                 WHERE summary_date = ?)
            """,
            [
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 6),
                date(2026, 3, 4),
                date(2026, 3, 6),
                date(2026, 3, 6),
            ],
        ).fetchone()

    assert int(row[0] or 0) == 3
    assert int(row[1] or 0) == 24
    assert int(row[2] or 0) == 24
    assert int(row[3] or 0) > 0
    assert int(row[4] or 0) > 0
