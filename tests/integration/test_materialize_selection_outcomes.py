from __future__ import annotations

from datetime import date

from app.evaluation.outcomes import materialize_selection_outcomes
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_materialize_selection_outcomes_freezes_snapshots(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings)

    result = materialize_selection_outcomes(
        settings,
        selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    assert result.row_count == 16
    assert result.matured_row_count == 16
    assert result.pending_row_count == 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT
                ranking_version,
                outcome_status,
                expected_excess_return_at_selection,
                realized_excess_return,
                band_status
            FROM fact_selection_outcome
            WHERE selection_date = ?
              AND symbol = '005930'
              AND horizon = 5
              AND ranking_version = 'selection_engine_v1'
            """,
            [date(2026, 3, 6)],
        ).fetchone()
        assert row[0] == "selection_engine_v1"
        assert row[1] == "matured"
        assert row[2] is not None
        assert row[3] is not None
        assert row[4] in {"in_band", "above_upper", "below_lower"}
