from __future__ import annotations

from datetime import date

from app.scheduler.jobs import run_evaluation_job
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_run_evaluation_job_materializes_ticket005_outputs(tmp_path):
    settings = build_test_settings(tmp_path)
    settings.discord.enabled = False
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(
        settings,
        selection_dates=[
            date(2026, 3, 2),
            date(2026, 3, 3),
            date(2026, 3, 4),
            date(2026, 3, 5),
            date(2026, 3, 6),
        ],
    )

    result = run_evaluation_job(settings)

    assert result.status == "success"

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM fact_selection_outcome),
                (SELECT COUNT(*) FROM fact_evaluation_summary),
                (SELECT COUNT(*) FROM fact_calibration_diagnostic)
            """
        ).fetchone()
        assert row[0] > 0
        assert row[1] > 0
        assert row[2] > 0
