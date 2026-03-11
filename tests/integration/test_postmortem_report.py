from __future__ import annotations

from datetime import date

from app.evaluation.calibration_diagnostics import materialize_calibration_diagnostics
from app.evaluation.summary import materialize_prediction_evaluation
from app.reports.postmortem import (
    publish_discord_postmortem_report,
    render_postmortem_report,
)
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_postmortem_report_render_and_publish_dry_run(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings)

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

    render_result = render_postmortem_report(
        settings,
        evaluation_date=date(2026, 3, 13),
        horizons=[1, 5],
        dry_run=True,
    )
    publish_result = publish_discord_postmortem_report(
        settings,
        evaluation_date=date(2026, 3, 13),
        horizons=[1, 5],
        dry_run=True,
    )

    assert any(path.endswith(".md") for path in render_result.artifact_paths)
    assert "StockMaster 사후 점검" in render_result.payload["content"]
    assert publish_result.published is False
