from __future__ import annotations

from datetime import date

from app.evaluation.alpha_shadow import (
    AlphaShadowEvaluationSummaryResult,
    AlphaShadowSelectionOutcomeResult,
)
from app.evaluation.calibration_diagnostics import CalibrationDiagnosticResult
from app.evaluation.summary import PredictionEvaluationResult
from app.scheduler.jobs import run_evaluation_job
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)
from app.evaluation.outcomes import SelectionOutcomeMaterializationResult


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


def test_run_evaluation_job_reuses_outcomes_for_summary_and_diagnostics(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)

    summary_flags: list[bool] = []
    diagnostic_flags: list[bool] = []
    shadow_summary_flags: list[bool] = []

    monkeypatch.setattr(
        "app.scheduler.jobs._count_same_day_ohlcv_rows",
        lambda *_args, **_kwargs: 10,
    )

    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_outcomes",
        lambda *args, **kwargs: SelectionOutcomeMaterializationResult(
            run_id="outcomes-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=10,
            matured_row_count=8,
            pending_row_count=2,
            artifact_paths=[],
            notes="ok",
        ),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_selection_outcomes",
        lambda *args, **kwargs: AlphaShadowSelectionOutcomeResult(
            run_id="shadow-outcomes-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=10,
            matured_row_count=8,
            pending_row_count=2,
            artifact_paths=[],
            notes="ok",
        ),
    )

    def _fake_summary(*args, **kwargs):
        summary_flags.append(bool(kwargs.get("ensure_selection_outcomes", True)))
        return PredictionEvaluationResult(
            run_id="summary-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=12,
            artifact_paths=[],
            notes="ok",
        )

    def _fake_shadow_summary(*args, **kwargs):
        shadow_summary_flags.append(bool(kwargs.get("ensure_shadow_selection_outcomes", True)))
        return AlphaShadowEvaluationSummaryResult(
            run_id="shadow-summary-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=12,
            artifact_paths=[],
            notes="ok",
        )

    def _fake_diagnostics(*args, **kwargs):
        diagnostic_flags.append(bool(kwargs.get("ensure_selection_outcomes", True)))
        return CalibrationDiagnosticResult(
            run_id="diagnostics-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=8,
            artifact_paths=[],
            notes="ok",
        )

    monkeypatch.setattr("app.scheduler.jobs.materialize_prediction_evaluation", _fake_summary)
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_evaluation_summary",
        _fake_shadow_summary,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_calibration_diagnostics",
        _fake_diagnostics,
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.publish_discord_postmortem_report",
        lambda *args, **kwargs: type(
            "PostmortemResult",
            (),
            {"run_id": "postmortem-run", "published": False, "artifact_paths": [], "notes": "ok"},
        )(),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.validate_evaluation_pipeline",
        lambda *args, **kwargs: type(
            "ValidationResult",
            (),
            {"run_id": "validation-run", "row_count": 5, "artifact_paths": [], "notes": "ok"},
        )(),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs._resolve_latest_matured_evaluation_date",
        lambda *args, **kwargs: date(2026, 3, 6),
    )

    result = run_evaluation_job(settings, selection_end_date=date(2026, 3, 6))

    assert result.status == "success"
    assert summary_flags == [False]
    assert diagnostic_flags == [False]
    assert shadow_summary_flags == [False]


def test_run_evaluation_job_syncs_same_day_ohlcv_before_postmortem(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)
    sync_calls: list[date] = []
    publish_dates: list[date] = []
    count_results = iter([0, 25])

    monkeypatch.setattr(
        "app.scheduler.jobs._count_same_day_ohlcv_rows",
        lambda *_args, **_kwargs: next(count_results),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.sync_daily_ohlcv",
        lambda *_args, **kwargs: sync_calls.append(kwargs["trading_date"]) or type(
            "DailyOhlcvResult",
            (),
            {"run_id": "ohlcv-run", "row_count": 25, "artifact_paths": [], "notes": "ok"},
        )(),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_selection_outcomes",
        lambda *args, **kwargs: SelectionOutcomeMaterializationResult(
            run_id="outcomes-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=10,
            matured_row_count=10,
            pending_row_count=0,
            artifact_paths=[],
            notes="ok",
        ),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_selection_outcomes",
        lambda *args, **kwargs: AlphaShadowSelectionOutcomeResult(
            run_id="shadow-outcomes-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=10,
            matured_row_count=10,
            pending_row_count=0,
            artifact_paths=[],
            notes="ok",
        ),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_prediction_evaluation",
        lambda *args, **kwargs: PredictionEvaluationResult(
            run_id="summary-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=12,
            artifact_paths=[],
            notes="ok",
        ),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_alpha_shadow_evaluation_summary",
        lambda *args, **kwargs: AlphaShadowEvaluationSummaryResult(
            run_id="shadow-summary-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=12,
            artifact_paths=[],
            notes="ok",
        ),
    )
    monkeypatch.setattr(
        "app.scheduler.jobs.materialize_calibration_diagnostics",
        lambda *args, **kwargs: CalibrationDiagnosticResult(
            run_id="diagnostics-run",
            start_selection_date=kwargs["start_selection_date"],
            end_selection_date=kwargs["end_selection_date"],
            row_count=8,
            artifact_paths=[],
            notes="ok",
        ),
    )

    def _fake_publish(*_args, **kwargs):
        publish_dates.append(kwargs["evaluation_date"])
        return type(
            "PostmortemResult",
            (),
            {"run_id": "postmortem-run", "published": False, "artifact_paths": [], "notes": "ok"},
        )()

    monkeypatch.setattr("app.scheduler.jobs.publish_discord_postmortem_report", _fake_publish)
    monkeypatch.setattr(
        "app.scheduler.jobs.validate_evaluation_pipeline",
        lambda *args, **kwargs: type(
            "ValidationResult",
            (),
            {"run_id": "validation-run", "row_count": 5, "artifact_paths": [], "notes": "ok"},
        )(),
    )

    def _fake_latest_matured(*_args, **kwargs):
        assert sync_calls == [date(2026, 3, 6)]
        return date(2026, 3, 6)

    monkeypatch.setattr(
        "app.scheduler.jobs._resolve_latest_matured_evaluation_date",
        _fake_latest_matured,
    )

    result = run_evaluation_job(settings, selection_end_date=date(2026, 3, 6))

    assert result.status == "success"
    assert sync_calls == [date(2026, 3, 6)]
    assert publish_dates == [date(2026, 3, 6)]
    assert "same_day_ohlcv_rows=25" in result.notes
