from __future__ import annotations

import duckdb

from app.intraday import research_mode as research_mode_module
from app.intraday.meta_dataset import build_intraday_meta_training_dataset
from app.intraday.meta_inference import (
    materialize_intraday_final_actions,
    materialize_intraday_meta_predictions,
)
from app.intraday.meta_report import render_intraday_meta_model_report
from app.intraday.meta_training import (
    calibrate_intraday_meta_thresholds,
    freeze_intraday_active_meta_model,
    train_intraday_meta_models,
)
from app.intraday.research_mode import (
    CAPABILITY_SPECS,
    materialize_intraday_research_capability,
    validate_intraday_research_mode,
)
from app.release.reporting import render_intraday_summary_report
from app.release.snapshot import build_report_index
from app.storage.duckdb import duckdb_connection
from tests.integration.test_intraday_meta_model_framework import _prepare_ticket010_data


def test_intraday_research_mode_end_to_end(tmp_path):
    settings, session_dates = _prepare_ticket010_data(tmp_path)
    settings.app.env = "server"
    settings.intraday_research.enabled = True
    settings.intraday_research.assist_enabled = True
    settings.intraday_research.postmortem_enabled = True
    settings.intraday_research.policy_adjustment_enabled = True
    settings.intraday_research.meta_model_enabled = True
    settings.intraday_research.research_reports_enabled = True

    build_intraday_meta_training_dataset(
        settings,
        start_session_date=min(session_dates),
        end_session_date=max(session_dates),
        horizons=[1, 5],
    )
    train_intraday_meta_models(
        settings,
        train_end_date=max(session_dates),
        horizons=[1, 5],
        start_session_date=min(session_dates),
        validation_sessions=1,
    )
    calibrate_intraday_meta_thresholds(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
    )
    freeze_intraday_active_meta_model(
        settings,
        as_of_date=max(session_dates),
        source="latest_training",
        note="freeze for research mode test",
        horizons=[1, 5],
    )
    materialize_intraday_meta_predictions(
        settings,
        session_date=max(session_dates),
        horizons=[1, 5],
    )
    materialize_intraday_final_actions(
        settings,
        session_date=max(session_dates),
        horizons=[1, 5],
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        render_intraday_summary_report(
            settings,
            connection=connection,
            session_date=max(session_dates),
            dry_run=False,
        )
    render_intraday_meta_model_report(
        settings,
        as_of_date=max(session_dates),
        horizons=[1, 5],
        dry_run=False,
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        build_report_index(
            settings,
            connection=connection,
            job_run_id="test-build-report-index",
        )

    capability_result = materialize_intraday_research_capability(
        settings,
        as_of_date=max(session_dates),
    )
    validation_result = validate_intraday_research_mode(
        settings,
        as_of_date=max(session_dates),
    )

    assert capability_result.row_count == len(CAPABILITY_SPECS)
    assert validation_result.check_count == 7

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        capability_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_research_capability
            WHERE as_of_date = ?
            """,
            [max(session_dates)],
        ).fetchone()[0]
        lineage_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM vw_intraday_decision_lineage
            WHERE session_date = ?
            """,
            [max(session_dates)],
        ).fetchone()[0]
        report_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_latest_report_index
            WHERE report_type IN ('intraday_summary_report', 'intraday_meta_model_report')
            """
        ).fetchone()[0]
        lineage_projection = connection.execute(
            """
            SELECT
                company_name,
                market,
                raw_action,
                predicted_class_probability,
                confidence_margin,
                uncertainty_score,
                disagreement_score,
                market_regime_state,
                final_selection_value
            FROM vw_intraday_decision_lineage
            WHERE session_date = ?
            LIMIT 1
            """,
            [max(session_dates)],
        ).fetchone()

    assert int(capability_count) == len(CAPABILITY_SPECS)
    assert int(lineage_count) > 0
    assert int(report_count) >= 2
    assert lineage_projection is not None


def test_intraday_research_mode_validation_falls_back_to_read_only(monkeypatch, tmp_path):
    settings, session_dates = _prepare_ticket010_data(tmp_path)
    settings.app.env = "server"
    settings.intraday_research.enabled = True
    materialize_intraday_research_capability(settings, as_of_date=max(session_dates))

    real_duckdb_connection = research_mode_module.duckdb_connection
    state = {"raised": False}

    def flaky_duckdb_connection(db_path, read_only: bool = False):
        if not read_only and not state["raised"]:
            state["raised"] = True
            raise duckdb.IOException("IO Error: Could not set lock on file test.duckdb")
        return real_duckdb_connection(db_path, read_only=read_only)

    monkeypatch.setattr(research_mode_module, "duckdb_connection", flaky_duckdb_connection)

    result = validate_intraday_research_mode(settings, as_of_date=max(session_dates))

    assert state["raised"] is True
    assert result.check_count == 7
    assert "read-only fallback" in result.notes
