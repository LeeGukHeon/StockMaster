from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from app.audit.checks import run_artifact_reference_checks, run_full_audit, run_latest_layer_checks
from app.audit.reporting import write_audit_artifacts, write_audit_docs
from app.portfolio.report import render_portfolio_report
from app.release.reporting import (
    render_daily_research_report,
    render_evaluation_report,
    render_intraday_summary_report,
    render_release_candidate_checklist,
)
from app.release.snapshot import (
    build_latest_app_snapshot,
    build_report_index,
    build_ui_freshness_snapshot,
)
from app.release.validation import validate_release_candidate
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_audit_framework_generates_docs_and_reports(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(
        settings,
        selection_dates=[date(2026, 3, 6)],
        limit_symbols=4,
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        render_daily_research_report(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
            job_run_id="test-audit-daily",
        )
        render_portfolio_report(
            settings,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
        )
        render_evaluation_report(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
            job_run_id="test-audit-evaluation",
        )
        render_intraday_summary_report(
            settings,
            connection=connection,
            session_date=date(2026, 3, 9),
            dry_run=True,
            job_run_id="test-audit-intraday",
        )
        render_release_candidate_checklist(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            dry_run=True,
            job_run_id="test-audit-checklist",
        )
        build_report_index(settings, connection=connection, job_run_id="test-audit-report-index")
        build_ui_freshness_snapshot(
            settings,
            connection=connection,
            job_run_id="test-audit-freshness",
        )
        build_latest_app_snapshot(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            job_run_id="test-audit-snapshot",
        )
        validate_release_candidate(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
        )

        full_suite = run_full_audit(
            settings,
            connection=connection,
            snapshot_ts=datetime(2026, 3, 8, 10, 0).astimezone(),
        )
        latest_suite = run_latest_layer_checks(
            settings,
            connection=connection,
            snapshot_ts=datetime(2026, 3, 8, 10, 0).astimezone(),
        )
        artifact_suite = run_artifact_reference_checks(settings, connection=connection)
        original_project_root = settings.paths.project_root
        settings.paths.project_root = tmp_path
        doc_paths = write_audit_docs(
            settings,
            audit_result=full_suite,
            generated_at=datetime(2026, 3, 8, 10, 0).astimezone(),
        )
        settings.paths.project_root = original_project_root
        artifact_paths = write_audit_artifacts(
            settings,
            audit_result=full_suite,
            generated_at=datetime(2026, 3, 8, 10, 0).astimezone(),
        )

        view_exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'fact_intraday_final_action'
            """
        ).fetchone()[0]

    assert view_exists == 1
    assert full_suite.fail_count == 0
    assert any(
        result.check_id == "latest:freshness:weekend_holiday_classification"
        and result.status == "PASS"
        for result in latest_suite.results
    )
    assert artifact_suite.fail_count == 0
    assert all(Path(path).exists() for path in doc_paths + artifact_paths)
    assert (tmp_path / "docs/AUDIT_T000_T013_STATUS.md").exists()
