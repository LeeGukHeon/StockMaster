from __future__ import annotations

from datetime import date
from pathlib import Path

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
from app.release.validation import (
    validate_navigation_integrity,
    validate_page_contracts,
    validate_release_candidate,
    validate_report_artifacts,
)
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_release_candidate_snapshot_and_report_index_flow(tmp_path) -> None:
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
        daily_report = render_daily_research_report(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
            job_run_id="test-daily-report",
        )
        evaluation_report = render_evaluation_report(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
            job_run_id="test-evaluation-report",
        )
        intraday_report = render_intraday_summary_report(
            settings,
            connection=connection,
            session_date=date(2026, 3, 9),
            dry_run=True,
            job_run_id="test-intraday-report",
        )
        portfolio_report = render_portfolio_report(
            settings,
            as_of_date=date(2026, 3, 6),
            dry_run=True,
        )
        report_index_first = build_report_index(
            settings,
            connection=connection,
            job_run_id="test-report-index-first",
        )
        freshness_result = build_ui_freshness_snapshot(
            settings,
            connection=connection,
            job_run_id="test-freshness",
        )
        snapshot_result = build_latest_app_snapshot(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 6),
            job_run_id="test-snapshot",
        )
        page_result = validate_page_contracts(
            settings,
            connection=connection,
        )
        navigation_result = validate_navigation_integrity(
            settings,
            connection=connection,
        )
        checklist_report = render_release_candidate_checklist(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            dry_run=True,
            job_run_id="test-release-checklist",
        )
        report_index_second = build_report_index(
            settings,
            connection=connection,
            job_run_id="test-report-index-second",
        )
        report_validation = validate_report_artifacts(
            settings,
            connection=connection,
        )
        release_validation = validate_release_candidate(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
        )

        snapshot_count = connection.execute(
            "SELECT COUNT(*) FROM fact_latest_app_snapshot"
        ).fetchone()[0]
        report_index_count = connection.execute(
            "SELECT COUNT(*) FROM fact_latest_report_index"
        ).fetchone()[0]
        freshness_count = connection.execute(
            "SELECT COUNT(*) FROM fact_ui_data_freshness_snapshot"
        ).fetchone()[0]
        check_count = connection.execute(
            "SELECT COUNT(*) FROM fact_release_candidate_check"
        ).fetchone()[0]

    artifact_paths = (
        daily_report.artifact_paths
        + evaluation_report.artifact_paths
        + intraday_report.artifact_paths
        + portfolio_report.artifact_paths
        + checklist_report.artifact_paths
    )
    assert all(Path(path).exists() for path in artifact_paths)
    assert report_index_first.row_count >= 4
    assert report_index_second.row_count >= report_index_first.row_count
    assert freshness_result.row_count > 0
    assert snapshot_result.row_count == 1
    assert page_result.check_count > 0
    assert navigation_result.check_count > 0
    assert report_validation.check_count > 0
    assert release_validation.check_count >= (
        page_result.check_count + navigation_result.check_count + report_validation.check_count
    )
    assert int(snapshot_count) > 0
    assert int(report_index_count) > 0
    assert int(freshness_count) > 0
    assert int(check_count) > 0
