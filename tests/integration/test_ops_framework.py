from __future__ import annotations

from datetime import date, datetime

from app.ops.bundles import run_ops_maintenance_bundle
from app.ops.health import check_pipeline_dependencies, materialize_health_snapshots
from app.ops.policy import freeze_active_ops_policy
from app.ops.report import render_ops_report
from app.ops.validation import validate_health_framework, validate_ops_framework
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_ops_framework_materializes_health_and_run_metadata(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    freeze_active_ops_policy(
        settings,
        as_of_at=datetime(2026, 3, 8, 9, 0, 0),
        promotion_type="MANUAL_FREEZE",
        note="integration test",
    )
    result = run_ops_maintenance_bundle(
        settings,
        as_of_date=date(2026, 3, 8),
        dry_run=True,
    )
    assert result.status in {"SUCCESS", "DEGRADED_SUCCESS", "SKIPPED"}
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        check_result = check_pipeline_dependencies(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            job_run_id="integration-check",
        )
        health_result = materialize_health_snapshots(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            job_run_id="integration-health",
        )
        report_result = render_ops_report(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            job_run_id="integration-report",
            dry_run=True,
        )
        health_validation = validate_health_framework(
            settings,
            connection=connection,
            job_run_id="integration-validate-health",
        )
        ops_validation = validate_ops_framework(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 8),
            job_run_id="integration-validate-ops",
        )
        job_run_count = connection.execute("SELECT COUNT(*) FROM fact_job_run").fetchone()[0]
        dependency_count = connection.execute(
            "SELECT COUNT(*) FROM fact_pipeline_dependency_state"
        ).fetchone()[0]
        health_count = connection.execute("SELECT COUNT(*) FROM fact_health_snapshot").fetchone()[0]
        disk_count = connection.execute(
            "SELECT COUNT(*) FROM fact_disk_watermark_event"
        ).fetchone()[0]
        cleanup_count = connection.execute(
            "SELECT COUNT(*) FROM fact_retention_cleanup_run"
        ).fetchone()[0]
        policy_count = connection.execute(
            "SELECT COUNT(*) FROM fact_active_ops_policy"
        ).fetchone()[0]
    assert check_result.row_count and check_result.row_count > 0
    assert health_result.row_count and health_result.row_count > 0
    assert len(report_result.artifact_paths) == 2
    assert health_validation.warning_count == 0
    assert ops_validation.warning_count == 0
    assert job_run_count > 0
    assert dependency_count > 0
    assert health_count > 0
    assert disk_count > 0
    assert cleanup_count > 0
    assert policy_count > 0
