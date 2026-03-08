from __future__ import annotations

from datetime import date

import pytest

from app.common.disk import DiskUsageReport, DiskWatermark
from app.ops.common import JobBlockedError
from app.ops.runtime import JobRunContext
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def _raise_runtime_error() -> None:
    raise RuntimeError("optional-step-boom")


def test_optional_step_failure_records_failed_step_and_degraded_job(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with JobRunContext(
            settings,
            connection,
            job_name="ops_runtime_test",
            as_of_date=date(2026, 3, 8),
            dry_run=False,
        ) as job:
            result = job.run_step(
                "optional_failure",
                _raise_runtime_error,
                critical=False,
            )
            run_id = job.run_id
            assert result is None
        step_row = connection.execute(
            """
            SELECT status, error_message
            FROM fact_job_step_run
            WHERE job_run_id = ?
              AND step_name = 'optional_failure'
            """,
            [run_id],
        ).fetchone()
        job_row = connection.execute(
            """
            SELECT status, failed_step_count
            FROM fact_job_run
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
    assert step_row == ("FAILED", "optional-step-boom")
    assert job_row == ("DEGRADED_SUCCESS", 1)


def test_emergency_disk_watermark_blocks_high_frequency_job(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)

    def _fake_measure_disk_usage(*args, **kwargs) -> DiskUsageReport:
        return DiskUsageReport(
            mount_point=settings.paths.data_dir,
            total_gb=100.0,
            used_gb=95.0,
            available_gb=5.0,
            usage_ratio=0.95,
            status=DiskWatermark.LIMIT,
            message="Disk usage is 95.0%. High-frequency collection should be reduced.",
        )

    monkeypatch.setattr("app.ops.runtime.measure_disk_usage", _fake_measure_disk_usage)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        with pytest.raises(JobBlockedError):
            with JobRunContext(
                settings,
                connection,
                job_name="sync_investor_flow",
                as_of_date=date(2026, 3, 8),
                dry_run=False,
                policy_config_path="config/ops/default_ops_policy.yaml",
            ):
                pass
        row = connection.execute(
            """
            SELECT status
            FROM fact_job_run
            WHERE job_name = 'sync_investor_flow'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row == ("BLOCKED",)
