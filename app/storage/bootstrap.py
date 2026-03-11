from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from app.common.disk import DiskUsageReport, measure_disk_usage
from app.common.paths import ensure_directories
from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class BootstrapResult:
    run_id: str
    duckdb_path: Path
    created_directories: list[Path]
    disk_report: DiskUsageReport


def ensure_storage_layout(settings: Settings) -> list[Path]:
    return ensure_directories(settings.paths.data_directories())


def log_disk_usage(
    connection: duckdb.DuckDBPyConnection,
    *,
    report: DiskUsageReport,
    measured_at,
    action_taken: str,
) -> None:
    connection.execute(
        """
        INSERT INTO ops_disk_usage_log (
            measured_at,
            mount_point,
            used_gb,
            available_gb,
            usage_ratio,
            action_taken
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            measured_at,
            str(report.mount_point),
            report.used_gb,
            report.available_gb,
            report.usage_ratio,
            action_taken,
        ],
    )


def bootstrap_storage(settings: Settings) -> BootstrapResult:
    created_directories = ensure_storage_layout(settings)
    as_of_date = today_local(settings.app.timezone)
    if settings.metadata.enabled and settings.metadata.backend == "postgres":
        from app.storage.metadata_postgres import bootstrap_postgres_metadata_store

        bootstrap_postgres_metadata_store(settings)

    with activate_run_context("bootstrap", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                notes="Initialize data directories and DuckDB foundation.",
            )
            try:
                measured_at = now_local(settings.app.timezone)
                disk_report = measure_disk_usage(
                    settings.paths.data_dir,
                    warning_ratio=settings.storage.warning_ratio,
                    prune_ratio=settings.storage.prune_ratio,
                    limit_ratio=settings.storage.limit_ratio,
                )
                log_disk_usage(
                    connection,
                    report=disk_report,
                    measured_at=measured_at,
                    action_taken="bootstrap",
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[str(settings.paths.duckdb_path)],
                    notes="Bootstrap completed successfully.",
                )
                return BootstrapResult(
                    run_id=run_context.run_id,
                    duckdb_path=settings.paths.duckdb_path,
                    created_directories=created_directories,
                    disk_report=disk_report,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[str(settings.paths.duckdb_path)],
                    notes="Bootstrap failed.",
                    error_message=str(exc),
                )
                raise
