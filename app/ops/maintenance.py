from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import copyfileobj

import duckdb
import pandas as pd

from app.common.disk import DiskWatermark, measure_disk_usage
from app.common.time import now_local, utc_now
from app.ops.common import AlertSeverity, JobStatus, OpsJobResult, RecoveryStatus, TriggerType
from app.ops.locks import LockManager
from app.ops.policy import load_active_or_default_ops_policy
from app.ops.repository import (
    insert_alert_event,
    insert_disk_watermark_event,
    insert_recovery_action,
    insert_retention_cleanup_run,
    update_recovery_action,
)
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


@dataclass(slots=True)
class _CleanupStats:
    removed_file_count: int = 0
    reclaimed_bytes: int = 0


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _cleanup_targets(settings: Settings) -> list[tuple[str, Path, int]]:
    intraday_dir = settings.paths.curated_dir / "intraday"
    return [
        ("raw_api", settings.paths.raw_dir, settings.retention.raw_api_days),
        ("cache", settings.paths.cache_dir, settings.retention.report_cache_days),
        ("logs", settings.paths.logs_dir, settings.retention.log_days),
        ("artifacts", settings.paths.artifacts_dir, settings.retention.report_cache_days),
        ("intraday_bar_1m", intraday_dir / "bar_1m", settings.retention.intraday_1m_days),
        (
            "intraday_trade_summary",
            intraday_dir / "trade_summary",
            settings.retention.intraday_5m_days,
        ),
        (
            "intraday_quote_summary",
            intraday_dir / "quote_summary",
            settings.retention.orderbook_summary_days,
        ),
    ]


def _latest_referenced_artifact_paths(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> set[str]:
    protected_paths: set[str] = set()
    rows = connection.execute(
        """
        SELECT artifact_path, summary_json
        FROM vw_latest_report_index
        """
    ).fetchall()
    for artifact_path, summary_json in rows:
        if artifact_path:
            protected_paths.add(
                _safe_relative(Path(str(artifact_path)), settings.paths.project_root),
            )
        if not summary_json:
            continue
        try:
            payload = json.loads(str(summary_json))
        except json.JSONDecodeError:
            continue
        payload_path = payload.get("payload_path")
        if payload_path:
            protected_paths.add(
                _safe_relative(Path(str(payload_path)), settings.paths.project_root),
            )
    return protected_paths


def summarize_storage_usage(
    settings: Settings,
    *,
    top_n: int = 20,
) -> OpsJobResult:
    rows: list[dict[str, object]] = []
    for root in settings.paths.data_dir.iterdir():
        if not root.exists():
            continue
        size_bytes = 0
        file_count = 0
        for candidate in root.rglob("*"):
            if candidate.is_file():
                size_bytes += candidate.stat().st_size
                file_count += 1
        rows.append(
            {
                "path": _safe_relative(root, settings.paths.project_root),
                "size_gb": size_bytes / (1024**3),
                "file_count": file_count,
                "cleanup_eligible": root.name in {"raw", "cache", "logs", "artifacts"},
            }
        )
    frame = pd.DataFrame(rows).sort_values(by=["size_gb", "path"], ascending=[False, True])
    artifact_dir = settings.paths.artifacts_dir / "ops" / "storage_usage"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "storage_usage_summary.json"
    artifact_path.write_text(
        json.dumps(frame.head(top_n).to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return OpsJobResult(
        run_id="embedded",
        job_name="summarize_storage_usage",
        status=JobStatus.SUCCESS,
        notes=f"Storage usage summarized. rows={len(frame)}",
        artifact_paths=[str(artifact_path)],
        row_count=len(frame),
    )


def enforce_retention_policies(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    dry_run: bool,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    resolved = load_active_or_default_ops_policy(
        settings,
        connection,
        as_of_at=utc_now(),
        policy_config_path=policy_config_path,
    )
    allowlist = {item.strip("/").replace("\\", "/") for item in resolved.policy.cleanup_allowlist}
    protected = {item.strip("/").replace("\\", "/") for item in resolved.policy.protected_prefixes}
    protected |= _latest_referenced_artifact_paths(connection, settings)
    now = datetime.now(tz=timezone.utc)
    stats = _CleanupStats()
    touched_paths: list[str] = []
    for _scope_name, base_path, max_age_days in _cleanup_targets(settings):
        if not base_path.exists():
            continue
        relative_base = _safe_relative(base_path, settings.paths.project_root)
        if allowlist and relative_base not in allowlist:
            continue
        for candidate in base_path.rglob("*"):
            if not candidate.is_file():
                continue
            relative_candidate = _safe_relative(candidate, settings.paths.project_root)
            if any(
                relative_candidate.startswith(prefix)
                and not relative_candidate.startswith(relative_base)
                for prefix in protected
            ):
                continue
            modified_at = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc)
            if modified_at >= now - timedelta(days=max_age_days):
                continue
            touched_paths.append(relative_candidate)
            stats.removed_file_count += 1
            stats.reclaimed_bytes += int(candidate.stat().st_size)
            if not dry_run:
                candidate.unlink(missing_ok=True)
    started_at = now_local(settings.app.timezone)
    finished_at = now_local(settings.app.timezone)
    insert_retention_cleanup_run(
        connection,
        cleanup_run_id=f"retention-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
        started_at=started_at,
        finished_at=finished_at,
        status=JobStatus.SUCCESS,
        dry_run=dry_run,
        cleanup_scope="ALLOWLIST_RETENTION",
        removed_file_count=stats.removed_file_count,
        reclaimed_bytes=stats.reclaimed_bytes,
        target_paths=touched_paths[:500],
        notes="Retention enforcement completed.",
        job_run_id=job_run_id,
        details={
            "allowlist": sorted(allowlist),
            "protected_prefixes": sorted(protected),
        },
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="enforce_retention_policies",
        status=JobStatus.SUCCESS,
        notes=f"Retention enforcement completed. files={stats.removed_file_count}",
        row_count=stats.removed_file_count,
    )


def cleanup_disk_watermark(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    dry_run: bool,
    policy_config_path: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    resolved = load_active_or_default_ops_policy(
        settings,
        connection,
        as_of_at=utc_now(),
        policy_config_path=policy_config_path,
    )
    disk_report = measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=resolved.policy.warn_ratio,
        prune_ratio=resolved.policy.cleanup_ratio,
        limit_ratio=resolved.policy.emergency_ratio,
    )
    cleanup_required = disk_report.status in {DiskWatermark.PRUNE, DiskWatermark.LIMIT}
    cleanup_result = None
    if cleanup_required:
        cleanup_result = enforce_retention_policies(
            settings,
            connection=connection,
            job_run_id=job_run_id,
            dry_run=dry_run,
            policy_config_path=policy_config_path,
        )
    insert_disk_watermark_event(
        connection,
        event_id=f"disk-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
        measured_at=now_local(settings.app.timezone),
        disk_status=str(disk_report.status),
        usage_ratio=disk_report.usage_ratio,
        used_gb=disk_report.used_gb,
        available_gb=disk_report.available_gb,
        total_gb=disk_report.total_gb,
        policy_id=resolved.policy.policy_id,
        policy_version=resolved.policy.policy_version,
        cleanup_required_flag=cleanup_required,
        emergency_block_flag=disk_report.status == DiskWatermark.LIMIT,
        notes=disk_report.message,
        details={
            "cleanup_executed": cleanup_result is not None,
            "dry_run": dry_run,
        },
        job_run_id=job_run_id,
    )
    if disk_report.status == DiskWatermark.LIMIT:
        insert_alert_event(
            connection,
            alert_id=f"alert-disk-limit-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
            created_at=now_local(settings.app.timezone),
            alert_type="DISK_EMERGENCY",
            severity=AlertSeverity.CRITICAL,
            component_name="disk",
            status="OPEN",
            message="Disk usage reached emergency watermark.",
            details={"usage_ratio": disk_report.usage_ratio},
            job_run_id=job_run_id,
        )
    status = JobStatus.SUCCESS
    if disk_report.status == DiskWatermark.WARNING:
        status = JobStatus.DEGRADED_SUCCESS
    if disk_report.status == DiskWatermark.PRUNE:
        status = JobStatus.PARTIAL_SUCCESS
    if disk_report.status == DiskWatermark.LIMIT:
        status = JobStatus.BLOCKED
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="cleanup_disk_watermark",
        status=status,
        notes=disk_report.message,
    )


def rotate_and_compress_logs(
    settings: Settings,
    *,
    dry_run: bool,
) -> OpsJobResult:
    now = datetime.now(tz=timezone.utc)
    compressed = 0
    artifact_paths: list[str] = []
    for candidate in settings.paths.logs_dir.glob("*.log"):
        modified_at = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc)
        if modified_at.date() >= now.date():
            continue
        target = candidate.with_suffix(candidate.suffix + ".gz")
        artifact_paths.append(str(target))
        compressed += 1
        if dry_run:
            continue
        with candidate.open("rb") as source, gzip.open(target, "wb") as sink:
            copyfileobj(source, sink)
        candidate.unlink(missing_ok=True)
    return OpsJobResult(
        run_id="embedded",
        job_name="rotate_and_compress_logs",
        status=JobStatus.SUCCESS,
        notes=f"Compressed {compressed} log files.",
        artifact_paths=artifact_paths[:50],
        row_count=compressed,
    )


def reconcile_failed_runs(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    limit: int = 20,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    rows = connection.execute(
        """
        SELECT run_id, job_name, status, started_at, root_run_id
        FROM fact_job_run
        WHERE status IN ('FAILED', 'BLOCKED')
        ORDER BY started_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    queued = 0
    for run_id, job_name, status, started_at, root_run_id in rows:
        exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_recovery_action
            WHERE target_job_run_id = ?
              AND status = 'OPEN'
            """,
            [run_id],
        ).fetchone()[0]
        if exists:
            continue
        insert_recovery_action(
            connection,
            recovery_action_id=(
                f"recovery-{run_id}-{utc_now().strftime('%Y%m%dT%H%M%S%f')}"
            ),
            created_at=utc_now(),
            action_type="QUEUE_RECOVERY",
            status=RecoveryStatus.OPEN,
            target_job_run_id=str(run_id),
            triggered_by_run_id=job_run_id,
            recovery_run_id=None,
            lock_name=str(job_name),
            notes="Queued failed run for recovery.",
            details={
                "job_name": job_name,
                "target_status": status,
                "root_run_id": root_run_id,
                "started_at": started_at,
            },
        )
        queued += 1
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="reconcile_failed_runs",
        status=JobStatus.SUCCESS,
        notes=f"Recovery queue reconciled. queued={queued}",
        row_count=queued,
    )


def recover_incomplete_runs(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    limit: int = 5,
    dry_run: bool = False,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    rows = connection.execute(
        """
        SELECT
            action.recovery_action_id,
            target.run_id,
            target.job_name,
            target.as_of_date,
            target.root_run_id
        FROM fact_recovery_action AS action
        JOIN fact_job_run AS target
          ON action.target_job_run_id = target.run_id
        WHERE action.status = 'OPEN'
        ORDER BY action.created_at
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    completed = 0
    for recovery_action_id, target_run_id, job_name, as_of_date, root_run_id in rows:
        if dry_run:
            update_recovery_action(
                connection,
                recovery_action_id=str(recovery_action_id),
                status=RecoveryStatus.SKIPPED,
                notes="Dry-run only.",
                details={"target_job_name": job_name},
                finished_at=utc_now(),
            )
            continue
        result = None
        from app.ops.bundles import (
            run_daily_audit_lite_bundle,
            run_daily_close_bundle,
            run_daily_evaluation_bundle,
            run_daily_post_close_bundle,
            run_daily_research_pipeline,
            run_evaluation_bundle,
            run_intraday_assist_bundle,
            run_news_sync_bundle,
            run_ops_maintenance_bundle,
            run_weekly_calibration_bundle,
            run_weekly_training_bundle,
        )

        bundle_map = {
            "run_news_sync_bundle": run_news_sync_bundle,
            "run_daily_close_bundle": run_daily_close_bundle,
            "run_evaluation_bundle": run_evaluation_bundle,
            "run_intraday_assist_bundle": run_intraday_assist_bundle,
            "run_weekly_training_bundle": run_weekly_training_bundle,
            "run_weekly_calibration_bundle": run_weekly_calibration_bundle,
            "run_daily_audit_lite_bundle": run_daily_audit_lite_bundle,
            "run_daily_research_pipeline": run_daily_research_pipeline,
            "run_daily_post_close_bundle": run_daily_post_close_bundle,
            "run_daily_evaluation_bundle": run_daily_evaluation_bundle,
            "run_ops_maintenance_bundle": run_ops_maintenance_bundle,
        }
        bundle = bundle_map.get(str(job_name))
        if bundle is None:
            update_recovery_action(
                connection,
                recovery_action_id=str(recovery_action_id),
                status=RecoveryStatus.SKIPPED,
                notes="Unsupported job for automatic recovery.",
                details={"target_job_name": job_name},
                finished_at=utc_now(),
            )
            continue
        try:
            result = bundle(
                settings,
                as_of_date=as_of_date,
                trigger_type=TriggerType.RECOVERY,
                parent_run_id=job_run_id,
                root_run_id=str(root_run_id or target_run_id),
                recovery_of_run_id=str(target_run_id),
                dry_run=False,
            )
            update_recovery_action(
                connection,
                recovery_action_id=str(recovery_action_id),
                status=RecoveryStatus.COMPLETED,
                notes="Recovery bundle completed.",
                details={"recovered_status": result.status},
                recovery_run_id=result.run_id,
                finished_at=utc_now(),
            )
            completed += 1
        except Exception as exc:
            update_recovery_action(
                connection,
                recovery_action_id=str(recovery_action_id),
                status=RecoveryStatus.FAILED,
                notes="Recovery bundle failed.",
                details={"error": str(exc)},
                finished_at=utc_now(),
            )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="recover_incomplete_runs",
        status=JobStatus.SUCCESS,
        notes=f"Recovery attempts completed. completed={completed}",
        row_count=completed,
    )


def force_release_stale_lock(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    lock_name: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    released = LockManager(connection).release_stale(
        lock_name=lock_name,
        triggered_by_run_id=job_run_id,
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="force_release_stale_lock",
        status=JobStatus.SUCCESS,
        notes=f"Stale locks released={released}",
        row_count=released,
    )
