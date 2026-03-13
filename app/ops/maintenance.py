from __future__ import annotations

import gzip
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import copyfileobj

import duckdb
import pandas as pd

from app.common.disk import DiskWatermark, measure_disk_usage
from app.common.time import now_local, utc_now
from app.ops.common import (
    AlertSeverity,
    JobStatus,
    OpsJobResult,
    RecoveryStatus,
    TriggerType,
    manifest_status,
)
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
from app.storage.metadata_postgres import execute_postgres_sql


@dataclass(slots=True)
class _CleanupStats:
    removed_file_count: int = 0
    reclaimed_bytes: int = 0


_SIZE_MULTIPLIERS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _normalize_relative_path(value: str) -> str:
    return value.strip("/").replace("\\", "/")


def _path_is_within_scope(path_value: str, scope_value: str) -> bool:
    normalized_path = _normalize_relative_path(path_value)
    normalized_scope = _normalize_relative_path(scope_value)
    if not normalized_scope:
        return False
    return normalized_path == normalized_scope or normalized_path.startswith(
        f"{normalized_scope}/"
    )


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


def _parse_reclaimed_bytes(output: str) -> int:
    match = re.search(
        r"Total reclaimed space:\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?B)",
        output,
        flags=re.IGNORECASE,
    )
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).upper()
    return int(value * _SIZE_MULTIPLIERS.get(unit, 1))


def _remove_empty_parents(path: Path, *, stop_roots: set[Path]) -> None:
    current = path.parent
    normalized_roots = {root.resolve() for root in stop_roots}
    while current.exists():
        resolved_current = current.resolve()
        if resolved_current in normalized_roots:
            return
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _safe_artifact_path(
    path_value: object,
    *,
    project_root: Path,
    allowed_roots: set[Path],
) -> Path | None:
    if path_value in (None, ""):
        return None
    candidate = Path(str(path_value))
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    else:
        candidate = candidate.resolve()
    normalized_roots = [root.resolve() for root in allowed_roots]
    if not any(candidate == root or root in candidate.parents for root in normalized_roots):
        return None
    return candidate


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


def cleanup_docker_build_cache(
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
    if not resolved.policy.docker_builder_prune_enabled:
        return OpsJobResult(
            run_id=job_run_id or "embedded",
            job_name="cleanup_docker_build_cache",
            status=JobStatus.SKIPPED,
            notes="Docker builder cache prune is disabled by ops policy.",
        )

    docker_bin = shutil.which("docker")
    if docker_bin is None:
        return OpsJobResult(
            run_id=job_run_id or "embedded",
            job_name="cleanup_docker_build_cache",
            status=JobStatus.SKIPPED,
            notes="Docker CLI is not available on this host.",
        )

    until_hours = max(1, int(resolved.policy.docker_builder_prune_until_hours))
    command = [
        docker_bin,
        "builder",
        "prune",
        "--force",
        "--filter",
        f"until={until_hours}h",
    ]
    artifact_dir = settings.paths.artifacts_dir / "ops" / "docker_build_cache"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{utc_now().strftime('%Y%m%dT%H%M%S%f')}.json"

    if dry_run:
        payload = {
            "mode": "dry_run",
            "command": command,
            "until_hours": until_hours,
            "policy_id": resolved.policy.policy_id,
            "policy_version": resolved.policy.policy_version,
        }
        artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return OpsJobResult(
            run_id=job_run_id or "embedded",
            job_name="cleanup_docker_build_cache",
            status=JobStatus.SUCCESS,
            notes=f"Dry-run: would prune Docker builder cache older than {until_hours}h.",
            artifact_paths=[str(artifact_path)],
        )

    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        raise RuntimeError(
            "Docker builder prune failed."
            + (f" stdout={stdout}" if stdout else "")
            + (f" stderr={stderr}" if stderr else "")
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Docker builder prune timed out.") from exc

    combined_output = "\n".join(
        part for part in [completed.stdout.strip(), completed.stderr.strip()] if part
    )
    reclaimed_bytes = _parse_reclaimed_bytes(combined_output)
    payload = {
        "mode": "execute",
        "command": command,
        "until_hours": until_hours,
        "policy_id": resolved.policy.policy_id,
        "policy_version": resolved.policy.policy_version,
        "reclaimed_bytes": reclaimed_bytes,
        "output": combined_output,
    }
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    insert_retention_cleanup_run(
        connection,
        cleanup_run_id=f"docker-build-cache-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
        started_at=now_local(settings.app.timezone),
        finished_at=now_local(settings.app.timezone),
        status=JobStatus.SUCCESS,
        dry_run=False,
        cleanup_scope="DOCKER_BUILD_CACHE",
        removed_file_count=0,
        reclaimed_bytes=reclaimed_bytes,
        target_paths=[],
        notes=f"Docker builder cache pruned for cache older than {until_hours}h.",
        job_run_id=job_run_id,
        details={
            "command": command,
            "output_artifact": str(artifact_path),
        },
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="cleanup_docker_build_cache",
        status=JobStatus.SUCCESS,
        notes=(
            f"Docker builder cache pruned. until={until_hours}h "
            f"reclaimed_bytes={reclaimed_bytes}"
        ),
        artifact_paths=[str(artifact_path)],
    )


def cleanup_model_artifacts(
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
    if not resolved.policy.model_artifact_cleanup_enabled:
        return OpsJobResult(
            run_id=job_run_id or "embedded",
            job_name="cleanup_model_artifacts",
            status=JobStatus.SKIPPED,
            notes="Model artifact cleanup is disabled by ops policy.",
        )

    keep_latest = max(1, int(resolved.policy.model_artifact_keep_latest_per_group))
    rows = connection.execute(
        """
        SELECT
            training_run_id,
            model_domain,
            model_spec_id,
            horizon,
            panel_name,
            train_end_date,
            created_at,
            status,
            artifact_uri,
            diagnostic_artifact_uri
        FROM fact_model_training_run
        WHERE artifact_uri IS NOT NULL OR diagnostic_artifact_uri IS NOT NULL
        ORDER BY created_at DESC
        """
    ).fetchdf()
    if rows.empty:
        return OpsJobResult(
            run_id=job_run_id or "embedded",
            job_name="cleanup_model_artifacts",
            status=JobStatus.SUCCESS,
            notes="No model artifacts were registered for cleanup.",
            row_count=0,
        )

    protected_training_run_ids = {
        str(value)
        for value in connection.execute(
            """
            SELECT training_run_id FROM fact_alpha_active_model
            UNION
            SELECT training_run_id FROM fact_intraday_active_meta_model
            """
        ).fetchnumpy()["training_run_id"].tolist()
        if value is not None
    }
    latest_training_run_ids = {
        str(row[0])
        for row in connection.execute(
            f"""
            WITH ranked AS (
                SELECT
                    training_run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            COALESCE(model_domain, 'default'),
                            COALESCE(model_spec_id, '__default__'),
                            horizon,
                            COALESCE(panel_name, '__all__')
                        ORDER BY train_end_date DESC, created_at DESC
                    ) AS row_number
                FROM fact_model_training_run
                WHERE status = 'success'
                  AND (artifact_uri IS NOT NULL OR diagnostic_artifact_uri IS NOT NULL)
            )
            SELECT training_run_id
            FROM ranked
            WHERE row_number <= {keep_latest}
            """
        ).fetchall()
        if row[0] is not None
    }
    keep_training_run_ids = protected_training_run_ids | latest_training_run_ids

    legacy_artifacts_root = (settings.paths.project_root / "data" / "artifacts").resolve()
    allowed_roots = {settings.paths.artifacts_dir.resolve()}
    if legacy_artifacts_root != settings.paths.artifacts_dir.resolve():
        allowed_roots.add(legacy_artifacts_root)

    records: list[dict[str, object]] = []
    removed_files: set[Path] = set()
    reclaimed_bytes = 0
    removed_count = 0
    for row in rows.itertuples(index=False):
        training_run_id = str(row.training_run_id)
        if training_run_id in keep_training_run_ids:
            continue
        candidate_paths = [
            _safe_artifact_path(
                row.artifact_uri,
                project_root=settings.paths.project_root,
                allowed_roots=allowed_roots,
            ),
            _safe_artifact_path(
                row.diagnostic_artifact_uri,
                project_root=settings.paths.project_root,
                allowed_roots=allowed_roots,
            ),
        ]
        existing_paths = [path for path in candidate_paths if path is not None and path.exists()]
        if not existing_paths:
            continue
        for path in existing_paths:
            if path in removed_files:
                continue
            file_size = int(path.stat().st_size)
            records.append(
                {
                    "training_run_id": training_run_id,
                    "model_domain": row.model_domain,
                    "model_spec_id": row.model_spec_id,
                    "horizon": row.horizon,
                    "panel_name": row.panel_name,
                    "train_end_date": str(row.train_end_date) if row.train_end_date is not None else None,
                    "path": str(path),
                    "size_bytes": file_size,
                }
            )
            removed_files.add(path)
            reclaimed_bytes += file_size
            removed_count += 1
            if not dry_run:
                path.unlink(missing_ok=True)
                _remove_empty_parents(path, stop_roots=allowed_roots)

    artifact_dir = settings.paths.artifacts_dir / "ops" / "model_artifact_cleanup"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{utc_now().strftime('%Y%m%dT%H%M%S%f')}.json"
    payload = {
        "mode": "dry_run" if dry_run else "execute",
        "keep_latest_per_group": keep_latest,
        "protected_training_run_ids": sorted(keep_training_run_ids),
        "removed_file_count": removed_count,
        "reclaimed_bytes": reclaimed_bytes,
        "records": records[:500],
    }
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    insert_retention_cleanup_run(
        connection,
        cleanup_run_id=f"model-artifacts-{utc_now().strftime('%Y%m%dT%H%M%S%f')}",
        started_at=now_local(settings.app.timezone),
        finished_at=now_local(settings.app.timezone),
        status=JobStatus.SUCCESS,
        dry_run=dry_run,
        cleanup_scope="MODEL_ARTIFACTS",
        removed_file_count=removed_count,
        reclaimed_bytes=reclaimed_bytes,
        target_paths=[str(record["path"]) for record in records[:500]],
        notes="Model artifact cleanup completed.",
        job_run_id=job_run_id,
        details={
            "keep_latest_per_group": keep_latest,
            "output_artifact": str(artifact_path),
        },
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="cleanup_model_artifacts",
        status=JobStatus.SUCCESS,
        notes=(
            f"Model artifact cleanup completed. files={removed_count} "
            f"reclaimed_bytes={reclaimed_bytes}"
        ),
        artifact_paths=[str(artifact_path)],
        row_count=removed_count,
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
    allowlist = {_normalize_relative_path(item) for item in resolved.policy.cleanup_allowlist}
    protected_prefixes = {
        _normalize_relative_path(item) for item in resolved.policy.protected_prefixes
    }
    protected_paths = {
        _normalize_relative_path(item)
        for item in _latest_referenced_artifact_paths(connection, settings)
    }
    now = datetime.now(tz=timezone.utc)
    stats = _CleanupStats()
    touched_paths: list[str] = []
    for _scope_name, base_path, max_age_days in _cleanup_targets(settings):
        if not base_path.exists():
            continue
        relative_base = _normalize_relative_path(
            _safe_relative(base_path, settings.paths.project_root)
        )
        if allowlist and relative_base not in allowlist:
            continue
        for candidate in base_path.rglob("*"):
            if not candidate.is_file():
                continue
            relative_candidate = _normalize_relative_path(
                _safe_relative(candidate, settings.paths.project_root)
            )
            if relative_candidate in protected_paths:
                continue
            if any(
                _path_is_within_scope(relative_candidate, prefix)
                and not _path_is_within_scope(relative_base, prefix)
                for prefix in protected_prefixes
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
            "protected_prefixes": sorted(protected_prefixes),
            "protected_paths": sorted(protected_paths),
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


def cleanup_stale_job_runs(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
    stale_after_minutes: int = 10,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    cutoff = utc_now() - timedelta(minutes=max(5, int(stale_after_minutes)))
    rows = connection.execute(
        """
        SELECT run_id, notes, details_json, artifact_count
        FROM fact_job_run
        WHERE status = 'RUNNING'
          AND finished_at IS NULL
          AND started_at < ?
          AND run_id NOT IN (
              SELECT owner_run_id
              FROM fact_active_lock
              WHERE released_at IS NULL
                AND owner_run_id IS NOT NULL
          )
        ORDER BY started_at ASC
        """,
        [cutoff],
    ).fetchall()
    cleared = 0
    for run_id, notes, details_json, artifact_count in rows:
        now = utc_now()
        step_query = """
            UPDATE fact_job_step_run
            SET finished_at = ?,
                status = 'FAILED',
                error_message = COALESCE(error_message, ?)
            WHERE job_run_id = ?
              AND status = 'RUNNING'
        """
        step_params = [now, "Cleared as stale during ops maintenance.", run_id]
        connection.execute(step_query, step_params)
        execute_postgres_sql(settings, step_query, step_params)
        details = json.loads(details_json) if details_json else {}
        details["cleanup_recovered"] = True
        details["cleanup_recovered_at"] = now.isoformat()
        merged_notes = " | ".join(
            part for part in [notes, "Cleared as stale during ops maintenance."] if part
        )
        step_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_job_step_run WHERE job_run_id = ?",
                [run_id],
            ).fetchone()[0]
        )
        failed_step_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_job_step_run WHERE job_run_id = ? AND status = 'FAILED'",
                [run_id],
            ).fetchone()[0]
        )
        run_query = """
            UPDATE fact_job_run
            SET finished_at = ?,
                status = 'FAILED',
                step_count = ?,
                failed_step_count = ?,
                artifact_count = ?,
                notes = ?,
                error_message = ?,
                details_json = ?
            WHERE run_id = ?
        """
        run_params = [
            now,
            step_count,
            failed_step_count,
            int(artifact_count or 0),
            merged_notes,
            "Cleared as stale during ops maintenance.",
            json.dumps(details, ensure_ascii=False, default=str),
            run_id,
        ]
        connection.execute(run_query, run_params)
        execute_postgres_sql(settings, run_query, run_params)
        manifest_query = """
            UPDATE ops_run_manifest
            SET finished_at = ?,
                status = ?,
                notes = ?,
                error_message = ?
            WHERE run_id = ?
        """
        manifest_params = [
            now,
            manifest_status(JobStatus.FAILED),
            merged_notes,
            "Cleared as stale during ops maintenance.",
            run_id,
        ]
        connection.execute(manifest_query, manifest_params)
        execute_postgres_sql(settings, manifest_query, manifest_params)
        cleared += 1
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="cleanup_stale_job_runs",
        status=JobStatus.SUCCESS,
        notes=f"Stale running job rows cleared={cleared}",
        row_count=cleared,
    )


def reset_open_recovery_actions(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    job_run_id: str | None = None,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    rows = connection.execute(
        """
        SELECT recovery_action_id, notes, details_json
        FROM fact_recovery_action
        WHERE status = 'OPEN'
        ORDER BY created_at
        """
    ).fetchall()
    reset_count = 0
    for recovery_action_id, notes, details_json in rows:
        details = json.loads(details_json) if details_json else {}
        details["daily_queue_reset"] = True
        details["daily_queue_reset_at"] = utc_now().isoformat()
        merged_notes = " | ".join(
            part for part in [notes, "Cleared during daily recovery queue reset."] if part
        )
        update_recovery_action(
            connection,
            recovery_action_id=str(recovery_action_id),
            status=RecoveryStatus.SKIPPED,
            notes=merged_notes,
            details=details,
            finished_at=utc_now(),
        )
        reset_count += 1
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="reset_open_recovery_actions",
        status=JobStatus.SUCCESS,
        notes=f"Open recovery actions reset={reset_count}",
        row_count=reset_count,
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
            run_docker_build_cache_cleanup_bundle,
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
            "run_docker_build_cache_cleanup_bundle": run_docker_build_cache_cleanup_bundle,
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
