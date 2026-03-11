from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml

from app.common.paths import project_root as detect_project_root
from app.common.paths import resolve_path
from app.common.run_context import make_run_id
from app.common.time import utc_now
from app.ops.repository import (
    deactivate_active_ops_policies,
    insert_active_ops_policy,
)
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class OpsPolicy:
    policy_id: str
    policy_version: str
    policy_name: str
    warn_ratio: float = 0.70
    cleanup_ratio: float = 0.80
    emergency_ratio: float = 0.90
    stale_lock_minutes: int = 180
    high_frequency_blocked_jobs: list[str] = field(default_factory=list)
    cleanup_allowlist: list[str] = field(default_factory=list)
    protected_prefixes: list[str] = field(default_factory=list)
    default_dry_run: bool = False
    log_compression_after_days: int = 1
    docker_builder_prune_enabled: bool = True
    docker_builder_prune_until_hours: int = 24
    model_artifact_cleanup_enabled: bool = True
    model_artifact_keep_latest_per_group: int = 1


@dataclass(slots=True)
class ResolvedOpsPolicy:
    policy: OpsPolicy
    source: str
    policy_path: str
    registry_id: str | None = None


@dataclass(slots=True)
class OpsPolicyRegistryResult:
    run_id: str
    policy_id: str
    policy_version: str
    registry_id: str | None
    notes: str


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Ops policy file must contain a mapping: {path}")
    return payload


def _parse_ops_policy(payload: dict[str, Any]) -> OpsPolicy:
    return OpsPolicy(
        policy_id=str(payload.get("policy_id") or "default_ops_policy"),
        policy_version=str(payload.get("policy_version") or "v1"),
        policy_name=str(payload.get("policy_name") or "Default Ops Policy"),
        warn_ratio=float(payload.get("warn_ratio", 0.70)),
        cleanup_ratio=float(payload.get("cleanup_ratio", 0.80)),
        emergency_ratio=float(payload.get("emergency_ratio", 0.90)),
        stale_lock_minutes=int(payload.get("stale_lock_minutes", 180)),
        high_frequency_blocked_jobs=[
            str(item) for item in payload.get("high_frequency_blocked_jobs", [])
        ],
        cleanup_allowlist=[str(item) for item in payload.get("cleanup_allowlist", [])],
        protected_prefixes=[str(item) for item in payload.get("protected_prefixes", [])],
        default_dry_run=bool(payload.get("default_dry_run", False)),
        log_compression_after_days=int(payload.get("log_compression_after_days", 1)),
        docker_builder_prune_enabled=bool(payload.get("docker_builder_prune_enabled", True)),
        docker_builder_prune_until_hours=int(
            payload.get("docker_builder_prune_until_hours", 24)
        ),
        model_artifact_cleanup_enabled=bool(payload.get("model_artifact_cleanup_enabled", True)),
        model_artifact_keep_latest_per_group=int(
            payload.get("model_artifact_keep_latest_per_group", 1)
        ),
    )


def default_ops_policy_path(project_root: Path | None = None) -> Path:
    base = project_root or detect_project_root()
    return (base / "config" / "ops" / "default_ops_policy.yaml").resolve()


def load_ops_policy_from_path(
    path: str | Path,
    *,
    project_root: Path | None = None,
) -> ResolvedOpsPolicy:
    root = project_root or detect_project_root()
    resolved_path = resolve_path(path, root)
    payload = _read_yaml(resolved_path)
    return ResolvedOpsPolicy(
        policy=_parse_ops_policy(payload),
        source="file",
        policy_path=str(resolved_path),
    )


class OpsPolicyResolver:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def resolve(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        as_of_at: datetime | None = None,
        policy_config_path: str | None = None,
    ) -> ResolvedOpsPolicy:
        as_of_at = as_of_at or utc_now()
        if policy_config_path:
            resolved = load_ops_policy_from_path(
                policy_config_path,
                project_root=self.settings.paths.project_root,
            )
            return ResolvedOpsPolicy(
                policy=resolved.policy,
                source="explicit_path",
                policy_path=resolved.policy_path,
                registry_id=resolved.registry_id,
            )
        row = connection.execute(
            """
            SELECT ops_policy_registry_id, policy_path, config_json
            FROM fact_active_ops_policy
            WHERE active_flag = TRUE
              AND effective_from_at <= ?
              AND (effective_to_at IS NULL OR effective_to_at >= ?)
            ORDER BY effective_from_at DESC, created_at DESC
            LIMIT 1
            """,
            [as_of_at, as_of_at],
        ).fetchone()
        if row is not None:
            registry_id, policy_path, config_json = row
            payload = yaml.safe_load(config_json) if config_json else _read_yaml(Path(policy_path))
            return ResolvedOpsPolicy(
                policy=_parse_ops_policy(payload or {}),
                source="active_registry",
                policy_path=str(policy_path),
                registry_id=str(registry_id),
            )
        resolved = load_ops_policy_from_path(
            default_ops_policy_path(self.settings.paths.project_root),
            project_root=self.settings.paths.project_root,
        )
        return ResolvedOpsPolicy(
            policy=resolved.policy,
            source="default_file",
            policy_path=resolved.policy_path,
            registry_id=resolved.registry_id,
        )


def load_active_or_default_ops_policy(
    settings: Settings,
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_at: datetime | None = None,
    policy_config_path: str | None = None,
) -> ResolvedOpsPolicy:
    return OpsPolicyResolver(settings).resolve(
        connection,
        as_of_at=as_of_at,
        policy_config_path=policy_config_path,
    )


def freeze_active_ops_policy(
    settings: Settings,
    *,
    as_of_at: datetime,
    policy_config_path: str | None = None,
    promotion_type: str,
    note: str | None,
) -> OpsPolicyRegistryResult:
    run_id = make_run_id("freeze_active_ops_policy")
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_run_start(
            connection,
            run_id=run_id,
            run_type="freeze_active_ops_policy",
            started_at=utc_now(),
            as_of_date=as_of_at.date(),
            notes="Freeze active ops policy.",
        )
        try:
            resolved = load_active_or_default_ops_policy(
                settings,
                connection,
                as_of_at=as_of_at,
                policy_config_path=policy_config_path,
            )
            registry_id = (
                f"ops-policy-{resolved.policy.policy_id}-"
                f"{as_of_at.strftime('%Y%m%dT%H%M%S')}"
            )
            deactivate_active_ops_policies(connection, effective_to_at=as_of_at)
            insert_active_ops_policy(
                connection,
                registry_id=registry_id,
                policy_id=resolved.policy.policy_id,
                policy_version=resolved.policy.policy_version,
                policy_name=resolved.policy.policy_name,
                policy_path=resolved.policy_path,
                effective_from_at=as_of_at,
                effective_to_at=None,
                active_flag=True,
                promotion_type=promotion_type,
                note=note,
                rollback_of_registry_id=None,
                config_payload=asdict(resolved.policy),
            )
            notes = (
                "Active ops policy frozen: "
                f"{resolved.policy.policy_id}:{resolved.policy.policy_version}"
            )
            record_run_finish(
                connection,
                run_id=run_id,
                finished_at=utc_now(),
                status="success",
                output_artifacts=[],
                notes=notes,
            )
            return OpsPolicyRegistryResult(
                run_id=run_id,
                policy_id=resolved.policy.policy_id,
                policy_version=resolved.policy.policy_version,
                registry_id=registry_id,
                notes=notes,
            )
        except Exception as exc:
            record_run_finish(
                connection,
                run_id=run_id,
                finished_at=utc_now(),
                status="failed",
                output_artifacts=[],
                notes="Freeze active ops policy failed.",
                error_message=str(exc),
            )
            raise


def rollback_active_ops_policy(
    settings: Settings,
    *,
    as_of_at: datetime,
    note: str | None,
) -> OpsPolicyRegistryResult:
    run_id = make_run_id("rollback_active_ops_policy")
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_run_start(
            connection,
            run_id=run_id,
            run_type="rollback_active_ops_policy",
            started_at=utc_now(),
            as_of_date=as_of_at.date(),
            notes="Rollback active ops policy.",
        )
        try:
            active_row = connection.execute(
                """
                SELECT ops_policy_registry_id
                FROM fact_active_ops_policy
                WHERE active_flag = TRUE
                  AND effective_from_at <= ?
                  AND (effective_to_at IS NULL OR effective_to_at >= ?)
                ORDER BY effective_from_at DESC, created_at DESC
                LIMIT 1
                """,
                [as_of_at, as_of_at],
            ).fetchone()
            active_registry_id = str(active_row[0]) if active_row is not None else None
            previous_row = connection.execute(
                """
                SELECT
                    ops_policy_registry_id,
                    policy_id,
                    policy_version,
                    policy_name,
                    policy_path,
                    config_json
                FROM fact_active_ops_policy
                WHERE effective_from_at < ?
                  AND (? IS NULL OR ops_policy_registry_id <> ?)
                ORDER BY effective_from_at DESC, created_at DESC
                LIMIT 1
                """,
                [as_of_at, active_registry_id, active_registry_id],
            ).fetchone()
            if previous_row is None:
                notes = "No previous ops policy was available for rollback."
                record_run_finish(
                    connection,
                    run_id=run_id,
                    finished_at=utc_now(),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                )
                return OpsPolicyRegistryResult(
                    run_id=run_id,
                    policy_id="none",
                    policy_version="none",
                    registry_id=None,
                    notes=notes,
                )
            _, policy_id, policy_version, policy_name, policy_path, config_json = previous_row
            payload = yaml.safe_load(config_json) if config_json else _read_yaml(Path(policy_path))
            deactivate_active_ops_policies(connection, effective_to_at=as_of_at)
            registry_id = f"ops-policy-rollback-{policy_id}-{as_of_at.strftime('%Y%m%dT%H%M%S')}"
            insert_active_ops_policy(
                connection,
                registry_id=registry_id,
                policy_id=str(policy_id),
                policy_version=str(policy_version),
                policy_name=str(policy_name),
                policy_path=str(policy_path),
                effective_from_at=as_of_at,
                effective_to_at=None,
                active_flag=True,
                promotion_type="ROLLBACK",
                note=note,
                rollback_of_registry_id=active_registry_id,
                config_payload=payload or {},
            )
            notes = f"Rolled back active ops policy to {policy_id}:{policy_version}"
            record_run_finish(
                connection,
                run_id=run_id,
                finished_at=utc_now(),
                status="success",
                output_artifacts=[],
                notes=notes,
            )
            return OpsPolicyRegistryResult(
                run_id=run_id,
                policy_id=str(policy_id),
                policy_version=str(policy_version),
                registry_id=registry_id,
                notes=notes,
            )
        except Exception as exc:
            record_run_finish(
                connection,
                run_id=run_id,
                finished_at=utc_now(),
                status="failed",
                output_artifacts=[],
                notes="Rollback active ops policy failed.",
                error_message=str(exc),
            )
            raise
