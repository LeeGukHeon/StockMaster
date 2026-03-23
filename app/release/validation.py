# ruff: noqa: E501, I001

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb

from app.common.artifacts import resolve_artifact_path
from app.common.time import now_local
from app.ops.common import JobStatus, OpsValidationResult
from app.ops.repository import json_text
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables
from app.storage.metadata_postgres import executemany_postgres_sql

REQUIRED_REPORT_TYPES: tuple[str, ...] = (
    "daily_research_report",
    "portfolio_report",
    "evaluation_report",
    "intraday_summary_report",
    "release_candidate_checklist",
)


@dataclass(frozen=True, slots=True)
class ValidationCheck:
    name: str
    status: str
    severity: str
    detail: dict[str, Any]
    recommended_action: str


def _insert_checks(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    checks: list[ValidationCheck],
) -> None:
    if not checks:
        return
    check_ts = now_local(settings.app.timezone)
    rows = [
        {
            "release_candidate_check_id": (
                f"rc-check-{check.name}-{check_ts.strftime('%Y%m%dT%H%M%S%f')}"
            ),
            "check_ts": check_ts,
            "environment": settings.app.env,
            "check_name": check.name,
            "status": check.status,
            "severity": check.severity,
            "detail_json": json_text(check.detail),
            "recommended_action": check.recommended_action,
            "created_at": check_ts,
        }
        for check in checks
    ]
    columns = list(rows[0].keys())
    values = [[row[column] for column in columns] for row in rows]
    sql = f"""
        INSERT INTO fact_release_candidate_check ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
    """
    connection.executemany(sql, values)
    executemany_postgres_sql(settings, sql, values)


def _view_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def validate_page_contracts(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None,
    persist_results: bool = True,
) -> OpsValidationResult:
    checks = [
        ValidationCheck(
            name="dashboard:retired",
            status=JobStatus.SKIPPED,
            severity="INFO",
            detail={"message": "Streamlit dashboard has been retired in favor of Discord bot."},
            recommended_action="none",
        )
    ]
    if persist_results and connection is not None:
        bootstrap_core_tables(connection)
        _insert_checks(connection, settings, checks)
    return OpsValidationResult(
        run_id="embedded",
        check_count=len(checks),
        warning_count=0,
        notes="Dashboard page-contract validation retired.",
    )


def validate_navigation_integrity(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None,
    persist_results: bool = True,
) -> OpsValidationResult:
    checks = [
        ValidationCheck(
            name="dashboard:navigation_retired",
            status=JobStatus.SKIPPED,
            severity="INFO",
            detail={"message": "Dashboard navigation is retired."},
            recommended_action="none",
        )
    ]
    if persist_results and connection is not None:
        bootstrap_core_tables(connection)
        _insert_checks(connection, settings, checks)
    return OpsValidationResult(
        run_id="embedded",
        check_count=len(checks),
        warning_count=0,
        notes="Dashboard navigation validation retired.",
    )


def validate_report_artifacts(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
) -> OpsValidationResult:
    bootstrap_core_tables(connection)
    rows = connection.execute(
        """
        SELECT report_type, artifact_path, summary_json, status
        FROM vw_latest_report_index
        """
    ).fetchall()
    latest_by_type = {str(row[0]): row for row in rows}
    checks: list[ValidationCheck] = []
    for report_type in REQUIRED_REPORT_TYPES:
        row = latest_by_type.get(report_type)
        if row is None:
            checks.append(
                ValidationCheck(
                    name=f"report:{report_type}",
                    status=JobStatus.FAILED,
                    severity="CRITICAL",
                    detail={"report_type": report_type, "artifact_path": None},
                    recommended_action=f"{report_type} 리포트를 다시 생성하세요.",
                )
            )
            continue
        artifact_path = resolve_artifact_path(settings, row[1])
        payload_exists = True
        payload_path = None
        if row[2]:
            try:
                summary = json.loads(str(row[2]))
            except json.JSONDecodeError:
                summary = {}
            payload_path = resolve_artifact_path(settings, summary.get("payload_path"))
            raw_payload_path = summary.get("payload_path")
            payload_exists = raw_payload_path is None or payload_path is not None
        checks.append(
            ValidationCheck(
                name=f"report:{report_type}",
                status=JobStatus.SUCCESS if artifact_path is not None else JobStatus.FAILED,
                severity="INFO" if artifact_path is not None else "CRITICAL",
                detail={
                    "report_type": report_type,
                    "artifact_path": str(artifact_path) if artifact_path is not None else str(row[1]),
                    "payload_path": str(payload_path) if payload_path is not None else None,
                    "payload_exists": payload_exists,
                    "status": row[3],
                },
                recommended_action="report artifact 경로를 복구하세요." if artifact_path is None else "none",
            )
        )
    _insert_checks(connection, settings, checks)
    warning_count = sum(1 for check in checks if check.severity != "INFO")
    return OpsValidationResult(
        run_id="embedded",
        check_count=len(checks),
        warning_count=warning_count,
        notes=f"Report artifacts validated. checks={len(checks)} warnings={warning_count}",
    )


def validate_release_candidate(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
    as_of_date: date | None = None,
    persist_results: bool = True,
) -> OpsValidationResult:
    if persist_results and connection is None:
        raise ValueError("connection is required when persist_results=True")
    if connection is None:
        raise ValueError(
            "connection is required when persist_results=False is not using a caller-provided snapshot"
        )
    bootstrap_core_tables(connection)
    subresults = [
        validate_page_contracts(settings, connection=connection, persist_results=persist_results),
        validate_navigation_integrity(settings, connection=connection, persist_results=persist_results),
        validate_report_artifacts(settings, connection=connection),
    ]
    bot_refresh_row = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_job_step_run
        WHERE step_name = 'materialize_discord_bot_read_store'
          AND status IN ('SUCCESS', 'DEGRADED_SUCCESS')
        """
    ).fetchone()
    bot_refresh_count = int(bot_refresh_row[0] or 0) if bot_refresh_row else 0
    checks = [
        ValidationCheck(
            name="release:report_index_present",
            status=JobStatus.SUCCESS if _view_count(connection, "vw_latest_report_index") > 0 else JobStatus.FAILED,
            severity="INFO" if _view_count(connection, "vw_latest_report_index") > 0 else "CRITICAL",
            detail={"row_count": _view_count(connection, "vw_latest_report_index")},
            recommended_action="build_report_index.py를 실행하세요.",
        ),
        ValidationCheck(
            name="release:discord_bot_snapshot_refresh_present",
            status=JobStatus.SUCCESS if bot_refresh_count > 0 else JobStatus.FAILED,
            severity="INFO" if bot_refresh_count > 0 else "CRITICAL",
            detail={"row_count": bot_refresh_count, "as_of_date": str(as_of_date) if as_of_date else None},
            recommended_action="materialize_discord_bot_read_store.py를 실행하세요.",
        ),
    ]
    if persist_results:
        _insert_checks(connection, settings, checks)
    total_checks = sum(result.check_count for result in subresults) + len(checks)
    total_warnings = sum(result.warning_count for result in subresults) + sum(
        1 for check in checks if check.severity != "INFO"
    )
    return OpsValidationResult(
        run_id="embedded",
        check_count=total_checks,
        warning_count=total_warnings,
        notes=f"Release candidate validated. checks={total_checks} warnings={total_warnings}",
    )
