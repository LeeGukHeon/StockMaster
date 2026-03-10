# ruff: noqa: E501, I001

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from app.common.time import now_local
from app.ops.common import JobStatus, OpsValidationResult
from app.ops.repository import json_text
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables
from app.ui.navigation import PAGE_SPECS


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
    connection.executemany(
        f"""
        INSERT INTO fact_release_candidate_check ({", ".join(columns)})
        VALUES ({", ".join("?" for _ in columns)})
        """,
        [[row[column] for column in columns] for row in rows],
    )


def _view_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def _page_contract_checks() -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    for spec in PAGE_SPECS:
        if spec.callable_name is not None:
            exists = True
            detail = {"page_key": spec.key, "callable_name": spec.callable_name}
        else:
            exists = spec.path is not None and spec.path.exists()
            detail = {"page_key": spec.key, "path": str(spec.path) if spec.path else None}
        checks.append(
            ValidationCheck(
                name=f"page_contract:{spec.key}",
                status=JobStatus.SUCCESS if exists else JobStatus.FAILED,
                severity="INFO" if exists else "CRITICAL",
                detail=detail,
                recommended_action=(
                    "페이지 파일/엔트리를 복구하세요." if not exists else "none"
                ),
            )
        )
    return checks


def validate_page_contracts(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None,
    persist_results: bool = True,
) -> OpsValidationResult:
    checks = _page_contract_checks()
    if persist_results:
        if connection is None:
            raise ValueError("connection is required when persist_results=True")
        bootstrap_core_tables(connection)
        _insert_checks(connection, settings, checks)
    warning_count = sum(1 for check in checks if check.severity != "INFO")
    return OpsValidationResult(
        run_id="embedded",
        check_count=len(checks),
        warning_count=warning_count,
        notes=f"Page contracts validated. checks={len(checks)} warnings={warning_count}",
    )


def _navigation_integrity_checks() -> list[ValidationCheck]:
    url_paths = [spec.url_path for spec in PAGE_SPECS]
    titles = [spec.title for spec in PAGE_SPECS]
    return [
        ValidationCheck(
            name="navigation:url_path_unique",
            status=JobStatus.SUCCESS if len(url_paths) == len(set(url_paths)) else JobStatus.FAILED,
            severity="INFO" if len(url_paths) == len(set(url_paths)) else "CRITICAL",
            detail={"url_paths": url_paths},
            recommended_action="중복 URL path를 제거하세요.",
        ),
        ValidationCheck(
            name="navigation:title_unique",
            status=JobStatus.SUCCESS if len(titles) == len(set(titles)) else JobStatus.FAILED,
            severity="INFO" if len(titles) == len(set(titles)) else "WARNING",
            detail={"titles": titles},
            recommended_action="중복 페이지 제목을 정리하세요.",
        ),
        ValidationCheck(
            name="navigation:docs_page_present",
            status=JobStatus.SUCCESS if any(spec.key == "docs" for spec in PAGE_SPECS) else JobStatus.FAILED,
            severity="INFO" if any(spec.key == "docs" for spec in PAGE_SPECS) else "CRITICAL",
            detail={"page_keys": [spec.key for spec in PAGE_SPECS]},
            recommended_action="Docs/Help 페이지를 navigation에 포함하세요.",
        ),
    ]


def validate_navigation_integrity(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection | None,
    persist_results: bool = True,
) -> OpsValidationResult:
    checks = _navigation_integrity_checks()
    if persist_results:
        if connection is None:
            raise ValueError("connection is required when persist_results=True")
        bootstrap_core_tables(connection)
        _insert_checks(connection, settings, checks)
    warning_count = sum(1 for check in checks if check.severity != "INFO")
    return OpsValidationResult(
        run_id="embedded",
        check_count=len(checks),
        warning_count=warning_count,
        notes=f"Navigation integrity validated. checks={len(checks)} warnings={warning_count}",
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
                    recommended_action=f"{report_type} 리포트를 생성하고 인덱스를 다시 빌드하세요.",
                )
            )
            continue
        artifact_path = Path(str(row[1]))
        payload_exists = False
        if row[2]:
            try:
                summary = json.loads(str(row[2]))
            except json.JSONDecodeError:
                summary = {}
            payload_path = summary.get("payload_path")
            payload_exists = bool(payload_path and Path(str(payload_path)).exists())
        checks.append(
            ValidationCheck(
                name=f"report:{report_type}",
                status=JobStatus.SUCCESS if artifact_path.exists() else JobStatus.FAILED,
                severity="INFO" if artifact_path.exists() else "CRITICAL",
                detail={
                    "report_type": report_type,
                    "artifact_path": str(artifact_path),
                    "payload_exists": payload_exists,
                    "status": row[3],
                },
                recommended_action="리포트 artifact 경로를 복구하세요." if not artifact_path.exists() else "none",
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
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date | None = None,
) -> OpsValidationResult:
    bootstrap_core_tables(connection)
    subresults = [
        validate_page_contracts(settings, connection=connection),
        validate_navigation_integrity(settings, connection=connection),
        validate_report_artifacts(settings, connection=connection),
    ]
    freshness_rows = connection.execute(
        """
        SELECT page_name, dataset_name, stale_flag, warning_level
        FROM vw_latest_ui_data_freshness_snapshot
        """
    ).fetchall()
    stale_rows = [row for row in freshness_rows if bool(row[2])]
    critical_rows = [row for row in freshness_rows if str(row[3]).upper() == "CRITICAL"]
    checks = [
        ValidationCheck(
            name="release:latest_app_snapshot_present",
            status=JobStatus.SUCCESS if _view_count(connection, "vw_latest_app_snapshot") > 0 else JobStatus.FAILED,
            severity="INFO" if _view_count(connection, "vw_latest_app_snapshot") > 0 else "CRITICAL",
            detail={"as_of_date": str(as_of_date) if as_of_date else None},
            recommended_action="build_latest_app_snapshot.py를 실행하세요.",
        ),
        ValidationCheck(
            name="release:report_index_present",
            status=JobStatus.SUCCESS if _view_count(connection, "vw_latest_report_index") > 0 else JobStatus.FAILED,
            severity="INFO" if _view_count(connection, "vw_latest_report_index") > 0 else "CRITICAL",
            detail={"row_count": _view_count(connection, "vw_latest_report_index")},
            recommended_action="build_report_index.py를 실행하세요.",
        ),
        ValidationCheck(
            name="release:ui_freshness_critical",
            status=JobStatus.SUCCESS if not critical_rows else JobStatus.DEGRADED_SUCCESS,
            severity="INFO" if not critical_rows else "WARNING",
            detail={"critical_rows": [list(row) for row in critical_rows]},
            recommended_action="critical stale dataset을 점검하세요.",
        ),
        ValidationCheck(
            name="release:ui_freshness_stale",
            status=JobStatus.SUCCESS if not stale_rows else JobStatus.DEGRADED_SUCCESS,
            severity="INFO" if not stale_rows else "WARNING",
            detail={"stale_rows": [list(row) for row in stale_rows]},
            recommended_action="stale dataset을 재생성하거나 배너를 확인하세요.",
        ),
    ]
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
