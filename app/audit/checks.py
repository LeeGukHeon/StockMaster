# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.audit.contracts import get_ticket_checklist, representative_contracts
from app.common.artifacts import resolve_artifact_path
from app.ops.common import JobStatus
from app.ops.maintenance import _latest_referenced_artifact_paths
from app.release.snapshot import _expected_trading_data_date
from app.settings import Settings

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
PSEUDO_SYMBOLS_BY_TABLE: dict[str, tuple[str, ...]] = {
    "fact_portfolio_target_book": ("__CASH__",),
    "fact_portfolio_rebalance_plan": ("__CASH__",),
    "fact_portfolio_position_snapshot": ("__CASH__",),
}


@dataclass(frozen=True, slots=True)
class AuditCheckResult:
    check_id: str
    category: str
    target: str
    status: str
    summary: str
    detail: dict[str, Any]
    remediation: str
    priority: str = "INFO"
    ticket_id: str | None = None


@dataclass(frozen=True, slots=True)
class AuditSuiteResult:
    results: tuple[AuditCheckResult, ...]

    @property
    def pass_count(self) -> int:
        return sum(1 for item in self.results if item.status == PASS)

    @property
    def warn_count(self) -> int:
        return sum(1 for item in self.results if item.status == WARN)

    @property
    def fail_count(self) -> int:
        return sum(1 for item in self.results if item.status == FAIL)

    @property
    def job_status(self) -> str:
        if self.fail_count:
            return JobStatus.FAILED
        if self.warn_count:
            return JobStatus.DEGRADED_SUCCESS
        return JobStatus.SUCCESS


def _object_registry(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = connection.execute(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
        """
    ).fetchall()
    return {str(name): str(table_type) for name, table_type in rows}


def _object_exists(
    connection: duckdb.DuckDBPyConnection, object_name: str
) -> tuple[bool, str | None]:
    registry = _object_registry(connection)
    object_type = registry.get(object_name)
    return (object_type is not None, object_type)


def _column_names(connection: duckdb.DuckDBPyConnection, object_name: str) -> tuple[str, ...]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [object_name],
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _scalar(
    connection: duckdb.DuckDBPyConnection, query: str, params: list[object] | None = None
) -> Any:
    row = connection.execute(query, params or []).fetchone()
    return row[0] if row else None


def _safe_relative_path(path: str | Path, root: Path) -> str:
    candidate = Path(path).resolve()
    try:
        return candidate.relative_to(root.resolve()).as_posix()
    except ValueError:
        return candidate.as_posix()


def _normalize_date_value(value: Any) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        try:
            rendered = value.isoformat()
            rendered_text = str(rendered)
            if rendered_text in {"NaT", "nan", "None"}:
                return None
            return rendered_text.split("T")[0].split(" ")[0]
        except TypeError:
            pass
    text = str(value)
    if text in {"None", "NaT", "nan"}:
        return None
    return text.split(" ")[0]


def _row_count(connection: duckdb.DuckDBPyConnection, object_name: str) -> int:
    return int(_scalar(connection, f"SELECT COUNT(*) FROM {object_name}") or 0)


def _duplicate_count(
    connection: duckdb.DuckDBPyConnection,
    object_name: str,
    unique_key: tuple[str, ...],
) -> int:
    if not unique_key:
        return 0
    key_expr = ", ".join(unique_key)
    return int(
        _scalar(
            connection,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {key_expr}, COUNT(*) AS duplicate_count
                FROM {object_name}
                GROUP BY {key_expr}
                HAVING COUNT(*) > 1
            )
            """,
        )
        or 0
    )


def _missing_symbol_reference_count(
    connection: duckdb.DuckDBPyConnection,
    object_name: str,
    columns: tuple[str, ...],
) -> int | None:
    if "symbol" not in columns or object_name == "dim_symbol":
        return None
    excluded_symbols = PSEUDO_SYMBOLS_BY_TABLE.get(object_name, ())
    excluded_clause = ""
    if excluded_symbols:
        rendered_symbols = ", ".join(f"'{symbol}'" for symbol in excluded_symbols)
        excluded_clause = f" AND source.symbol NOT IN ({rendered_symbols})"
    return int(
        _scalar(
            connection,
            f"""
            SELECT COUNT(*)
            FROM {object_name} AS source
            LEFT JOIN dim_symbol AS dim
              ON dim.symbol = source.symbol
            WHERE source.symbol IS NOT NULL
              {excluded_clause}
              AND dim.symbol IS NULL
            """,
        )
        or 0
    )


def _report_payload_path(summary_json: str | None) -> str | None:
    if not summary_json:
        return None
    try:
        payload = json.loads(summary_json)
    except json.JSONDecodeError:
        return None
    payload_path = payload.get("payload_path")
    return str(payload_path) if payload_path else None


def run_contract_checks(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
) -> AuditSuiteResult:
    results: list[AuditCheckResult] = []
    for contract in representative_contracts():
        exists, actual_type = _object_exists(connection, contract.name)
        if not exists:
            results.append(
                AuditCheckResult(
                    check_id=f"contract:{contract.name}:exists",
                    category="contract",
                    target=contract.name,
                    status=FAIL,
                    summary="대표 객체가 존재하지 않습니다.",
                    detail={"expected_type": contract.object_type},
                    remediation="스키마 bootstrap/migration을 다시 실행해 객체를 복구하세요.",
                    priority="P0",
                    ticket_id=contract.ticket_ids[-1] if contract.ticket_ids else None,
                )
            )
            continue
        columns = _column_names(connection, contract.name)
        missing_columns = [column for column in contract.required_columns if column not in columns]
        duplicate_rows = _duplicate_count(connection, contract.name, contract.unique_key)
        missing_lineage = [column for column in contract.lineage_columns if column not in columns]
        missing_symbol_refs = _missing_symbol_reference_count(connection, contract.name, columns)
        row_count = _row_count(connection, contract.name)
        if missing_columns or duplicate_rows > 0 or (missing_symbol_refs or 0) > 0:
            status = FAIL
            priority = "P0"
        elif (
            missing_lineage
            or row_count == 0
            or actual_type.lower()
            not in {
                contract.object_type.lower(),
                "base table" if contract.object_type == "table" else "view",
            }
        ):
            status = WARN
            priority = "P1"
        else:
            status = PASS
            priority = "INFO"
        results.append(
            AuditCheckResult(
                check_id=f"contract:{contract.name}",
                category="contract",
                target=contract.name,
                status=status,
                summary=f"{contract.grain} / rows={row_count} / duplicates={duplicate_rows}",
                detail={
                    "object_type": actual_type,
                    "expected_object_type": contract.object_type,
                    "grain": contract.grain,
                    "unique_key": list(contract.unique_key),
                    "required_columns": list(contract.required_columns),
                    "missing_columns": missing_columns,
                    "lineage_columns": list(contract.lineage_columns),
                    "missing_lineage_columns": missing_lineage,
                    "row_count": row_count,
                    "duplicate_row_groups": duplicate_rows,
                    "missing_symbol_references": missing_symbol_refs,
                    "rerun_rule": contract.rerun_rule,
                },
                remediation=(
                    "누락 컬럼, 중복 키, symbol 참조 무결성을 우선 수정하세요."
                    if status == FAIL
                    else "lineage 컬럼/row count를 점검하세요."
                    if status == WARN
                    else "none"
                ),
                priority=priority,
                ticket_id=contract.ticket_ids[-1] if contract.ticket_ids else None,
            )
        )
    return AuditSuiteResult(tuple(results))


def run_latest_layer_checks(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    snapshot_ts: datetime | None = None,
) -> AuditSuiteResult:
    now_ts = snapshot_ts or datetime.now().astimezone()
    expected_trading_date = _expected_trading_data_date(connection, now_ts)
    results: list[AuditCheckResult] = []

    latest_view_counts = {
        "vw_latest_app_snapshot": _row_count(connection, "vw_latest_app_snapshot"),
        "vw_latest_report_index": _row_count(connection, "vw_latest_report_index"),
        "vw_latest_release_candidate_check": _row_count(
            connection, "vw_latest_release_candidate_check"
        ),
        "vw_latest_ui_data_freshness_snapshot": _row_count(
            connection, "vw_latest_ui_data_freshness_snapshot"
        ),
    }
    for view_name, row_count in latest_view_counts.items():
        status = PASS if row_count > 0 else FAIL
        results.append(
            AuditCheckResult(
                check_id=f"latest:{view_name}:present",
                category="latest_layer",
                target=view_name,
                status=status,
                summary=f"{view_name} rows={row_count}",
                detail={"row_count": row_count},
                remediation="최신 snapshot/index 빌드 스크립트를 다시 실행하세요."
                if status == FAIL
                else "none",
                priority="P0" if status == FAIL else "INFO",
                ticket_id="T013",
            )
        )

    uniqueness_queries = {
        "fact_latest_app_snapshot": """
            SELECT CASE WHEN COUNT(*) = 1 THEN 0 ELSE ABS(COUNT(*) - 1) END
            FROM vw_latest_app_snapshot
        """,
        "fact_latest_report_index": """
            SELECT COUNT(*) FROM (
                SELECT report_type, COUNT(*) AS row_count
                FROM vw_latest_report_index
                GROUP BY report_type
                HAVING COUNT(*) > 1
            )
        """,
        "fact_release_candidate_check": """
            SELECT COUNT(*) FROM (
                SELECT check_name, COUNT(*) AS row_count
                FROM vw_latest_release_candidate_check
                GROUP BY check_name
                HAVING COUNT(*) > 1
            )
        """,
        "fact_ui_data_freshness_snapshot": """
            SELECT COUNT(*) FROM (
                SELECT page_name, dataset_name, COUNT(*) AS row_count
                FROM vw_latest_ui_data_freshness_snapshot
                GROUP BY page_name, dataset_name
                HAVING COUNT(*) > 1
            )
        """,
    }
    for target, query in uniqueness_queries.items():
        duplicate_groups = int(_scalar(connection, query) or 0)
        status = PASS if duplicate_groups == 0 else FAIL
        results.append(
            AuditCheckResult(
                check_id=f"latest:{target}:uniqueness",
                category="latest_layer",
                target=target,
                status=status,
                summary=f"canonical latest uniqueness duplicate_groups={duplicate_groups}",
                detail={"duplicate_groups": duplicate_groups},
                remediation="latest view grain과 write path를 재점검하세요."
                if status == FAIL
                else "none",
                priority="P0" if status == FAIL else "INFO",
                ticket_id="T013",
            )
        )

    snapshot_row = connection.execute("SELECT * FROM vw_latest_app_snapshot").fetchdf()
    if not snapshot_row.empty:
        row = snapshot_row.iloc[0]
        selection_max = _scalar(
            connection,
            """
            SELECT MAX(as_of_date)
            FROM fact_ranking
            WHERE ranking_version = 'selection_engine_v2'
            """,
        )
        evaluation_max = _scalar(
            connection, "SELECT MAX(summary_date) FROM fact_evaluation_summary"
        )
        portfolio_max = _scalar(
            connection, "SELECT MAX(as_of_date) FROM fact_portfolio_target_book"
        )
        report_run_exists = int(
            _scalar(
                connection,
                "SELECT COUNT(*) FROM fact_latest_report_index WHERE run_id = ?",
                [row["latest_report_bundle_id"]],
            )
            or 0
        )
        source_mismatch = {
            "as_of_date": _normalize_date_value(row["as_of_date"])
            != _normalize_date_value(selection_max),
            "latest_evaluation_date": _normalize_date_value(row["latest_evaluation_date"])
            != _normalize_date_value(evaluation_max),
            "latest_portfolio_as_of_date": _normalize_date_value(
                row["latest_portfolio_as_of_date"]
            )
            != _normalize_date_value(portfolio_max),
            "latest_report_bundle_missing": report_run_exists == 0
            and row["latest_report_bundle_id"] is not None,
        }
        mismatch_count = sum(1 for flag in source_mismatch.values() if flag)
        results.append(
            AuditCheckResult(
                check_id="latest:app_snapshot:source_consistency",
                category="latest_layer",
                target="fact_latest_app_snapshot",
                status=PASS if mismatch_count == 0 else FAIL,
                summary=f"source consistency mismatches={mismatch_count}",
                detail={
                    "snapshot_as_of_date": _normalize_date_value(row["as_of_date"]),
                    "selection_max_date": _normalize_date_value(selection_max),
                    "snapshot_evaluation_date": _normalize_date_value(
                        row["latest_evaluation_date"]
                    ),
                    "evaluation_max_date": _normalize_date_value(evaluation_max),
                    "snapshot_portfolio_date": _normalize_date_value(
                        row["latest_portfolio_as_of_date"]
                    ),
                    "portfolio_max_date": _normalize_date_value(portfolio_max),
                    "latest_report_bundle_id": row["latest_report_bundle_id"],
                    "source_mismatch": source_mismatch,
                },
                remediation="snapshot builder가 source max date / latest report와 같은 truth를 보도록 수정하세요."
                if mismatch_count
                else "none",
                priority="P0" if mismatch_count else "INFO",
                ticket_id="T013",
            )
        )

        effective_meta_ids = {
            str(active_id)
            for (active_id,) in connection.execute(
                "SELECT active_meta_model_id FROM vw_latest_intraday_active_meta_model",
            ).fetchall()
        }
        snapshot_meta_ids = {
            str(item["active_meta_model_id"])
            for item in json.loads(row["active_meta_model_ids_json"] or "[]")
            if item.get("active_meta_model_id")
        }
        invalid_meta_ids = sorted(snapshot_meta_ids - effective_meta_ids)
        results.append(
            AuditCheckResult(
                check_id="latest:active_meta_ids:effective_consistency",
                category="latest_layer",
                target="fact_latest_app_snapshot",
                status=PASS if not invalid_meta_ids else FAIL,
                summary=f"active meta ids invalid={len(invalid_meta_ids)}",
                detail={
                    "snapshot_meta_ids": sorted(snapshot_meta_ids),
                    "effective_meta_ids": sorted(effective_meta_ids),
                    "invalid_meta_ids": invalid_meta_ids,
                },
                remediation="future-effective 또는 inactive meta model id가 snapshot에 들어가지 않게 snapshot builder를 점검하세요."
                if invalid_meta_ids
                else "none",
                priority="P0" if invalid_meta_ids else "INFO",
                ticket_id="T013",
            )
        )

        registry_checks = [
            (
                "active_intraday_policy_id",
                row["active_intraday_policy_id"],
                "vw_latest_intraday_active_policy",
                "active_policy_id",
            ),
            (
                "active_portfolio_policy_id",
                row["active_portfolio_policy_id"],
                "vw_latest_portfolio_policy_registry",
                "active_portfolio_policy_id",
            ),
            (
                "active_ops_policy_id",
                row["active_ops_policy_id"],
                "vw_latest_active_ops_policy",
                "policy_id",
            ),
        ]
        for field_name, value, view_name, key_name in registry_checks:
            if value is None:
                status = PASS
                detail = {"value": None, "reason": "no active entry referenced"}
            else:
                exists = int(
                    _scalar(
                        connection,
                        f"SELECT COUNT(*) FROM {view_name} WHERE {key_name} = ?",
                        [value],
                    )
                    or 0
                )
                status = PASS if exists else FAIL
                detail = {"value": value, "view_name": view_name, "exists": bool(exists)}
            results.append(
                AuditCheckResult(
                    check_id=f"latest:{field_name}:active_consistency",
                    category="latest_layer",
                    target=field_name,
                    status=status,
                    summary=f"{field_name} consistency={'ok' if status == PASS else 'mismatch'}",
                    detail=detail,
                    remediation="active registry와 latest snapshot의 effective filter를 일치시키세요."
                    if status == FAIL
                    else "none",
                    priority="P0" if status == FAIL else "INFO",
                    ticket_id="T013",
                )
            )

    freshness_rows = connection.execute(
        """
        SELECT page_name, dataset_name, latest_available_ts, warning_level
        FROM vw_latest_ui_data_freshness_snapshot
        """
    ).fetchall()
    trading_day_aware_names = {
        "selection_v2",
        "market_regime",
        "alpha_prediction",
        "target_book",
        "nav_snapshot",
        "evaluation_summary",
        "calibration",
        "selection_outcome",
    }
    false_critical_rows: list[dict[str, Any]] = []
    for page_name, dataset_name, latest_available_ts, warning_level in freshness_rows:
        if str(dataset_name) not in trading_day_aware_names or latest_available_ts is None:
            continue
        latest_dt = (
            latest_available_ts
            if isinstance(latest_available_ts, datetime)
            else datetime.fromisoformat(str(latest_available_ts))
        )
        latest_date = latest_dt.astimezone().date()
        if latest_date >= expected_trading_date and str(warning_level).upper() == "CRITICAL":
            false_critical_rows.append(
                {
                    "page_name": page_name,
                    "dataset_name": dataset_name,
                    "latest_date": str(latest_date),
                    "expected_trading_date": str(expected_trading_date),
                    "warning_level": warning_level,
                }
            )
    results.append(
        AuditCheckResult(
            check_id="latest:freshness:weekend_holiday_classification",
            category="latest_layer",
            target="fact_ui_data_freshness_snapshot",
            status=PASS if not false_critical_rows else FAIL,
            summary=f"weekend/holiday false critical rows={len(false_critical_rows)}",
            detail={
                "expected_trading_date": str(expected_trading_date),
                "rows": false_critical_rows,
            },
            remediation="trading calendar aware freshness classification을 다시 맞추세요."
            if false_critical_rows
            else "none",
            priority="P0" if false_critical_rows else "INFO",
            ticket_id="T013",
        )
    )
    return AuditSuiteResult(tuple(results))


def run_artifact_reference_checks(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
) -> AuditSuiteResult:
    results: list[AuditCheckResult] = []
    rows = connection.execute(
        """
        SELECT report_type, artifact_path, summary_json, status, run_id
        FROM vw_latest_report_index
        ORDER BY report_type
        """
    ).fetchall()
    protected_paths = _latest_referenced_artifact_paths(connection, settings)
    for report_type, artifact_path, summary_json, status, run_id in rows:
        resolved_preview_path = resolve_artifact_path(settings, artifact_path)
        preview_exists = resolved_preview_path is not None
        payload_path = _report_payload_path(summary_json)
        resolved_payload_path = resolve_artifact_path(settings, payload_path)
        payload_exists = payload_path is None or resolved_payload_path is not None
        relative_preview = None
        relative_payload = None
        if resolved_preview_path is not None:
            relative_preview = _safe_relative_path(
                resolved_preview_path,
                settings.paths.project_root,
            )
        if resolved_payload_path is not None:
            relative_payload = _safe_relative_path(
                resolved_payload_path,
                settings.paths.project_root,
            )
        cleanup_safe = (relative_preview in protected_paths) and (
            relative_payload is None or relative_payload in protected_paths
        )
        if not preview_exists or not payload_exists:
            result_status = FAIL
            priority = "P0"
        elif not cleanup_safe:
            result_status = WARN
            priority = "P1"
        else:
            result_status = PASS
            priority = "INFO"
        results.append(
            AuditCheckResult(
                check_id=f"artifact:{report_type}",
                category="artifact_integrity",
                target=str(report_type),
                status=result_status,
                summary=f"preview_exists={preview_exists} payload_exists={payload_exists} cleanup_safe={cleanup_safe}",
                detail={
                    "artifact_path": str(resolved_preview_path) if resolved_preview_path is not None else artifact_path,
                    "payload_path": str(resolved_payload_path) if resolved_payload_path is not None else payload_path,
                    "run_id": run_id,
                    "status": status,
                    "cleanup_safe": cleanup_safe,
                },
                remediation=(
                    "누락된 preview/payload artifact를 다시 렌더링하세요."
                    if result_status == FAIL
                    else "latest referenced artifact가 retention 보호 대상에 포함되도록 유지하세요."
                    if result_status == WARN
                    else "none"
                ),
                priority=priority,
                ticket_id="T013",
            )
        )
    duplicate_report_keys = int(
        _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM (
                SELECT report_key, COUNT(*) AS row_count
                FROM fact_latest_report_index
                GROUP BY report_key
                HAVING COUNT(*) > 1
            )
            """,
        )
        or 0
    )
    results.append(
        AuditCheckResult(
            check_id="artifact:report_index:duplicate_report_key",
            category="artifact_integrity",
            target="fact_latest_report_index",
            status=PASS if duplicate_report_keys == 0 else WARN,
            summary=f"duplicate report_key groups={duplicate_report_keys}",
            detail={"duplicate_groups": duplicate_report_keys},
            remediation="report_key 설계와 rerun 시 report index key 충돌 여부를 점검하세요."
            if duplicate_report_keys
            else "none",
            priority="P1" if duplicate_report_keys else "INFO",
            ticket_id="T013",
        )
    )
    return AuditSuiteResult(tuple(results))


def run_ticket_coverage_checks(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
) -> AuditSuiteResult:
    results: list[AuditCheckResult] = []
    registry = _object_registry(connection)
    for item in get_ticket_checklist():
        missing_required_objects = [name for name in item.required_objects if name not in registry]
        missing_required_files = [
            relative
            for relative in item.required_files
            if not (settings.paths.project_root / relative).exists()
        ]
        missing_optional_objects = [name for name in item.optional_objects if name not in registry]
        missing_optional_files = [
            relative
            for relative in item.optional_files
            if not (settings.paths.project_root / relative).exists()
        ]
        if missing_required_objects or missing_required_files:
            status = FAIL
            priority = "P0"
        elif missing_optional_objects or missing_optional_files:
            status = WARN
            priority = "P1"
        else:
            status = PASS
            priority = "INFO"
        results.append(
            AuditCheckResult(
                check_id=f"ticket:{item.ticket_id}",
                category="ticket_coverage",
                target=item.ticket_id,
                status=status,
                summary=item.title,
                detail={
                    "required_objects": list(item.required_objects),
                    "missing_required_objects": missing_required_objects,
                    "required_files": list(item.required_files),
                    "missing_required_files": missing_required_files,
                    "optional_objects": list(item.optional_objects),
                    "missing_optional_objects": missing_optional_objects,
                    "optional_files": list(item.optional_files),
                    "missing_optional_files": missing_optional_files,
                },
                remediation=(
                    "필수 객체/파일 누락을 우선 복구하세요."
                    if status == FAIL
                    else "optional 항목을 backlog로 추적하세요."
                    if status == WARN
                    else "none"
                ),
                priority=priority,
                ticket_id=item.ticket_id,
            )
        )
    return AuditSuiteResult(tuple(results))


def run_full_audit(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    snapshot_ts: datetime | None = None,
) -> AuditSuiteResult:
    suites = [
        run_contract_checks(settings, connection=connection),
        run_latest_layer_checks(settings, connection=connection, snapshot_ts=snapshot_ts),
        run_artifact_reference_checks(settings, connection=connection),
        run_ticket_coverage_checks(settings, connection=connection),
    ]
    combined: list[AuditCheckResult] = []
    for suite in suites:
        combined.extend(suite.results)
    return AuditSuiteResult(tuple(combined))
