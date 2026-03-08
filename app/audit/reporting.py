# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from app.audit.checks import AuditCheckResult, AuditSuiteResult
from app.audit.contracts import TableContract, get_ticket_checklist, representative_contracts
from app.audit.remediation import RUNBOOK_CASES
from app.settings import Settings


def _markdown_table(headers: list[str], rows: Iterable[list[str]]) -> str:
    all_rows = list(rows)
    if not all_rows:
        return "_없음_\n"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in all_rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def _write_markdown(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _result_rows(results: Iterable[AuditCheckResult]) -> list[list[str]]:
    return [
        [
            result.status,
            result.priority,
            result.category,
            result.target,
            result.summary.replace("\n", " "),
            result.remediation.replace("\n", " "),
        ]
        for result in results
    ]


def render_contract_matrix(contracts: tuple[TableContract, ...] | None = None) -> str:
    resolved_contracts = contracts or representative_contracts()
    rows = []
    for contract in resolved_contracts:
        rows.append(
            [
                contract.name,
                contract.object_type,
                contract.grain,
                ", ".join(contract.unique_key),
                ", ".join(contract.required_columns),
                ", ".join(contract.lineage_columns),
                contract.rerun_rule,
                contract.freshness_column or "-",
                ", ".join(contract.ticket_ids),
            ]
        )
    body = _markdown_table(
        [
            "객체",
            "유형",
            "grain",
            "unique key",
            "required columns",
            "lineage",
            "rerun rule",
            "freshness",
            "tickets",
        ],
        rows,
    )
    return "# DB Contract Matrix\n\n" + body


def render_audit_status(
    settings: Settings,
    audit_result: AuditSuiteResult,
    *,
    generated_at: datetime,
) -> str:
    grouped = {
        "contract": [item for item in audit_result.results if item.category == "contract"],
        "latest_layer": [item for item in audit_result.results if item.category == "latest_layer"],
        "artifact_integrity": [
            item for item in audit_result.results if item.category == "artifact_integrity"
        ],
        "ticket_coverage": [
            item for item in audit_result.results if item.category == "ticket_coverage"
        ],
    }
    parts = [
        "# AUDIT T000-T013 STATUS",
        "",
        f"- generated_at: `{generated_at.isoformat()}`",
        f"- env: `{settings.app.env}`",
        f"- duckdb_path: `{settings.paths.duckdb_path}`",
        f"- PASS: `{audit_result.pass_count}`",
        f"- WARN: `{audit_result.warn_count}`",
        f"- FAIL: `{audit_result.fail_count}`",
        "",
    ]
    for category, results in grouped.items():
        parts.extend(
            [
                f"## {category}",
                "",
                _markdown_table(
                    ["status", "priority", "target", "summary", "remediation"],
                    [
                        [
                            item.status,
                            item.priority,
                            item.target,
                            item.summary.replace("\n", " "),
                            item.remediation.replace("\n", " "),
                        ]
                        for item in results
                    ],
                ),
            ]
        )
    return "\n".join(parts).strip() + "\n"


def render_gap_backlog(audit_result: AuditSuiteResult) -> str:
    unresolved = [item for item in audit_result.results if item.status != "PASS"]
    parts = [
        "# Gap Remediation Backlog",
        "",
        "## Unresolved Items",
        "",
        _markdown_table(
            ["priority", "status", "category", "target", "summary", "remediation"],
            _result_rows(unresolved),
        ),
        "",
        "## Ticket Coverage Snapshot",
        "",
        _markdown_table(
            ["ticket", "status", "summary"],
            [
                [item.target, item.status, item.summary]
                for item in audit_result.results
                if item.category == "ticket_coverage"
            ],
        ),
    ]
    return "\n".join(parts).strip() + "\n"


def render_case_runbook() -> str:
    parts = ["# CASE RUNBOOK T000-T013", ""]
    parts.append(
        _markdown_table(
            ["case_id", "symptom", "detection", "immediate action", "follow up"],
            [
                [
                    case.case_id,
                    case.symptom,
                    case.detection_hint,
                    case.immediate_action,
                    case.follow_up,
                ]
                for case in RUNBOOK_CASES
            ],
        )
    )
    parts.extend(
        [
            "",
            "## Ticket Checklist Scope",
            "",
            _markdown_table(
                ["ticket", "title", "required objects", "required files"],
                [
                    [
                        item.ticket_id,
                        item.title,
                        ", ".join(item.required_objects),
                        ", ".join(item.required_files),
                    ]
                    for item in get_ticket_checklist()
                ],
            ),
        ]
    )
    return "\n".join(parts).strip() + "\n"


def write_audit_docs(
    settings: Settings,
    *,
    audit_result: AuditSuiteResult,
    generated_at: datetime,
) -> list[Path]:
    project_root = settings.paths.project_root
    return [
        _write_markdown(
            project_root / "docs/DB_CONTRACT_MATRIX.md",
            render_contract_matrix(),
        ),
        _write_markdown(
            project_root / "docs/AUDIT_T000_T013_STATUS.md",
            render_audit_status(settings, audit_result, generated_at=generated_at),
        ),
        _write_markdown(
            project_root / "docs/GAP_REMEDIATION_BACKLOG.md",
            render_gap_backlog(audit_result),
        ),
        _write_markdown(
            project_root / "docs/CASE_RUNBOOK_T000_T013.md",
            render_case_runbook(),
        ),
    ]


def write_audit_artifacts(
    settings: Settings,
    *,
    audit_result: AuditSuiteResult,
    generated_at: datetime,
) -> list[Path]:
    run_id = f"audit-t000-t013-{generated_at.strftime('%Y%m%dT%H%M%S')}"
    artifact_dir = settings.paths.artifacts_dir / "audit" / "t000_t013" / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "generated_at": generated_at.isoformat(),
        "pass_count": audit_result.pass_count,
        "warn_count": audit_result.warn_count,
        "fail_count": audit_result.fail_count,
        "results": [asdict(item) for item in audit_result.results],
    }
    payload_path = artifact_dir / "audit_summary_payload.json"
    payload_path.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    preview_path = artifact_dir / "audit_summary_preview.md"
    preview_path.write_text(
        render_audit_status(settings, audit_result, generated_at=generated_at),
        encoding="utf-8",
    )
    return [preview_path, payload_path]
