# ruff: noqa: E402, E501

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.common import JobStatus, OpsJobResult
from app.providers.krx.registry import KRX_SERVICE_REGISTRY
from app.storage.duckdb import bootstrap_core_tables
from scripts._krx_cli import base_parser, load_cli_settings, resolve_cli_as_of_date
from scripts._ops_cli import run_standalone_job


def build_parser() -> argparse.ArgumentParser:
    parser = base_parser("Render a KRX service status report from persisted status tables.")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _build_markdown(as_of_date, status_frame, budget_frame, registry_frame, attribution_frame) -> str:
    lines = ["# KRX Service Status Report", "", f"- as_of_date: {as_of_date.isoformat()}", ""]
    lines.append("일부 시장 통계는 한국거래소 통계정보를 사용합니다.")
    lines.append("")
    lines.append("## Allowed Registry")
    if registry_frame.empty:
        lines.append("- no registry rows")
    else:
        for row in registry_frame.itertuples(index=False):
            lines.append(
                f"- {row.service_slug}: {row.display_name_ko} "
                f"category={row.category} usage={row.expected_usage} url={row.endpoint_url}"
            )
    lines.append("")
    lines.append("## Latest Service Status")
    if status_frame.empty:
        lines.append("- no KRX smoke status rows")
    else:
        for row in status_frame.itertuples(index=False):
            lines.append(
                f"- {row.service_slug}: status={row.last_smoke_status} "
                f"http={row.last_http_status or '-'} fallback={row.fallback_mode or '-'} "
                f"last_success={row.last_success_ts or '-'}"
            )
    lines.append("")
    lines.append("## Request Budget")
    if budget_frame.empty:
        lines.append("- no budget snapshot")
    else:
        row = budget_frame.iloc[0]
        lines.append(
            f"- budget={int(row['request_budget'])} used={int(row['requests_used'])} "
            f"usage_ratio={float(row['usage_ratio']):.2%} throttle_state={row['throttle_state']}"
        )
    lines.append("")
    lines.append("## Source Attribution Snapshot")
    if attribution_frame.empty:
        lines.append("- no attribution rows")
    else:
        for row in attribution_frame.itertuples(index=False):
            lines.append(
                f"- {row.page_slug}/{row.component_slug}: source={row.source_label} "
                f"active={bool(row.active_flag)}"
            )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    settings = load_cli_settings()
    as_of_date = resolve_cli_as_of_date(settings, args.as_of_date)

    def _runner(connection, job) -> OpsJobResult:
        bootstrap_core_tables(connection)
        status_frame = connection.execute(
            """
            SELECT service_slug, display_name_ko, last_smoke_status, last_success_ts,
                   last_http_status, fallback_mode
            FROM vw_latest_krx_service_status
            ORDER BY display_name_ko
            """
        ).fetchdf()
        budget_frame = connection.execute(
            """
            SELECT date_kst, request_budget, requests_used, usage_ratio, throttle_state, snapshot_ts
            FROM vw_latest_external_api_budget_snapshot
            WHERE provider_name = 'krx'
            ORDER BY date_kst DESC, snapshot_ts DESC
            LIMIT 1
            """
        ).fetchdf()
        attribution_frame = connection.execute(
            """
            SELECT page_slug, component_slug, source_label, active_flag, snapshot_ts
            FROM vw_latest_source_attribution_snapshot
            WHERE provider_name = 'krx'
            ORDER BY page_slug, component_slug
            """
        ).fetchdf()
        registry_frame = pd.DataFrame(
            [
                {
                    "service_slug": item.service_slug,
                    "display_name_ko": item.display_name_ko,
                    "category": item.category,
                    "expected_usage": item.expected_usage,
                    "endpoint_url": item.endpoint_url,
                }
                for item in KRX_SERVICE_REGISTRY
                if item.service_slug in set(settings.providers.krx.allowed_services)
            ]
        )
        content = _build_markdown(
            as_of_date,
            status_frame=status_frame,
            budget_frame=budget_frame,
            registry_frame=registry_frame,
            attribution_frame=attribution_frame,
        )
        artifact_dir = (
            settings.paths.artifacts_dir
            / "krx_service_status_report"
            / f"as_of_date={as_of_date.isoformat()}"
            / job.run_id
        )
        artifact_dir.mkdir(parents=True, exist_ok=True)
        preview_path = artifact_dir / "krx_service_status_report_preview.md"
        preview_path.write_text(content, encoding="utf-8")
        payload_path = artifact_dir / "krx_service_status_report_payload.json"
        payload_path.write_text(
            json.dumps(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "allowed_services": settings.providers.krx.allowed_services,
                    "dry_run": args.dry_run,
                    "latest_budget_snapshot": budget_frame.to_dict("records"),
                    "latest_service_status": status_frame.to_dict("records"),
                    "latest_attribution": attribution_frame.to_dict("records"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        return OpsJobResult(
            run_id=job.run_id,
            job_name="render_krx_service_status_report",
            status=JobStatus.SUCCESS,
            notes=f"KRX service status report rendered. dry_run={args.dry_run}",
            artifact_paths=[str(preview_path), str(payload_path)],
            as_of_date=as_of_date,
        )

    result = run_standalone_job(
        settings,
        job_name="render_krx_service_status_report",
        as_of_date=as_of_date,
        dry_run=args.dry_run,
        policy_config_path=None,
        runner=_runner,
    )
    print(
        "KRX service status report rendered. "
        f"run_id={result.run_id} status={result.status} artifacts={len(result.artifact_paths)}"
    )
    return 0 if result.status != JobStatus.FAILED else 1


if __name__ == "__main__":
    raise SystemExit(main())
