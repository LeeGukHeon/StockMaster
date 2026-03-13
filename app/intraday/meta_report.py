from __future__ import annotations

# ruff: noqa: E501
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.discord import (
    DiscordPublishDecision,
    publish_discord_messages,
    resolve_discord_publish_decision,
)
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.reports.discord_eod import _build_payload_messages
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .meta_common import INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION


@dataclass(slots=True)
class IntradayMetaModelReportResult:
    run_id: str
    as_of_date: date
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayMetaSummaryPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _build_content(
    *,
    as_of_date: date,
    training_runs: pd.DataFrame,
    active_registry: pd.DataFrame,
    overlay_metrics: pd.DataFrame,
) -> str:
    lines = [f"**StockMaster Intraday Meta-Model | {as_of_date.isoformat()}**", ""]
    lines.append(
        "This layer is a bounded ML overlay on top of active intraday policy. "
        "It does not replace candidate selection or policy hard guards."
    )
    lines.append("")
    lines.append("**Latest Training Runs**")
    if training_runs.empty:
        lines.append("- no intraday meta training runs")
    else:
        for row in training_runs.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} {row.panel_name}: train_end={row.train_end_date} "
                f"fallback={bool(row.fallback_flag)} rows={int(row.train_row_count or 0)}/{int(row.validation_row_count or 0)}"
            )
    lines.append("")
    lines.append("**Active Meta Registry**")
    if active_registry.empty:
        lines.append("- no active intraday meta registry rows")
    else:
        for row in active_registry.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} {row.panel_name}: training_run={row.training_run_id} "
                f"effective_from={row.effective_from_date}"
            )
    lines.append("")
    lines.append("**Overlay Evaluation Snapshot**")
    if overlay_metrics.empty:
        lines.append("- no overlay evaluation rows")
    else:
        for row in overlay_metrics.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} {row.panel_name}: "
                f"lift={float(row.same_exit_lift_mean_excess_return or 0.0):+.4f} "
                f"override_rate={float(row.override_rate or 0.0):.2%} "
                f"fallback_rate={float(row.fallback_rate or 0.0):.2%}"
            )
    return "\n".join(lines)


def render_intraday_meta_model_report(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayMetaModelReportResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_intraday_meta_model_report",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=[
                    "fact_model_training_run",
                    "fact_model_metric_summary",
                    "fact_intraday_active_meta_model",
                ],
                notes=f"Render intraday meta-model report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                training_runs = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_model_training_run
                    WHERE model_domain = ?
                      AND model_version = ?
                      AND horizon IN ({placeholders})
                    ORDER BY train_end_date DESC, horizon, panel_name
                    """,
                    [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, *horizons],
                ).fetchdf()
                active_registry = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_intraday_active_meta_model
                    WHERE horizon IN ({placeholders})
                    ORDER BY horizon, panel_name
                    """,
                    [*horizons],
                ).fetchdf()
                overlay_metrics = connection.execute(
                    f"""
                    SELECT
                        horizon,
                        panel_name,
                        MAX(CASE WHEN metric_name = 'same_exit_lift_mean_excess_return' THEN metric_value END) AS same_exit_lift_mean_excess_return,
                        MAX(CASE WHEN metric_name = 'override_rate' THEN metric_value END) AS override_rate,
                        MAX(CASE WHEN metric_name = 'fallback_rate' THEN metric_value END) AS fallback_rate
                    FROM fact_model_metric_summary
                    WHERE model_domain = ?
                      AND model_version = ?
                      AND split_name = 'evaluation'
                      AND metric_scope = 'overlay'
                      AND comparison_key = 'overall'
                      AND horizon IN ({placeholders})
                    GROUP BY horizon, panel_name
                    ORDER BY horizon, panel_name
                    """,
                    [INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION, *horizons],
                ).fetchdf()
                content = _build_content(
                    as_of_date=as_of_date,
                    training_runs=training_runs,
                    active_registry=active_registry,
                    overlay_metrics=overlay_metrics,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=as_of_date,
                    content=content,
                )
                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "intraday_meta_model"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "username": settings.discord.username,
                    "message_count": len(messages),
                    "messages": messages,
                    "dry_run": dry_run,
                }
                payload_path = artifact_dir / "intraday_meta_model_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "intraday_meta_model_preview.md"
                preview_path.write_text(content, encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    "Intraday meta-model report rendered. "
                    f"dry_run={dry_run} message_count={len(messages)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaModelReportResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta-model report rendering failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def publish_discord_intraday_meta_summary(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayMetaSummaryPublishResult:
    render_result = render_intraday_meta_model_report(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        dry_run=dry_run,
    )
    payload_path = next(
        (Path(path) for path in render_result.artifact_paths if path.endswith(".json")),
        None,
    )
    payload = json.loads(payload_path.read_text(encoding="utf-8")) if payload_path else {"messages": []}
    published = False
    decision = resolve_discord_publish_decision(
        enabled=settings.discord.enabled,
        webhook_url=settings.discord.webhook_url,
        dry_run=dry_run,
    )
    if decision == DiscordPublishDecision.PUBLISH:
        publish_discord_messages(
            settings.discord.webhook_url,
            list(payload["messages"]),
            timeout=15.0,
        )
        published = True
        notes = "Intraday meta-model Discord summary published."
    elif decision == DiscordPublishDecision.SKIP_DISABLED:
        notes = "Intraday meta-model Discord summary skipped. DISCORD_REPORT_ENABLED=false."
    elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
        notes = "Intraday meta-model Discord summary skipped. Webhook URL is not configured."
    else:
        notes = "Intraday meta-model Discord summary dry-run completed."
    return IntradayMetaSummaryPublishResult(
        run_id=render_result.run_id,
        as_of_date=as_of_date,
        dry_run=dry_run,
        published=published,
        artifact_paths=render_result.artifact_paths,
        notes=notes,
    )
