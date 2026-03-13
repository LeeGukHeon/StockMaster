from __future__ import annotations

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


@dataclass(slots=True)
class IntradayPolicyResearchReportResult:
    run_id: str
    as_of_date: date
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPolicySummaryPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _build_content(
    *,
    as_of_date: date,
    experiments: pd.DataFrame,
    recommendations: pd.DataFrame,
    active_policy: pd.DataFrame,
    ablation: pd.DataFrame,
) -> str:
    lines = [f"**StockMaster Intraday Policy Research | {as_of_date.isoformat()}**", ""]
    lines.append(
        "This layer calibrates deterministic intraday timing policy parameters "
        "on matured outcomes only. It is not an auto-promotion engine and "
        "does not place orders."
    )
    lines.append("")
    lines.append("**Latest Experiment Runs**")
    if experiments.empty:
        lines.append("- no policy experiment runs")
    else:
        for row in experiments.itertuples(index=False):
            lines.append(
                f"- {row.experiment_type} H{int(row.horizon) if row.horizon is not None else '-'} "
                f"status={row.status} candidates={int(row.candidate_count or 0)} "
                f"selected={row.selected_policy_candidate_id or '-'}"
            )
    lines.append("")
    lines.append("**Recommendations**")
    if recommendations.empty:
        lines.append("- no recommendation rows")
    else:
        for row in recommendations.itertuples(index=False):
            manual_review = (
                bool(row.manual_review_required_flag)
                if pd.notna(row.manual_review_required_flag)
                else False
            )
            lines.append(
                f"- H{int(row.horizon)} {row.scope_type}/{row.scope_key}: "
                f"{row.policy_candidate_id} objective={float(row.objective_score or 0.0):+.2f} "
                f"manual_review={manual_review} "
                f"fallback={row.fallback_scope_type or '-'}"
            )
    lines.append("")
    lines.append("**Active Policy Registry**")
    if active_policy.empty:
        lines.append("- no active policy rows")
    else:
        for row in active_policy.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} {row.scope_type}/{row.scope_key}: "
                f"{row.policy_candidate_id} template={row.template_id} "
                f"effective_from={row.effective_from_date}"
            )
    lines.append("")
    lines.append("**Ablation Snapshot**")
    if ablation.empty:
        lines.append("- no ablation result rows")
    else:
        for row in ablation.itertuples(index=False):
            lines.append(
                f"- H{int(row.horizon)} {row.ablation_name}: "
                f"objective_delta={float(row.objective_score_delta or 0.0):+.2f} "
                f"edge_delta_bps={float(row.mean_timing_edge_vs_open_bps_delta or 0.0):+.1f}"
            )
    return "\n".join(lines)


def render_intraday_policy_research_report(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayPolicyResearchReportResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_intraday_policy_research_report",
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
                    "fact_intraday_policy_experiment_run",
                    "fact_intraday_policy_selection_recommendation",
                    "fact_intraday_active_policy",
                    "fact_intraday_policy_ablation_result",
                ],
                notes=f"Render intraday policy research report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                experiments = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_intraday_policy_experiment_run
                    WHERE horizon IS NULL OR horizon IN ({placeholders})
                    ORDER BY experiment_type, horizon
                    """,
                    [*horizons],
                ).fetchdf()
                recommendations = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_intraday_policy_selection_recommendation
                    WHERE recommendation_date <= ?
                      AND horizon IN ({placeholders})
                      AND recommendation_rank = 1
                    ORDER BY horizon, scope_type, scope_key
                    """,
                    [as_of_date, *horizons],
                ).fetchdf()
                active_policy = connection.execute(
                    f"""
                    SELECT
                        active.*,
                        candidate.template_id
                    FROM fact_intraday_active_policy AS active
                    JOIN fact_intraday_policy_candidate AS candidate
                      ON active.policy_candidate_id = candidate.policy_candidate_id
                    WHERE active.effective_from_date <= ?
                      AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
                      AND active.horizon IN ({placeholders})
                    ORDER BY
                        active.horizon,
                        active.scope_type,
                        active.scope_key,
                        active.effective_from_date DESC
                    """,
                    [as_of_date, as_of_date, *horizons],
                ).fetchdf()
                ablation = connection.execute(
                    f"""
                    SELECT *
                    FROM vw_latest_intraday_policy_ablation_result
                    WHERE horizon IN ({placeholders})
                    ORDER BY horizon, ablation_name
                    """,
                    [*horizons],
                ).fetchdf()
                content = _build_content(
                    as_of_date=as_of_date,
                    experiments=experiments,
                    recommendations=recommendations,
                    active_policy=active_policy,
                    ablation=ablation,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=as_of_date,
                    content=content,
                )
                payload = {
                    "username": settings.discord.username,
                    "content": messages[0]["content"] if messages else "",
                    "message_count": len(messages),
                    "messages": messages,
                }
                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "intraday_policy_research"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "intraday_policy_research_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "intraday_policy_research_preview.md"
                preview_path.write_text(content, encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    "Intraday policy research report rendered. "
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
                return IntradayPolicyResearchReportResult(
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
                    notes="Intraday policy research report rendering failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def publish_discord_intraday_policy_summary(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayPolicySummaryPublishResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "publish_discord_intraday_policy_summary",
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
                input_sources=["render_intraday_policy_research_report"],
                notes=f"Publish intraday policy summary for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
        webhook_url = settings.discord.webhook_url
        published = False
        notes = f"Intraday policy summary publish skipped for {as_of_date.isoformat()}."
        manifest_status = "failed"
        error_message: str | None = None
        artifact_paths: list[str] = []
        try:
            render_result = render_intraday_policy_research_report(
                settings,
                as_of_date=as_of_date,
                horizons=horizons,
                dry_run=dry_run,
            )
            artifact_paths = list(render_result.artifact_paths)
            payload_path = next(
                path for path in render_result.artifact_paths if path.endswith("_payload.json")
            )
            payload = json.loads(Path(payload_path).read_text(encoding="utf-8"))
            decision = resolve_discord_publish_decision(
                enabled=settings.discord.enabled,
                webhook_url=webhook_url,
                dry_run=dry_run,
            )
            if decision == DiscordPublishDecision.SKIP_DISABLED:
                notes = (
                    f"Intraday policy summary publish skipped for {as_of_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_DRY_RUN:
                notes = (
                    f"Intraday policy summary publish dry-run completed for {as_of_date.isoformat()}."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
                notes = (
                    f"Intraday policy summary publish skipped for {as_of_date.isoformat()}. "
                    "Webhook URL is not configured."
                )
                manifest_status = "skipped"
            else:
                publish_discord_messages(
                    webhook_url,
                    list(payload.get("messages", [])),
                    timeout=15.0,
                )
                published = True
                manifest_status = "success"
                notes = f"Intraday policy summary published for {as_of_date.isoformat()}."
        except Exception as exc:
            notes = f"Intraday policy summary publish failed for {as_of_date.isoformat()}."
            error_message = str(exc)
            manifest_status = "failed"
            raise
        finally:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status=manifest_status,
                    output_artifacts=artifact_paths,
                    notes=notes,
                    error_message=error_message,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
        return IntradayPolicySummaryPublishResult(
            run_id=run_context.run_id,
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
