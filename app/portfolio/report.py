from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.reports.discord_eod import _build_payload_messages
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .common import PortfolioPublishResult, PortfolioReportResult


def _portfolio_report_content(
    *,
    as_of_date: date,
    active_policy: pd.DataFrame,
    target_book: pd.DataFrame,
    rebalance: pd.DataFrame,
    nav: pd.DataFrame,
    evaluation: pd.DataFrame,
) -> str:
    lines = [f"**StockMaster Portfolio Report | {as_of_date.isoformat()}**", ""]
    lines.append(
        "This layer is a deterministic long-only portfolio proposal downstream of "
        "selection v2 and the intraday timing overlay. It does not place orders."
    )
    lines.append("")
    lines.append("**Active Policy**")
    if active_policy.empty:
        lines.append("- active policy not frozen, using config fallback if present")
    else:
        for row in active_policy.itertuples(index=False):
            lines.append(
                f"- {row.display_name or row.portfolio_policy_id} "
                f"({row.portfolio_policy_version}) effective_from={row.effective_from_date}"
            )
    lines.append("")
    lines.append("**Target Holdings**")
    if target_book.empty:
        lines.append("- no target holdings")
    else:
        for row in target_book.head(8).itertuples(index=False):
            lines.append(
                f"- {row.symbol} {row.company_name or ''}: "
                f"weight={float(row.target_weight or 0.0):.2%} "
                f"shares={int(row.target_shares or 0)} gate={row.gate_status}"
            )
    lines.append("")
    lines.append("**Rebalance Monitor**")
    if rebalance.empty:
        lines.append("- no rebalance rows")
    else:
        for row in rebalance.head(8).itertuples(index=False):
            lines.append(
                f"- {row.rebalance_action} {row.symbol}: "
                f"delta_shares={int(row.delta_shares or 0)} "
                f"cash_delta={float(row.cash_delta or 0.0):,.0f}"
            )
    lines.append("")
    lines.append("**Latest NAV**")
    if nav.empty:
        lines.append("- no nav snapshots")
    else:
        for row in nav.head(4).itertuples(index=False):
            lines.append(
                f"- {row.execution_mode}: nav={float(row.nav_value or 0.0):,.0f} "
                f"cumret={float(row.cumulative_return or 0.0):+.2%} "
                f"drawdown={float(row.drawdown or 0.0):+.2%}"
            )
    lines.append("")
    lines.append("**Evaluation Snapshot**")
    if evaluation.empty:
        lines.append("- no evaluation summary rows")
    else:
        grouped = evaluation.groupby("comparison_key", sort=True)
        for comparison_key, part in grouped:
            metric_map = {
                row.metric_name: row.metric_value for row in part.itertuples(index=False)
            }
            lines.append(
                f"- {comparison_key}: "
                f"cumret={float(metric_map.get('cumulative_return') or 0.0):+.2%} "
                f"vol={float(metric_map.get('annualized_volatility') or 0.0):.2%} "
                f"sharpe_like={float(metric_map.get('sharpe_like_ratio') or 0.0):+.2f}"
            )
    return "\n".join(lines)


def render_portfolio_report(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> PortfolioReportResult:
    ensure_storage_layout(settings)
    with activate_run_context("render_portfolio_report", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=[
                    "fact_portfolio_policy_registry",
                    "fact_portfolio_target_book",
                    "fact_portfolio_rebalance_plan",
                    "fact_portfolio_nav_snapshot",
                    "fact_portfolio_evaluation_summary",
                ],
                notes=f"Render portfolio report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                active_policy = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_policy_registry
                    WHERE effective_from_date <= ?
                      AND (effective_to_date IS NULL OR effective_to_date >= ?)
                      AND active_flag = TRUE
                    ORDER BY effective_from_date DESC
                    """,
                    [as_of_date, as_of_date],
                ).fetchdf()
                target_book = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_target_book
                    WHERE as_of_date = ?
                      AND target_weight > 0
                    ORDER BY execution_mode, target_rank
                    """,
                    [as_of_date],
                ).fetchdf()
                rebalance = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_rebalance_plan
                    WHERE as_of_date = ?
                    ORDER BY execution_mode, action_sequence, symbol
                    """,
                    [as_of_date],
                ).fetchdf()
                nav = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_nav_snapshot
                    WHERE snapshot_date <= ?
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY execution_mode
                        ORDER BY snapshot_date DESC, created_at DESC
                    ) = 1
                    """,
                    [as_of_date],
                ).fetchdf()
                evaluation = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_evaluation_summary
                    WHERE evaluation_date <= ?
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY execution_mode, comparison_key, metric_name
                        ORDER BY evaluation_date DESC, created_at DESC
                    ) = 1
                    """,
                    [as_of_date],
                ).fetchdf()
                content = _portfolio_report_content(
                    as_of_date=as_of_date,
                    active_policy=active_policy,
                    target_book=target_book,
                    rebalance=rebalance,
                    nav=nav,
                    evaluation=evaluation,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=as_of_date,
                    content=content,
                )
                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "portfolio_report"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                preview_path = artifact_dir / "portfolio_report_preview.md"
                preview_path.write_text(content, encoding="utf-8")
                payload_path = artifact_dir / "portfolio_report_payload.json"
                payload_path.write_text(
                    json.dumps(
                        {
                            "username": settings.discord.username,
                            "messages": messages,
                            "message_count": len(messages),
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                artifact_paths = [str(preview_path), str(payload_path)]
                notes = (
                    "Portfolio report rendered. "
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
                return PortfolioReportResult(
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
                    notes="Portfolio report rendering failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def publish_discord_portfolio_summary(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> PortfolioPublishResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "publish_discord_portfolio_summary",
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
                input_sources=["fact_portfolio_target_book", "fact_portfolio_nav_snapshot"],
                notes=f"Publish portfolio summary for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                render_result = render_portfolio_report(
                    settings,
                    as_of_date=as_of_date,
                    dry_run=dry_run,
                )
                payload_path = next(
                    path
                    for path in render_result.artifact_paths
                    if path.endswith(".json")
                )
                payload = json.loads(Path(payload_path).read_text(encoding="utf-8"))
                published = False
                notes = "Dry run only."
                if not dry_run and settings.discord.webhook_url:
                    with httpx.Client(timeout=15.0) as client:
                        for message in payload.get("messages", []):
                            client.post(
                                settings.discord.webhook_url,
                                json=message,
                            ).raise_for_status()
                    published = True
                    notes = "Portfolio summary published."
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=render_result.artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return PortfolioPublishResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    dry_run=dry_run,
                    published=published,
                    artifact_paths=render_result.artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Portfolio summary publish failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
