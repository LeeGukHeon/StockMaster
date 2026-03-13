from __future__ import annotations

import json
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

from .common import PortfolioPublishResult, PortfolioReportResult

EXECUTION_MODE_LABELS = {
    "OPEN_ALL": "시가 일괄 진입",
    "TIMING_ASSISTED": "장중 보조 진입",
}

COMPARISON_KEY_LABELS = {
    "OPEN_ALL": "시가 일괄 진입",
    "TIMING_ASSISTED": "장중 보조 진입",
    "EQUAL_WEIGHT_BASELINE": "동일 비중 비교 기준",
}


def _portfolio_report_content(
    *,
    as_of_date: date,
    active_policy: pd.DataFrame,
    target_book: pd.DataFrame,
    rebalance: pd.DataFrame,
    nav: pd.DataFrame,
    evaluation: pd.DataFrame,
) -> str:
    lines = [f"**StockMaster 포트폴리오 요약 | {as_of_date.isoformat()}**", ""]
    lines.append(
        "이 보고서는 오늘 추천을 실제 보유안으로 바꾸면 어떤 모습이 되는지 정리한 제안서입니다. "
        "자동 주문을 넣는 화면이 아니라, 목표 비중과 리밸런스 방향을 읽는 참고 보고서입니다."
    )
    lines.append("")
    lines.append("**현재 적용 기준**")
    if active_policy.empty:
        lines.append("- 아직 고정된 포트폴리오 기준이 없어 기본 설정값을 참고합니다.")
    else:
        for row in active_policy.itertuples(index=False):
            lines.append(
                f"- {row.display_name or row.portfolio_policy_id} | 버전 {row.portfolio_policy_version} | 적용 시작일 {row.effective_from_date}"
            )
    lines.append("")
    lines.append("**목표 보유안**")
    if target_book.empty:
        lines.append("- 현재 목표 보유안이 없습니다.")
    else:
        for row in target_book.head(8).itertuples(index=False):
            lines.append(
                f"- {row.symbol} {row.company_name or ''} | 목표 비중 {float(row.target_weight or 0.0):.2%} "
                f"| 목표 수량 {int(row.target_shares or 0)} | 진입 판단 {row.gate_status}"
            )
    lines.append("")
    lines.append("**리밸런스 계획**")
    if rebalance.empty:
        lines.append("- 리밸런스 계획이 없습니다.")
    else:
        for row in rebalance.head(8).itertuples(index=False):
            lines.append(
                f"- {row.symbol} | 조치 {row.rebalance_action} | 수량 변화 {int(row.delta_shares or 0)} | 현금 변화 {float(row.cash_delta or 0.0):,.0f}"
            )
    lines.append("")
    lines.append("**최근 포트폴리오 흐름**")
    if nav.empty:
        lines.append("- 최근 포트폴리오 흐름 자료가 없습니다.")
    else:
        for row in nav.head(4).itertuples(index=False):
            lines.append(
                f"- {EXECUTION_MODE_LABELS.get(str(row.execution_mode), str(row.execution_mode))} | 순자산 가치 {float(row.nav_value or 0.0):,.0f} "
                f"| 누적 수익률 {float(row.cumulative_return or 0.0):+.2%} "
                f"| 최대 하락폭 {float(row.drawdown or 0.0):+.2%}"
            )
    lines.append("")
    lines.append("**방식별 비교 요약**")
    if evaluation.empty:
        lines.append("- 방식별 비교 자료가 없습니다.")
    else:
        grouped = evaluation.groupby("comparison_key", sort=True)
        for comparison_key, part in grouped:
            metric_map = {
                row.metric_name: row.metric_value for row in part.itertuples(index=False)
            }
            lines.append(
                f"- {COMPARISON_KEY_LABELS.get(str(comparison_key), str(comparison_key))} | 누적 수익률 {float(metric_map.get('cumulative_return') or 0.0):+.2%} "
                f"| 변동성 {float(metric_map.get('annualized_volatility') or 0.0):.2%} "
                f"| 위험 대비 점수 {float(metric_map.get('sharpe_like_ratio') or 0.0):+.2f}"
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
                webhook_url = settings.discord.webhook_url
                decision = resolve_discord_publish_decision(
                    enabled=settings.discord.enabled,
                    webhook_url=webhook_url,
                    dry_run=dry_run,
                )
                manifest_status = "skipped"
                if decision == DiscordPublishDecision.SKIP_DISABLED:
                    notes = (
                        f"Portfolio summary publish skipped for {as_of_date.isoformat()}. "
                        "DISCORD_REPORT_ENABLED=false."
                    )
                elif decision == DiscordPublishDecision.SKIP_DRY_RUN:
                    notes = (
                        f"Portfolio summary publish dry-run completed for {as_of_date.isoformat()}."
                    )
                elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
                    notes = (
                        f"Portfolio summary publish skipped for {as_of_date.isoformat()}. "
                        "Webhook URL is not configured."
                    )
                else:
                    publish_discord_messages(
                        webhook_url,
                        list(payload.get("messages", [])),
                        timeout=15.0,
                    )
                    published = True
                    manifest_status = "success"
                    notes = f"Portfolio summary published for {as_of_date.isoformat()}."
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status=manifest_status,
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
