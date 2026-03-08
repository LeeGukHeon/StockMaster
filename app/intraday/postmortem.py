from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

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


@dataclass(slots=True)
class IntradayPostmortemRenderResult:
    run_id: str
    session_date: date
    payload: dict[str, object]
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPostmortemPublishResult:
    run_id: str
    session_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _load_market_context(connection, *, session_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT *
        FROM fact_intraday_market_context_snapshot
        WHERE session_date = ?
          AND context_scope = 'market'
        ORDER BY checkpoint_time
        """,
        [session_date],
    ).fetchdf()


def _load_action_mix(connection, *, session_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            adjusted.checkpoint_time,
            adjusted.market_regime_family,
            adjusted.adjustment_profile,
            adjusted.raw_action,
            adjusted.adjusted_action,
            COUNT(*) AS row_count
        FROM fact_intraday_adjusted_entry_decision AS adjusted
        WHERE adjusted.session_date = ?
        GROUP BY
            adjusted.checkpoint_time,
            adjusted.market_regime_family,
            adjusted.adjustment_profile,
            adjusted.raw_action,
            adjusted.adjusted_action
        ORDER BY adjusted.checkpoint_time, adjusted.raw_action, adjusted.adjusted_action
        """,
        [session_date],
    ).fetchdf()


def _load_strategy_comparison(
    connection,
    *,
    session_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_strategy_comparison
        WHERE end_session_date = ?
          AND horizon IN ({placeholders})
          AND comparison_scope = 'all'
        ORDER BY horizon, strategy_id
        """,
        [session_date, *horizons],
    ).fetchdf()


def _load_regime_matrix(
    connection,
    *,
    session_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_strategy_comparison
        WHERE end_session_date = ?
          AND horizon IN ({placeholders})
          AND comparison_scope = 'regime_family'
        ORDER BY horizon, comparison_value, strategy_id
        """,
        [session_date, *horizons],
    ).fetchdf()


def _load_calibration(connection, *, session_date: date, horizons: list[int]) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_timing_calibration
        WHERE window_end_date = ?
          AND horizon IN ({placeholders})
          AND grouping_key IN ('overall', 'strategy_id', 'regime_family')
        ORDER BY horizon, grouping_key, grouping_value
        """,
        [session_date, *horizons],
    ).fetchdf()


def _load_symbol_trace(connection, *, session_date: date, limit: int = 10) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            result.horizon,
            result.symbol,
            result.company_name,
            result.strategy_id,
            result.market_regime_family,
            result.entry_checkpoint_time,
            result.executed_flag,
            result.realized_excess_return,
            result.timing_edge_vs_open_bps
        FROM fact_intraday_strategy_result AS result
        WHERE result.session_date = ?
          AND result.strategy_id IN (
              'SEL_V2_TIMING_RAW_FIRST_ENTER',
              'SEL_V2_TIMING_ADJ_FIRST_ENTER'
          )
        ORDER BY result.horizon, result.symbol, result.strategy_id
        LIMIT ?
        """,
        [session_date, limit],
    ).fetchdf()


def _build_report_content(
    *,
    session_date: date,
    context_frame: pd.DataFrame,
    action_mix: pd.DataFrame,
    comparison_frame: pd.DataFrame,
    regime_matrix: pd.DataFrame,
    calibration_frame: pd.DataFrame,
    symbol_trace: pd.DataFrame,
) -> str:
    lines = [f"**StockMaster Intraday Postmortem | {session_date.isoformat()}**", ""]
    lines.append(
        "Selection v2 remains the stock-picking layer. Intraday timing is a deterministic "
        "adjustment layer only; this report compares same-exit outcomes and no-entry diagnostics."
    )
    lines.append("")
    lines.append("**Market Context**")
    if context_frame.empty:
        lines.append("- no intraday market context snapshot")
    else:
        for row in context_frame.itertuples(index=False):
            breadth_text = (
                "n/a"
                if row.market_breadth_ratio is None
                else f"{float(row.market_breadth_ratio):.1%}"
            )
            lines.append(
                f"- {row.checkpoint_time} regime_prior={row.prior_daily_regime_state or 'n/a'} "
                f"breadth={breadth_text} "
                f"data_quality={row.data_quality_flag}"
            )
    lines.append("")
    lines.append("**Raw vs Adjusted Action Mix**")
    if action_mix.empty:
        lines.append("- no adjusted intraday decision rows")
    else:
        for row in action_mix.itertuples(index=False):
            lines.append(
                f"- {row.checkpoint_time} {row.market_regime_family}/{row.adjustment_profile}: "
                f"{row.raw_action} -> {row.adjusted_action} ({int(row.row_count)})"
            )
    lines.append("")
    lines.append("**Strategy Comparison**")
    if comparison_frame.empty:
        lines.append("- no strategy comparison summary")
    else:
        for row in comparison_frame.itertuples(index=False):
            exec_rate = "n/a" if row.execution_rate is None else f"{float(row.execution_rate):.1%}"
            mean_excess = (
                "n/a"
                if row.mean_realized_excess_return is None
                else f"{float(row.mean_realized_excess_return):+.2%}"
            )
            mean_edge = (
                "n/a"
                if row.mean_timing_edge_vs_open_bps is None
                else f"{float(row.mean_timing_edge_vs_open_bps):+.1f}"
            )
            lines.append(
                f"- H{int(row.horizon)} {row.strategy_id}: "
                f"exec_rate={exec_rate} "
                f"mean_excess={mean_excess} "
                f"mean_edge_bps={mean_edge}"
            )
    lines.append("")
    lines.append("**Regime Matrix**")
    if regime_matrix.empty:
        lines.append("- no regime-family strategy matrix")
    else:
        for row in regime_matrix.itertuples(index=False):
            exec_rate = "n/a" if row.execution_rate is None else f"{float(row.execution_rate):.1%}"
            skip_saved_loss = (
                "n/a"
                if row.skip_saved_loss_rate is None
                else f"{float(row.skip_saved_loss_rate):.1%}"
            )
            lines.append(
                f"- H{int(row.horizon)} {row.comparison_value} {row.strategy_id}: "
                f"exec_rate={exec_rate} "
                f"skip_saved_loss={skip_saved_loss}"
            )
    lines.append("")
    lines.append("**Timing Calibration**")
    if calibration_frame.empty:
        lines.append("- no intraday timing calibration rows")
    else:
        for row in calibration_frame.itertuples(index=False):
            mean_excess = (
                "n/a"
                if row.mean_realized_excess_return is None
                else f"{float(row.mean_realized_excess_return):+.2%}"
            )
            hit_rate = "n/a" if row.hit_rate is None else f"{float(row.hit_rate):.1%}"
            lines.append(
                f"- H{int(row.horizon)} {row.grouping_key}={row.grouping_value}: "
                f"mean_excess={mean_excess} "
                f"hit={hit_rate} "
                f"quality={row.quality_flag}"
            )
    lines.append("")
    lines.append("**Symbol Trace**")
    if symbol_trace.empty:
        lines.append("- no raw/adjusted strategy trace rows")
    else:
        for row in symbol_trace.itertuples(index=False):
            edge_bps = (
                "n/a"
                if row.timing_edge_vs_open_bps is None
                else f"{float(row.timing_edge_vs_open_bps):+.1f}"
            )
            lines.append(
                f"- H{int(row.horizon)} {row.symbol} {row.company_name} {row.strategy_id} "
                f"checkpoint={row.entry_checkpoint_time or '-'} exec={row.executed_flag} "
                f"edge_bps={edge_bps}"
            )
    return "\n".join(lines)


def render_intraday_postmortem_report(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayPostmortemRenderResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_intraday_postmortem_report",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_market_context_snapshot",
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_strategy_comparison",
                    "fact_intraday_timing_calibration",
                ],
                notes=f"Render intraday postmortem report for {session_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                context_frame = _load_market_context(connection, session_date=session_date)
                action_mix = _load_action_mix(connection, session_date=session_date)
                comparison_frame = _load_strategy_comparison(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                )
                regime_matrix = _load_regime_matrix(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                )
                calibration_frame = _load_calibration(
                    connection,
                    session_date=session_date,
                    horizons=horizons,
                )
                symbol_trace = _load_symbol_trace(connection, session_date=session_date)
                content = _build_report_content(
                    session_date=session_date,
                    context_frame=context_frame,
                    action_mix=action_mix,
                    comparison_frame=comparison_frame,
                    regime_matrix=regime_matrix,
                    calibration_frame=calibration_frame,
                    symbol_trace=symbol_trace,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=session_date,
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
                    / "intraday_postmortem"
                    / f"session_date={session_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "intraday_postmortem_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "intraday_postmortem_preview.md"
                preview_lines: list[str] = []
                for index, message in enumerate(messages, start=1):
                    preview_lines.append(f"## Message {index}")
                    preview_lines.append("")
                    preview_lines.append(str(message["content"]))
                    preview_lines.append("")
                preview_path.write_text("\n".join(preview_lines).strip(), encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    "Intraday postmortem report rendered. "
                    f"session_date={session_date.isoformat()} dry_run={dry_run}"
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
                return IntradayPostmortemRenderResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
                    payload=payload,
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
                    notes=f"Intraday postmortem render failed for {session_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def publish_discord_intraday_postmortem(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    dry_run: bool,
) -> IntradayPostmortemPublishResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "publish_discord_intraday_postmortem",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=["render_intraday_postmortem_report"],
                notes=f"Publish intraday postmortem report for {session_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )

        render_result = render_intraday_postmortem_report(
            settings,
            session_date=session_date,
            horizons=horizons,
            dry_run=dry_run,
        )
        artifact_paths = list(render_result.artifact_paths)
        published = False
        notes = f"Intraday postmortem publish dry-run completed for {session_date.isoformat()}."
        try:
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            if not settings.discord.enabled:
                notes = (
                    f"Intraday postmortem publish skipped for {session_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
            elif dry_run or not webhook_url:
                if not webhook_url:
                    notes = (
                        f"Intraday postmortem publish skipped for {session_date.isoformat()}. "
                        "Webhook URL is not configured."
                    )
            else:
                response_payloads: list[dict[str, object]] = []
                for index, message in enumerate(messages, start=1):
                    response = httpx.post(webhook_url, json=message, timeout=10.0)
                    response.raise_for_status()
                    response_payloads.append(
                        {"message_index": index, "status_code": response.status_code}
                    )
                publish_path = (
                    settings.paths.artifacts_dir
                    / "intraday_postmortem"
                    / f"session_date={session_date.isoformat()}"
                    / run_context.run_id
                    / "publish_response.json"
                )
                publish_path.parent.mkdir(parents=True, exist_ok=True)
                publish_path.write_text(
                    json.dumps(response_payloads, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                artifact_paths.append(str(publish_path))
                published = True
                notes = (
                    f"Intraday postmortem publish completed for {session_date.isoformat()}. "
                    f"message_count={len(messages)}"
                )
        except Exception as exc:
            notes = (
                f"Intraday postmortem publish warning for {session_date.isoformat()}: {exc}. "
                "The report was rendered but publish did not complete."
            )
        finally:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
        return IntradayPostmortemPublishResult(
            run_id=run_context.run_id,
            session_date=session_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
