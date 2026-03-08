# ruff: noqa: E501

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb

from app.ml.constants import SELECTION_ENGINE_VERSION as SELECTION_ENGINE_V2_VERSION
from app.ops.common import JobStatus, OpsJobResult
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables


@dataclass(frozen=True, slots=True)
class RenderedReport:
    artifact_paths: list[str]
    content: str
    payload: dict[str, Any]


def _frame(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: list[object] | None = None,
):
    return connection.execute(query, params or []).fetchdf()


def _payload_messages(content: str) -> list[dict[str, str]]:
    chunk_size = 1800
    return [
        {"content": content[index : index + chunk_size]}
        for index in range(0, len(content), chunk_size)
    ]


def _json_object_value(payload: str | None, key: str) -> float | None:
    if not payload:
        return None
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    value = loaded.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _write_report_artifacts(
    settings: Settings,
    *,
    folder_name: str,
    partition_key: str,
    preview_name: str,
    payload_name: str,
    run_id: str,
    content: str,
    dry_run: bool,
    extra_payload: dict[str, Any] | None = None,
) -> RenderedReport:
    artifact_dir = settings.paths.artifacts_dir / folder_name / partition_key / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    preview_path = artifact_dir / preview_name
    preview_path.write_text(content, encoding="utf-8")
    payload = {
        "username": settings.discord.username,
        "dry_run": dry_run,
        "message_count": max(1, (len(content) + 1799) // 1800),
        "messages": _payload_messages(content),
    }
    if extra_payload:
        payload.update(extra_payload)
    payload_path = artifact_dir / payload_name
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return RenderedReport(
        artifact_paths=[str(preview_path), str(payload_path)],
        content=content,
        payload=payload,
    )


def render_daily_research_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    regime = _frame(
        connection,
        """
        SELECT as_of_date, regime_state, breadth_up_ratio, market_realized_vol_20d
        FROM fact_market_regime_snapshot
        WHERE as_of_date <= ?
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 1
        """,
        [as_of_date],
    )
    leaderboard = _frame(
        connection,
        """
        SELECT
            ranking.symbol,
            symbol_dim.company_name,
            ranking.grade,
            ranking.final_selection_value,
            ranking.explanatory_score_json,
            prediction.expected_excess_return,
            prediction.uncertainty_score,
            prediction.disagreement_score
        FROM fact_ranking AS ranking
        LEFT JOIN dim_symbol AS symbol_dim
          ON symbol_dim.symbol = ranking.symbol
        LEFT JOIN fact_prediction AS prediction
          ON prediction.as_of_date = ranking.as_of_date
         AND prediction.symbol = ranking.symbol
         AND prediction.horizon = ranking.horizon
         AND prediction.prediction_version = 'alpha_prediction_v1'
        WHERE ranking.ranking_version = ?
          AND ranking.horizon = 5
          AND ranking.as_of_date = (
              SELECT MAX(as_of_date)
              FROM fact_ranking
              WHERE ranking_version = ?
                AND as_of_date <= ?
          )
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT 10
        """,
        [SELECTION_ENGINE_V2_VERSION, SELECTION_ENGINE_V2_VERSION, as_of_date],
    )
    news = _frame(
        connection,
        """
        SELECT signal_date, title, publisher, query_bucket
        FROM fact_news_item
        WHERE signal_date <= ?
        ORDER BY published_at DESC
        LIMIT 8
        """,
        [as_of_date],
    )
    portfolio = _frame(
        connection,
        """
        SELECT execution_mode, symbol, company_name, target_weight, gate_status
        FROM fact_portfolio_target_book
        WHERE as_of_date = (
            SELECT MAX(as_of_date)
            FROM fact_portfolio_target_book
            WHERE as_of_date <= ?
        )
        ORDER BY execution_mode, target_rank
        LIMIT 10
        """,
        [as_of_date],
    )
    lines = ["# Daily Research Report", "", f"- as_of_date: {as_of_date.isoformat()}", ""]
    lines.append("## Current Truth")
    if regime.empty:
        lines.append("- market regime snapshot unavailable")
    else:
        row = regime.iloc[0]
        lines.append(
            "- regime={state} breadth={breadth:.1%} vol20={vol:.2%}".format(
                state=row["regime_state"],
                breadth=float(row["breadth_up_ratio"] or 0.0),
                vol=float(row["market_realized_vol_20d"] or 0.0),
            )
        )
    lines.append("")
    lines.append("## Top Actionable Names")
    if leaderboard.empty:
        lines.append("- no selection v2 rows")
    else:
        for row in leaderboard.itertuples(index=False):
            flow_score = _json_object_value(row.explanatory_score_json, "flow_score")
            implementation_penalty = _json_object_value(
                row.explanatory_score_json,
                "implementation_penalty_score",
            )
            lines.append(
                f"- {row.symbol} {row.company_name or ''} "
                f"grade={row.grade} score={float(row.final_selection_value or 0.0):+.3f} "
                f"alpha={float(row.expected_excess_return or 0.0):+.2%} "
                f"uncertainty={float(row.uncertainty_score or 0.0):.2f} "
                f"disagreement={float(row.disagreement_score or 0.0):.2f} "
                f"flow={float(flow_score or 0.0):+.2f} "
                f"impl_penalty={float(implementation_penalty or 0.0):.2f}"
            )
    lines.append("")
    lines.append("## Portfolio Context")
    if portfolio.empty:
        lines.append("- no portfolio target book")
    else:
        for row in portfolio.itertuples(index=False):
            lines.append(
                f"- {row.execution_mode} {row.symbol}: "
                f"weight={float(row.target_weight or 0.0):.2%} gate={row.gate_status}"
            )
    lines.append("")
    lines.append("## News Clusters")
    if news.empty:
        lines.append("- no recent news metadata")
    else:
        for row in news.itertuples(index=False):
            lines.append(f"- {row.signal_date} [{row.query_bucket}] {row.title} / {row.publisher}")
    rendered = _write_report_artifacts(
        settings,
        folder_name="daily_research_report",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="daily_research_report_preview.md",
        payload_name="daily_research_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "daily_research_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_daily_research_report",
        status=JobStatus.SUCCESS,
        notes=f"Daily research report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_evaluation_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    summary = _frame(
        connection,
        """
        SELECT summary_date, window_type, horizon, ranking_version, segment_type, segment_value,
               count_evaluated, mean_realized_excess_return, hit_rate, band_coverage_rate
        FROM fact_evaluation_summary
        WHERE summary_date <= ?
        ORDER BY summary_date DESC, horizon, ranking_version
        LIMIT 16
        """,
        [as_of_date],
    )
    calibration = _frame(
        connection,
        """
        SELECT diagnostic_date, horizon, bin_type, bin_value, sample_count, coverage_rate, median_bias
        FROM fact_calibration_diagnostic
        WHERE diagnostic_date <= ?
        ORDER BY diagnostic_date DESC, horizon, bin_value
        LIMIT 16
        """,
        [as_of_date],
    )
    lines = ["# Evaluation Report", "", f"- as_of_date: {as_of_date.isoformat()}", ""]
    lines.append("## D+1 / D+5 Matured Summary")
    if summary.empty:
        lines.append("- no evaluation summary rows")
    else:
        for row in summary.itertuples(index=False):
            lines.append(
                f"- {row.summary_date} h={row.horizon} {row.ranking_version} "
                f"{row.segment_type}={row.segment_value} "
                f"evaluated={int(row.count_evaluated or 0)} "
                f"excess={float(row.mean_realized_excess_return or 0.0):+.3%} "
                f"hit={float(row.hit_rate or 0.0):+.2%} "
                f"band={float(row.band_coverage_rate or 0.0):.2%}"
            )
    lines.append("")
    lines.append("## Band Coverage / Calibration")
    if calibration.empty:
        lines.append("- no calibration diagnostics")
    else:
        for row in calibration.itertuples(index=False):
            lines.append(
                f"- {row.diagnostic_date} h={row.horizon} bin={row.bin_value} "
                f"sample={int(row.sample_count or 0)} "
                f"coverage={float(row.coverage_rate or 0.0):.2%} "
                f"bias={float(row.median_bias or 0.0):+.3%}"
            )
    rendered = _write_report_artifacts(
        settings,
        folder_name="evaluation_report",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="evaluation_report_preview.md",
        payload_name="evaluation_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "evaluation_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_evaluation_report",
        status=JobStatus.SUCCESS,
        notes=f"Evaluation report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_intraday_summary_report(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    session_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    decisions = _frame(
        connection,
        """
        SELECT checkpoint_time, adjusted_action, final_action, COUNT(*) AS row_count
        FROM fact_intraday_meta_decision
        WHERE session_date = ?
        GROUP BY checkpoint_time, adjusted_action, final_action
        ORDER BY checkpoint_time, adjusted_action, final_action
        """,
        [session_date],
    )
    timing = _frame(
        connection,
        """
        SELECT
            end_session_date,
            strategy_id,
            horizon,
            executed_count,
            execution_rate,
            mean_realized_excess_return,
            mean_timing_edge_vs_open_bps
        FROM fact_intraday_strategy_comparison
        WHERE end_session_date = (
            SELECT MAX(end_session_date)
            FROM fact_intraday_strategy_comparison
            WHERE end_session_date <= ?
        )
        ORDER BY strategy_id, horizon
        LIMIT 20
        """,
        [session_date],
    )
    lines = ["# Intraday Summary Report", "", f"- session_date: {session_date.isoformat()}", ""]
    lines.append("## Final Action Overlay")
    if decisions.empty:
        lines.append("- no intraday final action rows")
    else:
        for row in decisions.itertuples(index=False):
            lines.append(
                f"- {row.checkpoint_time} adjusted={row.adjusted_action} "
                f"final={row.final_action} count={int(row.row_count)}"
            )
    lines.append("")
    lines.append("## Timing Edge / Strategy Trace")
    if timing.empty:
        lines.append("- no intraday strategy comparison rows")
    else:
        for row in timing.itertuples(index=False):
            lines.append(
                f"- {row.end_session_date} {row.strategy_id} h={row.horizon} "
                f"executed={int(row.executed_count or 0)} "
                f"rate={float(row.execution_rate or 0.0):.2%} "
                f"excess={float(row.mean_realized_excess_return or 0.0):+.3%} "
                f"edge_bps={float(row.mean_timing_edge_vs_open_bps or 0.0):+.1f}"
            )
    rendered = _write_report_artifacts(
        settings,
        folder_name="intraday_summary_report",
        partition_key=f"session_date={session_date.isoformat()}",
        preview_name="intraday_summary_report_preview.md",
        payload_name="intraday_summary_report_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "intraday_summary_report"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_intraday_summary_report",
        status=JobStatus.SUCCESS,
        notes=f"Intraday summary report rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )


def render_release_candidate_checklist(
    settings: Settings,
    *,
    connection: duckdb.DuckDBPyConnection,
    as_of_date: date,
    job_run_id: str | None = None,
    dry_run: bool,
) -> OpsJobResult:
    bootstrap_core_tables(connection)
    checks = _frame(
        connection,
        """
        SELECT check_name, status, severity, recommended_action, check_ts
        FROM vw_latest_release_candidate_check
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'WARNING' THEN 2
                ELSE 3
            END,
            check_name
        """
    )
    freshness = _frame(
        connection,
        """
        SELECT page_name, dataset_name, warning_level, stale_flag, latest_available_ts
        FROM vw_latest_ui_data_freshness_snapshot
        ORDER BY page_name, dataset_name
        """
    )
    lines = ["# Release Candidate Checklist", "", f"- as_of_date: {as_of_date.isoformat()}", ""]
    lines.append("## Checks")
    if checks.empty:
        lines.append("- no release-candidate checks have been materialized")
    else:
        for row in checks.itertuples(index=False):
            lines.append(
                f"- [{row.severity}] {row.check_name}: {row.status} "
                f"action={row.recommended_action or 'none'}"
            )
    lines.append("")
    lines.append("## UI Freshness")
    if freshness.empty:
        lines.append("- no UI freshness snapshot rows")
    else:
        for row in freshness.itertuples(index=False):
            lines.append(
                f"- {row.page_name} / {row.dataset_name}: "
                f"{row.warning_level} stale={row.stale_flag} latest={row.latest_available_ts}"
            )
    rendered = _write_report_artifacts(
        settings,
        folder_name="release_candidate_checklist",
        partition_key=f"as_of_date={as_of_date.isoformat()}",
        preview_name="release_candidate_checklist_preview.md",
        payload_name="release_candidate_checklist_payload.json",
        run_id=job_run_id or "embedded",
        content="\n".join(lines),
        dry_run=dry_run,
        extra_payload={"report_type": "release_candidate_checklist"},
    )
    return OpsJobResult(
        run_id=job_run_id or "embedded",
        job_name="render_release_candidate_checklist",
        status=JobStatus.SUCCESS,
        notes=f"Release candidate checklist rendered. dry_run={dry_run}",
        artifact_paths=rendered.artifact_paths,
    )
