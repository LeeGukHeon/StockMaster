from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import httpx
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ranking.explanatory_score import RANKING_VERSION as EXPLANATORY_RANKING_VERSION
from app.reports.discord_eod import _build_payload_messages
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class PostmortemRenderResult:
    run_id: str
    evaluation_date: date
    payload: dict[str, object]
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PostmortemPublishResult:
    run_id: str
    evaluation_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _load_evaluation_summary(
    connection, *, evaluation_date: date, horizons: list[int]
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT
            horizon,
            ranking_version,
            COUNT(*) AS row_count,
            AVG(realized_excess_return) AS avg_realized_excess_return,
            AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
            AVG(expected_excess_return_at_selection) AS avg_expected_excess_return,
            AVG(
                CASE
                    WHEN band_available_flag AND in_band_flag THEN 1.0
                    WHEN band_available_flag THEN 0.0
                    ELSE NULL
                END
            ) AS band_coverage_rate
        FROM fact_selection_outcome
        WHERE evaluation_date = ?
          AND horizon IN ({horizon_placeholders})
          AND outcome_status = 'matured'
        GROUP BY horizon, ranking_version
        ORDER BY horizon, ranking_version
        """,
        [evaluation_date, *horizons],
    ).fetchdf()


def _load_comparison_rows(
    connection, *, evaluation_date: date, horizons: list[int]
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        WITH cohort AS (
            SELECT
                horizon,
                ranking_version,
                AVG(realized_excess_return) AS avg_realized_excess_return,
                AVG(CASE WHEN realized_excess_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate
            FROM fact_selection_outcome
            WHERE evaluation_date = ?
              AND horizon IN ({horizon_placeholders})
              AND outcome_status = 'matured'
              AND ranking_version IN (?, ?)
            GROUP BY horizon, ranking_version
        )
        SELECT
            selection.horizon,
            selection.avg_realized_excess_return AS selection_avg_excess,
            baseline.avg_realized_excess_return AS baseline_avg_excess,
            selection.hit_rate AS selection_hit_rate,
            baseline.hit_rate AS baseline_hit_rate,
            selection.avg_realized_excess_return
                - baseline.avg_realized_excess_return AS avg_excess_gap,
            selection.hit_rate - baseline.hit_rate AS hit_rate_gap
        FROM cohort AS selection
        JOIN cohort AS baseline
          ON selection.horizon = baseline.horizon
         AND selection.ranking_version = ?
         AND baseline.ranking_version = ?
        ORDER BY selection.horizon
        """,
        [
            evaluation_date,
            *horizons,
            SELECTION_ENGINE_VERSION,
            EXPLANATORY_RANKING_VERSION,
            SELECTION_ENGINE_VERSION,
            EXPLANATORY_RANKING_VERSION,
        ],
    ).fetchdf()


def _load_top_outcomes(
    connection,
    *,
    evaluation_date: date,
    horizon: int,
    limit: int = 5,
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            outcome.symbol,
            meta.company_name,
            meta.market,
            outcome.selection_date,
            outcome.realized_excess_return,
            outcome.expected_excess_return_at_selection,
            outcome.band_status,
            outcome.top_reason_tags_json
        FROM fact_selection_outcome AS outcome
        JOIN dim_symbol AS meta
          ON outcome.symbol = meta.symbol
        WHERE outcome.evaluation_date = ?
          AND outcome.horizon = ?
          AND outcome.outcome_status = 'matured'
          AND outcome.ranking_version = ?
        ORDER BY outcome.realized_excess_return DESC, outcome.symbol
        LIMIT ?
        """,
        [evaluation_date, horizon, SELECTION_ENGINE_VERSION, limit],
    ).fetchdf()


def _load_rolling_summary(connection, *, horizons: list[int]) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT
            summary_date,
            window_type,
            horizon,
            ranking_version,
            segment_value,
            count_evaluated,
            mean_realized_excess_return,
            hit_rate
        FROM vw_latest_evaluation_summary
        WHERE horizon IN ({horizon_placeholders})
          AND window_type IN ('rolling_20d', 'rolling_60d')
          AND segment_type = 'coverage'
          AND segment_value = 'all'
          AND ranking_version IN (?, ?)
        ORDER BY window_type, horizon, ranking_version
        """,
        [*horizons, SELECTION_ENGINE_VERSION, EXPLANATORY_RANKING_VERSION],
    ).fetchdf()


def _load_calibration_summary(connection, *, horizons: list[int]) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT
            horizon,
            coverage_rate,
            median_bias,
            quality_flag
        FROM vw_latest_calibration_diagnostic
        WHERE horizon IN ({horizon_placeholders})
          AND ranking_version = ?
          AND bin_type = 'overall'
        ORDER BY horizon
        """,
        [*horizons, SELECTION_ENGINE_VERSION],
    ).fetchdf()


def _format_summary_line(row: pd.Series) -> str:
    band_text = ""
    if pd.notna(row.get("band_coverage_rate")):
        band_text = f" | band_cov={float(row['band_coverage_rate']):.1%}"
    expected_text = ""
    if pd.notna(row.get("avg_expected_excess_return")):
        expected_text = f" | avg_proxy={float(row['avg_expected_excess_return']):+.2%}"
    return (
        f"- H{int(row['horizon'])} `{row['ranking_version']}` "
        f"rows={int(row['row_count'])} "
        f"avg_excess={float(row['avg_realized_excess_return']):+.2%} "
        f"hit={float(row['hit_rate']):.1%}{expected_text}{band_text}"
    )


def _format_top_line(row: pd.Series) -> str:
    reasons = ", ".join(json.loads(row["top_reason_tags_json"] or "[]")[:2])
    proxy = ""
    if pd.notna(row.get("expected_excess_return_at_selection")):
        proxy = f" | proxy={float(row['expected_excess_return_at_selection']):+.2%}"
    return (
        f"- `{row['symbol']}` {row['company_name']} ({row['market']}) "
        f"sel={row['selection_date']} realized_excess={float(row['realized_excess_return']):+.2%}"
        f"{proxy} | band={row['band_status']} | reasons: {reasons or '-'}"
    )


def _build_report_content(
    *,
    evaluation_date: date,
    summary: pd.DataFrame,
    comparison: pd.DataFrame,
    rolling_summary: pd.DataFrame,
    calibration_summary: pd.DataFrame,
    top_by_horizon: dict[int, pd.DataFrame],
) -> str:
    lines = [
        f"**StockMaster Postmortem | {evaluation_date.isoformat()}**",
        "",
        "Pre-cost evaluation only. Frozen ranking/prediction snapshots are compared against "
        "realized next-open to future-close labels; no transaction-cost simulator is applied.",
        "",
        "**Matured Cohorts**",
    ]
    if summary.empty:
        lines.append("- no matured outcomes were available for the requested evaluation date")
    else:
        lines.extend(_format_summary_line(row) for _, row in summary.iterrows())

    lines.append("")
    lines.append("**Selection v1 vs Explanatory v0**")
    if comparison.empty:
        lines.append("- no same-date comparison cohort was available")
    else:
        for _, row in comparison.iterrows():
            lines.append(
                f"- H{int(row['horizon'])} avg_excess_gap={float(row['avg_excess_gap']):+.2%} "
                f"hit_gap={float(row['hit_rate_gap']):+.1%}"
            )

    lines.append("")
    lines.append("**Rolling Evaluation Snapshot**")
    if rolling_summary.empty:
        lines.append("- no rolling evaluation summary is available yet")
    else:
        for _, row in rolling_summary.iterrows():
            lines.append(
                f"- {row['window_type']} H{int(row['horizon'])} `{row['ranking_version']}` "
                f"evaluated={int(row['count_evaluated'])} "
                f"avg_excess={float(row['mean_realized_excess_return']):+.2%} "
                f"hit={float(row['hit_rate']):.1%}"
            )

    lines.append("")
    lines.append("**Calibration Snapshot**")
    if calibration_summary.empty:
        lines.append("- no calibration diagnostics are available yet")
    else:
        for _, row in calibration_summary.iterrows():
            lines.append(
                f"- H{int(row['horizon'])} coverage={float(row['coverage_rate']):.1%} "
                f"median_bias={float(row['median_bias']):+.2%} quality={row['quality_flag']}"
            )

    for horizon, top_frame in sorted(top_by_horizon.items()):
        lines.append("")
        lines.append(f"**Top Matured Picks | H{int(horizon)}**")
        if top_frame.empty:
            lines.append("- no matured selection_engine_v1 outcomes")
        else:
            lines.extend(_format_top_line(row) for _, row in top_frame.iterrows())

    lines.append("")
    lines.append(
        "Proxy bands remain calibrated historical ranges, not ML forecasts. "
        "Selection engine v1 and explanatory ranking v0 are compared "
        "side-by-side without mixing cohorts."
    )
    return "\n".join(lines)


def render_postmortem_report(
    settings: Settings,
    *,
    evaluation_date: date,
    horizons: list[int],
    dry_run: bool,
) -> PostmortemRenderResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "render_postmortem_report",
        as_of_date=evaluation_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_selection_outcome",
                    "fact_evaluation_summary",
                    "fact_calibration_diagnostic",
                ],
                notes=f"Render postmortem report for {evaluation_date.isoformat()}",
                ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
            )
            try:
                summary = _load_evaluation_summary(
                    connection,
                    evaluation_date=evaluation_date,
                    horizons=horizons,
                )
                comparison = _load_comparison_rows(
                    connection,
                    evaluation_date=evaluation_date,
                    horizons=horizons,
                )
                rolling_summary = _load_rolling_summary(connection, horizons=horizons)
                calibration_summary = _load_calibration_summary(connection, horizons=horizons)
                top_by_horizon = {
                    int(horizon): _load_top_outcomes(
                        connection,
                        evaluation_date=evaluation_date,
                        horizon=int(horizon),
                    )
                    for horizon in horizons
                }
                content = _build_report_content(
                    evaluation_date=evaluation_date,
                    summary=summary,
                    comparison=comparison,
                    rolling_summary=rolling_summary,
                    calibration_summary=calibration_summary,
                    top_by_horizon=top_by_horizon,
                )
                messages = _build_payload_messages(
                    username=settings.discord.username,
                    as_of_date=evaluation_date,
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
                    / "postmortem"
                    / f"evaluation_date={evaluation_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "postmortem_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "postmortem_preview.md"
                preview_lines: list[str] = []
                for index, message in enumerate(messages, start=1):
                    preview_lines.append(f"## Message {index}")
                    preview_lines.append("")
                    preview_lines.append(str(message["content"]))
                    preview_lines.append("")
                preview_path.write_text("\n".join(preview_lines).strip(), encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    f"Postmortem report rendered. evaluation_date={evaluation_date.isoformat()} "
                    f"dry_run={dry_run} message_count={len(messages)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
                )
                return PostmortemRenderResult(
                    run_id=run_context.run_id,
                    evaluation_date=evaluation_date,
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
                    notes=f"Postmortem render failed for {evaluation_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
                )
                raise


def publish_discord_postmortem_report(
    settings: Settings,
    *,
    evaluation_date: date,
    horizons: list[int],
    dry_run: bool,
) -> PostmortemPublishResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "publish_discord_postmortem_report",
        as_of_date=evaluation_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["render_postmortem_report"],
                notes=f"Publish postmortem report for {evaluation_date.isoformat()}",
                ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
            )

        render_result = render_postmortem_report(
            settings,
            evaluation_date=evaluation_date,
            horizons=horizons,
            dry_run=dry_run,
        )
        artifact_paths = list(render_result.artifact_paths)
        notes = f"Postmortem publish dry-run completed for {evaluation_date.isoformat()}."
        published = False
        try:
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            if not settings.discord.enabled:
                notes = (
                    f"Postmortem publish skipped for {evaluation_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
            elif dry_run or not webhook_url:
                if not webhook_url:
                    notes = (
                        f"Postmortem publish skipped for {evaluation_date.isoformat()}. "
                        "Webhook URL is not configured."
                    )
            else:
                response_payloads: list[dict[str, object]] = []
                for index, message in enumerate(messages, start=1):
                    response = httpx.post(webhook_url, json=message, timeout=10.0)
                    response.raise_for_status()
                    response_payloads.append(
                        {
                            "message_index": index,
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                        }
                    )
                published = True
                publish_path = (
                    settings.paths.artifacts_dir
                    / "postmortem"
                    / f"evaluation_date={evaluation_date.isoformat()}"
                    / run_context.run_id
                    / "publish_response.json"
                )
                publish_path.parent.mkdir(parents=True, exist_ok=True)
                publish_path.write_text(
                    json.dumps(response_payloads, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                artifact_paths.append(str(publish_path))
                notes = (
                    f"Postmortem publish completed for {evaluation_date.isoformat()}. "
                    f"message_count={len(messages)}"
                )
        except Exception as exc:
            notes = (
                f"Postmortem publish warning for {evaluation_date.isoformat()}: {exc}. "
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
                    ranking_version=f"{SELECTION_ENGINE_VERSION},{EXPLANATORY_RANKING_VERSION}",
                )

        return PostmortemPublishResult(
            run_id=run_context.run_id,
            evaluation_date=evaluation_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
