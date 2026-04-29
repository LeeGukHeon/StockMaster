from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.discord import (
    DiscordPublishDecision,
    publish_discord_messages,
    resolve_discord_publish_decision,
)
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


def _build_postmortem_payload_messages(
    *,
    username: str,
    evaluation_date: date,
    content: str,
) -> list[dict[str, str]]:
    return _build_payload_messages(
        username=username,
        as_of_date=evaluation_date,
        content=content,
        continuation_title="StockMaster 사후 점검",
    )


def _window_label(window_type: str) -> str:
    mapping = {
        "rolling_20d": "최근 20거래일",
        "rolling_60d": "최근 60거래일",
    }
    return mapping.get(str(window_type), str(window_type))


def _format_percent_text(
    value: object,
    *,
    decimals: int,
    signed: bool = False,
    na_text: str = "n/a",
) -> str:
    if value is None or pd.isna(value):
        return na_text
    numeric_value = float(value)
    threshold = 0.5 * (10 ** (-(decimals + 2)))
    if 0 < abs(numeric_value) < threshold:
        sign = ""
        if signed:
            sign = "+" if numeric_value > 0 else "-"
        minimum_percent = f"{10 ** (-decimals):.{decimals}f}%"
        return f"{sign}<{minimum_percent}"
    format_spec = f"+.{decimals}%" if signed else f".{decimals}%"
    return format(numeric_value, format_spec)


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
            outcome.realized_excess_return,
            outcome.expected_excess_return_at_selection
        FROM fact_selection_outcome AS outcome
        JOIN dim_symbol AS meta
          ON outcome.symbol = meta.symbol
        LEFT JOIN fact_forward_return_label AS label
          ON outcome.selection_date = label.as_of_date
         AND outcome.symbol = label.symbol
         AND outcome.horizon = label.horizon
        LEFT JOIN fact_daily_ohlcv AS entry_day
          ON label.symbol = entry_day.symbol
         AND label.entry_date = entry_day.trading_date
        LEFT JOIN fact_daily_ohlcv AS exit_day
          ON label.symbol = exit_day.symbol
         AND label.exit_date = exit_day.trading_date
        WHERE outcome.evaluation_date = ?
          AND outcome.horizon = ?
          AND outcome.outcome_status = 'matured'
          AND outcome.ranking_version = ?
          AND COALESCE(entry_day.volume, 0) > 0
          AND COALESCE(exit_day.volume, 0) > 0
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
          AND count_evaluated > 0
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


def _result_label(avg_excess: object, hit_rate: object) -> str:
    avg = 0.0 if avg_excess is None or pd.isna(avg_excess) else float(avg_excess)
    hit = 0.0 if hit_rate is None or pd.isna(hit_rate) else float(hit_rate)
    if avg >= 0.01 and hit >= 0.55:
        return "양호"
    if avg > 0 and hit >= 0.5:
        return "보통+"
    if avg < -0.01 or hit < 0.45:
        return "부진"
    return "혼조"


def _format_current_result_line(row: pd.Series) -> str:
    label = _result_label(row.get("avg_realized_excess_return"), row.get("hit_rate"))
    return (
        f"- H{int(row['horizon'])} 현재모델: {label} · "
        "평균초과 "
        f"{_format_percent_text(row['avg_realized_excess_return'], decimals=2, signed=True)} · "
        f"적중률 {_format_percent_text(row['hit_rate'], decimals=0)} · "
        f"표본 {int(row['row_count'])}개"
    )


def _format_comparison_line(row: pd.Series) -> str:
    return (
        f"- H{int(row['horizon'])} 비교기준 대비: "
        f"평균 {_format_percent_text(row['avg_excess_gap'], decimals=2, signed=True)} · "
        f"적중률 {_format_percent_text(row['hit_rate_gap'], decimals=0, signed=True)}"
    )


def _format_rolling_line(row: pd.Series) -> str:
    return (
        f"- {_window_label(str(row['window_type']))} H{int(row['horizon'])}: "
        "평균초과 "
        f"{_format_percent_text(row['mean_realized_excess_return'], decimals=2, signed=True)} · "
        f"적중률 {_format_percent_text(row['hit_rate'], decimals=0)} · "
        f"표본 {int(row['count_evaluated'])}개"
    )


def _format_calibration_line(row: pd.Series) -> str:
    return (
        f"- H{int(row['horizon'])}: 예상범위 적중 "
        f"{_format_percent_text(row['coverage_rate'], decimals=0)} · "
        f"치우침 {_format_percent_text(row['median_bias'], decimals=2, signed=True)}"
    )


def _format_top_line(row: pd.Series) -> str:
    realized = _format_percent_text(row["realized_excess_return"], decimals=2, signed=True)
    expected = _format_percent_text(
        row["expected_excess_return_at_selection"],
        decimals=2,
        signed=True,
        na_text="n/a",
    )
    return (
        f"- `{row['symbol']}` {row['company_name']}: "
        f"실제초과 {realized} / 당시기대 {expected}"
    )


def _current_model_rows(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary
    current = summary.loc[
        summary["ranking_version"].astype(str).eq(SELECTION_ENGINE_VERSION)
    ].copy()
    return current.sort_values(["horizon", "row_count"], ascending=[False, False])


def _current_rolling_rows(rolling_summary: pd.DataFrame) -> pd.DataFrame:
    if rolling_summary.empty:
        return rolling_summary
    frame = rolling_summary.loc[
        rolling_summary["ranking_version"].astype(str).eq(SELECTION_ENGINE_VERSION)
        & rolling_summary["window_type"].astype(str).eq("rolling_20d")
        & pd.to_numeric(rolling_summary["count_evaluated"], errors="coerce").fillna(0).gt(0)
    ].copy()
    return frame.sort_values(["horizon", "count_evaluated"], ascending=[False, False])


def _build_report_content(
    *,
    evaluation_date: date,
    summary: pd.DataFrame,
    comparison: pd.DataFrame,
    rolling_summary: pd.DataFrame,
    calibration_summary: pd.DataFrame,
    top_by_horizon: dict[int, pd.DataFrame],
) -> str:
    current_summary = _current_model_rows(summary)
    current_rolling = _current_rolling_rows(rolling_summary)
    lines = [
        f"**StockMaster 사후평가 | {evaluation_date.isoformat()}**",
        "",
        "오늘 메시지는 ‘며칠 전 추천이 실제로 어땠는지’만 짧게 봅니다.",
        "- 평균초과: 같은 기간 비교기준보다 더 벌었는지",
        "- 적중률: 초과수익이 +였던 비율",
        "",
        "**결론**",
    ]

    if current_summary.empty:
        lines.append("- 아직 확정된 추천 결과가 없습니다.")
    else:
        for _, row in current_summary.iterrows():
            lines.append(_format_current_result_line(row))

    if not comparison.empty:
        lines.append("")
        lines.append("**비교기준 대비**")
        for _, row in comparison.sort_values("horizon", ascending=False).iterrows():
            lines.append(_format_comparison_line(row))

    lines.append("")
    lines.append("**최근 20거래일 흐름**")
    if current_rolling.empty:
        lines.append("- 최근 구간 요약이 아직 없습니다.")
    else:
        for _, row in current_rolling.iterrows():
            lines.append(_format_rolling_line(row))

    useful_calibration = calibration_summary.copy()
    if not useful_calibration.empty:
        useful_calibration = useful_calibration.sort_values("horizon", ascending=False).head(2)
        lines.append("")
        lines.append("**예상범위 참고**")
        for _, row in useful_calibration.iterrows():
            lines.append(_format_calibration_line(row))

    d5_cases = top_by_horizon.get(5)
    if d5_cases is not None and not d5_cases.empty:
        lines.append("")
        lines.append("**잘 맞은 H5 사례**")
        for _, row in d5_cases.head(2).iterrows():
            lines.append(_format_top_line(row))

    lines.append("")
    lines.append(
        "※ 이 평가는 과거 추천과 이후 가격 흐름의 사후 점검이며, "
        "수수료·세금은 제외됩니다."
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
                messages = _build_postmortem_payload_messages(
                    username=settings.discord.username,
                    evaluation_date=evaluation_date,
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

        artifact_paths: list[str] = []
        notes = f"Postmortem publish skipped for {evaluation_date.isoformat()}."
        published = False
        manifest_status = "failed"
        error_message: str | None = None
        try:
            render_result = render_postmortem_report(
                settings,
                evaluation_date=evaluation_date,
                horizons=horizons,
                dry_run=dry_run,
            )
            artifact_paths = list(render_result.artifact_paths)
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            decision = resolve_discord_publish_decision(
                enabled=settings.discord.enabled,
                webhook_url=webhook_url,
                dry_run=dry_run,
            )
            if decision == DiscordPublishDecision.SKIP_DISABLED:
                notes = (
                    f"Postmortem publish skipped for {evaluation_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_DRY_RUN:
                notes = (
                    f"Postmortem publish dry-run completed for {evaluation_date.isoformat()}."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
                notes = (
                    f"Postmortem publish skipped for {evaluation_date.isoformat()}. "
                    "Webhook URL is not configured."
                )
                manifest_status = "skipped"
            else:
                response_payloads = publish_discord_messages(
                    webhook_url,
                    list(messages),
                    timeout=10.0,
                )
                published = True
                manifest_status = "success"
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
            notes = f"Postmortem publish failed for {evaluation_date.isoformat()}."
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
