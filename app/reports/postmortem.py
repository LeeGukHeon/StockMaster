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

BAND_LABELS = {
    "in_band": "예상 범위 안",
    "above_upper": "예상보다 강함",
    "below_lower": "예상보다 약함",
    "band_missing": "예상 범위 없음",
    "label_pending": "아직 결과 대기",
}

REASON_LABELS = {
    "ml_alpha_supportive": "최근 흐름과 모델 판단이 함께 받쳐줌",
    "prediction_fallback_used": "예측 보조값을 함께 참고함",
}


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


def _horizon_label(horizon: int) -> str:
    return f"{int(horizon)}거래일"


def _window_label(window_type: str) -> str:
    mapping = {
        "rolling_20d": "최근 20거래일",
        "rolling_60d": "최근 60거래일",
    }
    return mapping.get(str(window_type), str(window_type))


def _ranking_label(ranking_version: str) -> str:
    if str(ranking_version) == SELECTION_ENGINE_VERSION:
        return "현재 추천 모델"
    if str(ranking_version) == EXPLANATORY_RANKING_VERSION:
        return "비교 기준 모델"
    return str(ranking_version)


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


def _format_summary_line(row: pd.Series) -> str:
    band_text = ""
    if pd.notna(row.get("band_coverage_rate")):
        band_text = (
            " | 범위 적중률="
            f"{_format_percent_text(row['band_coverage_rate'], decimals=1)}"
        )
    expected_text = ""
    if pd.notna(row.get("avg_expected_excess_return")):
        expected_text = (
            " | 평균 참고 기대수익="
            f"{_format_percent_text(row['avg_expected_excess_return'], decimals=2, signed=True)}"
        )
    return (
        f"- {_horizon_label(int(row['horizon']))} | {_ranking_label(str(row['ranking_version']))} "
        f"| 평가 수 {int(row['row_count'])} "
        f"| 평균 초과수익률 "
        f"{_format_percent_text(row['avg_realized_excess_return'], decimals=2, signed=True)} "
        f"| 수익 플러스 비율 "
        f"{_format_percent_text(row['hit_rate'], decimals=1)}{expected_text}{band_text}"
    )


def _format_top_line(row: pd.Series) -> str:
    try:
        raw_reasons = json.loads(row["top_reason_tags_json"] or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        raw_reasons = []
    reasons = ", ".join(REASON_LABELS.get(str(item), str(item)) for item in raw_reasons[:2])
    proxy = ""
    if pd.notna(row.get("expected_excess_return_at_selection")):
        proxy = (
            " | 당시 참고 기대수익="
            f"{_format_percent_text(row['expected_excess_return_at_selection'], decimals=2, signed=True)}"
        )
    return (
        f"- `{row['symbol']}` {row['company_name']} ({row['market']}) "
        f"| 선정일 {row['selection_date']} | 실제 초과수익률 "
        f"{_format_percent_text(row['realized_excess_return'], decimals=2, signed=True)}"
        f"{proxy} | 예상 범위 판정 {BAND_LABELS.get(str(row['band_status']), str(row['band_status']))} | 주요 근거 {reasons or '-'}"
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
        f"**StockMaster 사후 점검 | {evaluation_date.isoformat()}**",
        "",
        "- 이 보고서는 실제 자동매매 손익장이 아니라, 당시 추천과 이후 흐름을 비교한 사후 점검입니다.",
        "- 수수료·세금 시뮬레이션은 포함하지 않았고, 당시 저장된 추천과 예상 범위를 다시 계산하지 않고 그대로 사용합니다.",
        "",
        "**한눈에 요약**",
    ]
    if summary.empty:
        lines.append("- 요청한 평가일에 결과가 확정된 추천이 없습니다.")
    else:
        lines.extend(_format_summary_line(row) for _, row in summary.iterrows())

    lines.append("")
    lines.append("**추천 모델과 비교 기준의 차이**")
    if comparison.empty:
        lines.append("- 같은 날짜 기준 비교 결과가 없습니다.")
    else:
        for _, row in comparison.iterrows():
            lines.append(
                f"- {_horizon_label(int(row['horizon']))} 평균 초과수익률 차이="
                f"{_format_percent_text(row['avg_excess_gap'], decimals=2, signed=True)} "
                f"| 수익 플러스 비율 차이="
                f"{_format_percent_text(row['hit_rate_gap'], decimals=1, signed=True)}"
            )

    lines.append("")
    lines.append("**최근 구간 흐름**")
    valid_rolling_summary = rolling_summary.loc[
        pd.to_numeric(rolling_summary["count_evaluated"], errors="coerce").fillna(0).gt(0)
    ].copy()
    if valid_rolling_summary.empty:
        lines.append("- 최근 구간 요약이 아직 없습니다.")
    else:
        for _, row in valid_rolling_summary.iterrows():
            lines.append(
                f"- {_window_label(str(row['window_type']))} | {_horizon_label(int(row['horizon']))} | {_ranking_label(str(row['ranking_version']))} "
                f"| 평가 수 {int(row['count_evaluated'])} "
                f"| 평균 초과수익률 "
                f"{_format_percent_text(row['mean_realized_excess_return'], decimals=2, signed=True)} "
                f"| 수익 플러스 비율 "
                f"{_format_percent_text(row['hit_rate'], decimals=1)}"
            )

    lines.append("")
    lines.append("**예상 범위 점검**")
    if calibration_summary.empty:
        lines.append("- 예상 범위 점검 결과가 아직 없습니다.")
    else:
        for _, row in calibration_summary.iterrows():
            lines.append(
                f"- {_horizon_label(int(row['horizon']))} | 예상 범위 적중률 "
                f"{_format_percent_text(row['coverage_rate'], decimals=1)} "
                f"| 치우침 "
                f"{_format_percent_text(row['median_bias'], decimals=2, signed=True)} "
                f"| 품질 {row['quality_flag']}"
            )

    for horizon, top_frame in sorted(top_by_horizon.items()):
        lines.append("")
        lines.append(f"**대표 사례 | {_horizon_label(int(horizon))}**")
        if top_frame.empty:
            lines.append("- 결과가 확정된 종목이 없습니다.")
        else:
            lines.extend(_format_top_line(row) for _, row in top_frame.iterrows())

    lines.append("")
    lines.append(
        "여기서 말하는 예상 범위는 과거 통계 기반 참고 구간이며 미래를 보장하는 값은 아닙니다. "
        "비교는 같은 날짜에 기록된 추천 묶음끼리만 수행했습니다."
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
