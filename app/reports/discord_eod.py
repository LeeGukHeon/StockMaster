from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.discord import publish_discord_messages
from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.promotion import load_alpha_promotion_summary
from app.selection.calibration import PREDICTION_VERSION
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

DISCORD_MESSAGE_LIMIT = 1800


@dataclass(slots=True)
class DiscordRenderResult:
    run_id: str
    as_of_date: date
    payload: dict[str, object]
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class DiscordPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _load_market_pulse(connection, *, as_of_date: date) -> dict[str, object]:
    regime_row = connection.execute(
        """
        SELECT regime_state, regime_score, breadth_up_ratio, market_realized_vol_20d
        FROM fact_market_regime_snapshot
        WHERE as_of_date = ?
          AND market_scope = 'KR_ALL'
        """,
        [as_of_date],
    ).fetchone()
    flow_row = connection.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            AVG(CASE WHEN foreign_net_value > 0 THEN 1.0 ELSE 0.0 END) AS foreign_positive_ratio,
            AVG(
                CASE WHEN institution_net_value > 0 THEN 1.0 ELSE 0.0 END
            ) AS institution_positive_ratio
        FROM fact_investor_flow
        WHERE trading_date = ?
        """,
        [as_of_date],
    ).fetchone()
    return {
        "regime_state": regime_row[0] if regime_row else None,
        "regime_score": regime_row[1] if regime_row else None,
        "breadth_up_ratio": regime_row[2] if regime_row else None,
        "market_realized_vol_20d": regime_row[3] if regime_row else None,
        "flow_row_count": flow_row[0] if flow_row else 0,
        "foreign_positive_ratio": flow_row[1] if flow_row else None,
        "institution_positive_ratio": flow_row[2] if flow_row else None,
    }


def _load_top_selection_rows(
    connection,
    *,
    as_of_date: date,
    horizon: int,
    limit: int,
) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            ranking.symbol,
            symbol.company_name,
            symbol.market,
            ranking.final_selection_value,
            ranking.grade,
            ranking.top_reason_tags_json,
            ranking.risk_flags_json,
            prediction.expected_excess_return,
            prediction.lower_band,
            prediction.upper_band
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
         AND prediction.prediction_version = ?
         AND prediction.ranking_version = ?
        WHERE ranking.as_of_date = ?
          AND ranking.horizon = ?
          AND ranking.ranking_version = ?
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT ?
        """,
        [
            PREDICTION_VERSION,
            SELECTION_ENGINE_VERSION,
            as_of_date,
            horizon,
            SELECTION_ENGINE_VERSION,
            limit,
        ],
    ).fetchdf()


def _load_market_news(connection, *, as_of_date: date, limit: int = 3) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT title, publisher
        FROM fact_news_item
        WHERE signal_date = ?
          AND COALESCE(is_market_wide, FALSE)
        ORDER BY published_at DESC
        LIMIT ?
        """,
        [as_of_date, limit],
    ).fetchdf()


def _format_pick_line(row: pd.Series) -> str:
    reasons = ", ".join(json.loads(row["top_reason_tags_json"] or "[]")[:2])
    risks = ", ".join(json.loads(row["risk_flags_json"] or "[]")[:2])
    band = ""
    if pd.notna(row.get("expected_excess_return")):
        band = (
            f" | 참고 기대수익 {float(row['expected_excess_return']):+.2%}"
            f" (참고범위 {float(row['lower_band']):+.2%} ~ {float(row['upper_band']):+.2%})"
        )
    return (
        f"- `{row['symbol']}` {row['company_name']} ({row['market']})"
        f" 종합점수={float(row['final_selection_value']):.1f} 등급={row['grade']}{band}"
        f" | 근거: {reasons or '-'} | 주의: {risks or '-'}"
    )


def _format_alpha_promotion_line(row: pd.Series) -> str:
    p_value = ""
    if pd.notna(row.get("p_value")):
        p_value = f" | p={float(row['p_value']):.3f}"
    active_top10 = ""
    if pd.notna(row.get("active_top10_mean_excess_return")):
        active_top10 = f"{float(row['active_top10_mean_excess_return']):+.2%}"
    compare_top10 = ""
    if pd.notna(row.get("comparison_top10_mean_excess_return")):
        compare_top10 = f"{float(row['comparison_top10_mean_excess_return']):+.2%}"
    compare_text = str(row.get("comparison_model_label") or "-")
    if compare_top10:
        compare_text = f"{compare_text} {compare_top10}"
    active_text = str(row.get("active_model_label") or "-")
    if active_top10:
        active_text = f"{active_text} {active_top10}"
    return (
        f"- {int(row['horizon'])}거래일 모델 점검 | 결정={row['decision_label']} "
        f"| 현재 사용={active_text} | 비교 후보={compare_text} "
        f"| 비교 표본={int(row['sample_count'])}{p_value} | 사유={row['decision_reason_label']}"
    )


def _build_payload_content(
    *,
    as_of_date: date,
    market_pulse: dict[str, object],
    alpha_promotion: pd.DataFrame,
    d1_board: pd.DataFrame,
    d5_board: pd.DataFrame,
    market_news: pd.DataFrame,
) -> str:
    lines = [
        f"**StockMaster 장마감 요약 | {as_of_date.isoformat()}**",
        "",
        (
            f"시장 상황: 국면=`{market_pulse.get('regime_state') or '미확인'}` "
            f"점수={market_pulse.get('regime_score') or '미확인'} "
            f"| 상승 종목 비율={market_pulse.get('breadth_up_ratio') or '미확인'} "
            f"| 수급 집계 종목수={market_pulse.get('flow_row_count') or 0}"
        ),
        "아래 후보는 종목명, 점수, 최근 재무/수급 근거를 바탕으로 정리한 장마감 참고 목록입니다.",
        "표시된 기대수익과 범위는 과거 통계 기반 참고치이며, 실제 수익을 보장하는 예측값은 아닙니다.",
        "",
        "**알파 모델 교체 점검**",
    ]
    if alpha_promotion.empty:
        lines.append("- 아직 알파 모델 교체 점검 결과가 없습니다.")
    else:
        lines.extend(_format_alpha_promotion_line(row) for _, row in alpha_promotion.iterrows())
    lines.extend(
        [
            "",
            "**1거래일 기준 상위 후보**",
        ]
    )
    if d1_board.empty:
        lines.append("- 1거래일 기준 후보가 없습니다.")
    else:
        lines.extend(_format_pick_line(row) for _, row in d1_board.iterrows())
    lines.append("")
    lines.append("**5거래일 기준 상위 후보**")
    if d5_board.empty:
        lines.append("- 5거래일 기준 후보가 없습니다.")
    else:
        lines.extend(_format_pick_line(row) for _, row in d5_board.iterrows())
    lines.append("")
    lines.append("**시장 전체 주요 뉴스**")
    if market_news.empty:
        lines.append("- 해당 날짜의 시장 전체 뉴스가 없습니다.")
    else:
        for _, row in market_news.iterrows():
            lines.append(f"- {row['title']} ({row['publisher']})")
    return "\n".join(lines)


def _split_long_line(line: str, *, limit: int) -> list[str]:
    if len(line) <= limit:
        return [line]

    segments: list[str] = []
    remainder = line
    while len(remainder) > limit:
        split_at = remainder.rfind(" ", 0, limit)
        if split_at <= 0:
            split_at = limit
        segments.append(remainder[:split_at].rstrip())
        remainder = remainder[split_at:].lstrip()
    if remainder:
        segments.append(remainder)
    return segments


def _chunk_content(content: str, *, limit: int = DISCORD_MESSAGE_LIMIT) -> list[str]:
    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0

    def flush() -> None:
        nonlocal current_lines, current_length
        if current_lines:
            chunks.append("\n".join(current_lines).strip())
            current_lines = []
            current_length = 0

    for raw_line in content.splitlines():
        line_variants = _split_long_line(raw_line, limit=limit)
        if not line_variants:
            line_variants = [""]
        for line in line_variants:
            line_length = len(line)
            separator_length = 1 if current_lines else 0
            if current_lines and current_length + separator_length + line_length > limit:
                flush()
            current_lines.append(line)
            current_length += line_length + (1 if len(current_lines) > 1 else 0)
    flush()
    return [chunk for chunk in chunks if chunk]


def _build_payload_messages(
    *,
    username: str,
    as_of_date: date,
    content: str,
) -> list[dict[str, str]]:
    raw_chunks = _chunk_content(content)
    if len(raw_chunks) <= 1:
        return [{"username": username, "content": raw_chunks[0] if raw_chunks else ""}]

    total = len(raw_chunks)
    messages: list[dict[str, str]] = []
    for index, chunk in enumerate(raw_chunks, start=1):
        if index == 1:
            content_text = chunk
        else:
            header = (
                f"**StockMaster 장마감 요약 | {as_of_date.isoformat()} "
                f"(계속 {index}/{total})**"
            )
            content_text = f"{header}\n\n{chunk}"
        messages.append({"username": username, "content": content_text})
    return messages


def render_discord_eod_report(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
    top_limit: int = 5,
) -> DiscordRenderResult:
    ensure_storage_layout(settings)

    with activate_run_context("render_discord_eod_report", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_ranking",
                    "fact_prediction",
                    "fact_market_regime_snapshot",
                    "fact_news_item",
                    "fact_alpha_promotion_test",
                    "fact_alpha_active_model",
                ],
                notes=f"Render Discord EOD report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                market_pulse = _load_market_pulse(connection, as_of_date=as_of_date)
                alpha_promotion = load_alpha_promotion_summary(
                    connection,
                    as_of_date=as_of_date,
                )
                d1_board = _load_top_selection_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizon=1,
                    limit=top_limit,
                )
                d5_board = _load_top_selection_rows(
                    connection,
                    as_of_date=as_of_date,
                    horizon=5,
                    limit=top_limit,
                )
                market_news = _load_market_news(connection, as_of_date=as_of_date)
                content = _build_payload_content(
                    as_of_date=as_of_date,
                    market_pulse=market_pulse,
                    alpha_promotion=alpha_promotion,
                    d1_board=d1_board,
                    d5_board=d5_board,
                    market_news=market_news,
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
                    / "discord"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "discord_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "discord_preview.md"
                preview_lines: list[str] = []
                for index, message in enumerate(messages, start=1):
                    preview_lines.append(f"## Message {index}")
                    preview_lines.append("")
                    preview_lines.append(str(message["content"]))
                    preview_lines.append("")
                preview_path.write_text("\n".join(preview_lines).strip(), encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    f"Discord EOD report rendered. as_of_date={as_of_date.isoformat()} "
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
                return DiscordRenderResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
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
                    notes=f"Discord render failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def publish_discord_eod_report(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> DiscordPublishResult:
    ensure_storage_layout(settings)

    with activate_run_context("publish_discord_eod_report", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["render_discord_eod_report"],
                notes=f"Publish Discord EOD report for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )

        render_result = render_discord_eod_report(settings, as_of_date=as_of_date, dry_run=dry_run)
        artifact_paths = list(render_result.artifact_paths)
        notes = f"Discord publish dry-run completed for {as_of_date.isoformat()}."
        published = False

        try:
            webhook_url = settings.discord.webhook_url
            messages = render_result.payload.get("messages") or []
            if not settings.discord.enabled:
                notes = (
                    f"Discord publish skipped for {as_of_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
            elif dry_run or not webhook_url:
                if not webhook_url:
                    notes = (
                        f"Discord publish skipped for {as_of_date.isoformat()}. "
                        "Webhook URL is not configured."
                    )
            else:
                response_payloads = publish_discord_messages(
                    webhook_url,
                    list(messages),
                    timeout=10.0,
                )
                published = True
                publish_path = (
                    settings.paths.artifacts_dir
                    / "discord"
                    / f"as_of_date={as_of_date.isoformat()}"
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
                    f"Discord publish completed for {as_of_date.isoformat()}. "
                    f"message_count={len(messages)}"
                )
        except Exception as exc:
            notes = (
                f"Discord publish warning for {as_of_date.isoformat()}: {exc}. "
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

        return DiscordPublishResult(
            run_id=run_context.run_id,
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
