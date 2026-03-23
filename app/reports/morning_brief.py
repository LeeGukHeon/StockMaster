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
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .discord_eod import _build_payload_messages


@dataclass(slots=True)
class MorningBriefRenderResult:
    run_id: str
    as_of_date: date
    payload: dict[str, object]
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class MorningBriefPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str


def _load_market_news(connection, *, as_of_date: date, limit: int = 5) -> pd.DataFrame:
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


def _day_counts(connection, *, as_of_date: date) -> dict[str, int]:
    return {
        "ohlcv_rows": int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_daily_ohlcv WHERE trading_date = ?",
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
        "news_rows": int(
            connection.execute(
                "SELECT COUNT(*) FROM fact_news_item WHERE signal_date = ?",
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
        "ranking_rows": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM fact_ranking
                WHERE as_of_date = ?
                  AND ranking_version = 'selection_engine_v2'
                """,
                [as_of_date],
            ).fetchone()[0]
            or 0
        ),
    }


def _build_content(*, as_of_date: date, counts: dict[str, int], market_news: pd.DataFrame) -> str:
    lines = [
        f"**StockMaster 아침 브리핑 | {as_of_date.isoformat()}**",
        "",
        "- 밤사이 핵심 뉴스와 장 시작 전 준비 상태를 먼저 정리합니다.",
        "- 최종 추천 종목은 장마감 이후 `daily-close`가 끝난 뒤 별도로 갱신됩니다.",
        "",
        "**현재 준비 상태**",
        f"- 전일 종가 데이터: {'준비됨' if counts['ohlcv_rows'] > 0 else '대기'} ({counts['ohlcv_rows']}건)",
        f"- 아침 뉴스 데이터: {'준비됨' if counts['news_rows'] > 0 else '대기'} ({counts['news_rows']}건)",
        f"- 최신 추천 스냅샷: {'있음' if counts['ranking_rows'] > 0 else '아직 없음'} ({counts['ranking_rows']}건)",
        "",
        "**밤사이 주요 뉴스**",
    ]
    if market_news.empty:
        lines.append("- 아직 정리된 시장 전체 뉴스가 없습니다.")
    else:
        for _, row in market_news.iterrows():
            lines.append(f"- {row['title']} ({row['publisher']})")
    lines.extend(
        [
            "",
            "**다음 안내**",
            "- 장중에는 `/상태`, `/종목요약`, `/즉석종목분석`으로 최신 판단을 확인할 수 있습니다.",
            "- 오늘 최종 추천은 장마감 후 `daily-close` 완료 뒤 별도로 갱신됩니다.",
        ]
    )
    return "\n".join(lines)


def render_discord_morning_brief(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> MorningBriefRenderResult:
    ensure_storage_layout(settings)

    with activate_run_context("render_discord_morning_brief", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_news_item", "fact_daily_ohlcv", "fact_ranking"],
                notes=f"Render morning brief for {as_of_date.isoformat()}",
                ranking_version="selection_engine_v2",
            )
            try:
                counts = _day_counts(connection, as_of_date=as_of_date)
                market_news = _load_market_news(connection, as_of_date=as_of_date)
                content = _build_content(as_of_date=as_of_date, counts=counts, market_news=market_news)
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
                    "report_type": "morning_brief",
                    "dry_run": dry_run,
                }

                artifact_dir = (
                    settings.paths.artifacts_dir
                    / "morning_brief"
                    / f"as_of_date={as_of_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_dir.mkdir(parents=True, exist_ok=True)
                payload_path = artifact_dir / "morning_brief_payload.json"
                payload_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                preview_path = artifact_dir / "morning_brief_preview.md"
                preview_path.write_text(content, encoding="utf-8")
                artifact_paths = [str(payload_path), str(preview_path)]
                notes = (
                    f"Morning brief rendered. as_of_date={as_of_date.isoformat()} "
                    f"dry_run={dry_run} message_count={len(messages)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version="selection_engine_v2",
                )
                return MorningBriefRenderResult(
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
                    notes=f"Morning brief render failed for {as_of_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version="selection_engine_v2",
                )
                raise


def publish_discord_morning_brief(
    settings: Settings,
    *,
    as_of_date: date,
    dry_run: bool,
) -> MorningBriefPublishResult:
    ensure_storage_layout(settings)

    with activate_run_context("publish_discord_morning_brief", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["render_discord_morning_brief"],
                notes=f"Publish morning brief for {as_of_date.isoformat()}",
                ranking_version="selection_engine_v2",
            )

        artifact_paths: list[str] = []
        notes = f"Morning brief skipped for {as_of_date.isoformat()}."
        published = False
        manifest_status = "failed"
        error_message: str | None = None
        try:
            render_result = render_discord_morning_brief(
                settings,
                as_of_date=as_of_date,
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
                    f"Morning brief skipped for {as_of_date.isoformat()}. "
                    "DISCORD_REPORT_ENABLED=false."
                )
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_DRY_RUN:
                notes = f"Morning brief dry-run completed for {as_of_date.isoformat()}."
                manifest_status = "skipped"
            elif decision == DiscordPublishDecision.SKIP_MISSING_WEBHOOK:
                notes = (
                    f"Morning brief skipped for {as_of_date.isoformat()}. "
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
                    / "morning_brief"
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
                    f"Morning brief published for {as_of_date.isoformat()}. "
                    f"message_count={len(messages)}"
                )
        except Exception as exc:
            notes = f"Morning brief publish failed for {as_of_date.isoformat()}."
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
                    ranking_version="selection_engine_v2",
                )

        return MorningBriefPublishResult(
            run_id=run_context.run_id,
            as_of_date=as_of_date,
            dry_run=dry_run,
            published=published,
            artifact_paths=artifact_paths,
            notes=notes,
        )
