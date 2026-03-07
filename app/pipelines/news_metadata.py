from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import get_timezone, now_local, today_local
from app.domain.news.dedupe import canonicalize_link, compute_news_id, dedupe_news_items
from app.domain.news.query_plan import build_query_plan
from app.domain.news.symbol_linker import link_news_item
from app.providers.naver_news.client import NaverNewsProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from ._helpers import load_symbol_frame, slugify, write_json_payload

TAG_KEYWORDS = {
    "semiconductor": ["반도체", "semiconductor"],
    "battery": ["2차전지", "배터리", "battery"],
    "rates": ["금리", "rate"],
    "fx": ["환율", "fx", "dollar"],
    "short_selling": ["공매도", "short selling"],
    "foreign_flow": ["외국인", "foreign investor"],
}


@dataclass(slots=True)
class NewsMetadataSyncResult:
    run_id: str
    signal_date: date
    query_count: int
    row_count: int
    deduped_row_count: int
    unmatched_symbol_count: int
    artifact_paths: list[str]
    notes: str


def _publisher_from_link(value: str | None) -> str | None:
    if not value:
        return None
    netloc = urlsplit(value).netloc.lower()
    return netloc.removeprefix("www.") or None


def _parse_published_at(value: str | None):
    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=get_timezone("Asia/Seoul"))
    return parsed.astimezone(get_timezone("Asia/Seoul"))


def _freshness_score(published_at, signal_date: date) -> float:
    if published_at is None:
        return 0.0
    hours = abs((published_at.date() - signal_date).days) * 24
    return round(1.0 / (1.0 + (hours / 24.0)), 4)


def _detect_tags(text: str) -> list[str]:
    lowered = text.lower()
    return sorted(
        tag
        for tag, keywords in TAG_KEYWORDS.items()
        if any(keyword.lower() in lowered for keyword in keywords)
    )


def upsert_news_items(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("news_item_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_news_item
        WHERE news_id IN (SELECT news_id FROM news_item_stage)
        """
    )
    connection.execute(
        """
        INSERT INTO fact_news_item (
            news_id,
            signal_date,
            published_at,
            symbol_candidates,
            query_keyword,
            title,
            publisher,
            link,
            snippet,
            tags_json,
            catalyst_score,
            sentiment_score,
            freshness_score,
            source,
            canonical_link,
            match_method_json,
            query_bucket,
            is_market_wide,
            source_notes_json,
            ingested_at
        )
        SELECT
            news_id,
            signal_date,
            published_at,
            symbol_candidates,
            query_keyword,
            title,
            publisher,
            link,
            snippet,
            tags_json,
            catalyst_score,
            sentiment_score,
            freshness_score,
            source,
            canonical_link,
            match_method_json,
            query_bucket,
            is_market_wide,
            source_notes_json,
            ingested_at
        FROM news_item_stage
        """
    )
    connection.unregister("news_item_stage")


def sync_news_metadata(
    settings: Settings,
    *,
    signal_date: date,
    mode: str,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    force: bool = False,
    dry_run: bool = False,
    query_pack: str = "default",
    max_items_per_query: int = 50,
    naver_provider: NaverNewsProvider | None = None,
) -> NewsMetadataSyncResult:
    ensure_storage_layout(settings)
    owns_provider = naver_provider is None
    provider = naver_provider or NaverNewsProvider(settings)

    with activate_run_context("sync_news_metadata", as_of_date=signal_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["naver_news_search", "dim_symbol", f"news_query_pack:{query_pack}"],
                notes=f"Sync news metadata for {signal_date.isoformat()} using mode={mode}",
            )
            try:
                effective_limit = limit_symbols
                if effective_limit is None and mode != "market_only" and not symbols:
                    effective_limit = 25

                focus_frame = load_symbol_frame(
                    connection,
                    symbols=symbols,
                    limit_symbols=effective_limit,
                )
                query_tasks = build_query_plan(
                    project_root=settings.paths.project_root,
                    mode=mode,
                    focus_frame=focus_frame,
                    pack_name=query_pack,
                )

                if dry_run:
                    notes = (
                        f"Dry run only. signal_date={signal_date.isoformat()} "
                        f"query_count={len(query_tasks)}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return NewsMetadataSyncResult(
                        run_id=run_context.run_id,
                        signal_date=signal_date,
                        query_count=len(query_tasks),
                        row_count=0,
                        deduped_row_count=0,
                        unmatched_symbol_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                existing_news_ids: set[str] = set()
                if not force:
                    existing_news_ids = {
                        str(row[0])
                        for row in connection.execute(
                            """
                            SELECT news_id
                            FROM fact_news_item
                            WHERE signal_date = ?
                            """,
                            [signal_date],
                        ).fetchall()
                    }

                artifact_paths: list[str] = []
                staged_rows: list[dict[str, object]] = []

                for task in query_tasks:
                    payload = provider.search_news(
                        query=task.query,
                        limit=max_items_per_query,
                        start=1,
                        sort="date",
                    )
                    raw_path = (
                        settings.paths.raw_dir
                        / "naver_news"
                        / f"fetch_date={today_local(settings.app.timezone).isoformat()}"
                        / f"query_bucket={task.query_bucket}"
                        / f"{run_context.run_id}_{slugify(task.query)}.json"
                    )
                    artifact_paths.append(str(write_json_payload(raw_path, payload)))

                    for item in payload.get("items", []):
                        published_at = _parse_published_at(item.get("pubDate"))
                        if published_at is None or published_at.date() != signal_date:
                            continue

                        title = item.get("title_plain") or ""
                        snippet = item.get("description_plain") or ""
                        link = item.get("originallink") or item.get("link") or ""
                        canonical_link = canonicalize_link(link)
                        publisher = _publisher_from_link(link)
                        link_result = link_news_item(
                            symbol_frame=focus_frame,
                            title=title,
                            snippet=snippet,
                            query_symbol=task.symbol,
                            query_company_name=task.company_name,
                        )
                        tags = _detect_tags(f"{title} {snippet}")
                        news_id = compute_news_id(
                            canonical_link=canonical_link,
                            title=title,
                            publisher=publisher or "",
                            published_at=published_at.isoformat(),
                        )
                        if news_id in existing_news_ids and not force:
                            continue

                        staged_rows.append(
                            {
                                "news_id": news_id,
                                "signal_date": signal_date,
                                "published_at": published_at,
                                "symbol_candidates": json.dumps(
                                    link_result.symbols,
                                    ensure_ascii=False,
                                ),
                                "query_keyword": task.query,
                                "title": title,
                                "publisher": publisher,
                                "link": link,
                                "snippet": snippet,
                                "tags_json": json.dumps(tags, ensure_ascii=False),
                                "catalyst_score": round(min(len(tags) * 0.25, 1.0), 4),
                                "sentiment_score": None,
                                "freshness_score": _freshness_score(published_at, signal_date),
                                "source": "naver_news_search",
                                "canonical_link": canonical_link,
                                "match_method_json": link_result.match_method_json,
                                "query_bucket": task.query_bucket,
                                "is_market_wide": task.is_market_wide,
                                "source_notes_json": json.dumps(
                                    {
                                        "originallink": item.get("originallink"),
                                        "naver_link": item.get("link"),
                                    },
                                    ensure_ascii=False,
                                ),
                                "ingested_at": now_local(settings.app.timezone),
                            }
                        )

                staged_frame = pd.DataFrame(staged_rows)
                deduped_frame = (
                    dedupe_news_items(staged_frame) if not staged_frame.empty else staged_frame
                )
                unmatched_symbol_count = 0
                if not deduped_frame.empty:
                    unmatched_symbol_count = int(
                        deduped_frame["symbol_candidates"].map(lambda value: value == "[]").sum()
                    )
                    upsert_news_items(connection, deduped_frame)
                    curated_path = write_parquet(
                        deduped_frame,
                        base_dir=settings.paths.curated_dir,
                        dataset="news/items",
                        partitions={"signal_date": signal_date.isoformat()},
                        filename="news_items.parquet",
                    )
                    artifact_paths.append(str(curated_path))

                if len(query_tasks) > 0 and deduped_frame.empty:
                    raise RuntimeError(
                        "No news metadata rows were materialized for the requested signal date."
                    )

                notes = (
                    f"News metadata sync completed. signal_date={signal_date.isoformat()}, "
                    f"queries={len(query_tasks)}, staged_rows={len(staged_frame)}, "
                    f"deduped_rows={len(deduped_frame)}, unmatched_symbols={unmatched_symbol_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return NewsMetadataSyncResult(
                    run_id=run_context.run_id,
                    signal_date=signal_date,
                    query_count=len(query_tasks),
                    row_count=len(staged_frame),
                    deduped_row_count=len(deduped_frame),
                    unmatched_symbol_count=unmatched_symbol_count,
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
                    notes=f"News metadata sync failed for {signal_date.isoformat()}",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_provider:
                    provider.close()
