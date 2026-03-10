# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.logging import configure_logging, get_logger
from app.pipelines._helpers import iter_dates
from app.pipelines.daily_ohlcv import sync_daily_ohlcv
from app.pipelines.fundamentals_snapshot import sync_fundamentals_snapshot
from app.pipelines.news_metadata import sync_news_metadata
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def _is_empty_news_backfill_error(exc: Exception) -> bool:
    return "No news metadata rows were materialized for the requested signal date." in str(exc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill OHLCV, fundamentals, and news metadata.")
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument(
        "--mode",
        default="market_and_focus",
        choices=["market_only", "market_and_focus", "symbol_list"],
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--query-pack", default="default")
    parser.add_argument("--max-items-per-query", type=int, default=50)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    symbols = _parse_symbols(args.symbols)
    dates = list(iter_dates(args.start, args.end))

    with activate_run_context("backfill_core_research_data", as_of_date=args.end) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "sync_daily_ohlcv",
                    "sync_fundamentals_snapshot",
                    "sync_news_metadata",
                ],
                notes=(
                    "Backfill core research data from "
                    f"{args.start.isoformat()} to {args.end.isoformat()}"
                ),
            )
            artifact_paths: list[str] = []
            skipped_empty_news_dates: list[str] = []
            try:
                for current_date in dates:
                    ohlcv_result = sync_daily_ohlcv(
                        settings,
                        trading_date=current_date,
                        symbols=symbols,
                        limit_symbols=args.limit_symbols,
                        market=args.market,
                        force=args.force,
                        dry_run=args.dry_run,
                    )
                    fundamentals_result = sync_fundamentals_snapshot(
                        settings,
                        as_of_date=current_date,
                        symbols=symbols,
                        limit_symbols=args.limit_symbols,
                        force=args.force,
                        dry_run=args.dry_run,
                    )
                    try:
                        news_result = sync_news_metadata(
                            settings,
                            signal_date=current_date,
                            mode=args.mode,
                            symbols=symbols,
                            limit_symbols=args.limit_symbols,
                            force=args.force,
                            dry_run=args.dry_run,
                            query_pack=args.query_pack,
                            max_items_per_query=args.max_items_per_query,
                        )
                    except RuntimeError as exc:
                        if not _is_empty_news_backfill_error(exc):
                            raise
                        skipped_empty_news_dates.append(current_date.isoformat())
                        logger.warning(
                            "Backfill skipped empty historical news date.",
                            extra={"signal_date": current_date.isoformat()},
                        )
                        news_result = None
                    artifact_paths.extend(ohlcv_result.artifact_paths)
                    artifact_paths.extend(fundamentals_result.artifact_paths)
                    if news_result is not None:
                        artifact_paths.extend(news_result.artifact_paths)

                notes = (
                    f"Backfill completed for {len(dates)} dates. "
                    f"range={args.start.isoformat()}..{args.end.isoformat()}, "
                    f"skipped_empty_news_dates={len(skipped_empty_news_dates)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                logger.info(
                    "Backfill completed.",
                    extra={"run_id_value": run_context.run_id, "date_count": len(dates)},
                )
                print(
                    f"Backfill completed. run_id={run_context.run_id} "
                    f"dates={len(dates)} artifacts={len(artifact_paths)}"
                )
                return 0
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=artifact_paths,
                    notes="Backfill failed.",
                    error_message=str(exc),
                )
                raise


if __name__ == "__main__":
    raise SystemExit(main())
