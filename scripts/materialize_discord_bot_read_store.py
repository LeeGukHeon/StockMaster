# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.discord_bot.read_store import materialize_discord_bot_read_store
from app.logging import configure_logging, get_logger
from app.ops.common import JobStatus
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def _parse_date(value: str):
    from datetime import date

    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize Discord bot read store snapshots.")
    parser.add_argument("--as-of-date", type=_parse_date)
    parser.add_argument("--job-run-id", default="manual-discord-bot-read-store")
    args = parser.parse_args()

    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    target_date = args.as_of_date or today_local(settings.app.timezone)

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        result = materialize_discord_bot_read_store(
            settings,
            connection=connection,
            as_of_date=target_date,
            job_run_id=args.job_run_id,
        )
    logger.info(
        "Discord bot read store completed.",
        extra={
            "run_id_value": result.run_id,
            "row_count": result.row_count,
            "status": result.status,
        },
    )
    if str(result.status).upper() == JobStatus.SKIPPED:
        print(
            "Discord bot read store skipped. "
            f"as_of_date={target_date.isoformat()} notes={result.notes}"
        )
    else:
        print(
            f"Discord bot read store materialized. as_of_date={target_date.isoformat()} "
            f"rows={result.row_count}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
