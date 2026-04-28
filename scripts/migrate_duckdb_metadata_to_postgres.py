# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.settings import load_settings
from app.storage.metadata_postgres import (
    copy_duckdb_metadata_to_postgres,
    duckdb_metadata_row_counts,
    metadata_postgres_enabled,
    postgres_metadata_row_counts,
)
from app.storage.metadata_schema import METADATA_TABLES


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy operational metadata tables from DuckDB to Postgres."
    )
    parser.add_argument("--truncate-first", action="store_true")
    parser.add_argument(
        "--if-target-empty",
        action="store_true",
        help="Run the migration only when the Postgres metadata store is empty.",
    )
    parser.add_argument("--tables", nargs="*")
    args = parser.parse_args()

    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    if not metadata_postgres_enabled(settings):
        print("Metadata migration skipped. Enable postgres metadata store first.")
        return 0
    selected_tables = tuple(args.tables) if args.tables else METADATA_TABLES
    if args.if_target_empty:
        target_counts = postgres_metadata_row_counts(settings, tables=selected_tables)
        target_total = sum(target_counts.values())
        if target_total > 0:
            print(
                "Metadata migration skipped. Postgres metadata store already contains rows."
            )
            return 0
        source_counts = duckdb_metadata_row_counts(settings, tables=selected_tables)
        source_total = sum(source_counts.values())
        if source_total == 0:
            print("Metadata migration skipped. DuckDB metadata source is empty.")
            return 0
    results = copy_duckdb_metadata_to_postgres(
        settings,
        tables=selected_tables,
        truncate_first=args.truncate_first,
    )
    get_logger(__name__).info(
        "Metadata migration completed.",
        extra={"tables": results},
    )
    for table_name, row_count in results.items():
        print(f"{table_name}: {row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
