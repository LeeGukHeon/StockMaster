# ruff: noqa: E402

from __future__ import annotations

import argparse
import copy
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.labels.forward_returns import (  # noqa: E402
    build_forward_labels,
    ensure_forward_path_label_table,
    recreate_forward_path_label_table,
)
from app.settings import load_settings  # noqa: E402
from app.storage.bootstrap import ensure_storage_layout  # noqa: E402
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection  # noqa: E402

PATH_OVERLAY_COLUMNS = [
    "run_id",
    "as_of_date",
    "symbol",
    "horizon",
    "max_forward_return",
    "min_forward_return",
    "take_profit_3_hit",
    "take_profit_3_date",
    "take_profit_5_hit",
    "take_profit_5_date",
    "stop_loss_3_hit",
    "stop_loss_3_date",
    "stop_loss_5_hit",
    "stop_loss_5_date",
    "path_return_tp3_sl3_conservative",
    "path_return_tp5_sl3_conservative",
    "path_excess_return_tp3_sl3_conservative",
    "path_excess_return_tp5_sl3_conservative",
    "label_available_flag",
    "created_at",
]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [token.strip().zfill(6) for token in value.split(",") if token.strip()]


def _sql_date(value: date) -> str:
    return value.isoformat()


def _sql_string(value: Path) -> str:
    return str(value).replace("'", "''")


def _resolve_relevant_end(
    source_connection,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
) -> tuple[list[date], date]:
    trading_days = [
        pd.Timestamp(row[0]).date()
        for row in source_connection.execute(
            """
            SELECT trading_date
            FROM source_db.dim_trading_calendar
            WHERE is_trading_day
            ORDER BY trading_date
            """
        ).fetchall()
    ]
    trading_day_index = {value: index for index, value in enumerate(trading_days)}
    as_of_dates = [
        trading_date for trading_date in trading_days if start_date <= trading_date <= end_date
    ]
    if not as_of_dates:
        raise RuntimeError("No trading dates available in the requested range.")
    future_end_index = max(
        trading_day_index[date_value] + max(horizons)
        for date_value in as_of_dates
        if date_value in trading_day_index
    )
    return as_of_dates, trading_days[min(future_end_index, len(trading_days) - 1)]


def _prepare_work_db(
    *,
    source_db_path: Path,
    work_db_path: Path,
    start_date: date,
    end_date: date,
    horizons: list[int],
    force: bool,
) -> tuple[int, int, date]:
    if work_db_path.exists():
        if not force:
            raise FileExistsError(
                f"Work DB already exists: {work_db_path}. Pass --force-workdb to replace it."
            )
        work_db_path.unlink()
    tmp_dir = work_db_path.with_suffix(work_db_path.suffix + ".tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    work_db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb_connection(work_db_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(f"ATTACH '{_sql_string(source_db_path)}' AS source_db (READ_ONLY)")
        try:
            as_of_dates, relevant_end = _resolve_relevant_end(
                connection,
                start_date=start_date,
                end_date=end_date,
                horizons=horizons,
            )
            connection.execute(
                "CREATE OR REPLACE TABLE dim_trading_calendar AS "
                "SELECT * FROM source_db.dim_trading_calendar"
            )
            connection.execute(
                "CREATE OR REPLACE TABLE dim_symbol AS SELECT * FROM source_db.dim_symbol"
            )
            connection.execute(
                """
                CREATE OR REPLACE TABLE fact_daily_ohlcv AS
                SELECT *
                FROM source_db.fact_daily_ohlcv
                WHERE trading_date BETWEEN ? AND ?
                """,
                [start_date, relevant_end],
            )
            ohlcv_count = int(
                connection.execute("SELECT COUNT(*) FROM fact_daily_ohlcv").fetchone()[0] or 0
            )
        finally:
            connection.execute("DETACH source_db")
    return len(as_of_dates), ohlcv_count, relevant_end


def _export_overlay(work_db_path: Path, overlay_path: Path) -> int:
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_path.unlink(missing_ok=True)
    column_list = ", ".join(PATH_OVERLAY_COLUMNS)
    with duckdb_connection(work_db_path, read_only=True) as connection:
        row_count = int(
            connection.execute("SELECT COUNT(*) FROM fact_forward_return_path_label").fetchone()[0]
            or 0
        )
        connection.execute(
            f"""
            COPY (
                SELECT {column_list}
                FROM fact_forward_return_path_label
            )
            TO ? (FORMAT PARQUET)
            """,
            [str(overlay_path)],
        )
    return row_count


def _merge_overlay(
    *,
    source_db_path: Path,
    overlay_path: Path,
    start_date: date,
    end_date: date,
    horizons: list[int],
    recreate_target_table: bool,
) -> int:
    horizon_list = ", ".join(str(int(horizon)) for horizon in horizons)
    column_list = ", ".join(PATH_OVERLAY_COLUMNS)
    start_sql = _sql_date(start_date)
    end_sql = _sql_date(end_date)
    with duckdb_connection(source_db_path) as connection:
        if recreate_target_table:
            recreate_forward_path_label_table(connection)
        else:
            ensure_forward_path_label_table(connection)
            connection.execute(
                f"""
                DELETE FROM fact_forward_return_path_label
                WHERE as_of_date BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'
                  AND horizon IN ({horizon_list})
                """
            )
        connection.execute(
            f"""
            INSERT INTO fact_forward_return_path_label ({column_list})
            SELECT {column_list}
            FROM read_parquet(?)
            """,
            [str(overlay_path)],
        )
        return int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM fact_forward_return_path_label
                WHERE as_of_date BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'
                  AND horizon IN ({horizon_list})
                """
            ).fetchone()[0]
            or 0
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild path-overlay forward labels in a small work DuckDB, then merge the "
            "result back into the production mart."
        )
    )
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--symbols")
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument("--chunk-trading-days", type=int, default=5)
    parser.add_argument("--work-db", type=Path)
    parser.add_argument("--overlay-parquet", type=Path)
    parser.add_argument("--force-workdb", action="store_true")
    parser.add_argument(
        "--recreate-target-table",
        action="store_true",
        help="Drop and recreate the production path-overlay table before merging.",
    )
    parser.add_argument("--keep-workdb", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    ensure_storage_layout(settings)
    work_root = settings.paths.data_dir / "work" / "path_overlay_rebuild"
    work_db_path = args.work_db or (
        work_root / f"path_overlay_{args.start.isoformat()}_{args.end.isoformat()}.duckdb"
    )
    overlay_path = args.overlay_parquet or work_db_path.with_suffix(".parquet")

    as_of_count, ohlcv_count, relevant_end = _prepare_work_db(
        source_db_path=settings.paths.duckdb_path,
        work_db_path=work_db_path,
        start_date=args.start,
        end_date=args.end,
        horizons=args.horizons,
        force=args.force_workdb,
    )
    print(
        "Prepared work DB. "
        f"work_db={work_db_path} as_of_dates={as_of_count} "
        f"ohlcv_rows={ohlcv_count} relevant_end={relevant_end.isoformat()}"
    )

    work_settings = copy.deepcopy(settings)
    work_settings.paths.duckdb_path = work_db_path
    result = build_forward_labels(
        work_settings,
        start_date=args.start,
        end_date=args.end,
        horizons=args.horizons,
        symbols=_parse_symbols(args.symbols),
        limit_symbols=args.limit_symbols,
        market=args.market,
        force=True,
        bootstrap=True,
        path_overlay_only=True,
        chunk_trading_days=args.chunk_trading_days,
        recreate_path_overlay_table=True,
    )
    overlay_count = _export_overlay(work_db_path, overlay_path)
    merged_count = _merge_overlay(
        source_db_path=settings.paths.duckdb_path,
        overlay_path=overlay_path,
        start_date=args.start,
        end_date=args.end,
        horizons=args.horizons,
        recreate_target_table=args.recreate_target_table,
    )
    if not args.keep_workdb:
        work_db_path.unlink(missing_ok=True)
        shutil.rmtree(work_db_path.with_suffix(work_db_path.suffix + ".tmp"), ignore_errors=True)
    print(
        "Path overlay rebuild completed. "
        f"run_id={result.run_id} rows={result.row_count} available={result.available_row_count} "
        f"overlay_rows={overlay_count} merged_range_rows={merged_count} overlay={overlay_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
