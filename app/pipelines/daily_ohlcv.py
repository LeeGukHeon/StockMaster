from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.domain.validation.market_data import validate_daily_ohlcv
from app.providers.kis.client import KISProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from ._helpers import load_symbol_frame, write_json_payload


@dataclass(slots=True)
class DailyOhlcvSyncResult:
    run_id: str
    trading_date: date
    requested_symbol_count: int
    row_count: int
    skipped_symbol_count: int
    failed_symbol_count: int
    artifact_paths: list[str]
    notes: str


def _normalize_daily_ohlcv(symbol: str, trading_date: date, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    normalized = frame.copy()
    normalized["trading_date"] = pd.to_datetime(
        normalized["stck_bsop_date"], format="%Y%m%d", errors="coerce"
    ).dt.date
    normalized = normalized.loc[normalized["trading_date"] == trading_date].copy()
    if normalized.empty:
        return normalized

    normalized["symbol"] = symbol
    normalized["open"] = pd.to_numeric(normalized["stck_oprc"], errors="coerce")
    normalized["high"] = pd.to_numeric(normalized["stck_hgpr"], errors="coerce")
    normalized["low"] = pd.to_numeric(normalized["stck_lwpr"], errors="coerce")
    normalized["close"] = pd.to_numeric(normalized["stck_clpr"], errors="coerce")
    normalized["volume"] = (
        pd.to_numeric(normalized["acml_vol"], errors="coerce").fillna(0).astype("int64")
    )
    normalized["turnover_value"] = pd.to_numeric(normalized["acml_tr_pbmn"], errors="coerce")
    normalized["market_cap"] = pd.NA
    normalized["source"] = "kis_daily_ohlcv"
    normalized["source_notes_json"] = normalized.apply(
        lambda row: json.dumps(
            {
                "mod_yn": row.get("mod_yn"),
                "price_change_sign": row.get("prdy_vrss_sign"),
                "price_change_value": row.get("prdy_vrss"),
            },
            ensure_ascii=False,
        ),
        axis=1,
    )
    return normalized[
        [
            "trading_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_value",
            "market_cap",
            "source",
            "source_notes_json",
        ]
    ]


def upsert_daily_ohlcv(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("daily_ohlcv_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_daily_ohlcv
        WHERE (trading_date, symbol) IN (
            SELECT trading_date, symbol
            FROM daily_ohlcv_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_daily_ohlcv (
            trading_date,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            turnover_value,
            market_cap,
            source,
            source_notes_json,
            ingested_at
        )
        SELECT
            trading_date,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            turnover_value,
            market_cap,
            source,
            source_notes_json,
            ingested_at
        FROM daily_ohlcv_stage
        """
    )
    connection.unregister("daily_ohlcv_stage")


def sync_daily_ohlcv(
    settings: Settings,
    *,
    trading_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
    kis_provider: KISProvider | None = None,
) -> DailyOhlcvSyncResult:
    ensure_storage_layout(settings)
    owns_provider = kis_provider is None
    provider = kis_provider or KISProvider(settings)

    with activate_run_context("sync_daily_ohlcv", as_of_date=trading_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "kis_daily_ohlcv",
                    "dim_trading_calendar",
                    "vw_universe_active_common_stock",
                ],
                notes=f"Sync daily OHLCV for {trading_date.isoformat()}",
            )
            try:
                calendar_row = connection.execute(
                    """
                    SELECT is_trading_day
                    FROM dim_trading_calendar
                    WHERE trading_date = ?
                    """,
                    [trading_date],
                ).fetchone()
                if calendar_row is None:
                    raise RuntimeError(
                        "Trading calendar is missing the requested date. "
                        "Run scripts/sync_trading_calendar.py first."
                    )

                symbol_frame = load_symbol_frame(
                    connection,
                    symbols=symbols,
                    market=market,
                    limit_symbols=limit_symbols,
                )
                requested_symbol_count = len(symbol_frame)

                if not bool(calendar_row[0]):
                    notes = f"{trading_date.isoformat()} is not a trading day. No rows fetched."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return DailyOhlcvSyncResult(
                        run_id=run_context.run_id,
                        trading_date=trading_date,
                        requested_symbol_count=requested_symbol_count,
                        row_count=0,
                        skipped_symbol_count=requested_symbol_count,
                        failed_symbol_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                existing_symbols: set[str] = set()
                if not force:
                    existing_symbols = {
                        str(row[0]).zfill(6)
                        for row in connection.execute(
                            "SELECT symbol FROM fact_daily_ohlcv WHERE trading_date = ?",
                            [trading_date],
                        ).fetchall()
                    }

                if dry_run:
                    notes = (
                        f"Dry run only. trading_date={trading_date.isoformat()} "
                        f"symbols={requested_symbol_count} skipped_existing={len(existing_symbols)}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                    )
                    return DailyOhlcvSyncResult(
                        run_id=run_context.run_id,
                        trading_date=trading_date,
                        requested_symbol_count=requested_symbol_count,
                        row_count=0,
                        skipped_symbol_count=len(existing_symbols),
                        failed_symbol_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                output_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                skipped_symbol_count = 0
                failed_symbol_count = 0
                failed_symbols: list[str] = []

                for row in symbol_frame.itertuples(index=False):
                    symbol = str(row.symbol).zfill(6)
                    if symbol in existing_symbols:
                        skipped_symbol_count += 1
                        continue

                    try:
                        probe = provider.fetch_daily_ohlcv(
                            symbol=symbol,
                            start_date=trading_date.strftime("%Y%m%d"),
                            end_date=trading_date.strftime("%Y%m%d"),
                        )
                        raw_path = (
                            settings.paths.raw_dir
                            / "kis"
                            / "daily_ohlcv"
                            / f"trading_date={trading_date.isoformat()}"
                            / f"symbol={symbol}"
                            / f"{run_context.run_id}.json"
                        )
                        artifact_paths.append(str(write_json_payload(raw_path, probe.payload)))
                        normalized = _normalize_daily_ohlcv(symbol, trading_date, probe.frame)
                        if normalized.empty:
                            skipped_symbol_count += 1
                            continue
                        output_frames.append(normalized)
                    except Exception:
                        failed_symbol_count += 1
                        failed_symbols.append(symbol)

                combined = (
                    pd.concat(output_frames, ignore_index=True)
                    if output_frames
                    else pd.DataFrame(
                        columns=[
                            "trading_date",
                            "symbol",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "turnover_value",
                            "market_cap",
                            "source",
                            "source_notes_json",
                        ]
                    )
                )
                if not combined.empty:
                    combined["ingested_at"] = now_local(settings.app.timezone)
                    validate_daily_ohlcv(combined)
                    upsert_daily_ohlcv(connection, combined)
                    curated_path = write_parquet(
                        combined,
                        base_dir=settings.paths.curated_dir,
                        dataset="market/daily_ohlcv",
                        partitions={"trading_date": trading_date.isoformat()},
                        filename="daily_ohlcv.parquet",
                    )
                    artifact_paths.append(str(curated_path))

                if requested_symbol_count > 0 and combined.empty and failed_symbol_count > 0:
                    raise RuntimeError(
                        f"No OHLCV rows were loaded. failed_symbols={failed_symbols[:10]}"
                    )

                notes = (
                    f"OHLCV sync completed. trading_date={trading_date.isoformat()}, "
                    f"rows={len(combined)}, requested_symbols={requested_symbol_count}, "
                    f"skipped={skipped_symbol_count}, failed={failed_symbol_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return DailyOhlcvSyncResult(
                    run_id=run_context.run_id,
                    trading_date=trading_date,
                    requested_symbol_count=requested_symbol_count,
                    row_count=len(combined),
                    skipped_symbol_count=skipped_symbol_count,
                    failed_symbol_count=failed_symbol_count,
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
                    notes=f"OHLCV sync failed for {trading_date.isoformat()}",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_provider:
                    provider.close()
