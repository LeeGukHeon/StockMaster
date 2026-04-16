from __future__ import annotations

import gc
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from time import perf_counter
from typing import Iterable

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.providers.kis.client import KISProvider
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from ._helpers import load_symbol_frame, write_json_payload

DATE_KEYS = ("stck_bsop_date", "bsop_date", "base_dt", "date")
FOREIGN_NET_VOLUME_KEYS = (
    "frgn_ntby_qty",
    "frgn_ntby_qty_smtn",
    "frgn_ntby_qty_sum",
)
INSTITUTION_NET_VOLUME_KEYS = (
    "orgn_ntby_qty",
    "orgn_ntby_qty_smtn",
    "orgn_ntby_qty_sum",
)
INDIVIDUAL_NET_VOLUME_KEYS = (
    "prsn_ntby_qty",
    "prsn_ntby_qty_smtn",
    "indv_ntby_qty",
    "indv_ntby_qty_sum",
)
FOREIGN_NET_VALUE_KEYS = (
    "frgn_ntby_tr_pbmn",
    "frgn_ntby_amt",
    "frgn_ntby_tr_amt",
)
INSTITUTION_NET_VALUE_KEYS = (
    "orgn_ntby_tr_pbmn",
    "orgn_ntby_amt",
    "orgn_ntby_tr_amt",
)
INDIVIDUAL_NET_VALUE_KEYS = (
    "prsn_ntby_tr_pbmn",
    "indv_ntby_amt",
    "indv_ntby_tr_amt",
)
DEFAULT_INVESTOR_FLOW_FLUSH_BATCH_SIZE = 100
DEFAULT_INVESTOR_FLOW_MAX_WORKERS = 4
DEFAULT_INVESTOR_FLOW_PROVIDER_RECYCLE_INTERVAL = 100


@dataclass(slots=True)
class InvestorFlowSyncResult:
    run_id: str
    trading_date: date
    requested_symbol_count: int
    row_count: int
    skipped_symbol_count: int
    failed_symbol_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class _InvestorFlowFetchResult:
    symbol: str
    normalized: pd.DataFrame
    artifact_paths: list[str]


def _pick_numeric(frame: pd.DataFrame, keys: tuple[str, ...]) -> pd.Series:
    for key in keys:
        if key in frame.columns:
            return pd.to_numeric(frame[key], errors="coerce")
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="float64")


def _pick_date(frame: pd.DataFrame, keys: tuple[str, ...], *, fallback: date) -> pd.Series:
    for key in keys:
        if key in frame.columns:
            values = pd.to_datetime(frame[key], format="%Y%m%d", errors="coerce")
            if values.notna().any():
                return values.dt.date
    return pd.Series([fallback] * len(frame), index=frame.index)


def _normalize_investor_flow(
    *,
    symbol: str,
    market: str,
    trading_date: date,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    matched_fields = {
        "foreign_net_volume": next(
            (key for key in FOREIGN_NET_VOLUME_KEYS if key in frame.columns),
            None,
        ),
        "institution_net_volume": next(
            (key for key in INSTITUTION_NET_VOLUME_KEYS if key in frame.columns),
            None,
        ),
        "individual_net_volume": next(
            (key for key in INDIVIDUAL_NET_VOLUME_KEYS if key in frame.columns),
            None,
        ),
        "foreign_net_value": next(
            (key for key in FOREIGN_NET_VALUE_KEYS if key in frame.columns),
            None,
        ),
        "institution_net_value": next(
            (key for key in INSTITUTION_NET_VALUE_KEYS if key in frame.columns),
            None,
        ),
        "individual_net_value": next(
            (key for key in INDIVIDUAL_NET_VALUE_KEYS if key in frame.columns),
            None,
        ),
    }

    trading_dates = _pick_date(frame, DATE_KEYS, fallback=trading_date)
    matching_mask = trading_dates == trading_date
    if not matching_mask.any():
        return pd.DataFrame()
    normalized = frame.loc[matching_mask].copy()
    normalized["trading_date"] = trading_dates.loc[matching_mask]

    normalized["symbol"] = symbol
    normalized["market"] = market
    normalized["foreign_net_volume"] = _pick_numeric(normalized, FOREIGN_NET_VOLUME_KEYS)
    normalized["institution_net_volume"] = _pick_numeric(normalized, INSTITUTION_NET_VOLUME_KEYS)
    normalized["individual_net_volume"] = _pick_numeric(normalized, INDIVIDUAL_NET_VOLUME_KEYS)
    normalized["foreign_net_value"] = _pick_numeric(normalized, FOREIGN_NET_VALUE_KEYS)
    normalized["institution_net_value"] = _pick_numeric(normalized, INSTITUTION_NET_VALUE_KEYS)
    normalized["individual_net_value"] = _pick_numeric(normalized, INDIVIDUAL_NET_VALUE_KEYS)
    numeric_columns = [
        "foreign_net_volume",
        "institution_net_volume",
        "individual_net_volume",
        "foreign_net_value",
        "institution_net_value",
        "individual_net_value",
    ]
    if normalized[numeric_columns].isna().all(axis=None):
        return pd.DataFrame()

    normalized["source"] = "kis_investor_flow_daily"
    normalized["source_notes_json"] = json.dumps(
        {
            "matched_fields": matched_fields,
            "payload_columns": list(frame.columns),
        },
        ensure_ascii=False,
    )
    normalized = normalized.sort_values("trading_date").tail(1)
    return normalized[
        [
            "trading_date",
            "symbol",
            "market",
            "foreign_net_volume",
            "institution_net_volume",
            "individual_net_volume",
            "foreign_net_value",
            "institution_net_value",
            "individual_net_value",
            "source",
            "source_notes_json",
        ]
    ]


def upsert_investor_flow(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("investor_flow_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_investor_flow
        WHERE (trading_date, symbol) IN (
            SELECT trading_date, symbol
            FROM investor_flow_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_investor_flow (
            run_id,
            trading_date,
            symbol,
            market,
            foreign_net_volume,
            institution_net_volume,
            individual_net_volume,
            foreign_net_value,
            institution_net_value,
            individual_net_value,
            source,
            source_notes_json,
            created_at
        )
        SELECT
            run_id,
            trading_date,
            symbol,
            market,
            foreign_net_volume,
            institution_net_volume,
            individual_net_volume,
            foreign_net_value,
            institution_net_value,
            individual_net_value,
            source,
            source_notes_json,
            created_at
        FROM investor_flow_stage
        """
    )
    connection.unregister("investor_flow_stage")


def _load_sync_inputs(
    settings: Settings,
    *,
    trading_date: date,
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
    force: bool,
) -> tuple[bool, pd.DataFrame, int, set[str]]:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
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
            as_of_date=trading_date,
        )
        requested_symbol_count = len(symbol_frame)
        existing_symbols: set[str] = set()
        if not force:
            existing_symbols = {
                str(row[0]).zfill(6)
                for row in connection.execute(
                    "SELECT symbol FROM fact_investor_flow WHERE trading_date = ?",
                    [trading_date],
                ).fetchall()
            }
    return bool(calendar_row[0]), symbol_frame, requested_symbol_count, existing_symbols


def _flush_investor_flow_batch(
    settings: Settings,
    *,
    run_id: str,
    rows: Iterable[pd.DataFrame],
) -> int:
    frames = [frame for frame in rows if not frame.empty]
    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    combined["run_id"] = run_id
    combined["created_at"] = now_local(settings.app.timezone)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_investor_flow(connection, combined)
    return len(combined)


def _fetch_investor_flow_result(
    *,
    provider: KISProvider,
    settings: Settings,
    run_id: str,
    symbol: str,
    market: str,
    trading_date: date,
    persist_raw_artifacts: bool,
    persist_probe_artifacts: bool,
) -> _InvestorFlowFetchResult:
    probe = provider.fetch_investor_flow(
        symbol=symbol,
        trading_date=trading_date,
        persist_probe_artifacts=persist_probe_artifacts,
    )
    artifact_paths: list[str] = []
    if persist_raw_artifacts:
        raw_json_path = (
            settings.paths.raw_dir
            / "kis"
            / "investor_flow"
            / f"trading_date={trading_date.isoformat()}"
            / f"symbol={symbol}"
            / f"{run_id}.json"
        )
        raw_parquet_path = raw_json_path.with_suffix(".parquet")
        artifact_paths.append(str(write_json_payload(raw_json_path, probe.payload)))
        probe.frame.to_parquet(raw_parquet_path, index=False)
        artifact_paths.append(str(raw_parquet_path))

    normalized = _normalize_investor_flow(
        symbol=symbol,
        market=market,
        trading_date=trading_date,
        frame=probe.frame,
    )
    return _InvestorFlowFetchResult(
        symbol=symbol,
        normalized=normalized,
        artifact_paths=artifact_paths,
    )


def _recycle_provider(
    settings: Settings,
    *,
    provider: KISProvider,
) -> KISProvider:
    provider.close()
    gc.collect()
    refreshed = KISProvider(settings)
    if hasattr(refreshed, "get_access_token"):
        refreshed.get_access_token()
    return refreshed


def _load_persisted_investor_flow_frame(settings: Settings, *, trading_date: date) -> pd.DataFrame:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        return connection.execute(
            """
            SELECT
                run_id,
                trading_date,
                symbol,
                market,
                foreign_net_volume,
                institution_net_volume,
                individual_net_volume,
                foreign_net_value,
                institution_net_value,
                individual_net_value,
                source,
                source_notes_json,
                created_at
            FROM fact_investor_flow
            WHERE trading_date = ?
            ORDER BY symbol
            """,
            [trading_date],
        ).fetchdf()


def _record_investor_flow_finish(
    settings: Settings,
    *,
    run_id: str,
    finished_at,
    status: str,
    output_artifacts: list[str],
    notes: str,
    error_message: str | None = None,
) -> None:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        record_run_finish(
            connection,
            run_id=run_id,
            finished_at=finished_at,
            status=status,
            output_artifacts=output_artifacts,
            notes=notes,
            error_message=error_message,
        )


def sync_investor_flow(
    settings: Settings,
    *,
    trading_date: date,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    force: bool = False,
    dry_run: bool = False,
    persist_raw_artifacts: bool = False,
    persist_probe_artifacts: bool = False,
    flush_batch_size: int = DEFAULT_INVESTOR_FLOW_FLUSH_BATCH_SIZE,
    max_workers: int = DEFAULT_INVESTOR_FLOW_MAX_WORKERS,
    provider_recycle_interval: int = DEFAULT_INVESTOR_FLOW_PROVIDER_RECYCLE_INTERVAL,
    kis_provider: KISProvider | None = None,
) -> InvestorFlowSyncResult:
    ensure_storage_layout(settings)
    owns_provider = kis_provider is None
    provider = kis_provider or KISProvider(settings)
    effective_flush_batch_size = max(1, int(flush_batch_size))
    effective_max_workers = max(1, int(max_workers))
    effective_provider_recycle_interval = max(0, int(provider_recycle_interval))

    with activate_run_context("sync_investor_flow", as_of_date=trading_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "kis_investor_flow_daily",
                    "dim_trading_calendar",
                    "vw_universe_active_common_stock",
                ],
                notes=f"Sync investor flow for {trading_date.isoformat()}",
            )
        try:
            (
                is_trading_day,
                symbol_frame,
                requested_symbol_count,
                existing_symbols,
            ) = _load_sync_inputs(
                settings,
                trading_date=trading_date,
                symbols=symbols,
                limit_symbols=limit_symbols,
                market=market,
                force=force,
            )

            if not is_trading_day:
                notes = (
                    f"{trading_date.isoformat()} is not a trading day. "
                    "No flow rows fetched."
                )
                _record_investor_flow_finish(
                    settings,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                )
                return InvestorFlowSyncResult(
                    run_id=run_context.run_id,
                    trading_date=trading_date,
                    requested_symbol_count=requested_symbol_count,
                    row_count=0,
                    skipped_symbol_count=requested_symbol_count,
                    failed_symbol_count=0,
                    artifact_paths=[],
                    notes=notes,
                )

            if dry_run:
                notes = (
                    f"Dry run only. trading_date={trading_date.isoformat()} "
                    f"symbols={requested_symbol_count} skipped_existing={len(existing_symbols)}"
                )
                _record_investor_flow_finish(
                    settings,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                )
                return InvestorFlowSyncResult(
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
            persisted_row_count = 0
            flush_count = 0
            fetch_targets: list[tuple[str, str]] = []
            for row in symbol_frame.itertuples(index=False):
                symbol = str(row.symbol).zfill(6)
                if symbol in existing_symbols:
                    skipped_symbol_count += 1
                    continue
                fetch_targets.append((symbol, str(row.market)))

            if fetch_targets and hasattr(provider, "get_access_token"):
                provider.get_access_token()

            fetch_started_at = perf_counter()
            completed_fetches = 0
            if effective_max_workers == 1 or len(fetch_targets) <= 1:
                for symbol, market_name in fetch_targets:
                    try:
                        result = _fetch_investor_flow_result(
                            provider=provider,
                            settings=settings,
                            run_id=run_context.run_id,
                            symbol=symbol,
                            market=market_name,
                            trading_date=trading_date,
                            persist_raw_artifacts=persist_raw_artifacts,
                            persist_probe_artifacts=persist_probe_artifacts,
                        )
                        artifact_paths.extend(result.artifact_paths)
                        completed_fetches += 1
                        if result.normalized.empty:
                            skipped_symbol_count += 1
                        else:
                            output_frames.append(result.normalized)
                            if len(output_frames) >= effective_flush_batch_size:
                                persisted_row_count += _flush_investor_flow_batch(
                                    settings,
                                    run_id=run_context.run_id,
                                    rows=output_frames,
                                )
                                flush_count += 1
                                output_frames.clear()
                        del result
                        if (
                            owns_provider
                            and effective_provider_recycle_interval > 0
                            and completed_fetches < len(fetch_targets)
                            and completed_fetches % effective_provider_recycle_interval == 0
                        ):
                            if output_frames:
                                persisted_row_count += _flush_investor_flow_batch(
                                    settings,
                                    run_id=run_context.run_id,
                                    rows=output_frames,
                                )
                                flush_count += 1
                                output_frames.clear()
                            provider = _recycle_provider(settings, provider=provider)
                    except Exception:
                        failed_symbol_count += 1
                        failed_symbols.append(symbol)
            else:
                with ThreadPoolExecutor(
                    max_workers=min(effective_max_workers, len(fetch_targets))
                ) as executor:
                    future_map = {
                        executor.submit(
                            _fetch_investor_flow_result,
                            provider=provider,
                            settings=settings,
                            run_id=run_context.run_id,
                            symbol=symbol,
                            market=market_name,
                            trading_date=trading_date,
                            persist_raw_artifacts=persist_raw_artifacts,
                            persist_probe_artifacts=persist_probe_artifacts,
                        ): symbol
                        for symbol, market_name in fetch_targets
                    }
                    for future in as_completed(future_map):
                        symbol = future_map[future]
                        try:
                            result = future.result()
                        except Exception:
                            failed_symbol_count += 1
                            failed_symbols.append(symbol)
                            continue
                        artifact_paths.extend(result.artifact_paths)
                        completed_fetches += 1
                        if result.normalized.empty:
                            skipped_symbol_count += 1
                        else:
                            output_frames.append(result.normalized)
                            if len(output_frames) >= effective_flush_batch_size:
                                persisted_row_count += _flush_investor_flow_batch(
                                    settings,
                                    run_id=run_context.run_id,
                                    rows=output_frames,
                                )
                                flush_count += 1
                                output_frames.clear()
                        del result
            fetch_elapsed_seconds = perf_counter() - fetch_started_at

            if output_frames:
                persisted_row_count += _flush_investor_flow_batch(
                    settings,
                    run_id=run_context.run_id,
                    rows=output_frames,
                )
                flush_count += 1
                output_frames.clear()

            combined = _load_persisted_investor_flow_frame(
                settings,
                trading_date=trading_date,
            )

            if not combined.empty:
                curated_path = write_parquet(
                    combined,
                    base_dir=settings.paths.curated_dir,
                    dataset="market/investor_flow",
                    partitions={"trading_date": trading_date.isoformat()},
                    filename="investor_flow.parquet",
                )
                artifact_paths.append(str(curated_path))

            if requested_symbol_count > 0 and combined.empty and failed_symbol_count > 0:
                raise RuntimeError(
                    f"No investor flow rows were loaded. failed_symbols={failed_symbols[:10]}"
                )

            notes = (
                f"Investor flow sync completed. trading_date={trading_date.isoformat()}, "
                f"rows={len(combined)}, requested_symbols={requested_symbol_count}, "
                f"skipped={skipped_symbol_count}, failed={failed_symbol_count}, "
                f"flushes={flush_count}, flush_batch_size={effective_flush_batch_size}, "
                f"max_workers={effective_max_workers}, "
                f"provider_recycle_interval={effective_provider_recycle_interval}, "
                f"fetch_seconds={fetch_elapsed_seconds:.2f}"
            )
            _record_investor_flow_finish(
                settings,
                run_id=run_context.run_id,
                finished_at=now_local(settings.app.timezone),
                status="success",
                output_artifacts=artifact_paths,
                notes=notes,
            )
            return InvestorFlowSyncResult(
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
            _record_investor_flow_finish(
                settings,
                run_id=run_context.run_id,
                finished_at=now_local(settings.app.timezone),
                status="failed",
                output_artifacts=[],
                notes=f"Investor flow sync failed for {trading_date.isoformat()}",
                error_message=str(exc),
            )
            raise
        finally:
            if owns_provider:
                provider.close()
