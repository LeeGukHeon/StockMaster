from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local, today_local
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.reference import KrxReferenceAdapter
from app.reference.dart_mapper import build_dart_mapping
from app.reference.symbol_normalizer import normalize_symbol_master
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class UniverseSyncResult:
    run_id: str
    row_count: int
    active_common_stock_count: int
    dart_mapped_count: int
    artifact_paths: list[str]
    notes: str


def _merge_seed_reference(frame: pd.DataFrame, seed_frame: pd.DataFrame) -> pd.DataFrame:
    if seed_frame.empty:
        return frame
    merged = frame.merge(
        seed_frame[["symbol", "sector", "industry", "market_segment"]].drop_duplicates("symbol"),
        on="symbol",
        how="left",
        suffixes=("", "_seed"),
    )
    for column in ("sector", "industry", "market_segment"):
        merged[column] = merged[column].fillna(merged[f"{column}_seed"])
        merged = merged.drop(columns=[f"{column}_seed"])
    return merged


def _attach_dart_mapping(
    frame: pd.DataFrame, corp_codes: pd.DataFrame
) -> tuple[pd.DataFrame, pd.Series]:
    mapping = build_dart_mapping(frame, corp_codes)
    merged = frame.merge(mapping, on="symbol", how="left", suffixes=("", "_mapped"))
    merged["dart_corp_code"] = merged["dart_corp_code_mapped"].combine_first(
        merged["dart_corp_code"]
    )
    merged["dart_corp_name"] = merged["dart_corp_name_mapped"].combine_first(
        merged["dart_corp_name"]
    )
    match_method = merged["match_method"].fillna("unmapped")
    merged = merged.drop(columns=["dart_corp_code_mapped", "dart_corp_name_mapped", "match_method"])
    return merged, match_method


def upsert_dim_symbol(connection, frame: pd.DataFrame) -> None:
    ordered = frame[
        [
            "symbol",
            "company_name",
            "market",
            "market_segment",
            "sector",
            "industry",
            "listing_date",
            "security_type",
            "is_common_stock",
            "is_preferred_stock",
            "is_etf",
            "is_etn",
            "is_spac",
            "is_reit",
            "is_delisted",
            "is_trading_halt",
            "is_management_issue",
            "status_flags",
            "dart_corp_code",
            "dart_corp_name",
            "source",
            "as_of_date",
            "updated_at",
        ]
    ].copy()
    connection.register("dim_symbol_stage", ordered)
    connection.execute(
        """
        DELETE FROM dim_symbol
        WHERE symbol IN (SELECT symbol FROM dim_symbol_stage)
        """
    )
    connection.execute(
        """
        INSERT INTO dim_symbol (
            symbol,
            company_name,
            market,
            market_segment,
            sector,
            industry,
            listing_date,
            security_type,
            is_common_stock,
            is_preferred_stock,
            is_etf,
            is_etn,
            is_spac,
            is_reit,
            is_delisted,
            is_trading_halt,
            is_management_issue,
            status_flags,
            dart_corp_code,
            dart_corp_name,
            source,
            as_of_date,
            updated_at
        )
        SELECT
            symbol,
            company_name,
            market,
            market_segment,
            sector,
            industry,
            listing_date,
            security_type,
            is_common_stock,
            is_preferred_stock,
            is_etf,
            is_etn,
            is_spac,
            is_reit,
            is_delisted,
            is_trading_halt,
            is_management_issue,
            status_flags,
            dart_corp_code,
            dart_corp_name,
            source,
            as_of_date,
            updated_at
        FROM dim_symbol_stage
        """
    )
    connection.unregister("dim_symbol_stage")


def sync_universe(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    kis_provider: KISProvider | None = None,
    dart_provider: DartProvider | None = None,
    krx_adapter: KrxReferenceAdapter | None = None,
) -> UniverseSyncResult:
    ensure_storage_layout(settings)
    run_date = as_of_date or today_local(settings.app.timezone)
    owns_kis = kis_provider is None
    owns_dart = dart_provider is None
    kis = kis_provider or KISProvider(settings)
    dart = dart_provider or DartProvider(settings)
    krx = krx_adapter or KrxReferenceAdapter(settings)

    with activate_run_context("sync_universe", as_of_date=run_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["kis_symbol_master", "dart_corp_codes"],
                notes="Sync KOSPI/KOSDAQ symbol master into dim_symbol.",
            )
            try:
                snapshot = kis.fetch_symbol_master(as_of_date=run_date)
                normalized = normalize_symbol_master(snapshot.frame, as_of_date=run_date)

                corp_codes = pd.DataFrame(
                    columns=["corp_code", "corp_name", "stock_code", "modify_date"]
                )
                artifact_paths = list(snapshot.artifact_paths)
                match_counts = pd.Series(dtype="int64")

                try:
                    corp_snapshot = dart.download_corp_codes(force=False)
                    corp_codes = corp_snapshot.frame
                    if corp_snapshot.raw_zip_path:
                        artifact_paths.append(corp_snapshot.raw_zip_path)
                    artifact_paths.append(corp_snapshot.cache_path)
                except Exception:
                    corp_codes = pd.DataFrame(
                        columns=["corp_code", "corp_name", "stock_code", "modify_date"]
                    )

                normalized, match_methods = _attach_dart_mapping(normalized, corp_codes)
                match_counts = match_methods.value_counts(dropna=False)
                normalized = _merge_seed_reference(normalized, krx.load_seed_fallback())
                normalized["updated_at"] = now_local(settings.app.timezone)

                upsert_dim_symbol(connection, normalized)
                bootstrap_core_tables(connection)

                active_common_stock_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM vw_universe_active_common_stock"
                    ).fetchone()[0]
                )
                dart_mapped_count = int(normalized["dart_corp_code"].notna().sum())
                notes = (
                    f"Universe sync completed. rows={len(normalized)}, "
                    f"active_common={active_common_stock_count}, dart_mapped={dart_mapped_count}, "
                    f"match_breakdown={match_counts.to_dict()}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                )
                return UniverseSyncResult(
                    run_id=run_context.run_id,
                    row_count=len(normalized),
                    active_common_stock_count=active_common_stock_count,
                    dart_mapped_count=dart_mapped_count,
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
                    notes="Universe sync failed.",
                    error_message=str(exc),
                )
                raise
            finally:
                if owns_kis:
                    kis.close()
                if owns_dart:
                    dart.close()
