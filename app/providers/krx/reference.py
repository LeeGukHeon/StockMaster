from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from app.providers.krx.client import KrxProvider
from app.settings import Settings


@dataclass(frozen=True, slots=True)
class KrxReferenceResult:
    frame: pd.DataFrame
    source: str
    fallback_used: bool
    fallback_reason: str | None
    service_slugs: tuple[str, ...]


class KrxReferenceAdapter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.provider = KrxProvider(settings)

    @property
    def seed_path(self) -> Path:
        return self.settings.paths.project_root / "config" / "seeds" / "symbol_master_seed.csv"

    def close(self) -> None:
        self.provider.close()

    def load_seed_fallback(self) -> pd.DataFrame:
        if not self.seed_path.exists():
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "sector",
                    "industry",
                    "market_segment",
                    "source_note",
                    "reference_source",
                ]
            )
        frame = pd.read_csv(self.seed_path, dtype={"symbol": str})
        if frame.empty:
            return frame
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
        frame["reference_source"] = "krx_seed"
        return frame

    def load_reference_enrichment(
        self,
        *,
        as_of_date,
        connection: duckdb.DuckDBPyConnection | None = None,
        run_id: str | None = None,
    ) -> KrxReferenceResult:
        seed = self.load_seed_fallback()
        live_services = [
            slug
            for slug in ("stock_kospi_symbol_master", "stock_kosdaq_symbol_master")
            if self.provider.service_enabled(slug)
        ]
        if not live_services:
            return KrxReferenceResult(
                frame=seed,
                source="krx_seed",
                fallback_used=True,
                fallback_reason="krx_live_disabled_or_not_allowed",
                service_slugs=tuple(),
            )

        frames: list[pd.DataFrame] = []
        fallback_reason: str | None = None
        for service_slug in live_services:
            result = self.provider.fetch_service_rows(
                service_slug=service_slug,
                params={"as_of_date": as_of_date.isoformat()},
                as_of_date=as_of_date,
                run_id=run_id,
                connection=connection,
            )
            if result.frame.empty:
                fallback_reason = result.fallback_reason or "empty_live_response"
                continue
            frame = result.frame.copy()
            if "reference_source" not in frame.columns:
                frame["reference_source"] = "krx_live"
            if "source_note" not in frame.columns:
                frame["source_note"] = service_slug
            frames.append(frame)

        if not frames:
            return KrxReferenceResult(
                frame=seed,
                source="krx_seed",
                fallback_used=True,
                fallback_reason=fallback_reason or "krx_live_unavailable",
                service_slugs=tuple(live_services),
            )

        live_frame = pd.concat(frames, ignore_index=True)
        live_frame = live_frame.sort_values(["symbol", "krx_service_slug"]).drop_duplicates(
            subset=["symbol"],
            keep="first",
        )

        combined = live_frame
        if not seed.empty:
            combined = combined.merge(
                seed[["symbol", "sector", "industry", "market_segment", "source_note"]].rename(
                    columns={"source_note": "source_note_seed"}
                ),
                on="symbol",
                how="left",
                suffixes=("", "_seed"),
            )
            for column in ("sector", "industry", "market_segment"):
                if column not in combined.columns:
                    combined[column] = pd.NA
                seed_column = f"{column}_seed"
                if seed_column in combined.columns:
                    combined[column] = combined[column].fillna(combined[seed_column])
                    combined = combined.drop(columns=[seed_column])
            if "source_note_seed" in combined.columns:
                combined["source_note"] = combined["source_note"].fillna(
                    combined["source_note_seed"]
                )
                combined = combined.drop(columns=["source_note_seed"])

        return KrxReferenceResult(
            frame=combined,
            source="krx_live",
            fallback_used=False,
            fallback_reason=None,
            service_slugs=tuple(live_services),
        )

    def load_index_statistics(
        self,
        *,
        as_of_date,
        connection: duckdb.DuckDBPyConnection | None = None,
        run_id: str | None = None,
    ) -> KrxReferenceResult:
        service_slugs = [
            slug
            for slug in ("index_krx_daily", "index_kospi_daily", "index_kosdaq_daily")
            if self.provider.service_enabled(slug)
        ]
        frames: list[pd.DataFrame] = []
        fallback_reason: str | None = None
        for service_slug in service_slugs:
            result = self.provider.fetch_service_rows(
                service_slug=service_slug,
                params={"as_of_date": as_of_date.isoformat()},
                as_of_date=as_of_date,
                run_id=run_id,
                connection=connection,
            )
            if result.frame.empty:
                fallback_reason = result.fallback_reason or "empty_live_response"
                continue
            frame = result.frame.copy()
            frame["reference_source"] = result.source
            frames.append(frame)
        if not frames:
            return KrxReferenceResult(
                frame=pd.DataFrame(),
                source="krx_fallback",
                fallback_used=True,
                fallback_reason=fallback_reason or "krx_index_services_unavailable",
                service_slugs=tuple(service_slugs),
            )
        return KrxReferenceResult(
            frame=pd.concat(frames, ignore_index=True),
            source="krx_live",
            fallback_used=False,
            fallback_reason=None,
            service_slugs=tuple(service_slugs),
        )

    def load_etf_statistics(
        self,
        *,
        as_of_date,
        connection: duckdb.DuckDBPyConnection | None = None,
        run_id: str | None = None,
    ) -> KrxReferenceResult:
        service_slug = "etf_daily_trade"
        if not self.provider.service_enabled(service_slug):
            return KrxReferenceResult(
                frame=pd.DataFrame(),
                source="krx_fallback",
                fallback_used=True,
                fallback_reason="etf_service_not_enabled",
                service_slugs=(service_slug,),
            )
        result = self.provider.fetch_service_rows(
            service_slug=service_slug,
            params={"as_of_date": as_of_date.isoformat()},
            as_of_date=as_of_date,
            run_id=run_id,
            connection=connection,
        )
        return KrxReferenceResult(
            frame=result.frame,
            source=result.source,
            fallback_used=result.fallback_used,
            fallback_reason=result.fallback_reason,
            service_slugs=(service_slug,),
        )
