from __future__ import annotations

import threading
import time
from datetime import date

import pandas as pd

from app.pipelines.investor_flow import sync_investor_flow
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


class _StubKisProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_investor_flow(
        self,
        *,
        symbol: str,
        trading_date=None,
        market_code: str = "J",
        adjusted_price_flag: str = "",
        extra_class_code: str = "",
        persist_probe_artifacts: bool = False,
    ):
        self.calls.append(symbol)
        frame = pd.DataFrame(
            [
                {
                    "stck_bsop_date": date(2026, 3, 11).strftime("%Y%m%d"),
                    "frgn_ntby_qty": 100,
                    "orgn_ntby_qty": 50,
                    "prsn_ntby_qty": -150,
                    "frgn_ntby_tr_pbmn": 1000,
                    "orgn_ntby_tr_pbmn": 500,
                    "prsn_ntby_tr_pbmn": -1500,
                }
            ]
        )
        return type(
            "Probe",
            (),
            {
                "frame": frame,
                "payload": {"symbol": symbol},
                "raw_json_path": None,
                "raw_parquet_path": None,
            },
        )()

    def close(self) -> None:  # pragma: no cover - compatibility hook
        return None


class _ConcurrentStubKisProvider:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active_calls = 0
        self.max_active_calls = 0

    def fetch_investor_flow(
        self,
        *,
        symbol: str,
        trading_date=None,
        market_code: str = "J",
        adjusted_price_flag: str = "",
        extra_class_code: str = "",
        persist_probe_artifacts: bool = False,
    ):
        del symbol, trading_date, market_code, adjusted_price_flag, extra_class_code
        del persist_probe_artifacts
        with self._lock:
            self.active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self.active_calls)
        try:
            time.sleep(0.05)
            frame = pd.DataFrame(
                [
                    {
                        "stck_bsop_date": date(2026, 3, 11).strftime("%Y%m%d"),
                        "frgn_ntby_qty": 100,
                        "orgn_ntby_qty": 50,
                        "prsn_ntby_qty": -150,
                        "frgn_ntby_tr_pbmn": 1000,
                        "orgn_ntby_tr_pbmn": 500,
                        "prsn_ntby_tr_pbmn": -1500,
                    }
                ]
            )
            return type(
                "Probe",
                (),
                {
                    "frame": frame,
                    "payload": {"ok": True},
                    "raw_json_path": None,
                    "raw_parquet_path": None,
                },
            )()
        finally:
            with self._lock:
                self.active_calls -= 1

    def close(self) -> None:  # pragma: no cover - compatibility hook
        return None


def test_sync_investor_flow_flushes_batches_and_resume_skips_existing(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    provider = _StubKisProvider()

    first = sync_investor_flow(
        settings,
        trading_date=date(2026, 3, 11),
        flush_batch_size=2,
        kis_provider=provider,
    )

    assert first.row_count == 4
    assert len(provider.calls) == 4

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM fact_investor_flow WHERE trading_date = ?",
            [date(2026, 3, 11)],
        ).fetchone()[0]
    assert int(count) == 4

    second_provider = _StubKisProvider()
    second = sync_investor_flow(
        settings,
        trading_date=date(2026, 3, 11),
        flush_batch_size=2,
        kis_provider=second_provider,
    )

    assert second.row_count == 4
    assert second.skipped_symbol_count == 4
    assert second.failed_symbol_count == 0
    assert second_provider.calls == []


def test_sync_investor_flow_uses_concurrent_fetch_workers(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    provider = _ConcurrentStubKisProvider()

    result = sync_investor_flow(
        settings,
        trading_date=date(2026, 3, 11),
        flush_batch_size=2,
        max_workers=4,
        kis_provider=provider,
    )

    assert result.row_count == 4
    assert provider.max_active_calls >= 2


def test_sync_investor_flow_filters_future_listings(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol,
                company_name,
                market,
                is_common_stock,
                listing_date,
                source,
                updated_at
            )
            VALUES ('394420', 'RecentListing', 'KOSDAQ', TRUE, DATE '2026-03-31', 'test', now())
            """
        )

    provider = _StubKisProvider()
    result = sync_investor_flow(
        settings,
        trading_date=date(2026, 3, 11),
        flush_batch_size=2,
        kis_provider=provider,
    )

    assert result.requested_symbol_count == 4
    assert result.row_count == 4
    assert "394420" not in provider.calls
