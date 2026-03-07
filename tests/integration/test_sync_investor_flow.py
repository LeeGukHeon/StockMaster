from __future__ import annotations

from datetime import date

import pandas as pd

from app.pipelines.investor_flow import sync_investor_flow
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


class _FakeKisProvider:
    def __init__(self) -> None:
        self.closed = False

    def fetch_investor_flow(self, *, symbol: str, trading_date):
        frame = pd.DataFrame(
            [
                {
                    "stck_bsop_date": trading_date.strftime("%Y%m%d"),
                    "frgn_ntby_qty": "1000",
                    "orgn_ntby_qty": "700",
                    "prsn_ntby_qty": "-1400",
                    "frgn_ntby_tr_pbmn": "10000000",
                    "orgn_ntby_tr_pbmn": "7000000",
                    "prsn_ntby_tr_pbmn": "-14000000",
                }
            ]
        )
        return type(
            "Probe",
            (),
            {
                "frame": frame,
                "payload": {"rt_cd": "0", "output2": frame.to_dict("records"), "symbol": symbol},
            },
        )()

    def close(self) -> None:
        self.closed = True


def test_sync_investor_flow_writes_curated_rows(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    provider = _FakeKisProvider()

    result = sync_investor_flow(
        settings,
        trading_date=date(2026, 3, 6),
        limit_symbols=2,
        kis_provider=provider,
    )

    assert result.row_count == 2
    assert result.failed_symbol_count == 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_investor_flow
            WHERE trading_date = ?
            """,
            [date(2026, 3, 6)],
        ).fetchone()[0]
        assert row_count == 2

    assert provider.closed is False
    assert any(path.endswith(".parquet") for path in result.artifact_paths)
