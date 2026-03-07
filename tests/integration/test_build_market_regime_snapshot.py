from __future__ import annotations

from datetime import date

from app.regime.snapshot import REGIME_VERSION, build_market_regime_snapshot
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_build_market_regime_snapshot_populates_three_market_scopes(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    result = build_market_regime_snapshot(
        settings,
        as_of_date=date(2026, 3, 6),
    )

    assert result.regime_version == REGIME_VERSION
    assert result.row_count == 3

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        scopes = {
            row[0]
            for row in connection.execute(
                """
                SELECT market_scope
                FROM fact_market_regime_snapshot
                WHERE as_of_date = ?
                """,
                [date(2026, 3, 6)],
            ).fetchall()
        }
        assert scopes == {"KR_ALL", "KOSPI", "KOSDAQ"}
