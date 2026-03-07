from __future__ import annotations

from datetime import date

from app.features.constants import FEATURE_NAMES, FEATURE_VERSION
from app.features.feature_store import build_feature_store
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_build_feature_store_populates_snapshot_and_manifest(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    result = build_feature_store(
        settings,
        as_of_date=date(2026, 3, 6),
        limit_symbols=4,
    )

    assert result.feature_version == FEATURE_VERSION
    assert result.feature_row_count == 4 * len(FEATURE_NAMES)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_feature_snapshot
            WHERE as_of_date = ?
            """,
            [date(2026, 3, 6)],
        ).fetchone()[0]
        latest_manifest = connection.execute(
            """
            SELECT run_type, status, feature_version
            FROM ops_run_manifest
            WHERE run_type = 'build_feature_store'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert row_count == 4 * len(FEATURE_NAMES)
        assert latest_manifest == ("build_feature_store", "success", FEATURE_VERSION)
