from __future__ import annotations

from datetime import date

from app.features.constants import FEATURE_NAMES, FEATURE_VERSION
import pandas as pd

from app.features.feature_store import build_feature_store, load_feature_matrix
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data
import pytest


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


def test_build_feature_store_fails_when_trading_day_has_no_same_day_ohlcv(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            DELETE FROM fact_daily_ohlcv
            WHERE trading_date = ?
            """,
            [date(2026, 3, 6)],
        )

    with pytest.raises(
        RuntimeError,
        match="same-day OHLCV is missing for trading date 2026-03-06",
    ):
        build_feature_store(
            settings,
            as_of_date=date(2026, 3, 6),
            limit_symbols=4,
        )


def test_load_feature_matrix_fallback_filters_future_listings(tmp_path, monkeypatch):
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol, company_name, market, dart_corp_code, listing_date, is_common_stock, source, updated_at
            ) VALUES
                ('111111', 'OldCo', 'KOSPI', 'corp1', DATE '2026-03-01', TRUE, 'test', now()),
                ('222222', 'FutureCo', 'KOSPI', 'corp2', DATE '2026-03-09', TRUE, 'test', now())
            """
        )
        connection.execute(
            """
            INSERT INTO fact_feature_snapshot (
                run_id, as_of_date, symbol, feature_name, feature_value, feature_group,
                source_version, feature_rank_pct, feature_zscore, is_imputed, notes_json, created_at
            ) VALUES
                ('run', DATE '2026-03-06', '111111', 'ret_1d', 0.01, 'price_trend', 'v', 0.5, 0.0, FALSE, '{}', now()),
                ('run', DATE '2026-03-06', '222222', 'ret_1d', 0.99, 'price_trend', 'v', 1.0, 2.0, FALSE, '{}', now())
            """
        )

        monkeypatch.setattr(
            "app.features.feature_store.load_symbol_frame",
            lambda *args, **kwargs: pd.DataFrame(),
        )

        frame = load_feature_matrix(connection, as_of_date=date(2026, 3, 6))

    assert frame["symbol"].tolist() == ["111111"]
