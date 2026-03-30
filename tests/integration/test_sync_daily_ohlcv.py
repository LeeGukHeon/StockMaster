from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.paths import project_root
from app.pipelines.daily_ohlcv import sync_daily_ohlcv
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


@dataclass
class FakeDailyProbe:
    frame: pd.DataFrame
    payload: dict[str, object]
    raw_json_path: str
    raw_parquet_path: str


class FakeKisProvider:
    def fetch_daily_ohlcv(
        self,
        *,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        del symbol, start_date, end_date
        row = {
            "stck_bsop_date": "20260306",
            "stck_oprc": "100",
            "stck_hgpr": "110",
            "stck_lwpr": "95",
            "stck_clpr": "105",
            "acml_vol": "1000",
            "acml_tr_pbmn": "100000",
            "mod_yn": "N",
            "prdy_vrss_sign": "2",
            "prdy_vrss": "5",
        }
        return FakeDailyProbe(
            frame=pd.DataFrame([row]),
            payload={"output2": [row]},
            raw_json_path="raw.json",
            raw_parquet_path="raw.parquet",
        )


class MixedOutcomeKisProvider:
    def fetch_daily_ohlcv(
        self,
        *,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        del start_date, end_date
        if symbol == "005930":
            row = {
                "stck_bsop_date": "20260306",
                "stck_oprc": "100",
                "stck_hgpr": "110",
                "stck_lwpr": "95",
                "stck_clpr": "105",
                "acml_vol": "1000",
                "acml_tr_pbmn": "100000",
                "mod_yn": "N",
                "prdy_vrss_sign": "2",
                "prdy_vrss": "5",
            }
            return FakeDailyProbe(
                frame=pd.DataFrame([row]),
                payload={"output2": [row]},
                raw_json_path="raw.json",
                raw_parquet_path="raw.parquet",
            )
        if symbol == "000660":
            row = {
                "stck_bsop_date": "20260305",
                "stck_oprc": "0",
                "stck_hgpr": "0",
                "stck_lwpr": "0",
                "stck_clpr": "0",
                "acml_vol": "0",
                "acml_tr_pbmn": "0",
                "mod_yn": "N",
                "prdy_vrss_sign": "3",
                "prdy_vrss": "0",
            }
            return FakeDailyProbe(
                frame=pd.DataFrame([row]),
                payload={"output2": [row]},
                raw_json_path="raw.json",
                raw_parquet_path="raw.parquet",
            )
        raise RuntimeError(f"provider failure for {symbol}")


def _build_settings(tmp_path):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "integration.duckdb"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"APP_DATA_DIR={data_dir.as_posix()}",
                f"APP_DUCKDB_PATH={duckdb_path.as_posix()}",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_settings(project_root=project_root(), env_file=env_file)
    bootstrap_storage(settings)
    return settings


def test_sync_daily_ohlcv_populates_fact_table_and_filters_future_listings(tmp_path):
    settings = _build_settings(tmp_path)

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
            VALUES
                ('005930', 'SamsungElec', 'KOSPI', TRUE, DATE '1975-06-11', 'test', now()),
                ('000660', 'SKHynix', 'KOSPI', TRUE, DATE '1996-12-26', 'test', now()),
                ('394420', 'RecentListing', 'KOSDAQ', TRUE, DATE '2026-03-31', 'test', now())
            """
        )
        connection.execute(
            """
            INSERT INTO dim_trading_calendar (
                trading_date,
                is_trading_day,
                market_session_type,
                weekday,
                is_weekend,
                is_public_holiday,
                source,
                source_confidence,
                is_override,
                updated_at
            )
            VALUES (?, TRUE, 'regular', 4, FALSE, FALSE, 'test', 'high', FALSE, now())
            """,
            [date(2026, 3, 6)],
        )

    result = sync_daily_ohlcv(
        settings,
        trading_date=date(2026, 3, 6),
        limit_symbols=3,
        kis_provider=FakeKisProvider(),
    )

    assert result.requested_symbol_count == 2
    assert result.eligible_symbol_count == 2
    assert result.row_count == 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM fact_daily_ohlcv").fetchone()[0]
        latest_manifest = connection.execute(
            """
            SELECT run_type, status
            FROM ops_run_manifest
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    assert row_count == 2
    assert latest_manifest == ("sync_daily_ohlcv", "success")


def test_sync_daily_ohlcv_tracks_provider_empty_and_errors(tmp_path):
    settings = _build_settings(tmp_path)

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
            VALUES
                ('005930', 'SamsungElec', 'KOSPI', TRUE, DATE '1975-06-11', 'test', now()),
                ('000660', 'SKHynix', 'KOSPI', TRUE, DATE '1996-12-26', 'test', now()),
                ('035420', 'Naver', 'KOSPI', TRUE, DATE '2002-10-29', 'test', now())
            """
        )
        connection.execute(
            """
            INSERT INTO dim_trading_calendar (
                trading_date,
                is_trading_day,
                market_session_type,
                weekday,
                is_weekend,
                is_public_holiday,
                source,
                source_confidence,
                is_override,
                updated_at
            )
            VALUES (?, TRUE, 'regular', 4, FALSE, FALSE, 'test', 'high', FALSE, now())
            """,
            [date(2026, 3, 6)],
        )

    result = sync_daily_ohlcv(
        settings,
        trading_date=date(2026, 3, 6),
        limit_symbols=3,
        kis_provider=MixedOutcomeKisProvider(),
    )

    assert result.row_count == 1
    assert result.provider_empty_symbol_count == 1
    assert result.provider_error_symbol_count == 1
    assert "provider_empty=1" in result.notes
    assert "provider_error=1" in result.notes
