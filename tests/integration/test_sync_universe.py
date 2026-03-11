from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.common.paths import project_root
from app.ingestion.universe_sync import sync_universe
from app.providers.krx.reference import KrxReferenceResult
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


@dataclass
class FakeSymbolSnapshot:
    frame: pd.DataFrame
    artifact_paths: list[str]


@dataclass
class FakeCorpSnapshot:
    frame: pd.DataFrame
    raw_zip_path: str | None
    cache_path: str
    cached: bool


class FakeKISProvider:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def fetch_symbol_master(self, *, as_of_date=None):
        return FakeSymbolSnapshot(self.frame.copy(), [])


class FakeDartProvider:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def download_corp_codes(self, *, force: bool = False):
        return FakeCorpSnapshot(
            frame=self.frame.copy(),
            raw_zip_path=None,
            cache_path="cache://corp_codes",
            cached=True,
        )


class FakeKrxAdapter:
    def __init__(self, seed: pd.DataFrame) -> None:
        self.seed = seed

    def load_seed_fallback(self) -> pd.DataFrame:
        return self.seed.copy()

    def load_reference_enrichment(self, *, as_of_date, connection=None, run_id=None):
        return KrxReferenceResult(
            frame=pd.DataFrame(),
            source="test",
            fallback_used=True,
            fallback_reason="test_empty",
            service_slugs=tuple(),
        )


def test_sync_universe_populates_dimension_and_view(tmp_path):
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

    symbol_frame = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "company_name": "삼성전자",
                "market": "KOSPI",
                "group_code": "ST",
                "sector_code": "0027",
                "industry_code": "0013",
                "subindustry_code": "0000",
                "etp_flag_raw": "",
                "spac_flag_raw": "N",
                "trading_halt_flag_raw": "N",
                "liquidation_flag_raw": "N",
                "management_flag_raw": "N",
                "market_warning_flag_raw": "N",
                "listing_date_raw": "19750611",
                "preferred_flag_raw": "0",
                "source_file": "kospi_code.mst",
            },
            {
                "symbol": "005935",
                "company_name": "삼성전자우",
                "market": "KOSPI",
                "group_code": "ST",
                "sector_code": "0027",
                "industry_code": "0013",
                "subindustry_code": "0000",
                "etp_flag_raw": "",
                "spac_flag_raw": "N",
                "trading_halt_flag_raw": "N",
                "liquidation_flag_raw": "N",
                "management_flag_raw": "N",
                "market_warning_flag_raw": "N",
                "listing_date_raw": "19890925",
                "preferred_flag_raw": "1",
                "source_file": "kospi_code.mst",
            },
            {
                "symbol": "069500",
                "company_name": "KODEX 200",
                "market": "KOSPI",
                "group_code": "EF",
                "sector_code": "0000",
                "industry_code": "0000",
                "subindustry_code": "0000",
                "etp_flag_raw": "2",
                "spac_flag_raw": "N",
                "trading_halt_flag_raw": "N",
                "liquidation_flag_raw": "N",
                "management_flag_raw": "N",
                "market_warning_flag_raw": "N",
                "listing_date_raw": "20021014",
                "preferred_flag_raw": "0",
                "source_file": "kospi_code.mst",
            },
        ]
    )
    corp_codes = pd.DataFrame(
        [
            {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "modify_date": pd.Timestamp("2024-01-01").date(),
            }
        ]
    )
    seed = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "sector": "Information Technology",
                "industry": "Semiconductors",
                "market_segment": "KOSPI",
                "source_note": "test",
            }
        ]
    )

    result = sync_universe(
        settings,
        kis_provider=FakeKISProvider(symbol_frame),
        dart_provider=FakeDartProvider(corp_codes),
        krx_adapter=FakeKrxAdapter(seed),
    )

    assert result.row_count == 3
    assert result.active_common_stock_count == 1
    assert result.dart_mapped_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        counts = connection.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE dart_corp_code IS NOT NULL)
            FROM dim_symbol
            """
        ).fetchone()
        assert counts == (3, 1)
        active_common_count = connection.execute(
            "SELECT COUNT(*) FROM vw_universe_active_common_stock"
        ).fetchone()[0]
        assert active_common_count == 1
        samsung_row = connection.execute(
            """
            SELECT sector, industry, sector_code, industry_code, subindustry_code, dart_corp_code
            FROM dim_symbol
            WHERE symbol = '005930'
            """
        ).fetchone()
        assert samsung_row[0] == "Information Technology"
        assert samsung_row[1] == "Semiconductors"
        assert samsung_row[2] == "0027"
        assert samsung_row[3] == "0013"
        assert samsung_row[4] is None
        assert samsung_row[5] == "00126380"
