from __future__ import annotations

from datetime import date

import pandas as pd

from app.reference.symbol_normalizer import normalize_symbol_master


def test_normalize_symbol_master_sets_security_flags():
    raw = pd.DataFrame(
        [
            {
                "symbol": "5930",
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
                "symbol": "5935",
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
                "symbol": "69500",
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
            {
                "symbol": "365550",
                "company_name": "ESR켄달스퀘어리츠",
                "market": "KOSPI",
                "group_code": "RT",
                "sector_code": "0000",
                "industry_code": "0000",
                "subindustry_code": "0000",
                "etp_flag_raw": "",
                "spac_flag_raw": "N",
                "trading_halt_flag_raw": "N",
                "liquidation_flag_raw": "N",
                "management_flag_raw": "N",
                "market_warning_flag_raw": "N",
                "listing_date_raw": "20201223",
                "preferred_flag_raw": "0",
                "source_file": "kospi_code.mst",
            },
            {
                "symbol": "123456",
                "company_name": "테스트스팩",
                "market": "KOSDAQ",
                "group_code": "ST",
                "sector_code": "0000",
                "industry_code": "0000",
                "subindustry_code": "0000",
                "etp_flag_raw": "",
                "spac_flag_raw": "Y",
                "trading_halt_flag_raw": "Y",
                "liquidation_flag_raw": "N",
                "management_flag_raw": "Y",
                "market_warning_flag_raw": "1",
                "listing_date_raw": "20240101",
                "preferred_flag_raw": "0",
                "source_file": "kosdaq_code.mst",
            },
        ]
    )

    normalized = normalize_symbol_master(raw, as_of_date=date(2026, 3, 7)).set_index("symbol")

    assert bool(normalized.loc["005930", "is_common_stock"]) is True
    assert normalized.loc["005930", "listing_date"] == date(1975, 6, 11)
    assert bool(normalized.loc["005935", "is_preferred_stock"]) is True
    assert bool(normalized.loc["069500", "is_etf"]) is True
    assert bool(normalized.loc["365550", "is_reit"]) is True
    assert bool(normalized.loc["123456", "is_spac"]) is True
    assert bool(normalized.loc["123456", "is_trading_halt"]) is True
    assert bool(normalized.loc["123456", "is_management_issue"]) is True
    assert "market_warning:1" in normalized.loc["123456", "status_flags"]
