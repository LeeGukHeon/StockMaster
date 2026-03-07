from __future__ import annotations

from datetime import date

import pandas as pd

from app.common.time import utc_now

SECURITY_TYPE_MAP = {
    "ST": "stock",
    "EF": "etf",
    "EN": "etn",
    "RT": "reit",
    "BC": "beneficiary_certificate",
    "SW": "warrant",
    "PF": "fund",
    "IF": "fund",
    "MF": "fund",
    "SR": "subscription_right",
    "DR": "dr",
    "FS": "foreign_stock",
}


def flag_is_true(value: object) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().upper()
    return normalized not in {"", "0", "N", "NONE", "NULL", "NAN"}


def parse_yyyymmdd(value: object) -> date | None:
    normalized = str(value).strip()
    if not normalized or normalized in {"0", "nan", "NaT"}:
        return None
    parsed = pd.to_datetime(normalized, format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def build_status_flags(row: pd.Series) -> str | None:
    flags: list[str] = []
    for key in (
        "is_preferred_stock",
        "is_etf",
        "is_etn",
        "is_spac",
        "is_reit",
        "is_delisted",
        "is_trading_halt",
        "is_management_issue",
    ):
        if bool(row[key]):
            flags.append(key.removeprefix("is_"))
    warning = str(row.get("market_warning_flag_raw", "")).strip()
    if warning and warning not in {"0", "N"}:
        flags.append(f"market_warning:{warning}")
    return ",".join(flags) if flags else None


def normalize_symbol_master(frame: pd.DataFrame, *, as_of_date: date) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
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
        )

    normalized = frame.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.zfill(6)
    normalized["company_name"] = normalized["company_name"].astype(str).str.strip()
    normalized["market"] = normalized["market"].astype(str).str.strip().str.upper()
    normalized["market_segment"] = normalized["market"]
    normalized["security_type"] = normalized["group_code"].map(SECURITY_TYPE_MAP).fillna("other")
    normalized["listing_date"] = normalized["listing_date_raw"].map(parse_yyyymmdd)
    normalized["is_preferred_stock"] = normalized["preferred_flag_raw"].map(flag_is_true)
    normalized["is_etf"] = normalized["security_type"].eq("etf")
    normalized["is_etn"] = normalized["security_type"].eq("etn")
    normalized["is_spac"] = normalized["spac_flag_raw"].map(flag_is_true) | normalized[
        "company_name"
    ].str.contains("스팩", na=False)
    normalized["is_reit"] = normalized["security_type"].eq("reit") | normalized[
        "company_name"
    ].str.contains("리츠", na=False)
    normalized["is_delisted"] = normalized["liquidation_flag_raw"].map(flag_is_true)
    normalized["is_trading_halt"] = normalized["trading_halt_flag_raw"].map(flag_is_true)
    normalized["is_management_issue"] = normalized["management_flag_raw"].map(flag_is_true)
    normalized["is_common_stock"] = (
        normalized["security_type"].eq("stock")
        & ~normalized["is_preferred_stock"]
        & ~normalized["is_spac"]
        & ~normalized["is_reit"]
    )
    normalized["sector"] = pd.NA
    normalized["industry"] = pd.NA
    normalized["status_flags"] = normalized.apply(build_status_flags, axis=1)
    normalized["dart_corp_code"] = pd.NA
    normalized["dart_corp_name"] = pd.NA
    normalized["source"] = "kis_master"
    normalized["as_of_date"] = as_of_date
    normalized["updated_at"] = utc_now()
    return normalized[
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
    ].drop_duplicates(subset=["symbol"], keep="last")
