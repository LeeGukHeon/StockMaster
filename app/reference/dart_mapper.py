from __future__ import annotations

import re

import pandas as pd

_COMPANY_NAME_STRIP_RE = re.compile(r"[\s\.\-_/,&()\[\]{}]")


def normalize_company_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.upper()
    normalized = normalized.replace("(주)", "").replace("주식회사", "")
    normalized = _COMPANY_NAME_STRIP_RE.sub("", normalized)
    return normalized


def build_dart_mapping(symbol_frame: pd.DataFrame, corp_code_frame: pd.DataFrame) -> pd.DataFrame:
    base = symbol_frame[["symbol", "company_name"]].drop_duplicates().copy()
    base["dart_corp_code"] = pd.NA
    base["dart_corp_name"] = pd.NA
    base["match_method"] = pd.NA

    if corp_code_frame.empty:
        return base[["symbol", "dart_corp_code", "dart_corp_name", "match_method"]]

    corp = corp_code_frame.copy()
    corp["stock_code"] = corp["stock_code"].astype("string")
    stock_exact = corp[corp["stock_code"].notna()].drop_duplicates(
        subset=["stock_code"], keep="first"
    )

    mapped = base.merge(
        stock_exact[["stock_code", "corp_code", "corp_name"]],
        left_on="symbol",
        right_on="stock_code",
        how="left",
    )
    mapped["dart_corp_code"] = mapped["corp_code"]
    mapped["dart_corp_name"] = mapped["corp_name"]
    mapped["match_method"] = (
        mapped["dart_corp_code"].notna().map(lambda value: "stock_code_exact" if value else pd.NA)
    )

    unresolved = mapped["dart_corp_code"].isna()
    if unresolved.any():
        symbol_names = base.loc[unresolved, ["symbol", "company_name"]].copy()
        symbol_names["name_key"] = symbol_names["company_name"].map(normalize_company_name)
        symbol_names = symbol_names.drop_duplicates(subset=["name_key"], keep=False)

        corp_names = corp.copy()
        corp_names["name_key"] = corp_names["corp_name"].map(normalize_company_name)
        corp_names = corp_names[corp_names["name_key"].ne("")]
        corp_names = corp_names.drop_duplicates(subset=["name_key"], keep=False)

        by_name = symbol_names.merge(
            corp_names[["name_key", "corp_code", "corp_name"]],
            on="name_key",
            how="inner",
        )
        if not by_name.empty:
            fill_map = by_name.set_index("symbol")[["corp_code", "corp_name"]]
            needs_fill = mapped["symbol"].isin(fill_map.index) & mapped["dart_corp_code"].isna()
            mapped.loc[needs_fill, "dart_corp_code"] = mapped.loc[needs_fill, "symbol"].map(
                fill_map["corp_code"]
            )
            mapped.loc[needs_fill, "dart_corp_name"] = mapped.loc[needs_fill, "symbol"].map(
                fill_map["corp_name"]
            )
            mapped.loc[needs_fill, "match_method"] = "corp_name_exact"

    return mapped[["symbol", "dart_corp_code", "dart_corp_name", "match_method"]]
