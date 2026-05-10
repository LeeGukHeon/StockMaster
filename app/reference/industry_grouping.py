from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

MISSING_GROUP_LABEL = "미분류"


@dataclass(frozen=True, slots=True)
class IndustryGroup:
    group_type: str
    key: str
    label: str
    source_column: str


def _clean(value: object) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"} or text == "-":
        return None
    return text


def _get(row: Mapping[str, Any] | pd.Series | object, key: str) -> object:
    if isinstance(row, pd.Series):
        return row.get(key)
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def industry_group(row: Mapping[str, Any] | pd.Series | object) -> IndustryGroup:
    """Return the canonical analysis group for sector-like logic.

    Business logic should group by the most specific stable industry identifier
    available, then fall back to broad sector identifiers only when industry
    metadata is missing.  Human-facing labels prefer names over codes.
    """

    industry_code = _clean(_get(row, "industry_code"))
    industry = _clean(_get(row, "industry"))
    sector_code = _clean(_get(row, "sector_code"))
    sector = _clean(_get(row, "sector"))

    if industry_code:
        return IndustryGroup("industry", industry_code, industry or industry_code, "industry_code")
    if industry:
        return IndustryGroup("industry", industry, industry, "industry")
    if sector_code:
        return IndustryGroup("sector", sector_code, sector or sector_code, "sector_code")
    if sector:
        return IndustryGroup("sector", sector, sector, "sector")
    return IndustryGroup("unknown", MISSING_GROUP_LABEL, MISSING_GROUP_LABEL, "missing")


def industry_group_key(row: Mapping[str, Any] | pd.Series | object) -> str:
    return industry_group(row).key


def industry_group_label(row: Mapping[str, Any] | pd.Series | object) -> str:
    return industry_group(row).label


def add_industry_group_columns(
    frame: pd.DataFrame,
    *,
    key_column: str = "industry_group_key",
    label_column: str = "industry_group_label",
    type_column: str | None = "industry_group_type",
) -> pd.DataFrame:
    working = frame.copy()
    if working.empty:
        working[key_column] = pd.Series(dtype="object")
        working[label_column] = pd.Series(dtype="object")
        if type_column:
            working[type_column] = pd.Series(dtype="object")
        return working

    groups = working.apply(industry_group, axis=1)
    working[key_column] = groups.map(lambda group: group.key)
    working[label_column] = groups.map(lambda group: group.label)
    if type_column:
        working[type_column] = groups.map(lambda group: group.group_type)
    return working
