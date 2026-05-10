from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

UNSUPPORTED_FLAG_COLUMNS = (
    "is_preferred_stock",
    "is_etf",
    "is_etn",
    "is_spac",
    "is_reit",
    "is_delisted",
    "is_trading_halt",
    "is_management_issue",
)


@dataclass(frozen=True, slots=True)
class PeerBaseline:
    target_symbol: str
    group_type: str | None
    group_value: str | None
    peer_count: int
    valid_pbr_count: int
    valid_per_count: int
    pbr_coverage: float
    per_coverage: float
    median_pbr: float | None
    median_per: float | None
    pbr_percentile: float | None
    per_percentile: float | None
    filter_note: str
    reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        return not self.reasons

    def to_payload(self) -> dict[str, object]:
        return {
            "target_symbol": self.target_symbol,
            "group_type": self.group_type,
            "group_value": self.group_value,
            "peer_count": self.peer_count,
            "valid_pbr_count": self.valid_pbr_count,
            "valid_per_count": self.valid_per_count,
            "pbr_coverage": self.pbr_coverage,
            "per_coverage": self.per_coverage,
            "median_pbr": self.median_pbr,
            "median_per": self.median_per,
            "pbr_percentile": self.pbr_percentile,
            "per_percentile": self.per_percentile,
            "filter_note": self.filter_note,
            "reasons": list(self.reasons),
        }


def _truthy(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def _filter_supported(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "is_common_stock" in working.columns:
        working = working.loc[_truthy(working["is_common_stock"])]
    for column in UNSUPPORTED_FLAG_COLUMNS:
        if column in working.columns:
            working = working.loc[~_truthy(working[column])]
    return working


def _percentile(peer_values: pd.Series, target_value: Any) -> float | None:
    target = pd.to_numeric(pd.Series([target_value]), errors="coerce").iloc[0]
    if pd.isna(target):
        return None
    values = pd.to_numeric(peer_values, errors="coerce").dropna()
    values = values.loc[values > 0]
    if values.empty:
        return None
    return float((values <= float(target)).mean() * 100.0)


def _winsorized(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    numeric = numeric.loc[numeric > 0]
    if len(numeric) < 20:
        return numeric
    return numeric.clip(lower=numeric.quantile(0.05), upper=numeric.quantile(0.95))


def build_sector_valuation_baseline(
    frame: pd.DataFrame,
    *,
    target_symbol: str,
    sector: str | None,
    industry: str | None = None,
) -> PeerBaseline:
    if frame.empty:
        return PeerBaseline(
            target_symbol,
            None,
            None,
            0,
            0,
            0,
            0.0,
            0.0,
            None,
            None,
            None,
            None,
            "empty",
            ("insufficient_peer_sample",),
        )

    working = _filter_supported(frame)
    target_rows = working.loc[working["symbol"].astype(str) == str(target_symbol)]
    target_row = target_rows.iloc[0] if not target_rows.empty else None

    group_type = None
    group_value = None
    if sector and "sector" in working.columns:
        peers = working.loc[working["sector"].astype(str) == str(sector)].copy()
        group_type = "sector"
        group_value = str(sector)
    else:
        peers = pd.DataFrame()
    peers = (
        peers.loc[peers["symbol"].astype(str) != str(target_symbol)] if not peers.empty else peers
    )
    if len(peers) < 8 and industry and "industry" in working.columns:
        industry_peers = working.loc[working["industry"].astype(str) == str(industry)].copy()
        industry_peers = industry_peers.loc[
            industry_peers["symbol"].astype(str) != str(target_symbol)
        ]
        if len(industry_peers) >= len(peers):
            peers = industry_peers
            group_type = "industry"
            group_value = str(industry)

    peer_count = len(peers)
    pbr_values = (
        _winsorized(peers.get("pbr", pd.Series(dtype=float)))
        if not peers.empty
        else pd.Series(dtype=float)
    )
    per_values = (
        _winsorized(peers.get("per", pd.Series(dtype=float)))
        if not peers.empty
        else pd.Series(dtype=float)
    )
    valid_pbr_count = len(pbr_values)
    valid_per_count = len(per_values)
    pbr_coverage = 0.0 if peer_count == 0 else valid_pbr_count / peer_count
    per_coverage = 0.0 if peer_count == 0 else valid_per_count / peer_count
    reasons: list[str] = []
    if peer_count < 8:
        reasons.append("insufficient_peer_sample")
    if pbr_coverage < 0.60 or per_coverage < 0.60:
        reasons.append("low_peer_metric_coverage")
    target_pbr = target_row.get("pbr") if target_row is not None else None
    target_per = target_row.get("per") if target_row is not None else None
    filter_note = "winsorized_5_95" if peer_count >= 20 else "thin_sample_no_winsorization"
    return PeerBaseline(
        target_symbol=str(target_symbol),
        group_type=group_type,
        group_value=group_value,
        peer_count=peer_count,
        valid_pbr_count=valid_pbr_count,
        valid_per_count=valid_per_count,
        pbr_coverage=pbr_coverage,
        per_coverage=per_coverage,
        median_pbr=None if pbr_values.empty else float(pbr_values.median()),
        median_per=None if per_values.empty else float(per_values.median()),
        pbr_percentile=_percentile(pbr_values, target_pbr),
        per_percentile=_percentile(per_values, target_per),
        filter_note=filter_note,
        reasons=tuple(dict.fromkeys(reasons)),
    )
