from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from app.domain.fundamentals.account_normalizer import parse_numeric

COMMON_SHARE_LABELS = {"보통주", "보통주식", "COMMON", "COMMONSTOCK", "COMMONSHARE"}
AGGREGATE_SHARE_LABELS = {"합계", "계", "총계", "합계주식", "TOTAL"}
PREFERRED_MARKERS = ("우선", "PREFERRED", "PREF")


@dataclass(frozen=True, slots=True)
class ShareSourceSelection:
    share_count: float | None
    denominator_basis: str | None
    selected_se: str | None
    istc_totqy: float | None
    tesstk_co: float | None
    distb_stock_co: float | None
    stlm_dt: str | None
    reasons: tuple[str, ...] = field(default_factory=tuple)
    source_type: str = "official_disclosure"

    @property
    def available(self) -> bool:
        return self.share_count is not None and self.share_count > 0

    @property
    def share_basis_pass(self) -> bool:
        return self.available and not any(
            reason
            in {
                "share_basis_aggregate",
                "share_denominator_float_based",
                "ambiguous_share_class",
                "missing_share_count",
            }
            for reason in self.reasons
        )

    def lineage(self) -> dict[str, object]:
        return {
            "selected_se": self.selected_se,
            "istc_totqy": self.istc_totqy,
            "tesstk_co": self.tesstk_co,
            "distb_stock_co": self.distb_stock_co,
            "stlm_dt": self.stlm_dt,
            "denominator_basis": self.denominator_basis,
            "share_count": self.share_count,
            "reasons": list(self.reasons),
            "source_type": self.source_type,
        }


def _rows_to_frame(rows: Iterable[dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(list(rows or []))


def _normalize_label(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip().upper() if not ch.isspace())


def _is_common(value: object) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return False
    return normalized in COMMON_SHARE_LABELS or normalized.replace(" ", "") in COMMON_SHARE_LABELS


def _is_aggregate(value: object) -> bool:
    normalized = _normalize_label(value)
    return normalized in AGGREGATE_SHARE_LABELS or "합계" in str(value or "")


def _is_preferred(value: object) -> bool:
    normalized = _normalize_label(value)
    return any(marker in normalized for marker in PREFERRED_MARKERS)


def _number(row: pd.Series, column: str) -> float | None:
    if column not in row.index:
        return None
    return parse_numeric(row.get(column))


def _selection_from_row(
    row: pd.Series, *, extra_reasons: list[str] | None = None
) -> ShareSourceSelection:
    istc_totqy = _number(row, "istc_totqy")
    tesstk_co = _number(row, "tesstk_co")
    distb_stock_co = _number(row, "distb_stock_co")
    reasons = list(extra_reasons or [])
    share_count = None
    denominator_basis = None
    if istc_totqy is not None and istc_totqy > 0:
        share_count = float(istc_totqy)
        denominator_basis = "issued_shares"
    elif distb_stock_co is not None and distb_stock_co > 0:
        share_count = float(distb_stock_co)
        denominator_basis = "distributed_stock"
        reasons.append("share_denominator_float_based")
    else:
        reasons.append("missing_share_count")
    return ShareSourceSelection(
        share_count=share_count,
        denominator_basis=denominator_basis,
        selected_se=None if pd.isna(row.get("se")) else str(row.get("se")),
        istc_totqy=istc_totqy,
        tesstk_co=tesstk_co,
        distb_stock_co=distb_stock_co,
        stlm_dt=None if pd.isna(row.get("stlm_dt")) else str(row.get("stlm_dt")),
        reasons=tuple(dict.fromkeys(reasons)),
    )


def select_share_source(rows: Iterable[dict[str, Any]] | pd.DataFrame) -> ShareSourceSelection:
    """Select the official common-stock denominator from OpenDART stockTotqySttus rows.

    The approved first-pass contract is intentionally conservative: common-stock rows pass;
    aggregate rows and float/distributed-share fallback may preserve ingredients but withhold the
    final valuation label through reason codes.
    """

    frame = _rows_to_frame(rows)
    if frame.empty or "se" not in frame.columns:
        return ShareSourceSelection(
            share_count=None,
            denominator_basis=None,
            selected_se=None,
            istc_totqy=None,
            tesstk_co=None,
            distb_stock_co=None,
            stlm_dt=None,
            reasons=("missing_share_count",),
        )

    common = frame.loc[frame["se"].map(_is_common)].copy()
    if len(common) == 1:
        return _selection_from_row(common.iloc[0])
    if len(common) > 1:
        return ShareSourceSelection(
            share_count=None,
            denominator_basis=None,
            selected_se=None,
            istc_totqy=None,
            tesstk_co=None,
            distb_stock_co=None,
            stlm_dt=None,
            reasons=("ambiguous_share_class",),
        )

    aggregate = frame.loc[frame["se"].map(_is_aggregate)].copy()
    if len(aggregate) == 1:
        return _selection_from_row(aggregate.iloc[0], extra_reasons=["share_basis_aggregate"])

    non_preferred = frame.loc[~frame["se"].map(_is_preferred)].copy()
    if len(non_preferred) == 1 and len(frame) == 1:
        return _selection_from_row(non_preferred.iloc[0], extra_reasons=["ambiguous_share_class"])

    return ShareSourceSelection(
        share_count=None,
        denominator_basis=None,
        selected_se=None,
        istc_totqy=None,
        tesstk_co=None,
        distb_stock_co=None,
        stlm_dt=None,
        reasons=("ambiguous_share_class",),
    )
