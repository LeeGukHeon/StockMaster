from __future__ import annotations

VALUATION_LABEL_UNDER = "저평가"
VALUATION_LABEL_FAIR = "적정"
VALUATION_LABEL_OVER = "고평가"
VALUATION_LABEL_DEFER = "판단 보류"


def assign_valuation_label(
    *,
    confidence_pass: bool,
    pbr_percentile: float | None,
    per_percentile: float | None,
    net_income: float | None,
    equity: float | None,
    roe: float | None,
) -> str:
    if not confidence_pass:
        return VALUATION_LABEL_DEFER
    if net_income is None or net_income <= 0:
        return VALUATION_LABEL_DEFER
    if equity is None or equity <= 0:
        return VALUATION_LABEL_DEFER
    if pbr_percentile is None or per_percentile is None:
        return VALUATION_LABEL_DEFER
    if pbr_percentile <= 35.0 and per_percentile <= 35.0 and roe is not None and roe > 0:
        return VALUATION_LABEL_UNDER
    if pbr_percentile >= 70.0 or per_percentile >= 70.0:
        return VALUATION_LABEL_OVER
    return VALUATION_LABEL_FAIR
