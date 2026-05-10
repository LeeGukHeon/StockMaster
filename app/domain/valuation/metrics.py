from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

FORMULA_VERSION = "valuation_metrics_v1"
INTERNAL_CALCULATED = "internal_calculated_from_disclosure"
UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class MetricValue:
    name: str
    value: float | None
    metric_unit: str | None
    source_type: str
    formula_version: str = FORMULA_VERSION
    reason: str | None = None
    formula_inputs: dict[str, object] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return self.value is not None and self.source_type != UNAVAILABLE

    def to_payload(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value": self.value,
            "metric_unit": self.metric_unit,
            "source_type": self.source_type,
            "formula_version": self.formula_version,
            "reason": self.reason,
            "formula_inputs": self.formula_inputs,
        }


@dataclass(frozen=True, slots=True)
class ValuationMetricInput:
    net_income: float | None
    equity: float | None
    revenue: float | None
    operating_income: float | None
    liabilities: float | None
    shares_outstanding: float | None
    basis_close: float | None
    currency: str | None = "KRW"
    official_market_cap: float | None = None
    source_lineage: dict[str, object] = field(default_factory=dict)


def _positive(value: float | None) -> bool:
    return value is not None and float(value) > 0


def _metric(
    name: str, value: float | None, unit: str | None, inputs: dict[str, Any]
) -> MetricValue:
    return MetricValue(
        name=name,
        value=None if value is None else float(value),
        metric_unit=unit,
        source_type=INTERNAL_CALCULATED,
        formula_inputs=inputs,
    )


def _unavailable(name: str, unit: str | None, reason: str, inputs: dict[str, Any]) -> MetricValue:
    return MetricValue(
        name=name,
        value=None,
        metric_unit=unit,
        source_type=UNAVAILABLE,
        reason=reason,
        formula_inputs=inputs,
    )


def calculate_valuation_metrics(inputs: ValuationMetricInput) -> dict[str, MetricValue]:
    base_inputs = {
        "net_income": inputs.net_income,
        "equity": inputs.equity,
        "revenue": inputs.revenue,
        "operating_income": inputs.operating_income,
        "liabilities": inputs.liabilities,
        "shares_outstanding": inputs.shares_outstanding,
        "basis_close": inputs.basis_close,
        "currency": inputs.currency,
        "source_lineage": inputs.source_lineage,
    }
    metrics: dict[str, MetricValue] = {}

    if str(inputs.currency or "").upper() != "KRW":
        for name, unit in {
            "eps": "KRW/share",
            "bps": "KRW/share",
            "per": "multiple",
            "pbr": "multiple",
            "market_cap": "KRW",
        }.items():
            metrics[name] = _unavailable(name, unit, "unknown_currency", base_inputs)
    elif not _positive(inputs.shares_outstanding):
        for name, unit in {
            "eps": "KRW/share",
            "bps": "KRW/share",
            "per": "multiple",
            "pbr": "multiple",
            "market_cap": "KRW",
        }.items():
            metrics[name] = _unavailable(name, unit, "missing_or_zero_shares", base_inputs)
    else:
        shares = float(inputs.shares_outstanding or 0.0)
        eps = None if inputs.net_income is None else float(inputs.net_income) / shares
        bps = None if inputs.equity is None else float(inputs.equity) / shares
        metrics["eps"] = (
            _unavailable("eps", "KRW/share", "missing_net_income", base_inputs)
            if inputs.net_income is None
            else _metric("eps", eps, "KRW/share", base_inputs)
        )
        metrics["bps"] = (
            _unavailable("bps", "KRW/share", "missing_equity", base_inputs)
            if inputs.equity is None
            else _metric("bps", bps, "KRW/share", base_inputs)
        )
        if inputs.net_income is None:
            metrics["per"] = _unavailable("per", "multiple", "missing_eps", base_inputs)
        elif not _positive(eps):
            metrics["per"] = _unavailable("per", "multiple", "negative_or_zero_eps", base_inputs)
        elif not _positive(inputs.basis_close):
            metrics["per"] = _unavailable("per", "multiple", "missing_basis_close", base_inputs)
        else:
            metrics["per"] = _metric(
                "per", float(inputs.basis_close or 0.0) / float(eps), "multiple", base_inputs
            )
        if inputs.equity is None:
            metrics["pbr"] = _unavailable("pbr", "multiple", "missing_bps", base_inputs)
        elif not _positive(bps):
            metrics["pbr"] = _unavailable("pbr", "multiple", "negative_or_zero_bps", base_inputs)
        elif not _positive(inputs.basis_close):
            metrics["pbr"] = _unavailable("pbr", "multiple", "missing_basis_close", base_inputs)
        else:
            metrics["pbr"] = _metric(
                "pbr", float(inputs.basis_close or 0.0) / float(bps), "multiple", base_inputs
            )
        if _positive(inputs.official_market_cap):
            metrics["market_cap"] = MetricValue(
                name="market_cap",
                value=float(inputs.official_market_cap or 0.0),
                metric_unit="KRW",
                source_type="market_data_input",
                formula_inputs=base_inputs,
            )
        elif _positive(inputs.basis_close):
            metrics["market_cap"] = _metric(
                "market_cap",
                float(inputs.basis_close or 0.0) * shares,
                "KRW",
                base_inputs,
            )
        else:
            metrics["market_cap"] = _unavailable(
                "market_cap", "KRW", "missing_basis_close", base_inputs
            )

    if not _positive(inputs.revenue):
        metrics["net_margin"] = _unavailable(
            "net_margin",
            "percent",
            "missing_or_zero_revenue",
            base_inputs,
        )
    else:
        metrics["net_margin"] = (
            _unavailable("net_margin", "percent", "missing_net_income", base_inputs)
            if inputs.net_income is None
            else _metric(
                "net_margin",
                float(inputs.net_income) / float(inputs.revenue) * 100.0,
                "percent",
                base_inputs,
            )
        )

    if not _positive(inputs.equity):
        metrics["roe"] = _unavailable("roe", "percent", "missing_or_zero_equity", base_inputs)
    elif inputs.net_income is None:
        metrics["roe"] = _unavailable("roe", "percent", "missing_net_income", base_inputs)
    else:
        metrics["roe"] = _metric(
            "roe",
            float(inputs.net_income) / float(inputs.equity or 0.0) * 100.0,
            "percent",
            base_inputs,
        )

    if not _positive(inputs.revenue):
        metrics["operating_margin"] = _unavailable(
            "operating_margin", "percent", "missing_or_zero_revenue", base_inputs
        )
    elif inputs.operating_income is None:
        metrics["operating_margin"] = _unavailable(
            "operating_margin", "percent", "missing_operating_income", base_inputs
        )
    else:
        metrics["operating_margin"] = _metric(
            "operating_margin",
            float(inputs.operating_income) / float(inputs.revenue or 0.0) * 100.0,
            "percent",
            base_inputs,
        )

    if not _positive(inputs.equity):
        metrics["debt_ratio"] = _unavailable(
            "debt_ratio", "percent", "missing_or_zero_equity", base_inputs
        )
    elif inputs.liabilities is None:
        metrics["debt_ratio"] = _unavailable(
            "debt_ratio", "percent", "missing_liabilities", base_inputs
        )
    else:
        metrics["debt_ratio"] = _metric(
            "debt_ratio",
            float(inputs.liabilities) / float(inputs.equity or 0.0) * 100.0,
            "percent",
            base_inputs,
        )
    return metrics
