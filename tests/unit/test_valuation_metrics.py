from __future__ import annotations

import pytest

from app.domain.valuation.metrics import ValuationMetricInput, calculate_valuation_metrics


def _inputs(**overrides):
    data = {
        "net_income": 100.0,
        "equity": 500.0,
        "revenue": 1000.0,
        "operating_income": 150.0,
        "liabilities": 250.0,
        "shares_outstanding": 10.0,
        "basis_close": 100.0,
        "currency": "KRW",
        "source_lineage": {"selected_se": "보통주"},
    }
    data.update(overrides)
    return ValuationMetricInput(**data)


def test_computes_core_valuation_metrics_from_valid_ingredients() -> None:
    metrics = calculate_valuation_metrics(_inputs())

    assert metrics["eps"].value == pytest.approx(10.0)
    assert metrics["bps"].value == pytest.approx(50.0)
    assert metrics["per"].value == pytest.approx(10.0)
    assert metrics["pbr"].value == pytest.approx(2.0)
    assert metrics["market_cap"].value == pytest.approx(1000.0)
    assert metrics["market_cap"].source_type == "internal_calculated_from_disclosure"


def test_computes_net_margin_as_percent_not_raw_ratio() -> None:
    metrics = calculate_valuation_metrics(_inputs(net_income=50.0, revenue=400.0))

    assert metrics["net_margin"].value == pytest.approx(12.5)
    assert metrics["net_margin"].metric_unit == "percent"
    assert metrics["net_margin"].value != pytest.approx(0.125)


def test_net_margin_unavailable_when_revenue_missing_or_zero() -> None:
    metrics = calculate_valuation_metrics(_inputs(revenue=0.0))

    assert not metrics["net_margin"].available
    assert metrics["net_margin"].reason == "missing_or_zero_revenue"


def test_per_unavailable_when_eps_non_positive() -> None:
    metrics = calculate_valuation_metrics(_inputs(net_income=-100.0))

    assert not metrics["per"].available
    assert metrics["per"].reason == "negative_or_zero_eps"


def test_pbr_unavailable_when_bps_non_positive() -> None:
    metrics = calculate_valuation_metrics(_inputs(equity=-500.0))

    assert not metrics["pbr"].available
    assert metrics["pbr"].reason == "negative_or_zero_bps"


def test_missing_disclosure_ingredients_are_not_marked_internal_calculations() -> None:
    metrics = calculate_valuation_metrics(_inputs(net_income=None, equity=None))

    assert not metrics["eps"].available
    assert metrics["eps"].source_type == "unavailable"
    assert metrics["eps"].reason == "missing_net_income"
    assert not metrics["bps"].available
    assert metrics["bps"].source_type == "unavailable"
    assert metrics["bps"].reason == "missing_equity"
    assert metrics["per"].reason == "missing_eps"
    assert metrics["pbr"].reason == "missing_bps"


def test_quality_metrics_explain_missing_component_not_internal_nulls() -> None:
    metrics = calculate_valuation_metrics(
        _inputs(net_income=None, operating_income=None, liabilities=None)
    )

    assert metrics["net_margin"].source_type == "unavailable"
    assert metrics["net_margin"].reason == "missing_net_income"
    assert metrics["roe"].reason == "missing_net_income"
    assert metrics["operating_margin"].reason == "missing_operating_income"
    assert metrics["debt_ratio"].reason == "missing_liabilities"


def test_no_core_computation_when_shares_missing_or_currency_unknown() -> None:
    no_shares = calculate_valuation_metrics(_inputs(shares_outstanding=0.0))
    unknown_currency = calculate_valuation_metrics(_inputs(currency=None))

    assert no_shares["eps"].reason == "missing_or_zero_shares"
    assert unknown_currency["eps"].reason == "unknown_currency"


def test_internal_calculated_metrics_carry_formula_lineage() -> None:
    metrics = calculate_valuation_metrics(_inputs())

    assert metrics["per"].source_type == "internal_calculated_from_disclosure"
    assert metrics["per"].formula_version == "valuation_metrics_v1"
    assert metrics["per"].formula_inputs["source_lineage"] == {"selected_se": "보통주"}
