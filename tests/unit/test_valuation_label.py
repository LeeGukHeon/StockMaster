from __future__ import annotations

from app.domain.valuation.labeling import assign_valuation_label


def test_under_valued_requires_low_pbr_low_per_positive_profit_and_roe() -> None:
    assert assign_valuation_label(
        confidence_pass=True,
        pbr_percentile=30,
        per_percentile=30,
        net_income=100,
        equity=500,
        roe=10,
    ) == "저평가"


def test_over_valued_when_high_pbr_or_per_and_gates_pass() -> None:
    assert assign_valuation_label(
        confidence_pass=True,
        pbr_percentile=75,
        per_percentile=50,
        net_income=100,
        equity=500,
        roe=1,
    ) == "고평가"


def test_fair_when_gates_pass_and_no_low_or_high_threshold() -> None:
    assert assign_valuation_label(
        confidence_pass=True,
        pbr_percentile=45,
        per_percentile=55,
        net_income=100,
        equity=500,
        roe=5,
    ) == "적정"


def test_defer_when_confidence_or_profit_quality_fails() -> None:
    assert assign_valuation_label(
        confidence_pass=False,
        pbr_percentile=80,
        per_percentile=80,
        net_income=100,
        equity=500,
        roe=5,
    ) == "판단 보류"
    assert assign_valuation_label(
        confidence_pass=True,
        pbr_percentile=80,
        per_percentile=80,
        net_income=-1,
        equity=500,
        roe=-1,
    ) == "판단 보류"
    assert assign_valuation_label(
        confidence_pass=True,
        pbr_percentile=20,
        per_percentile=20,
        net_income=100,
        equity=500,
        roe=-1,
    ) == "적정"
