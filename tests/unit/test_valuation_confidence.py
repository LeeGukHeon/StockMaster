from __future__ import annotations

from app.domain.valuation.confidence import ConfidenceInput, evaluate_confidence
from app.domain.valuation.metrics import MetricValue


def _metric(name: str, value: float = 1.0) -> MetricValue:
    return MetricValue(
        name=name,
        value=value,
        metric_unit="multiple",
        source_type="internal_calculated_from_disclosure",
    )


def _inputs(**overrides):
    data = {
        "disclosure_age_days": 120,
        "metrics": {name: _metric(name) for name in ("eps", "bps", "per", "pbr")},
        "peer_count": 8,
        "pbr_peer_coverage": 0.75,
        "per_peer_coverage": 0.75,
        "source_lineage_present": True,
        "share_basis_reasons": tuple(),
        "net_income": 100.0,
        "equity": 500.0,
    }
    data.update(overrides)
    return ConfidenceInput(**data)


def test_full_data_sufficient_peers_passes_with_internal_annotation() -> None:
    result = evaluate_confidence(_inputs())

    assert result.passed
    assert result.hard_gate_reasons == tuple()
    assert "internal_calculated_from_disclosure" in result.annotations


def test_stale_disclosure_fails() -> None:
    assert (
        "stale_disclosure"
        in evaluate_confidence(_inputs(disclosure_age_days=541)).hard_gate_reasons
    )


def test_missing_metric_fails() -> None:
    metrics = {name: _metric(name) for name in ("eps", "bps", "per", "pbr")}
    metrics["per"] = MetricValue(
        "per", None, "multiple", "unavailable", reason="negative_or_zero_eps"
    )

    assert "missing_core_metric" in evaluate_confidence(_inputs(metrics=metrics)).hard_gate_reasons


def test_thin_peers_and_coverage_fail() -> None:
    result = evaluate_confidence(
        _inputs(peer_count=7, pbr_peer_coverage=0.5, per_peer_coverage=0.75)
    )

    assert "insufficient_peer_sample" in result.hard_gate_reasons
    assert "low_peer_metric_coverage" in result.hard_gate_reasons


def test_share_basis_and_financial_quality_failures_withhold_label() -> None:
    result = evaluate_confidence(
        _inputs(
            share_basis_reasons=("share_denominator_float_based",),
            net_income=-1.0,
            equity=0.0,
        )
    )

    assert not result.final_label_allowed
    assert "share_denominator_float_based" in result.hard_gate_reasons
    assert "non_positive_net_income" in result.hard_gate_reasons
    assert "non_positive_equity" in result.hard_gate_reasons
