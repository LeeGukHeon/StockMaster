from __future__ import annotations

from app.domain.valuation.basis import describe_evaluation_basis


def test_describe_evaluation_basis_for_normal_peer_percentile() -> None:
    assert (
        describe_evaluation_basis(
            metrics={
                "per": {"value": 12.0, "source_type": "internal_calculated_from_disclosure"},
                "pbr": {"value": 1.1, "source_type": "internal_calculated_from_disclosure"},
            },
            peer={"per_percentile": 35.0, "pbr_percentile": 25.0},
            hard_gate_reasons=[],
            financial_quality={"net_income": 100.0, "equity": 500.0},
        )
        == "PER/PBR 동종업계 percentile 비교"
    )


def test_describe_evaluation_basis_for_loss_company() -> None:
    assert (
        describe_evaluation_basis(
            metrics={
                "per": {
                    "value": None,
                    "source_type": "unavailable",
                    "reason": "negative_or_zero_eps",
                },
                "pbr": {"value": 1.1, "source_type": "internal_calculated_from_disclosure"},
            },
            peer={"pbr_percentile": 25.0},
            hard_gate_reasons=["missing_core_metric", "non_positive_net_income"],
            financial_quality={"net_income": -10.0, "equity": 500.0},
        )
        == "적자/비양수 순이익: PER 제외, PBR·재무품질 참고"
    )


def test_describe_evaluation_basis_for_peer_coverage_withhold() -> None:
    assert (
        describe_evaluation_basis(
            metrics={
                "per": {"value": 12.0, "source_type": "internal_calculated_from_disclosure"},
                "pbr": {"value": 1.1, "source_type": "internal_calculated_from_disclosure"},
            },
            peer={"per_percentile": 35.0, "pbr_percentile": 25.0},
            hard_gate_reasons=["low_peer_metric_coverage"],
            financial_quality={"net_income": 100.0, "equity": 500.0},
        )
        == "PER/PBR 산출값은 있으나 동종비교 표본·커버리지 부족"
    )
