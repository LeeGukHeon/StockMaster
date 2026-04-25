from __future__ import annotations

from app.recommendation.judgement import ScoreBandEvidence, classify_recommendation


def test_classify_recommendation_uses_positive_score_band_evidence() -> None:
    judgement = classify_recommendation(
        final_selection_value=68.2,
        expected_excess_return=0.012,
        evidence_by_band={"65-75": ScoreBandEvidence("65-75", 120, 0.006, 0.47)},
    )

    assert judgement.label == "매수해볼 가치 있음"
    assert "65-75점대 과거 평균 +0.6%" in judgement.summary


def test_classify_recommendation_caps_severe_risk_to_observation() -> None:
    judgement = classify_recommendation(
        final_selection_value=70,
        expected_excess_return=0.02,
        risk_flags=["thin_liquidity"],
        evidence_by_band={"65-75": ScoreBandEvidence("65-75", 120, 0.006, 0.47)},
    )

    assert judgement.label == "관찰 우선"
    assert "리스크 확인 필요" in judgement.summary


def test_classify_recommendation_rejects_negative_expected_return() -> None:
    judgement = classify_recommendation(
        final_selection_value=80,
        expected_excess_return=-0.001,
        evidence_by_band={"75+": ScoreBandEvidence("75+", 3, -0.04, 0.0)},
    )

    assert judgement.label == "매수 보류"


def test_classify_recommendation_distinguishes_missing_evidence_from_small_sample() -> None:
    judgement = classify_recommendation(
        final_selection_value=60,
        expected_excess_return=0.01,
        evidence_by_band={},
    )

    assert judgement.label == "관찰 우선"
    assert "점수대 성과 미연결" in judgement.summary
