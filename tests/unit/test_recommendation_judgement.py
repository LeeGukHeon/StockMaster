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


def test_classify_recommendation_treats_thin_liquidity_as_buyability_blocker() -> None:
    judgement = classify_recommendation(
        final_selection_value=70,
        expected_excess_return=0.02,
        risk_flags=["thin_liquidity"],
        evidence_by_band={"65-75": ScoreBandEvidence("65-75", 120, 0.006, 0.47)},
    )

    assert judgement.label == "매수 보류"
    assert "차단 리스크" in judgement.summary


def test_classify_recommendation_keeps_joint_model_instability_observable() -> None:
    judgement = classify_recommendation(
        final_selection_value=70,
        expected_excess_return=0.02,
        risk_flags=["model_joint_instability_high"],
        evidence_by_band={"65-75": ScoreBandEvidence("65-75", 120, 0.006, 0.47)},
    )

    assert judgement.label == "매수해볼 가치 있음"
    assert "65-75점대 과거 평균 +0.6%" in judgement.summary


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


def test_classify_recommendation_treats_sparse_bad_75_band_as_overconfidence() -> None:
    judgement = classify_recommendation(
        final_selection_value=82,
        expected_excess_return=0.05,
        evidence_by_band={"75+": ScoreBandEvidence("75+", 3, -0.046, 0.0)},
    )

    assert judgement.label == "관찰 우선"
    assert "과확신 경고" in judgement.summary
    assert "75+점대 과거 평균 -4.6%" in judgement.summary


def test_classify_recommendation_blocks_buy_when_band_sample_is_too_small() -> None:
    judgement = classify_recommendation(
        final_selection_value=68,
        expected_excess_return=0.02,
        evidence_by_band={"65-75": ScoreBandEvidence("65-75", 5, 0.01, 0.6)},
    )

    assert judgement.label == "매수 보류"
    assert "점수대 우위 약함" in judgement.summary


def test_classify_recommendation_keeps_selected_candidate_observable_on_sparse_band() -> None:
    judgement = classify_recommendation(
        final_selection_value=50,
        expected_excess_return=0.02,
        evidence_by_band={"<55": ScoreBandEvidence("<55", 5, 0.01, 0.6)},
        candidate_selected=True,
    )

    assert judgement.label == "관찰 우선"
    assert "후보권" in judgement.summary


def test_buyability_priority_score_penalizes_model_risk() -> None:
    from app.recommendation.buyability import buyability_priority_score, has_buyability_blocker

    stable = buyability_priority_score(
        expected_excess_return=0.03,
        uncertainty_score=5,
        disagreement_score=10,
    )
    unstable = buyability_priority_score(
        expected_excess_return=0.03,
        uncertainty_score=80,
        disagreement_score=90,
    )

    assert stable > unstable
    assert round(stable, 6) == 2.6
    assert has_buyability_blocker(["thin_liquidity"])
    assert not has_buyability_blocker(["model_disagreement_high"])
    assert not has_buyability_blocker(["model_joint_instability_high"])
    assert not has_buyability_blocker(["prediction_fallback"])
