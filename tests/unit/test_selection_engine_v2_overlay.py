from __future__ import annotations

import pandas as pd

from app.ml.constants import D5_PRACTICAL_V2_MODEL_SPEC_ID, D5_PRACTICAL_V3_MODEL_SPEC_ID
from app.selection.engine_v2 import (
    D5_RAW_PRESERVATION_PRIORITY_COUNT,
    _alpha_core_score,
    _apply_d5_buyability_risk_gate,
    _apply_d5_raw_preservation_guardrail,
    _augment_reason_tags,
    _augment_risk_flags,
    _compute_crowding_penalty_score,
    _compute_d5_raw_preservation_blocker_mask,
    _compute_late_entry_penalty_score,
    _resolve_selection_weights,
    _select_report_candidate_mask,
)


def test_crowding_penalty_scores_hot_names_higher():
    frame = pd.DataFrame(
        {
            "ret_10d_rank_pct": [0.95, 0.20],
            "dist_from_20d_high_rank_pct": [0.98, 0.30],
            "turnover_burst_persistence_5d_rank_pct": [0.97, 0.40],
            "news_burst_share_1d_rank_pct": [0.99, 0.20],
        }
    )

    score = _compute_crowding_penalty_score(frame, horizon=5)

    assert float(score.iloc[0]) > float(score.iloc[1])


def test_reason_tags_prefer_relative_and_persistence_signals():
    tags = _augment_reason_tags(
        pd.Series(
            {
                "relative_alpha_score": 75,
                "flow_persistence_score": 72,
                "news_drift_score": 99,
                "crowding_penalty_score": 30,
                "expected_excess_return": 0.02,
                "fallback_flag": False,
            }
        ),
        ["short_term_momentum_strong"],
    )

    assert "residual_strength_improving" in tags
    assert "flow_persistence_supportive" in tags
    assert "news_drift_underreacted" not in tags


def test_model_risk_flags_separate_error_bucket_disagreement_and_joint_risk():
    high_error_only = _augment_risk_flags(
        pd.Series({"uncertainty_score": 89.0, "disagreement_score": 70.0}),
        [],
    )
    high_disagreement_only = _augment_risk_flags(
        pd.Series({"uncertainty_score": 68.0, "disagreement_score": 95.0}),
        [],
    )
    joint = _augment_risk_flags(
        pd.Series({"uncertainty_score": 89.0, "disagreement_score": 95.0}),
        [],
    )

    assert high_error_only == ["prediction_error_bucket_high"]
    assert high_disagreement_only == ["model_disagreement_high"]
    assert joint == [
        "model_disagreement_high",
        "model_joint_instability_high",
        "prediction_error_bucket_high",
    ]


def test_d5_alpha_core_score_uses_magnitude_to_separate_outsized_raw_leader():
    frame = pd.DataFrame(
        {
            "expected_excess_return": [0.18, 0.07, 0.06, 0.05],
        }
    )

    generic_score = _alpha_core_score(frame)
    d5_score = _alpha_core_score(frame, d5_primary_focus=True)

    assert float(d5_score.iloc[0] - d5_score.iloc[1]) > float(
        generic_score.iloc[0] - generic_score.iloc[1]
    )


def test_d5_late_entry_penalty_scores_overheated_weak_names_higher():
    frame = pd.DataFrame(
        {
            "crowding_penalty_score": [92, 35],
            "relative_alpha_score": [24, 82],
            "flow_persistence_score": [28, 80],
            "news_drift_score": [32, 75],
        }
    )

    score = _compute_late_entry_penalty_score(frame)

    assert float(score.iloc[0]) > float(score.iloc[1])


def test_d5_late_entry_penalty_gives_relief_to_high_alpha_leaders():
    frame = pd.DataFrame(
        {
            "crowding_penalty_score": [92, 92],
            "alpha_core_score": [95, 20],
            "relative_alpha_score": [55, 55],
            "flow_persistence_score": [55, 55],
            "news_drift_score": [55, 55],
        }
    )

    score = _compute_late_entry_penalty_score(frame)

    assert float(score.iloc[0]) < float(score.iloc[1])


def test_d5_raw_preservation_guardrail_keeps_safe_raw_leaders_in_top_slice():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEFG"),
            "expected_excess_return": [0.16, 0.15, 0.09, 0.08, 0.07, 0.06, 0.05],
            "final_selection_value": [70.0, 69.0, 95.0, 94.0, 93.0, 92.0, 91.0],
            "final_selection_rank_pct": [2 / 7, 1 / 7, 1.0, 6 / 7, 5 / 7, 4 / 7, 3 / 7],
            "eligible_flag": [True, True, True, True, True, True, True],
            "critical_risk_flag": [False, False, False, False, False, False, False],
            "fallback_flag": [False, False, False, False, False, False, False],
            "uncertainty_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [15.0, 15.0, 15.0, 15.0, 15.0, 15.0, 15.0],
        }
    )

    guarded = _apply_d5_raw_preservation_guardrail(scored)
    top_symbols = (
        guarded.sort_values(["final_selection_value", "symbol"], ascending=[False, True])
        .head(5)["symbol"]
        .tolist()
    )

    assert "A" in top_symbols
    assert "B" in top_symbols
    assert (
        guarded.loc[
            guarded["symbol"] == "A",
            "raw_preservation_guardrail_applied",
        ].item()
        is True
    )
    assert (
        guarded.loc[
            guarded["symbol"] == "B",
            "raw_preservation_guardrail_applied",
        ].item()
        is True
    )


def test_d5_raw_preservation_guardrail_can_preserve_three_safe_raw_leaders():
    assert D5_RAW_PRESERVATION_PRIORITY_COUNT == 3

    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEFGHI"),
            "expected_excess_return": [0.20, 0.19, 0.18, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05],
            "final_selection_value": [70.0, 69.0, 68.0, 99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [
                3 / 9,
                2 / 9,
                1 / 9,
                1.0,
                8 / 9,
                7 / 9,
                6 / 9,
                5 / 9,
                4 / 9,
            ],
            "eligible_flag": [True] * 9,
            "critical_risk_flag": [False] * 9,
            "fallback_flag": [False] * 9,
            "uncertainty_score": [20.0] * 9,
            "disagreement_score": [15.0] * 9,
        }
    )

    guarded = _apply_d5_raw_preservation_guardrail(scored)
    top_symbols = (
        guarded.sort_values(["final_selection_value", "symbol"], ascending=[False, True])
        .head(5)["symbol"]
        .tolist()
    )

    assert "A" in top_symbols
    assert "B" in top_symbols
    assert "C" in top_symbols
    assert (
        guarded.loc[
            guarded["symbol"] == "C",
            "raw_preservation_guardrail_applied",
        ].item()
        is True
    )


def test_d5_raw_preservation_blocker_mask_does_not_block_on_disagreement_alone():
    scored = pd.DataFrame(
        {
            "eligible_flag": [True],
            "critical_risk_flag": [False],
            "drawdown_20d": [0.0],
            "fallback_flag": [False],
            "uncertainty_score": [20.0],
            "disagreement_score": [100.0],
        }
    )

    blocker = _compute_d5_raw_preservation_blocker_mask(scored)

    assert bool(blocker.iloc[0]) is False


def test_d5_raw_preservation_blocker_mask_still_blocks_high_uncertainty():
    scored = pd.DataFrame(
        {
            "eligible_flag": [True],
            "critical_risk_flag": [False],
            "drawdown_20d": [0.0],
            "fallback_flag": [False],
            "uncertainty_score": [95.0],
            "disagreement_score": [100.0],
        }
    )

    blocker = _compute_d5_raw_preservation_blocker_mask(scored)

    assert bool(blocker.iloc[0]) is True


def test_d5_raw_preservation_blocker_mask_allows_volatility_only_cases():
    scored = pd.DataFrame(
        {
            "eligible_flag": [True],
            "critical_risk_flag": [True],
            "drawdown_20d": [-0.05],
            "fallback_flag": [False],
            "uncertainty_score": [88.0],
            "disagreement_score": [95.0],
        }
    )

    blocker = _compute_d5_raw_preservation_blocker_mask(scored)

    assert bool(blocker.iloc[0]) is False


def test_d5_raw_preservation_blocker_mask_still_blocks_large_drawdown():
    scored = pd.DataFrame(
        {
            "eligible_flag": [True],
            "critical_risk_flag": [True],
            "drawdown_20d": [-0.20],
            "fallback_flag": [False],
            "uncertainty_score": [88.0],
            "disagreement_score": [95.0],
        }
    )

    blocker = _compute_d5_raw_preservation_blocker_mask(scored)

    assert bool(blocker.iloc[0]) is True


def test_d5_primary_weights_apply_only_to_focus_spec():
    focus_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_swing_d5_v2",
        target_variant="top5_binary",
    )
    generic_top5_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_rank_rolling_120_v1",
        target_variant="top5_binary",
    )

    assert focus_weights["alpha_core_score"] != generic_top5_weights["alpha_core_score"]
    assert focus_weights["crowding_penalty_score"] != generic_top5_weights["crowding_penalty_score"]


def test_d5_primary_weights_soften_disagreement_penalty_for_focus_spec():
    focus_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_swing_d5_v2",
        target_variant="top5_binary",
    )
    generic_top5_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_rank_rolling_120_v1",
        target_variant="top5_binary",
    )

    assert focus_weights["disagreement_score"] == -2
    assert focus_weights["disagreement_score"] > generic_top5_weights["disagreement_score"]


def test_d5_buyable_weights_are_more_conservative_than_return_top5_focus():
    buyable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_buyable_d5_v1",
        target_variant="buyable_top5",
    )
    return_top5_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_swing_d5_v2",
        target_variant="top5_binary",
    )

    assert buyable_weights["alpha_core_score"] < return_top5_weights["alpha_core_score"]
    assert buyable_weights["quality_score"] > return_top5_weights["quality_score"]
    assert buyable_weights["value_safety_score"] > return_top5_weights["value_safety_score"]
    assert "news_catalyst_score" not in buyable_weights
    assert "news_drift_score" not in buyable_weights
    assert "news_catalyst_score" not in return_top5_weights
    assert "news_drift_score" not in return_top5_weights
    assert (
        buyable_weights["crowding_penalty_score"]
        < return_top5_weights["crowding_penalty_score"]
    )
    assert (
        buyable_weights["late_entry_penalty_score"]
        < return_top5_weights["late_entry_penalty_score"]
    )


def test_buyable_top5_report_candidate_mask_uses_ranked_top_five():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
        }
    )

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id="alpha_buyable_d5_v1",
        target_variant="buyable_top5",
        horizon=5,
    )

    assert scored.loc[mask, "symbol"].tolist() == ["A", "B", "C", "D", "E"]


def test_active_d5_report_candidate_mask_prefers_rank_two_to_six_when_rank_one_risky():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
            "expected_excess_return": [0.02, 0.02, 0.02, 0.02, 0.02, 0.02],
            "fallback_flag": [True, False, False, False, False, False],
            "uncertainty_score": [90.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [90.0, 20.0, 20.0, 20.0, 20.0, 20.0],
        }
    )
    risk_flags = pd.Series([["prediction_fallback"], [], [], [], [], []], index=scored.index)

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id="alpha_swing_d5_v2",
        target_variant="top5_binary",
        horizon=5,
        risk_flags=risk_flags,
    )

    assert scored.loc[mask, "symbol"].tolist() == ["B", "C", "D", "E", "F"]


def test_d5_practical_v2_report_candidate_mask_blocks_negative_validation_edge():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
            "expected_excess_return": [0.02, 0.018, 0.016, 0.014, 0.012, 0.010],
            "fallback_flag": [False, False, False, False, False, False],
            "uncertainty_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "validation_top5_mean_excess_return": [-0.018, -0.018, -0.018, -0.018, -0.018, -0.018],
        }
    )
    risk_flags = pd.Series([[], [], [], [], [], []], index=scored.index)

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id=D5_PRACTICAL_V2_MODEL_SPEC_ID,
        target_variant="practical_excess_return_v2",
        horizon=5,
        risk_flags=risk_flags,
    )

    assert scored.loc[mask, "symbol"].tolist() == []


def test_d5_practical_v3_report_candidate_mask_blocks_negative_validation_edge():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
            "expected_excess_return": [0.02, 0.018, 0.016, 0.014, 0.012, 0.010],
            "fallback_flag": [False, False, False, False, False, False],
            "uncertainty_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "validation_top5_mean_excess_return": [-0.001, -0.001, -0.001, -0.001, -0.001, -0.001],
        }
    )
    risk_flags = pd.Series([[], [], [], [], [], []], index=scored.index)

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id=D5_PRACTICAL_V3_MODEL_SPEC_ID,
        target_variant="practical_path_return_v3",
        horizon=5,
        risk_flags=risk_flags,
    )

    assert scored.loc[mask, "symbol"].tolist() == []


def test_d5_practical_v3_report_candidate_mask_allows_top_five_on_positive_path_edge():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
            "expected_excess_return": [0.02, 0.018, 0.016, 0.014, 0.012, 0.010],
            "fallback_flag": [False, False, False, False, False, False],
            "uncertainty_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "validation_top5_mean_excess_return": [0.004, 0.004, 0.004, 0.004, 0.004, 0.004],
        }
    )
    risk_flags = pd.Series([[], [], [], [], [], []], index=scored.index)

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id=D5_PRACTICAL_V3_MODEL_SPEC_ID,
        target_variant="practical_path_return_v3",
        horizon=5,
        risk_flags=risk_flags,
    )

    assert scored.loc[mask, "symbol"].tolist() == ["A", "B", "C", "D", "E"]


def test_d5_practical_v2_report_candidate_mask_allows_zero_to_n_with_positive_edge():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCDEF"),
            "eligible_flag": [True, True, True, True, True, True],
            "final_selection_value": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
            "final_selection_rank_pct": [1.0, 5 / 6, 4 / 6, 3 / 6, 2 / 6, 1 / 6],
            "expected_excess_return": [0.02, 0.009, 0.0, 0.006, 0.007, -0.001],
            "fallback_flag": [False, False, False, False, False, False],
            "uncertainty_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "disagreement_score": [20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
            "validation_top5_mean_excess_return": [0.012, 0.012, 0.012, 0.012, 0.012, 0.012],
        }
    )
    risk_flags = pd.Series(
        [[], ["thin_liquidity"], [], [], [], []],
        index=scored.index,
    )

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id=D5_PRACTICAL_V2_MODEL_SPEC_ID,
        target_variant="practical_excess_return_v2",
        horizon=5,
        risk_flags=risk_flags,
    )

    assert scored.loc[mask, "symbol"].tolist() == ["A", "D", "E"]


def test_non_topk_report_candidate_mask_requires_eligibility_and_rank_threshold():
    scored = pd.DataFrame(
        {
            "symbol": list("ABCD"),
            "eligible_flag": [True, False, True, True],
            "final_selection_value": [90.0, 89.0, 88.0, 87.0],
            "final_selection_rank_pct": [0.90, 0.95, 0.84, 0.85],
        }
    )

    mask = _select_report_candidate_mask(
        scored,
        model_spec_id=None,
        target_variant=None,
        horizon=1,
    )

    assert scored.loc[mask, "symbol"].tolist() == ["A", "D"]


def test_d5_buyability_risk_gate_demotes_data_missingness_and_joint_model_risk():
    scored = pd.DataFrame(
        {
            "symbol": ["A", "B", "C", "D"],
            "final_selection_value": [70.0, 70.0, 70.0, 70.0],
        }
    )
    risk_flags = pd.Series(
        [
            ["data_missingness_high"],
            ["model_joint_instability_high"],
            [
                "data_missingness_high",
                "model_joint_instability_high",
            ],
            [],
        ]
    )

    gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_buyable_d5_v1",
        horizon=5,
    )

    assert gated["d5_buyability_risk_gate_penalty_score"].tolist() == [
        14.0,
        10.0,
        24.0,
        0.0,
    ]
    assert gated["final_selection_value"].tolist() == [56.0, 60.0, 46.0, 70.0]
    assert float(gated.loc[gated["symbol"].eq("D"), "final_selection_rank_pct"].item()) == 1.0


def test_d5_buyability_risk_gate_penalizes_thin_liquidity_for_buyability():
    scored = pd.DataFrame({"symbol": ["A"], "final_selection_value": [70.0]})
    risk_flags = pd.Series([["thin_liquidity"]])

    gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_buyable_d5_v1",
        horizon=5,
    )

    assert gated["d5_buyability_risk_gate_penalty_score"].item() == 7.0
    assert gated["final_selection_value"].item() == 63.0


def test_d5_buyability_risk_gate_applies_to_active_d5_and_skips_other_specs():
    scored = pd.DataFrame({"symbol": ["A"], "final_selection_value": [70.0]})
    risk_flags = pd.Series([["data_missingness_high"]])

    active_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_swing_d5_v2",
        horizon=5,
    )
    other_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_recursive_expanding_v1",
        horizon=5,
    )

    assert active_gated["d5_buyability_risk_gate_penalty_score"].item() == 14.0
    assert active_gated["final_selection_value"].item() == 56.0
    assert other_gated["d5_buyability_risk_gate_penalty_score"].item() == 0.0
    assert other_gated["final_selection_value"].item() == 70.0


def test_d5_practical_weights_use_buyability_profile_without_raw_preservation():
    practical_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_practical_d5_v1",
        target_variant="practical_excess_return",
    )
    buyable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_buyable_d5_v1",
        target_variant="buyable_top5",
    )

    assert practical_weights == buyable_weights
    assert practical_weights["quality_score"] == 10
    assert practical_weights["crowding_penalty_score"] == -10


def test_d5_practical_v2_quarantines_high_model_disagreement():
    scored = pd.DataFrame({"symbol": ["A"], "final_selection_value": [70.0]})
    risk_flags = pd.Series([["model_disagreement_high"]])

    active_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_swing_d5_v2",
        horizon=5,
    )
    v2_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_practical_d5_v2",
        horizon=5,
    )

    assert active_gated["d5_buyability_risk_gate_penalty_score"].item() == 0.0
    assert v2_gated["d5_buyability_risk_gate_penalty_score"].item() == 45.0
    assert v2_gated["final_selection_value"].item() == 25.0


def test_d5_stable_buyable_penalizes_high_model_disagreement_less_than_v2():
    scored = pd.DataFrame({"symbol": ["A"], "final_selection_value": [70.0]})
    risk_flags = pd.Series([["model_disagreement_high"]])

    stable_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_stable_buyable_d5_v1",
        horizon=5,
    )

    assert stable_gated["d5_buyability_risk_gate_penalty_score"].item() == 25.0
    assert stable_gated["final_selection_value"].item() == 45.0


def test_d5_robust_buyable_penalizes_high_model_disagreement_like_stable():
    scored = pd.DataFrame({"symbol": ["A"], "final_selection_value": [70.0]})
    risk_flags = pd.Series([["model_disagreement_high"]])

    robust_gated = _apply_d5_buyability_risk_gate(
        scored,
        risk_flags,
        model_spec_id="alpha_robust_buyable_d5_v1",
        horizon=5,
    )

    assert robust_gated["d5_buyability_risk_gate_penalty_score"].item() == 25.0
    assert robust_gated["final_selection_value"].item() == 45.0


def test_d5_practical_v2_weights_use_buyability_profile_without_raw_preservation():
    practical_v2_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_practical_d5_v2",
        target_variant="practical_excess_return_v2",
    )
    buyable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_buyable_d5_v1",
        target_variant="buyable_top5",
    )

    assert practical_v2_weights == buyable_weights


def test_d5_stable_buyable_weights_are_more_quality_and_risk_heavy_than_buyable():
    stable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_stable_buyable_d5_v1",
        target_variant="stable_practical_excess_return",
    )
    buyable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_buyable_d5_v1",
        target_variant="buyable_top5",
    )

    assert stable_weights["alpha_core_score"] < buyable_weights["alpha_core_score"]
    assert stable_weights["quality_score"] > buyable_weights["quality_score"]
    assert stable_weights["risk_penalty_score"] < buyable_weights["risk_penalty_score"]
    assert stable_weights["late_entry_penalty_score"] < buyable_weights["late_entry_penalty_score"]
    assert "news_catalyst_score" not in stable_weights
    assert "news_drift_score" not in stable_weights


def test_d5_robust_buyable_weights_are_more_quality_and_risk_heavy_than_stable():
    robust_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_robust_buyable_d5_v1",
        target_variant="robust_buyable_excess_return",
    )
    stable_weights = _resolve_selection_weights(
        horizon=5,
        model_spec_id="alpha_stable_buyable_d5_v1",
        target_variant="stable_practical_excess_return",
    )

    assert robust_weights["alpha_core_score"] < stable_weights["alpha_core_score"]
    assert robust_weights["quality_score"] > stable_weights["quality_score"]
    assert robust_weights["risk_penalty_score"] < stable_weights["risk_penalty_score"]
    assert robust_weights["regime_fit_score"] > stable_weights["regime_fit_score"]
    assert "news_catalyst_score" not in robust_weights
    assert "news_drift_score" not in robust_weights
