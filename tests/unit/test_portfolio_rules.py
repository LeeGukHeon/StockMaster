import pandas as pd

from app.portfolio.allocation import _action_from_delta
from app.portfolio.candidate_book import _coalesce_variant_columns, _empty_timing_actions_frame


def test_action_from_delta_mapping():
    assert _action_from_delta(0, 10) == "BUY_NEW"
    assert _action_from_delta(10, 15) == "ADD"
    assert _action_from_delta(10, 10) == "HOLD"
    assert _action_from_delta(10, 5) == "TRIM"
    assert _action_from_delta(10, 0) == "EXIT"
    assert _action_from_delta(0, 0) == "NO_ACTION"


def test_coalesce_variant_columns_prefers_prediction_columns():
    frame = pd.DataFrame(
        {
            "symbol": ["005930"],
            "uncertainty_score_x": [12.0],
            "uncertainty_score_y": [34.0],
            "disagreement_score_x": [45.0],
            "disagreement_score_y": [56.0],
            "fallback_flag_y": [True],
            "fallback_reason_y": ["meta_fallback"],
        }
    )

    result = _coalesce_variant_columns(
        frame,
        {
            "uncertainty_score": ("uncertainty_score_y", "uncertainty_score_x"),
            "disagreement_score": ("disagreement_score_y", "disagreement_score_x"),
            "fallback_flag": ("fallback_flag_y", "fallback_flag_x"),
            "fallback_reason": ("fallback_reason_y", "fallback_reason_x"),
        },
    )

    assert result.loc[0, "uncertainty_score"] == 34.0
    assert result.loc[0, "disagreement_score"] == 56.0
    assert bool(result.loc[0, "fallback_flag"]) is True
    assert result.loc[0, "fallback_reason"] == "meta_fallback"


def test_empty_timing_actions_frame_uses_timing_prefixed_columns():
    frame = _empty_timing_actions_frame()
    assert frame.columns.tolist() == [
        "symbol",
        "timing_action",
        "timing_confidence_margin",
        "timing_uncertainty_score",
        "timing_disagreement_score",
        "timing_fallback_flag",
        "timing_fallback_reason",
    ]
