from __future__ import annotations

import pandas as pd

from app.ml.constants import D5_PRACTICAL_V3_MODEL_SPEC_ID
from app.selection.engine_v2 import (
    _d5_cash_path_basket_gate_payload,
    _select_report_candidate_mask,
)
from app.selection.swing_3_5d import (
    Swing35DConfig,
    _score_rows,
    apply_swing_3_5d_overlay,
    swing_explanatory_payload,
)


def test_swing_feature_scoring_finds_box_breakout_candidate() -> None:
    features = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "company_name": "테스트",
                "market": "KOSPI",
                "open": 10_200.0,
                "high": 10_900.0,
                "low": 10_000.0,
                "close": 10_800.0,
                "ma5": 10_150.0,
                "ma5_prev": 10_000.0,
                "ma20": 10_000.0,
                "ma20_prev": 9_950.0,
                "ma5_cross_ma20_up": False,
                "ma60": 9_850.0,
                "ma120": 9_700.0,
                "ma20_slope_5": 0.01,
                "ma60_slope_20": 0.002,
                "ma120_slope_20": 0.001,
                "dist_ma20": 0.08,
                "dist_ma60": 0.096,
                "ret1": 0.04,
                "ret5": 0.09,
                "ret10": 0.10,
                "ret20": 0.18,
                "vol_rel20": 2.2,
                "vol_rel60": 2.0,
                "turnover_rel20": 1.8,
                "vol_z20": 1.2,
                "volume_dry_up_then_expand": True,
                "close_loc": 0.89,
                "upper_wick_ratio": 0.11,
                "lower_wick_ratio": 0.22,
                "body_ratio": 0.67,
                "high_20_prev": 10_700.0,
                "low_20_prev": 9_500.0,
                "high_10": 10_900.0,
                "resistance_20": 11_400.0,
                "resistance_60": 12_000.0,
                "drawdown_from_high_10": -0.009,
                "box_width_20": 0.126,
                "ma_compression_5_20_60": 0.028,
                "bb_width_rank_120": 0.30,
                "rsi14": 62.0,
                "rsi5": 70.0,
                "atr_pct": 0.04,
                "consecutive_up_days": 2,
                "history_days": 130,
                "market_cap": 200_000_000_000,
                "avg_turnover_20": 3_000_000_000,
                "median_turnover_20": 2_500_000_000,
                "avg_volume_20": 100_000,
                "is_management_issue": False,
                "equity": 100_000_000_000,
                "debt_ratio": 80.0,
                "operating_income": 10_000_000_000,
                "net_income": 8_000_000_000,
                "revenue": 50_000_000_000,
                "market_regime": "strong",
                "market_ret5": 0.01,
                "market_ret20": 0.02,
                "sector_ret5": 0.03,
                "sector_ret20": 0.04,
                "sector_rank_20": 0.2,
                "ml_score_scaled": 80.0,
            }
        ]
    )

    scored = _score_rows(features, config=Swing35DConfig(recommendation_threshold=70.0))

    row = scored.iloc[0]
    assert row["swing_pattern"] == "box_breakout"
    assert bool(row["swing_candidate_pass"])
    assert bool(row["swing_recommendation_pass"])
    assert row["swing_rule_score"] >= 70
    assert "swing_box_breakout_pattern" in row["swing_reason_tags"]


def test_swing_overlay_replaces_h5_score_and_eligibility() -> None:
    base = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "final_selection_value": 10.0,
                "eligible_flag": False,
                "final_selection_rank_pct": 0.1,
            },
            {
                "symbol": "000002",
                "final_selection_value": 99.0,
                "eligible_flag": True,
                "final_selection_rank_pct": 1.0,
            },
        ]
    )
    swing = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "swing_hybrid_score": 88.0,
                "swing_rule_score": 82.0,
                "swing_candidate_pass": True,
                "swing_recommendation_pass": True,
                "swing_pattern": "pullback",
            },
            {
                "symbol": "000002",
                "swing_hybrid_score": 90.0,
                "swing_rule_score": 90.0,
                "swing_candidate_pass": False,
                "swing_recommendation_pass": False,
                "swing_pattern": None,
            },
        ]
    )

    overlaid = apply_swing_3_5d_overlay(base, swing, horizon=5)

    assert overlaid.loc[overlaid["symbol"] == "000001", "final_selection_value"].iloc[0] == 88.0
    assert bool(overlaid.loc[overlaid["symbol"] == "000001", "eligible_flag"].iloc[0])
    assert overlaid.loc[overlaid["symbol"] == "000002", "final_selection_value"].iloc[0] == 0.0
    assert not bool(overlaid.loc[overlaid["symbol"] == "000002", "eligible_flag"].iloc[0])
    assert swing_explanatory_payload(overlaid.iloc[0])["methodology_version"]


def test_swing_overlay_bypasses_legacy_d5_validation_and_cash_path_gates() -> None:
    scored = pd.DataFrame(
        {
            "symbol": [f"00000{idx}" for idx in range(1, 7)],
            "final_selection_value": [90.0, 89.0, 88.0, 87.0, 86.0, 85.0],
            "eligible_flag": [True] * 6,
            "validation_top5_mean_excess_return": [-0.01] * 6,
            "fallback_flag": [False] * 6,
            "swing_3_5d_overlay_applied": [True] * 6,
        }
    )
    risk_flags = pd.Series([[] for _ in range(len(scored))], index=scored.index)

    candidate_mask = _select_report_candidate_mask(
        scored,
        model_spec_id=D5_PRACTICAL_V3_MODEL_SPEC_ID,
        target_variant="practical_path_return_v3",
        horizon=5,
        risk_flags=risk_flags,
    )
    gate_payload = _d5_cash_path_basket_gate_payload(
        scored,
        risk_flags,
        model_spec_id=D5_PRACTICAL_V3_MODEL_SPEC_ID,
        horizon=5,
    )

    assert candidate_mask.sum() == 5
    assert gate_payload == {"applied": False, "reasons": ["swing_3_5d_overlay_active"]}
