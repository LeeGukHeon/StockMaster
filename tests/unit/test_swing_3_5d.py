from __future__ import annotations

import pandas as pd

from app.ml.constants import D5_PRACTICAL_V3_MODEL_SPEC_ID
from app.selection.engine_v2 import (
    _d5_cash_path_basket_gate_payload,
    _select_report_candidate_mask,
)
from app.selection.swing_3_5d import (
    Swing35DConfig,
    _entry_policy_status_series,
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
                "low": 10_600.0,
                "close": 10_800.0,
                "ma5": 10_650.0,
                "ma5_prev": 10_000.0,
                "ma20": 10_500.0,
                "ma20_prev": 10_250.0,
                "ma5_cross_ma20_up": False,
                "ma60": 9_850.0,
                "ma120": 9_700.0,
                "ma20_slope_5": 0.01,
                "ma60_slope_20": 0.002,
                "ma120_slope_20": 0.001,
                "dist_ma20": 0.028,
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
                "ma_compression_5_20_60": 0.074,
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


def test_pullback_pattern_does_not_require_prior_volume_dry_up() -> None:
    features = pd.DataFrame(
        [
            {
                "symbol": "000003",
                "company_name": "눌림목",
                "market": "KOSPI",
                "open": 49_200.0,
                "high": 50_100.0,
                "low": 48_900.0,
                "close": 50_000.0,
                "ma5": 48_500.0,
                "ma5_prev": 48_000.0,
                "ma20": 49_700.0,
                "ma20_prev": 49_100.0,
                "ma5_cross_ma20_up": False,
                "ma60": 43_500.0,
                "ma120": 41_000.0,
                "ma20_slope_5": 0.03,
                "ma60_slope_20": 0.08,
                "ma120_slope_20": 0.02,
                "dist_ma20": 0.029,
                "dist_ma60": 0.149,
                "ret1": 0.03,
                "ret5": 0.07,
                "ret10": 0.03,
                "ret20": 0.17,
                "vol_rel20": 1.5,
                "vol_rel60": 1.4,
                "turnover_rel20": 1.4,
                "vol_z20": 0.9,
                "volume_dry_up_then_expand": False,
                "close_loc": 0.92,
                "upper_wick_ratio": 0.05,
                "lower_wick_ratio": 0.20,
                "body_ratio": 0.60,
                "high_20_prev": 52_700.0,
                "low_20_prev": 42_000.0,
                "high_10": 52_700.0,
                "resistance_20": 53_500.0,
                "resistance_60": 56_000.0,
                "drawdown_from_high_10": -0.05,
                "box_width_20": 0.25,
                "ma_compression_5_20_60": 0.10,
                "bb_width_rank_120": 0.25,
                "rsi14": 59.0,
                "rsi5": 65.0,
                "atr_pct": 0.04,
                "consecutive_up_days": 2,
                "history_days": 130,
                "market_cap": 500_000_000_000,
                "avg_turnover_20": 5_000_000_000,
                "median_turnover_20": 4_000_000_000,
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

    assert row["swing_pattern"] == "pullback"
    assert bool(row["swing_candidate_pass"])
    assert bool(row["swing_recommendation_pass"])


def test_recovery_breakout_uses_v3_entry_policy_revalidation() -> None:
    features = pd.DataFrame(
        [
            {
                "symbol": "000004",
                "company_name": "회복돌파",
                "market": "KOSPI",
                "open": 10_200.0,
                "high": 10_950.0,
                "low": 10_650.0,
                "close": 10_900.0,
                "current_price": 11_460.0,
                "ma5": 10_700.0,
                "ma5_prev": 10_200.0,
                "ma20": 10_500.0,
                "ma20_prev": 10_200.0,
                "ma5_cross_ma20_up": False,
                "ma60": 10_300.0,
                "ma120": 10_100.0,
                "ma20_slope_5": 0.01,
                "ma60_slope_20": -0.005,
                "ma120_slope_20": 0.0,
                "dist_ma20": 0.038,
                "dist_ma60": 0.058,
                "ret1": 0.035,
                "ret5": 0.08,
                "ret10": 0.10,
                "ret20": 0.18,
                "vol_rel20": 2.0,
                "vol_rel60": 1.8,
                "turnover_rel20": 1.7,
                "vol_z20": 1.1,
                "volume_dry_up_then_expand": True,
                "close_loc": 0.83,
                "upper_wick_ratio": 0.10,
                "lower_wick_ratio": 0.15,
                "body_ratio": 0.55,
                "high_20_prev": 10_700.0,
                "low_20_prev": 9_800.0,
                "high_10": 10_950.0,
                "resistance_20": 12_200.0,
                "resistance_60": 12_400.0,
                "drawdown_from_high_10": -0.005,
                "box_width_20": 0.20,
                "ma_compression_5_20_60": 0.055,
                "bb_width_rank_120": 0.55,
                "rsi14": 63.0,
                "rsi5": 68.0,
                "atr_pct": 0.04,
                "consecutive_up_days": 2,
                "history_days": 130,
                "market_cap": 300_000_000_000,
                "avg_turnover_20": 4_000_000_000,
                "median_turnover_20": 3_500_000_000,
                "avg_volume_20": 120_000,
                "is_management_issue": False,
                "equity": 120_000_000_000,
                "debt_ratio": 90.0,
                "operating_income": 12_000_000_000,
                "net_income": 9_000_000_000,
                "revenue": 80_000_000_000,
                "market_regime": "strong",
                "market_ret5": 0.01,
                "market_ret20": 0.02,
                "sector_ret5": 0.03,
                "sector_ret20": 0.04,
                "sector_rank_20": 0.2,
                "ml_probability_target_first": 0.62,
            }
        ]
    )

    scored = _score_rows(features, config=Swing35DConfig())
    row = scored.iloc[0]

    assert row["swing_pattern"] == "recovery_breakout"
    assert row["entry_status"] == "TARGET_ZONE_REACHED"
    assert row["swing_final_status"] == "TARGET_ZONE_REACHED"
    assert not bool(row["swing_recommendation_pass"])
    assert "swing_target_zone_reached" in row["swing_risk_flags"]


def test_v3_final_score_combines_rule_ml_market_sector_and_liquidity() -> None:
    features = pd.DataFrame(
        [
            {
                "symbol": "000005",
                "company_name": "최종점수",
                "market": "KOSPI",
                "open": 49_200.0,
                "high": 50_100.0,
                "low": 49_000.0,
                "close": 50_000.0,
                "ma5": 48_500.0,
                "ma5_prev": 48_000.0,
                "ma20": 49_700.0,
                "ma20_prev": 49_100.0,
                "ma5_cross_ma20_up": False,
                "ma60": 43_500.0,
                "ma120": 41_000.0,
                "ma20_slope_5": 0.03,
                "ma60_slope_20": 0.08,
                "ma120_slope_20": 0.02,
                "dist_ma20": 0.029,
                "dist_ma60": 0.149,
                "ret1": 0.03,
                "ret5": 0.07,
                "ret10": 0.03,
                "ret20": 0.17,
                "vol_rel20": 1.5,
                "vol_rel60": 1.4,
                "turnover_rel20": 1.4,
                "vol_z20": 0.9,
                "volume_dry_up_then_expand": False,
                "close_loc": 0.92,
                "upper_wick_ratio": 0.05,
                "lower_wick_ratio": 0.20,
                "body_ratio": 0.60,
                "high_20_prev": 52_700.0,
                "low_20_prev": 42_000.0,
                "high_10": 52_700.0,
                "resistance_20": 54_500.0,
                "resistance_60": 56_000.0,
                "drawdown_from_high_10": -0.05,
                "box_width_20": 0.25,
                "ma_compression_5_20_60": 0.10,
                "bb_width_rank_120": 0.25,
                "rsi14": 59.0,
                "rsi5": 65.0,
                "atr_pct": 0.04,
                "consecutive_up_days": 2,
                "history_days": 130,
                "market_cap": 500_000_000_000,
                "avg_turnover_20": 5_000_000_000,
                "median_turnover_20": 4_000_000_000,
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
                "ml_probability_target_first": 0.61,
            }
        ]
    )

    row = _score_rows(features, config=Swing35DConfig()).iloc[0]
    expected = (
        0.40 * row["swing_rule_score"]
        + 0.35 * row["ml_probability_score"]
        + 0.10 * row["market_regime_score_scaled"]
        + 0.10 * row["sector_strength_score_scaled"]
        + 0.05 * row["liquidity_score_scaled"]
    )

    assert abs(row["swing_hybrid_score"] - expected) < 1e-9
    assert row["swing_final_status"] in {"CANDIDATE", "HIGH_CONFIDENCE"}
    assert row["entry_status"] == "BUYABLE"
    assert row["max_buy_price"] <= row["target_1"]
    assert row["reward_risk_ratio"] >= 1.5


def test_v3_explanatory_payload_contains_entry_policy_contract() -> None:
    base = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "final_selection_value": 10.0,
                "eligible_flag": False,
                "final_selection_rank_pct": 0.1,
            }
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
                "entry_status": "BUYABLE",
                "signal_close": 10_000.0,
                "entry_lower_price": 9_900.0,
                "max_buy_price": 10_250.0,
                "chase_warning_price": 10_400.0,
                "target_zone_price": 10_500.0,
                "extended_price": 10_800.0,
                "stop_price": 9_700.0,
                "invalidation_price": 9_700.0,
                "target_1": 10_500.0,
                "target_2": 10_800.0,
                "nearest_support": 9_800.0,
                "nearest_resistance": 11_000.0,
            }
        ]
    )

    overlaid = apply_swing_3_5d_overlay(base, swing, horizon=5)
    payload = swing_explanatory_payload(overlaid.iloc[0])

    assert payload["methodology_version"] == "stockmaster_cycle_ml_hybrid_v3"
    assert payload["entry_policy"]["status"] == "WAIT_FOR_NEXT_DAY_PRICE"
    assert payload["entry_policy"]["buyable_range"] == [9_900.0, 10_250.0]
    assert payload["max_buy_price"] == 10_250.0
    assert payload["target_1"] == 10_500.0


def test_v3_entry_status_marks_target_zone_at_threshold() -> None:
    status = _entry_policy_status_series(
        current_price=pd.Series([9_699.0, 10_250.0, 10_399.0, 10_500.0, 10_801.0]),
        invalidation_price=pd.Series([9_700.0] * 5),
        max_buy_price=pd.Series([10_250.0] * 5),
        chase_warning_price=pd.Series([10_400.0] * 5),
        target_zone_price=pd.Series([10_500.0] * 5),
        extended_price=pd.Series([10_800.0] * 5),
    )

    assert status.tolist() == [
        "INVALIDATED",
        "BUYABLE",
        "WATCH_CAUTION",
        "TARGET_ZONE_REACHED",
        "EXTENDED",
    ]


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
