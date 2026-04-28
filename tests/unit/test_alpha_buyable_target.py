from __future__ import annotations

import pandas as pd
import pytest

from app.ml.dataset import (
    _buyable_candidate_scores,
    _practical_excess_return_targets,
    _practical_excess_return_v2_targets,
    _robust_buyable_excess_return_targets,
    _stable_practical_excess_return_targets,
)


def test_buyable_candidate_target_caps_outliers_and_penalizes_losses() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 6,
            "horizon": [5] * 6,
            "market": ["KOSDAQ"] * 6,
            "excess_forward_return": [-0.20, -0.08, -0.01, 0.02, 0.05, 1.50],
        }
    )

    scores = _buyable_candidate_scores(frame)

    assert float(scores.iloc[0]) < float(scores.iloc[2])
    assert float(scores.iloc[2]) < float(scores.iloc[3])
    assert float(scores.iloc[4]) > float(scores.iloc[3])
    assert float(scores.iloc[5]) <= 0.90


def test_practical_excess_target_keeps_return_units_and_penalizes_ex_ante_risk() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 5,
            "market": ["KOSDAQ"] * 5,
            "target_h5": [0.04, 0.04, -0.04, -0.04, 0.50],
            "liquidity_rank_pct": [0.50, 0.05, 0.50, 0.05, 0.50],
            "adv_20": [500, 10, 400, 8, 450],
            "realized_vol_20d": [0.02, 0.02, 0.02, 0.30, 0.02],
            "drawdown_20d": [-0.02, -0.02, -0.02, -0.25, -0.02],
            "max_loss_20d": [-0.02, -0.02, -0.02, -0.20, -0.02],
            "missing_key_feature_count": [0, 0, 0, 0, 0],
            "data_confidence_score": [100, 100, 100, 100, 100],
            "stale_price_flag": [0, 0, 0, 0, 0],
        }
    )

    targets = _practical_excess_return_targets(frame, horizon=5)

    assert float(targets.iloc[0]) > float(targets.iloc[1])
    assert float(targets.iloc[2]) > float(targets.iloc[3])
    assert float(targets.iloc[4]) <= 0.12
    assert float(targets.iloc[0]) <= 0.04


def test_practical_excess_target_penalizes_low_confidence_on_0_to_100_scale() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 2,
            "market": ["KOSDAQ"] * 2,
            "target_h5": [0.04, 0.04],
            "liquidity_rank_pct": [0.50, 0.50],
            "adv_20": [500, 500],
            "realized_vol_20d": [0.02, 0.02],
            "drawdown_20d": [-0.02, -0.02],
            "max_loss_20d": [-0.02, -0.02],
            "missing_key_feature_count": [0, 0],
            "data_confidence_score": [100.0, 50.0],
            "stale_price_flag": [0, 0],
        }
    )

    targets = _practical_excess_return_targets(frame, horizon=5)

    assert float(targets.iloc[0]) == 0.04
    assert float(targets.iloc[1]) == 0.02


def test_practical_excess_v2_target_preserves_v1_return_unit_contract() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 5,
            "market": ["KOSDAQ"] * 5,
            "target_h5": [0.04, 0.04, -0.04, -0.04, 0.50],
            "liquidity_rank_pct": [0.50, 0.05, 0.50, 0.05, 0.50],
            "adv_20": [500, 10, 400, 8, 450],
            "realized_vol_20d": [0.02, 0.02, 0.02, 0.30, 0.02],
            "drawdown_20d": [-0.02, -0.02, -0.02, -0.25, -0.02],
            "max_loss_20d": [-0.02, -0.02, -0.02, -0.20, -0.02],
            "missing_key_feature_count": [0, 0, 0, 0, 0],
            "data_confidence_score": [100, 100, 100, 100, 100],
            "stale_price_flag": [0, 0, 0, 0, 0],
        }
    )

    v1_targets = _practical_excess_return_targets(frame, horizon=5)
    v2_targets = _practical_excess_return_v2_targets(frame, horizon=5)

    pd.testing.assert_series_equal(v2_targets, v1_targets)
    assert float(v2_targets.iloc[2]) > float(v2_targets.iloc[3])
    assert float(v2_targets.iloc[4]) <= 0.12


def test_stable_practical_target_tightens_outliers_and_fragile_buyability() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 6,
            "market": ["KOSDAQ"] * 6,
            "target_h5": [0.05, 0.05, -0.04, -0.04, 0.40, -0.12],
            "liquidity_rank_pct": [0.60, 0.05, 0.60, 0.05, 0.60, 0.05],
            "adv_20": [500, 10, 400, 8, 450, 6],
            "realized_vol_20d": [0.02, 0.02, 0.02, 0.30, 0.02, 0.35],
            "hl_range_1d": [0.02, 0.02, 0.02, 0.18, 0.02, 0.20],
            "drawdown_20d": [-0.02, -0.02, -0.02, -0.25, -0.02, -0.25],
            "max_loss_20d": [-0.02, -0.02, -0.02, -0.20, -0.02, -0.25],
            "dist_from_20d_high": [0.20, 0.20, 0.20, 0.20, 0.98, 0.20],
            "volume_ratio_1d_vs_20d": [1.0, 1.0, 1.0, 1.0, 4.0, 1.0],
            "missing_key_feature_count": [0, 0, 0, 0, 0, 0],
            "data_confidence_score": [100, 100, 100, 100, 100, 100],
            "stale_price_flag": [0, 0, 0, 0, 0, 0],
        }
    )

    practical = _practical_excess_return_targets(frame, horizon=5)
    stable = _stable_practical_excess_return_targets(frame, horizon=5)

    assert float(stable.iloc[0]) <= float(practical.iloc[0])
    assert float(stable.iloc[0]) > float(stable.iloc[1])
    assert float(stable.iloc[2]) > float(stable.iloc[3])
    assert float(stable.iloc[4]) <= 0.10
    assert float(stable.iloc[5]) < float(stable.iloc[2])


def test_stable_practical_target_penalizes_low_confidence_on_0_to_100_scale() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 2,
            "market": ["KOSDAQ"] * 2,
            "target_h5": [0.05, 0.05],
            "liquidity_rank_pct": [0.60, 0.60],
            "adv_20": [500, 500],
            "realized_vol_20d": [0.02, 0.02],
            "hl_range_1d": [0.02, 0.02],
            "drawdown_20d": [-0.02, -0.02],
            "max_loss_20d": [-0.02, -0.02],
            "dist_from_20d_high": [0.20, 0.20],
            "volume_ratio_1d_vs_20d": [1.0, 1.0],
            "missing_key_feature_count": [0, 0],
            "data_confidence_score": [100.0, 50.0],
            "stale_price_flag": [0, 0],
        }
    )

    stable = _stable_practical_excess_return_targets(frame, horizon=5)

    assert float(stable.iloc[0]) == 0.05
    assert float(stable.iloc[1]) == pytest.approx(0.0175)


def test_robust_buyable_target_is_more_conservative_for_fragile_winners() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 6,
            "market": ["KOSDAQ"] * 6,
            "target_h5": [0.05, 0.05, -0.03, -0.03, 0.40, -0.08],
            "liquidity_rank_pct": [0.60, 0.05, 0.60, 0.05, 0.60, 0.05],
            "adv_20": [500, 10, 400, 8, 450, 6],
            "realized_vol_20d": [0.02, 0.02, 0.02, 0.30, 0.02, 0.35],
            "hl_range_1d": [0.02, 0.02, 0.02, 0.18, 0.02, 0.20],
            "drawdown_20d": [-0.02, -0.02, -0.02, -0.25, -0.02, -0.25],
            "max_loss_20d": [-0.02, -0.02, -0.02, -0.20, -0.02, -0.25],
            "dist_from_20d_high": [0.20, 0.20, 0.20, 0.20, 0.98, 0.20],
            "volume_ratio_1d_vs_20d": [1.0, 1.0, 1.0, 1.0, 4.0, 1.0],
            "missing_key_feature_count": [0, 0, 0, 0, 0, 0],
            "data_confidence_score": [100, 100, 100, 100, 100, 100],
            "stale_price_flag": [0, 0, 0, 0, 0, 0],
            "market_regime_coverage_flag": [1, 1, 1, 1, 1, 1],
            "market_regime_panic_flag": [0, 0, 0, 0, 0, 0],
            "market_regime_risk_off_flag": [0, 0, 0, 0, 0, 0],
            "market_breadth_up_ratio": [0.60, 0.60, 0.60, 0.60, 0.60, 0.60],
            "market_breadth_down_ratio": [0.20, 0.20, 0.20, 0.20, 0.20, 0.20],
        }
    )

    stable = _stable_practical_excess_return_targets(frame, horizon=5)
    robust = _robust_buyable_excess_return_targets(frame, horizon=5)

    assert float(robust.iloc[0]) <= float(stable.iloc[0])
    assert float(robust.iloc[0]) > float(robust.iloc[1])
    assert float(robust.iloc[1]) <= 0.002
    assert float(robust.iloc[4]) <= 0.06
    assert float(robust.iloc[5]) < float(robust.iloc[2])


def test_robust_buyable_target_penalizes_bad_data_and_weak_regime() -> None:
    frame = pd.DataFrame(
        {
            "as_of_date": ["2026-03-03"] * 3,
            "market": ["KOSDAQ"] * 3,
            "target_h5": [0.05, 0.05, -0.05],
            "liquidity_rank_pct": [0.60, 0.60, 0.60],
            "adv_20": [500, 500, 500],
            "realized_vol_20d": [0.02, 0.02, 0.02],
            "hl_range_1d": [0.02, 0.02, 0.02],
            "drawdown_20d": [-0.02, -0.02, -0.02],
            "max_loss_20d": [-0.02, -0.02, -0.02],
            "dist_from_20d_high": [0.20, 0.20, 0.20],
            "volume_ratio_1d_vs_20d": [1.0, 1.0, 1.0],
            "missing_key_feature_count": [0, 2, 0],
            "data_confidence_score": [100, 50, 100],
            "stale_price_flag": [0, 0, 0],
            "market_regime_coverage_flag": [1, 1, 1],
            "market_regime_panic_flag": [0, 0, 1],
            "market_regime_risk_off_flag": [0, 0, 0],
            "market_breadth_up_ratio": [0.60, 0.60, 0.20],
            "market_breadth_down_ratio": [0.20, 0.20, 0.70],
        }
    )

    robust = _robust_buyable_excess_return_targets(frame, horizon=5)

    assert float(robust.iloc[0]) > float(robust.iloc[1])
    assert float(robust.iloc[1]) <= 0.002
    assert float(robust.iloc[2]) < -0.05
