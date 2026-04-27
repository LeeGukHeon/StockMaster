from __future__ import annotations

import pandas as pd

from app.ml.dataset import _buyable_candidate_scores, _practical_excess_return_targets


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
            "data_confidence_score": [1, 1, 1, 1, 1],
            "stale_price_flag": [0, 0, 0, 0, 0],
        }
    )

    targets = _practical_excess_return_targets(frame, horizon=5)

    assert float(targets.iloc[0]) > float(targets.iloc[1])
    assert float(targets.iloc[2]) > float(targets.iloc[3])
    assert float(targets.iloc[4]) <= 0.12
    assert float(targets.iloc[0]) <= 0.04
