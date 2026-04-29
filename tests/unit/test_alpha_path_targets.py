from __future__ import annotations

import pandas as pd

from app.ml.dataset import (
    _practical_excess_return_v2_targets,
    _practical_path_return_v3_targets,
)


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "as_of_date": ["2026-03-02", "2026-03-02", "2026-03-03"],
            "market": ["KOSPI", "KOSPI", "KOSPI"],
            "target_h5": [-0.10, 0.01, -0.02],
            "path_return_tp3_sl3_h5": [0.03, -0.03, pd.NA],
            "path_excess_tp5_sl3_h5": [0.05, -0.03, pd.NA],
            "path_excess_tp3_sl3_h5": [0.02, -0.04, pd.NA],
            "liquidity_rank_pct": [0.5, 0.5, 0.5],
            "adv_20": [100.0, 100.0, 100.0],
            "realized_vol_20d": [0.1, 0.1, 0.1],
            "drawdown_20d": [0.0, 0.0, 0.0],
            "max_loss_20d": [0.0, 0.0, 0.0],
            "missing_key_feature_count": [0.0, 0.0, 0.0],
            "data_confidence_score": [100.0, 100.0, 100.0],
            "stale_price_flag": [0.0, 0.0, 0.0],
        }
    )


def test_practical_v2_target_uses_path_label_before_endpoint_return() -> None:
    targets = _practical_excess_return_v2_targets(_base_frame(), horizon=5)

    assert targets.iloc[0] > 0.0
    assert targets.iloc[1] < 0.0
    assert targets.iloc[2] < 0.0


def test_practical_v3_target_prioritizes_tp3_cash_path_with_excess_cost() -> None:
    targets = _practical_path_return_v3_targets(_base_frame(), horizon=5)

    assert targets.iloc[0] > 0.0
    assert targets.iloc[0] < 0.03
    assert targets.iloc[1] < 0.0
    assert targets.iloc[2] < 0.0


def test_practical_v3_target_removes_reward_from_hard_blocked_winner() -> None:
    frame = _base_frame()
    frame.loc[0, "liquidity_rank_pct"] = 0.01
    frame.loc[0, "adv_20"] = 1.0

    targets = _practical_path_return_v3_targets(frame, horizon=5)

    assert 0.0 < targets.iloc[0] <= 0.001
