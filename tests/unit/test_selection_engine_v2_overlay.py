from __future__ import annotations

import pandas as pd

from app.selection.engine_v2 import _augment_reason_tags, _compute_crowding_penalty_score


def test_crowding_penalty_scores_hot_names_higher():
    frame = pd.DataFrame(
        {
            "ret_5d_rank_pct": [0.95, 0.20],
            "ret_10d_rank_pct": [0.90, 0.25],
            "dist_from_20d_high_rank_pct": [0.98, 0.30],
            "turnover_z_5_20_rank_pct": [0.96, 0.35],
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
                "news_drift_score": 68,
                "crowding_penalty_score": 30,
                "expected_excess_return": 0.02,
                "fallback_flag": False,
            }
        ),
        ["short_term_momentum_strong"],
    )

    assert "residual_strength_improving" in tags
    assert "flow_persistence_supportive" in tags
