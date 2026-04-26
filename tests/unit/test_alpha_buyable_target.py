from __future__ import annotations

import pandas as pd

from app.ml.dataset import _buyable_candidate_scores


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
