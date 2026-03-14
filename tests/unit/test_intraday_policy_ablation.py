from __future__ import annotations

import pandas as pd

from app.intraday.policy import _dedupe_ablation_base_rows


def test_dedupe_ablation_base_rows_keeps_one_row_per_horizon_and_candidate() -> None:
    frame = pd.DataFrame(
        [
            {
                "horizon": 1,
                "policy_candidate_id": "cand-a",
                "scope_type": "GLOBAL",
                "scope_key": "H1|GLOBAL",
            },
            {
                "horizon": 1,
                "policy_candidate_id": "cand-a",
                "scope_type": "HORIZON",
                "scope_key": "H1",
            },
            {
                "horizon": 1,
                "policy_candidate_id": "cand-b",
                "scope_type": "GLOBAL",
                "scope_key": "H1|GLOBAL",
            },
            {
                "horizon": 5,
                "policy_candidate_id": "cand-a",
                "scope_type": "GLOBAL",
                "scope_key": "H5|GLOBAL",
            },
        ]
    )

    result = _dedupe_ablation_base_rows(frame)

    assert result[["horizon", "policy_candidate_id"]].to_dict(orient="records") == [
        {"horizon": 1, "policy_candidate_id": "cand-a"},
        {"horizon": 1, "policy_candidate_id": "cand-b"},
        {"horizon": 5, "policy_candidate_id": "cand-a"},
    ]
