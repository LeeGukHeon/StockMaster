from __future__ import annotations

import pandas as pd

from app.ui.helpers import localize_frame


def test_localize_frame_formats_percent_like_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "expected_excess_return": 0.0123,
                "lower_band": -0.0345,
                "target_weight": 0.25,
                "execution_rate": 0.6,
                "final_selection_rank_pct": 0.125,
                "final_selection_value": 0.87,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        "1.23%",
        "-3.45%",
        "25.00%",
        "60.00%",
        "12.50%",
        0.87,
    ]


def test_localize_frame_formats_missing_values_as_dash() -> None:
    frame = pd.DataFrame(
        [
            {
                "target_price": float("nan"),
                "action_target_price": None,
                "target_weight": float("nan"),
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == ["-", "-", "-"]
