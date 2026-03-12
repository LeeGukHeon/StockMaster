from __future__ import annotations

import pandas as pd

from app.ui.helpers import (
    format_ui_date,
    format_ui_datetime,
    format_ui_run_id,
    localize_frame,
)


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


def test_localize_frame_formats_dates_times_and_run_ids() -> None:
    started_at = pd.Timestamp("2026-03-12 04:14:54", tz="Asia/Seoul")
    run_id = "run_daily_close_bundle-20260311T191454-837676de"
    frame = pd.DataFrame(
        [
            {
                "as_of_date": pd.Timestamp("2026-03-11"),
                "checkpoint_time": "0900",
                "started_at": started_at,
                "run_id": run_id,
            }
        ]
    )

    localized = localize_frame(frame)

    assert localized.iloc[0].tolist() == [
        format_ui_date(pd.Timestamp("2026-03-11")),
        "09:00",
        format_ui_datetime(started_at),
        format_ui_run_id(run_id),
    ]
