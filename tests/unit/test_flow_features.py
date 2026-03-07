from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.builders.flow_features import build_flow_feature_frame


def test_build_flow_feature_frame_marks_coverage_and_ratios():
    flow_history = pd.DataFrame(
        [
            {
                "trading_date": "2026-03-05",
                "symbol": "005930",
                "foreign_net_value": 100.0,
                "institution_net_value": 80.0,
                "individual_net_value": -120.0,
            },
            {
                "trading_date": "2026-03-06",
                "symbol": "005930",
                "foreign_net_value": 120.0,
                "institution_net_value": 90.0,
                "individual_net_value": -140.0,
            },
        ]
    )
    ohlcv_history = pd.DataFrame(
        [
            {
                "trading_date": "2026-03-05",
                "symbol": "005930",
                "turnover_value": 1000.0,
                "close": 100.0,
                "volume": 10.0,
            },
            {
                "trading_date": "2026-03-06",
                "symbol": "005930",
                "turnover_value": 1200.0,
                "close": 120.0,
                "volume": 10.0,
            },
        ]
    )

    result = build_flow_feature_frame(
        flow_history,
        ohlcv_history=ohlcv_history,
        as_of_date=date(2026, 3, 6),
    )

    assert len(result) == 1
    row = result.iloc[0]
    assert row["flow_coverage_flag"] == 1.0
    assert row["foreign_net_value_ratio_1d"] > 0
    assert row["smart_money_flow_ratio_5d"] > 0
    assert row["flow_alignment_score"] == 1.0
