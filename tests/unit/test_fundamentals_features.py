from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.builders.fundamentals_features import build_fundamentals_feature_frame


def test_build_fundamentals_feature_frame_handles_timezone_strings():
    frame = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "revenue": 1000.0,
                "operating_income": 180.0,
                "net_income": 140.0,
                "roe": 0.18,
                "debt_ratio": 0.4,
                "operating_margin": 0.18,
                "disclosed_at": "2026-03-01T18:00:00+09:00",
            },
            {
                "symbol": "000660",
                "revenue": 900.0,
                "operating_income": 120.0,
                "net_income": 90.0,
                "roe": 0.12,
                "debt_ratio": 0.7,
                "operating_margin": 0.13,
                "disclosed_at": "2026-03-01 09:00:00",
            },
        ]
    )

    features = build_fundamentals_feature_frame(frame, as_of_date=date(2026, 3, 6))

    assert features["days_since_latest_report"].tolist() == [5, 5]
    assert features["fundamental_coverage_flag"].tolist() == [1.0, 1.0]
    assert features["net_income_positive_flag"].tolist() == [1.0, 1.0]
