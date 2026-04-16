from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.builders.flow_features import build_flow_feature_frame
from app.features.builders.news_features import build_news_feature_frame


def test_flow_persistence_requires_full_five_day_history():
    flow_history = pd.DataFrame(
        [
            {
                "trading_date": date(2026, 3, 4),
                "symbol": "005930",
                "market": "KOSPI",
                "foreign_net_value": 1.0,
                "institution_net_value": 1.0,
                "individual_net_value": -1.0,
                "foreign_net_volume": 1.0,
                "institution_net_volume": 1.0,
                "individual_net_volume": -1.0,
            },
            {
                "trading_date": date(2026, 3, 5),
                "symbol": "005930",
                "market": "KOSPI",
                "foreign_net_value": 2.0,
                "institution_net_value": 2.0,
                "individual_net_value": -2.0,
                "foreign_net_volume": 2.0,
                "institution_net_volume": 2.0,
                "individual_net_volume": -2.0,
            },
        ]
    )
    ohlcv_history = pd.DataFrame(
        [
            {
                "trading_date": date(2026, 3, 4),
                "symbol": "005930",
                "close": 100,
                "turnover_value": 1000,
                "volume": 10,
            },
            {
                "trading_date": date(2026, 3, 5),
                "symbol": "005930",
                "close": 101,
                "turnover_value": 1100,
                "volume": 11,
            },
        ]
    )

    frame = build_flow_feature_frame(
        flow_history,
        ohlcv_history=ohlcv_history,
        as_of_date=date(2026, 3, 5),
    )

    assert frame["foreign_flow_persistence_5d"].isna().all()
    assert frame["institution_flow_persistence_5d"].isna().all()


def test_news_persistence_requires_more_than_single_day_burst():
    recent_news = pd.DataFrame(
        [
            {
                "published_at": "2026-03-06T16:00:00+09:00",
                "symbol_candidates": '["005930"]',
                "publisher": "alpha",
                "catalyst_score": 1.0,
                "tags_json": "[]",
                "match_method_json": '{"005930": "name_exact"}',
            }
        ]
    )

    frame = build_news_feature_frame(
        recent_news,
        as_of_date=date(2026, 3, 6),
        cutoff_time="17:30",
    )

    assert frame["news_burst_share_1d"].isna().all()
    assert frame["news_drift_persistence_score"].isna().all()
