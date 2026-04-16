from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.builders.news_features import build_news_feature_frame


def test_build_news_feature_frame_excludes_items_after_cutoff():
    recent_news = pd.DataFrame(
        [
            {
                "published_at": "2026-03-06T16:00:00+09:00",
                "symbol_candidates": '["005930"]',
                "publisher": "alpha",
                "catalyst_score": 1.0,
                "tags_json": "[]",
                "match_method_json": '{"005930": "name_exact"}',
            },
            {
                "published_at": "2026-03-06T18:00:00+09:00",
                "symbol_candidates": '["005930"]',
                "publisher": "beta",
                "catalyst_score": 1.0,
                "tags_json": "[]",
                "match_method_json": '{"005930": "name_exact"}',
            },
        ]
    )

    frame = build_news_feature_frame(
        recent_news,
        as_of_date=date(2026, 3, 6),
        cutoff_time="17:30",
    )

    assert frame.loc[0, "news_count_1d"] == 1.0
    assert frame.loc[0, "distinct_publishers_3d"] == 1.0
