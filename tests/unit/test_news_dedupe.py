from __future__ import annotations

import json

import pandas as pd

from app.domain.news.dedupe import dedupe_news_items


def test_dedupe_news_items_merges_query_and_symbol_metadata():
    frame = pd.DataFrame(
        [
            {
                "news_id": "same",
                "signal_date": "2026-03-06",
                "published_at": "2026-03-06T09:00:00+09:00",
                "symbol_candidates": json.dumps(["005930"]),
                "query_keyword": "삼성전자",
                "title": "삼성전자 실적",
                "publisher": "example.com",
                "link": "https://example.com/a",
                "snippet": "실적 개선",
                "tags_json": json.dumps(["earnings"]),
                "catalyst_score": 0.5,
                "sentiment_score": None,
                "freshness_score": 1.0,
                "source": "naver_news_search",
                "canonical_link": "https://example.com/a",
                "match_method_json": json.dumps({"005930": "name_exact"}),
                "query_bucket": "focus_symbol",
                "is_market_wide": False,
                "source_notes_json": json.dumps({}),
                "ingested_at": "2026-03-06T10:00:00+09:00",
            },
            {
                "news_id": "same",
                "signal_date": "2026-03-06",
                "published_at": "2026-03-06T09:00:00+09:00",
                "symbol_candidates": json.dumps(["000660"]),
                "query_keyword": "반도체",
                "title": "삼성전자 실적",
                "publisher": "example.com",
                "link": "https://example.com/a",
                "snippet": "실적 개선",
                "tags_json": json.dumps(["semiconductor"]),
                "catalyst_score": 0.75,
                "sentiment_score": None,
                "freshness_score": 1.0,
                "source": "naver_news_search",
                "canonical_link": "https://example.com/a",
                "match_method_json": json.dumps({"000660": "query_context_exact"}),
                "query_bucket": "market_index",
                "is_market_wide": True,
                "source_notes_json": json.dumps({}),
                "ingested_at": "2026-03-06T10:00:00+09:00",
            },
        ]
    )

    deduped = dedupe_news_items(frame)

    assert len(deduped) == 1
    row = deduped.iloc[0]
    assert sorted(json.loads(row["symbol_candidates"])) == ["000660", "005930"]
    assert sorted(json.loads(row["tags_json"])) == ["earnings", "semiconductor"]
    assert json.loads(row["source_notes_json"])["dedupe_count"] == 2
