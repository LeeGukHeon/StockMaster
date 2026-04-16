from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta

import pandas as pd

NEGATIVE_NEWS_TAGS = {"rates", "fx", "short_selling"}
MATCH_CONFIDENCE = {
    "name_exact": 1.0,
    "query_context_exact": 0.65,
}


def _explode_news_frame(news_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in news_frame.itertuples(index=False):
        symbols = json.loads(row.symbol_candidates or "[]")
        tags = json.loads(row.tags_json or "[]")
        match_methods = json.loads(row.match_method_json or "{}")
        if not symbols:
            continue
        for symbol in symbols:
            rows.append(
                {
                    "symbol": str(symbol).zfill(6),
                    "published_at": row.published_at,
                    "publisher": row.publisher,
                    "catalyst_score": row.catalyst_score,
                    "tags": tags,
                    "match_method": match_methods.get(symbol),
                }
            )
    return pd.DataFrame(rows)


def build_news_feature_frame(
    recent_news: pd.DataFrame,
    *,
    as_of_date: date,
    cutoff_time: str | None = None,
) -> pd.DataFrame:
    exploded = _explode_news_frame(recent_news)
    if exploded.empty:
        return pd.DataFrame(columns=["symbol"])

    published = pd.to_datetime(exploded["published_at"], utc=True, errors="coerce")
    published = published.dt.tz_convert("Asia/Seoul")
    exploded["published_at"] = published
    if cutoff_time:
        cutoff_end = pd.Timestamp(
            datetime.combine(as_of_date, time.fromisoformat(cutoff_time)),
            tz="Asia/Seoul",
        )
    else:
        cutoff_end = pd.Timestamp(as_of_date).tz_localize("Asia/Seoul") + pd.Timedelta(days=1)
    exploded = exploded.loc[exploded["published_at"].le(cutoff_end)].copy()
    if exploded.empty:
        return pd.DataFrame(columns=["symbol"])
    exploded["age_hours"] = (cutoff_end - exploded["published_at"]).dt.total_seconds() / 3600.0
    exploded["published_date"] = exploded["published_at"].dt.date
    exploded["positive_catalyst_flag"] = exploded["catalyst_score"].fillna(0.0).gt(0).astype(int)
    exploded["negative_catalyst_flag"] = exploded["tags"].map(
        lambda tags: int(any(tag in NEGATIVE_NEWS_TAGS for tag in tags))
    )
    exploded["confidence"] = exploded["match_method"].map(MATCH_CONFIDENCE).fillna(0.4)

    def aggregate(symbol: str, group: pd.DataFrame) -> dict[str, object]:
        one_day = group["published_date"].ge(as_of_date)
        three_day = group["published_date"].ge(as_of_date - timedelta(days=2))
        five_day = group["published_date"].ge(as_of_date - timedelta(days=4))
        within_three = group.loc[three_day]
        latest_age = group["age_hours"].min() if not group["age_hours"].isna().all() else None
        return {
            "symbol": symbol,
            "news_count_1d": float(one_day.sum()),
            "news_count_3d": float(three_day.sum()),
            "news_count_5d": float(five_day.sum()),
            "distinct_publishers_3d": float(within_three["publisher"].dropna().nunique()),
            "latest_news_age_hours": latest_age,
            "fresh_news_flag": float(latest_age is not None and latest_age <= 24.0),
            "positive_catalyst_count_3d": float(within_three["positive_catalyst_flag"].sum()),
            "negative_catalyst_count_3d": float(within_three["negative_catalyst_flag"].sum()),
            "news_link_confidence_score": float(group["confidence"].mean()),
            "news_coverage_flag": 1.0,
        }

    aggregated = [
        aggregate(symbol, group) for symbol, group in exploded.groupby("symbol", sort=False)
    ]
    return pd.DataFrame(aggregated)
