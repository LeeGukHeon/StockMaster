from __future__ import annotations

import json
from hashlib import sha1
from urllib.parse import urlsplit, urlunsplit

import pandas as pd


def canonicalize_link(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlsplit(value)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def compute_news_id(*, canonical_link: str, title: str, publisher: str, published_at) -> str:
    base = canonical_link or f"{title}|{publisher}|{published_at}"
    return sha1(base.encode("utf-8")).hexdigest()


def dedupe_news_items(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    rows: list[dict[str, object]] = []
    for _, group in frame.groupby("news_id", sort=False):
        first = group.iloc[0].to_dict()
        symbol_values = sorted(
            {
                symbol
                for payload in group["symbol_candidates"]
                for symbol in json.loads(payload or "[]")
            }
        )
        tags = sorted(
            {
                tag
                for payload in group["tags_json"]
                for tag in json.loads(payload or "[]")
            }
        )
        query_keywords = [str(value) for value in group["query_keyword"].dropna().unique()]
        query_buckets = [str(value) for value in group["query_bucket"].dropna().unique()]
        match_methods: dict[str, str] = {}
        for payload in group["match_method_json"].dropna():
            match_methods.update(json.loads(payload))

        source_notes = {}
        if first.get("source_notes_json"):
            source_notes.update(json.loads(str(first["source_notes_json"])))
        source_notes["query_keywords"] = query_keywords
        source_notes["query_buckets"] = query_buckets
        source_notes["dedupe_count"] = len(group)

        first["symbol_candidates"] = json.dumps(symbol_values, ensure_ascii=False)
        first["tags_json"] = json.dumps(tags, ensure_ascii=False)
        first["match_method_json"] = json.dumps(match_methods, ensure_ascii=False, sort_keys=True)
        first["query_keyword"] = query_keywords[0] if query_keywords else None
        first["query_bucket"] = ",".join(query_buckets) if query_buckets else None
        first["source_notes_json"] = json.dumps(source_notes, ensure_ascii=False, sort_keys=True)
        first["freshness_score"] = float(group["freshness_score"].max())
        first["catalyst_score"] = float(group["catalyst_score"].max())
        sentiment = group["sentiment_score"].dropna()
        first["sentiment_score"] = float(sentiment.iloc[0]) if not sentiment.empty else None
        rows.append(first)

    return pd.DataFrame(rows)
