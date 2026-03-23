from __future__ import annotations

import json
from typing import Any

import pandas as pd

from app.discord_bot.read_store import fetch_discord_bot_snapshot_rows
from app.providers.kis.client import KISProvider
from app.providers.naver_news.client import NaverNewsProvider
from app.settings import Settings


def _safe_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text in {"nan", "NaN", "NaT", "None"}:
        return fallback
    return text


def _pct_from_quote(value: object) -> str:
    if value in (None, "", "-", "0"):
        return "-"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return _safe_text(value)


def _int_text(value: object) -> str:
    if value in (None, "", "-"):
        return "-"
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return _safe_text(value)


def _pct_text(value: object, *, signed: bool = True) -> str:
    if value in (None, "", "-"):
        return "-"
    try:
        format_spec = "+.2%" if signed else ".2%"
        return format(float(value), format_spec)
    except (TypeError, ValueError):
        return _safe_text(value)


def _parse_payload(value: object) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _latest_news_lines(provider: NaverNewsProvider, *, company_name: str) -> list[str]:
    payload = provider.search_news(query=company_name, limit=3, start=1, sort="date")
    lines: list[str] = []
    for item in payload.get("items", [])[:3]:
        title = _safe_text(item.get("title_plain"))
        if title == "-":
            continue
        lines.append(title)
    return lines


def render_live_stock_analysis(settings: Settings, *, query: str) -> str:
    rows = fetch_discord_bot_snapshot_rows(
        settings,
        snapshot_type="stock_summary",
        query=query,
        limit=5,
    )
    if rows.empty:
        return f"`{query}` 기준으로 찾은 종목이 없습니다."
    if len(rows) > 1:
        lines = ["**종목 후보**"]
        for row in rows.head(5).itertuples(index=False):
            lines.append(f"- {row.title}")
        return "\n".join(lines)

    row = rows.iloc[0]
    payload = _parse_payload(row.get("payload_json"))
    symbol = _safe_text(row.get("symbol"))
    company_name = _safe_text(row.get("company_name"))
    market = _safe_text(row.get("market"))

    kis = KISProvider(settings)
    news = NaverNewsProvider(settings)
    try:
        quote_payload = kis.fetch_current_quote(
            symbol=symbol,
            persist_probe_artifacts=False,
        )
        quote = quote_payload.get("output") or {}
        headlines = _latest_news_lines(news, company_name=company_name)
    finally:
        kis.close()
        news.close()

    current_price = _int_text(quote.get("stck_prpr"))
    change_price = _int_text(quote.get("prdy_vrss"))
    change_rate = _pct_from_quote(quote.get("prdy_ctrt"))
    high_price = _int_text(quote.get("stck_hgpr"))
    low_price = _int_text(quote.get("stck_lwpr"))
    volume = _int_text(quote.get("acml_vol"))

    lines = [
        f"**{symbol} {company_name}**",
        f"{market} · 최신 안정 스냅샷 + 실시간 시세 기준",
        f"현재가 {current_price}원 · 전일 대비 {change_price}원 ({change_rate})",
        f"D1 {payload.get('d1_grade', '-')} · D5 {payload.get('d5_grade', '-')}",
        f"D5 예상 초과수익률 {_pct_text(payload.get('d5_expected_excess_return'))}",
        f"최근 5일 수익률 {_pct_text(payload.get('ret_5d'))}",
        f"당일 고가 {high_price}원 · 저가 {low_price}원 · 누적 거래량 {volume}",
    ]
    if headlines:
        lines.append("최근 뉴스")
        lines.extend(f"- {headline}" for headline in headlines)
    return "\n".join(lines)
