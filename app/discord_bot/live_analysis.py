from __future__ import annotations

import json
from typing import Any

import pandas as pd

from app.discord_bot.live_recalc import (
    build_live_analysis_payload,
    compute_live_stock_recommendation,
)
from app.discord_bot.read_store import fetch_discord_bot_snapshot_rows
from app.providers.kis.client import KISProvider
from app.providers.naver_news.client import NaverNewsProvider
from app.reports.discord_eod import REASON_LABELS, RISK_LABELS
from app.settings import Settings

LIVE_RISK_LABELS = {
    **RISK_LABELS,
    "model_disagreement_high": "앙상블 내부 판단이 엇갈림",
    "model_uncertainty_high": "모델 불확실성이 큼",
}


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


def _score_text(value: object) -> str:
    if value in (None, "", "-"):
        return "-"
    try:
        return f"{float(value):.1f}"
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


def _translate_tag(value: object, mapping: dict[str, str]) -> str:
    text = _safe_text(value)
    if text == "-":
        return ""
    return mapping.get(text, text)


def _translate_tag_list(values: object, mapping: dict[str, str], *, limit: int = 3) -> list[str]:
    if not isinstance(values, list):
        return []
    labels: list[str] = []
    for value in values:
        label = _translate_tag(value, mapping)
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= limit:
            break
    return labels


def _translated_why_now(analysis_payload: dict[str, object]) -> str:
    d5_reasons = _translate_tag_list(analysis_payload.get("d5_reason_tags"), REASON_LABELS, limit=2)
    d1_reasons = _translate_tag_list(analysis_payload.get("d1_reason_tags"), REASON_LABELS, limit=1)
    reasons = d5_reasons + [label for label in d1_reasons if label not in d5_reasons]
    if reasons:
        return " · ".join(reasons)
    return _safe_text(analysis_payload.get("why_now"))


def _latest_news_lines(provider: NaverNewsProvider, *, company_name: str) -> list[str]:
    payload = provider.search_news(query=company_name, limit=3, start=1, sort="date")
    lines: list[str] = []
    for item in payload.get("items", [])[:3]:
        title = _safe_text(item.get("title_plain"))
        if title == "-":
            continue
        lines.append(title)
    return lines


def _close_provider(provider: object | None) -> None:
    if provider is None:
        return
    close = getattr(provider, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:
        return


def _fetch_quote(settings: Settings, *, symbol: str) -> tuple[dict[str, object], str]:
    provider = None
    try:
        provider = KISProvider(settings)
        quote_payload = provider.fetch_current_quote(
            symbol=symbol,
            persist_probe_artifacts=False,
        )
    except Exception:
        return {}, "KIS 실시간 시세 미수신"
    finally:
        _close_provider(provider)
    quote = quote_payload.get("output") or {}
    return quote, "KIS 실시간 시세 기준" if quote else "KIS 실시간 시세 미수신"


def _fetch_news(settings: Settings, *, company_name: str) -> tuple[list[str], str]:
    provider = None
    try:
        provider = NaverNewsProvider(settings)
        headlines = _latest_news_lines(provider, company_name=company_name)
    except Exception:
        return [], "Naver 최신 뉴스 미수신"
    finally:
        _close_provider(provider)
    basis = f"Naver 최신 뉴스 {len(headlines)}건 반영" if headlines else "최근 뉴스 미반영"
    return headlines, basis



def _signal_value(signal_payload: dict[str, object], section: str, key: str) -> object:
    values = signal_payload.get(section)
    if not isinstance(values, dict):
        return None
    return values.get(key)


def _score_level(value: object, *, high_is_good: bool = True) -> str | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(score):
        return None
    if high_is_good:
        if score >= 70:
            return "강함"
        if score >= 55:
            return "양호"
        return "약함"
    if score <= 35:
        return "낮음"
    if score <= 65:
        return "보통"
    return "높음"


def _render_signal_summary(signal_payload: dict[str, object]) -> str:
    trend = _score_level(_signal_value(signal_payload, "price", "d5_trend_momentum_score"))
    flow = _score_level(_signal_value(signal_payload, "flow", "d5_flow_score"))
    persistence = _score_level(
        _signal_value(signal_payload, "flow", "d5_flow_persistence_score")
    )
    risk = _score_level(
        _signal_value(signal_payload, "crowding_risk", "d5_risk_penalty_score"),
        high_is_good=False,
    )
    parts: list[str] = []
    if trend:
        parts.append(f"추세 {trend}")
    if flow:
        parts.append(f"수급 {flow}")
    if persistence and persistence != flow:
        parts.append(f"지속성 {persistence}")
    if risk:
        parts.append(f"위험 {risk}")
    return " · ".join(parts) if parts else "확인 제한"


def _short_list_text(
    items: list[str],
    *,
    empty: str,
    limit: int = 1,
    show_suffix: bool = True,
) -> str:
    if not items:
        return empty
    shown = items[:limit]
    suffix = f" 외 {len(items) - limit}" if show_suffix and len(items) > limit else ""
    return ", ".join(shown) + suffix


def _render_candidate_list(query: str, rows: pd.DataFrame) -> str:
    lines = ["**종목 후보**", f"`{query}`가 여러 종목과 매칭됩니다. 6자리 코드로 다시 조회하세요."]
    for row in rows.head(5).itertuples(index=False):
        lines.append(f"- {row.title}")
    return "\n".join(lines)


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
        return _render_candidate_list(query, rows)

    row = rows.iloc[0]
    payload = _parse_payload(row.get("payload_json"))
    symbol = _safe_text(row.get("symbol"))
    company_name = _safe_text(row.get("company_name"))
    live_result = compute_live_stock_recommendation(settings, symbol=symbol)
    live_row = live_result.frame.iloc[0] if not live_result.frame.empty else None

    quote, quote_basis = _fetch_quote(settings, symbol=symbol)
    headlines, news_basis = _fetch_news(settings, company_name=company_name)

    current_price = _int_text(quote.get("stck_prpr"))
    change_rate = _pct_from_quote(quote.get("prdy_ctrt"))
    analysis_payload = build_live_analysis_payload(
        payload,
        live_result,
        quote_timestamp_or_basis=quote_basis,
        news_basis=news_basis,
    )
    d5_grade = _safe_text(analysis_payload.get("d5_grade"))
    d5_expected = analysis_payload.get("d5_expected_excess_return")
    judgement_label = _safe_text(analysis_payload.get("d5_judgement_label"), "판단 보류")
    judgement_summary = _safe_text(analysis_payload.get("d5_judgement_summary"))
    d5_score = _score_text(analysis_payload.get("d5_final_selection_value"))
    risk_flags = _translate_tag_list(analysis_payload.get("risk_flags"), LIVE_RISK_LABELS, limit=2)
    risk_text = _short_list_text(risk_flags, empty="특이 리스크 없음", limit=1)
    reason_text = _short_list_text(
        _translated_why_now(analysis_payload).split(" · "),
        empty="근거 제한",
        limit=2,
        show_suffix=False,
    )
    signal_text = _render_signal_summary(analysis_payload.get("signal_decomposition", {}))

    lines = [
        f"**{symbol} {company_name} · {judgement_label}**",
        (
            f"현재 {current_price}원 ({change_rate}) · "
            f"D5 {d5_grade}/{d5_score}점 · 기대 {_pct_text(d5_expected)}"
        ),
        f"판단: {judgement_summary}",
        f"근거: {reason_text} · 신호 {signal_text}",
        f"주의: {risk_text}",
    ]
    if live_row is not None:
        target_price = _int_text(live_row.get("live_d5_target_price"))
        stop_price = _int_text(live_row.get("live_d5_stop_price"))
        lines.append(f"가격: 목표 {target_price}원 · 손절 {stop_price}원")
    if analysis_payload.get("snapshot_reused_flag"):
        lines.append(f"상태: {analysis_payload.get('degradation_mode')} · snapshot 재사용")
    if headlines:
        lines.append(f"뉴스: {headlines[0]}")
    elif quote_basis != "KIS 실시간 시세 기준" or news_basis != "Naver 최신 뉴스 0건 반영":
        lines.append(f"데이터: {quote_basis} · {news_basis}")
    return "\n".join(lines)
