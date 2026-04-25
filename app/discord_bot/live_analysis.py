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

SIGNAL_METRIC_LABELS = {
    "d1_trend_momentum_score": "D1 추세 탄력",
    "d5_trend_momentum_score": "D5 추세 탄력",
    "d1_relative_alpha_score": "D1 상대 강도",
    "d5_relative_alpha_score": "D5 상대 강도",
    "d1_flow_score": "D1 수급",
    "d5_flow_score": "D5 수급",
    "d5_flow_persistence_score": "D5 수급 지속성",
    "d1_news_drift_score": "D1 뉴스 재평가",
    "d5_news_drift_score": "D5 뉴스 재평가",
    "d1_crowding_penalty_score": "D1 과열 부담",
    "d5_crowding_penalty_score": "D5 과열 부담",
    "d5_risk_penalty_score": "D5 위험 부담",
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


def _basis_line(mode: str, note: str | None) -> str:
    if mode == "live":
        return "D5 스윙 주력 판단은 실시간 재계산 head와 최신 안정 스냅샷을 함께 사용했습니다."
    if note:
        return note
    if mode == "busy":
        return "배치 점유 중이라 최신 안정 스냅샷을 우선 사용했습니다."
    if mode == "missing":
        return "실시간 재계산 입력이 부족해 안정 스냅샷 기준으로 안내합니다."
    if mode == "unavailable":
        return "분석 DB를 읽을 수 없어 안정 스냅샷/실시간 시세 기준으로 안내합니다."
    return "최신 스냅샷 기준으로 안내합니다."


def _render_signal_decomposition(signal_payload: dict[str, object]) -> list[str]:
    lines: list[str] = []
    for label, values in (
        ("가격/추세", signal_payload.get("price")),
        ("수급", signal_payload.get("flow")),
        ("뉴스", signal_payload.get("news")),
        ("혼잡/리스크", signal_payload.get("crowding_risk")),
    ):
        if not isinstance(values, dict):
            continue
        non_empty = [
            f"{SIGNAL_METRIC_LABELS.get(key, key)} {float(value):.1f}"
            for key, value in values.items()
            if value is not None and not (isinstance(value, float) and pd.isna(value))
        ]
        if non_empty:
            lines.append(f"- {label}: " + ", ".join(non_empty))
    return lines


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
    market = _safe_text(row.get("market"))
    live_result = compute_live_stock_recommendation(settings, symbol=symbol)
    live_row = live_result.frame.iloc[0] if not live_result.frame.empty else None

    quote, quote_basis = _fetch_quote(settings, symbol=symbol)
    headlines, news_basis = _fetch_news(settings, company_name=company_name)

    current_price = _int_text(quote.get("stck_prpr"))
    change_price = _int_text(quote.get("prdy_vrss"))
    change_rate = _pct_from_quote(quote.get("prdy_ctrt"))
    high_price = _int_text(quote.get("stck_hgpr"))
    low_price = _int_text(quote.get("stck_lwpr"))
    volume = _int_text(quote.get("acml_vol"))
    analysis_payload = build_live_analysis_payload(
        payload,
        live_result,
        quote_timestamp_or_basis=quote_basis,
        news_basis=news_basis,
    )
    d1_grade = _safe_text(analysis_payload.get("d1_grade"))
    d5_grade = _safe_text(analysis_payload.get("d5_grade"))
    d5_expected = analysis_payload.get("d5_expected_excess_return")

    lines = [
        f"**{symbol} {company_name} · D5 스윙 즉석 분석**",
        f"{market} · 2~5거래일 보유 관점의 주력 판단입니다.",
        _basis_line(live_result.mode, live_result.note),
        f"현재가 {current_price}원, 전일 대비 {change_price}원 ({change_rate})",
        f"주력 판단: D5 {d5_grade} · 예상 초과수익률 {_pct_text(d5_expected)}",
        f"보조 참고: D1 {d1_grade} · 최근 5일 수익률 {_pct_text(analysis_payload.get('ret_5d'))}",
        (
            f"운영 head: D5 {_safe_text(analysis_payload.get('d5_head_spec_id'))} "
            f"· D1 참고 {_safe_text(analysis_payload.get('d1_head_spec_id'))}"
        ),
        f"왜 지금 보나: {_translated_why_now(analysis_payload)}",
    ]
    if live_row is not None:
        target_price = _int_text(live_row.get("live_d5_target_price"))
        stop_price = _int_text(live_row.get("live_d5_stop_price"))
        lines.append(f"실시간 목표가 {target_price}원 · 손절 참고선 {stop_price}원")
    signal_lines = _render_signal_decomposition(analysis_payload.get("signal_decomposition", {}))
    if signal_lines:
        lines.append("신호 분해")
        lines.extend(signal_lines)
    risk_flags = _translate_tag_list(analysis_payload.get("risk_flags"), RISK_LABELS, limit=4)
    if risk_flags:
        lines.append("리스크 플래그")
        lines.extend(f"- {item}" for item in risk_flags)
    invalidations = analysis_payload.get("invalidation_conditions") or []
    if invalidations:
        lines.append("무효화 조건")
        lines.extend(f"- {item}" for item in invalidations)
    if analysis_payload.get("snapshot_reused_flag"):
        lines.append(
            f"분석 모드 {analysis_payload.get('degradation_mode')} · snapshot 재사용 "
            f"({', '.join(analysis_payload.get('source_precedence') or [])})"
        )
    lines.append(f"당일 고가 {high_price}원 · 저가 {low_price}원 · 누적 거래량 {volume}")
    lines.append(f"시세 기준 {analysis_payload.get('quote_timestamp_or_basis')}")
    lines.append(f"뉴스 기준 {analysis_payload.get('news_basis')}")
    if headlines:
        lines.append("최근 뉴스")
        lines.extend(f"- {headline}" for headline in headlines)
    return "\n".join(lines)
