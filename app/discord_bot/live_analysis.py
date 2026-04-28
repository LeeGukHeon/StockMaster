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
from app.reports.discord_eod import REASON_LABELS, RISK_LABELS
from app.settings import Settings

LIVE_RISK_LABELS = {
    **RISK_LABELS,
    "prediction_error_bucket_high": "고예측 구간 오차 큼",
    "model_disagreement_high": "앙상블 판단 차이 큼",
    "model_joint_instability_high": "모델 불안정성 큼",
    "model_uncertainty_high": "고예측 구간 오차 큼",
}

LIVE_SHORT_LABELS = {
    "상대 강도가 살아나는 흐름": "상대강도 개선",
    "원점수 상위 신호를 최대한 보존함": "원점수 상위",
    "수급 지속성이 받쳐줌": "수급 지속성",
    "단기 탄력 강함": "단기 탄력",
    "뉴스 재평가가 덜 반영됨": "뉴스 재평가",
    "과열 혼잡 부담이 낮음": "과열 낮음",
    "고예측 구간 오차 큼": "고예측 오차",
    "앙상블 판단 차이 큼": "모델 이견",
    "모델 불안정성 큼": "모델 불안정",
    "최근 흔들림이 큼": "변동성 큼",
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




def _compact_judgement_text(value: object) -> str:
    text = _safe_text(value)
    return text.split(" · ", 1)[0] if " · " in text else text


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


def _short_label(label: str) -> str:
    return LIVE_SHORT_LABELS.get(label, label)


def _translated_why_now(analysis_payload: dict[str, object]) -> str:
    d5_reasons = _translate_tag_list(analysis_payload.get("d5_reason_tags"), REASON_LABELS, limit=2)
    d1_reasons = _translate_tag_list(analysis_payload.get("d1_reason_tags"), REASON_LABELS, limit=1)
    reasons = d5_reasons + [label for label in d1_reasons if label not in d5_reasons]
    if reasons:
        return " · ".join(reasons)
    return _safe_text(analysis_payload.get("why_now"))


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
    risk = _score_level(
        _signal_value(signal_payload, "crowding_risk", "d5_risk_penalty_score"),
        high_is_good=False,
    )
    parts: list[str] = []
    if trend and flow and trend == flow:
        parts.append(f"추세·수급 {trend}")
    else:
        if trend:
            parts.append(f"추세 {trend}")
        if flow:
            parts.append(f"수급 {flow}")
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
    shown = [_short_label(item) for item in items[:limit]]
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
    current_price = _int_text(quote.get("stck_prpr"))
    change_rate = _pct_from_quote(quote.get("prdy_ctrt"))
    analysis_payload = build_live_analysis_payload(
        payload,
        live_result,
        quote_timestamp_or_basis=quote_basis,
        news_basis="뉴스 미사용",
    )
    live_d5_grade = _safe_text(analysis_payload.get("d5_grade"))
    live_d5_expected = analysis_payload.get("d5_expected_excess_return")
    live_judgement_label = _safe_text(analysis_payload.get("d5_judgement_label"), "판단 보류")
    live_d5_score = _score_text(analysis_payload.get("d5_final_selection_value"))
    stable_d5_grade = _safe_text(payload.get("d5_grade") or live_d5_grade)
    stable_d5_expected = payload.get("d5_expected_excess_return", live_d5_expected)
    stable_d5_score = _score_text(
        payload.get("d5_final_selection_value")
        or analysis_payload.get("d5_final_selection_value")
    )
    stable_judgement_label = _safe_text(
        payload.get("d5_judgement_label") or analysis_payload.get("d5_judgement_label"),
        "판단 보류",
    )
    stable_judgement_summary = _safe_text(
        payload.get("d5_judgement_summary") or analysis_payload.get("d5_judgement_summary")
    )
    risk_flags = _translate_tag_list(analysis_payload.get("risk_flags"), LIVE_RISK_LABELS, limit=2)
    risk_text = _short_list_text(risk_flags, empty="특이 리스크 없음", limit=1)
    reason_text = _short_list_text(
        _translated_why_now(analysis_payload).split(" · "),
        empty="장마감 추천 기준" if live_result.mode == "closed" else "근거 제한",
        limit=2,
        show_suffix=False,
    )

    lines = [
        f"**{symbol} {company_name} · {stable_judgement_label}**",
        (
            f"장마감 D5 {stable_d5_grade}/{stable_d5_score}점 "
            f"· 기대 {_pct_text(stable_d5_expected)} "
            f"· 현재 {current_price}원 ({change_rate})"
        ),
        f"판단: {_compact_judgement_text(stable_judgement_summary)}",
    ]
    if live_row is not None and live_d5_score != stable_d5_score:
        live_label = (
            "추격매수 보류"
            if live_judgement_label == "매수 보류"
            else live_judgement_label
        )
        lines.append(f"실시간: 점수 {live_d5_score}점 · {live_label}")
    lines.append(f"근거: {reason_text}")
    if risk_flags or live_result.mode != "closed":
        lines.append(f"주의: {risk_text}")
    if live_row is not None:
        target_price = _int_text(live_row.get("live_d5_target_price"))
        stop_price = _int_text(live_row.get("live_d5_stop_price"))
        lines.append(f"가격: 목표 {target_price}원 · 손절 {stop_price}원")
    if analysis_payload.get("snapshot_reused_flag") and live_result.mode != "closed":
        lines.append(f"상태: {analysis_payload.get('degradation_mode')} · snapshot 재사용")
    if quote_basis != "KIS 실시간 시세 기준":
        lines.append(f"데이터: {quote_basis}")
    return "\n".join(lines)
