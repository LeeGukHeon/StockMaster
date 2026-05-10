from __future__ import annotations

import json
from typing import Any

import pandas as pd

from app.discord_bot.read_store import fetch_discord_bot_snapshot_rows
from app.domain.valuation.basis import describe_evaluation_basis
from app.settings import Settings

INTERNAL_CALCULATION_DISCLOSURE = (
    "공시 원자료에는 해당 지표가 비어 있어 StockMaster가 "
    "공시 재무항목과 기준가로 내부 산출했습니다."
)

REASON_LABELS = {
    "stale_disclosure": "공시 데이터가 오래됨",
    "missing_core_metric": "핵심 가치지표 부족",
    "insufficient_peer_sample": "동종 비교 표본 부족",
    "low_peer_metric_coverage": "동종 PER/PBR 커버리지 부족",
    "missing_lineage": "출처 계보 부족",
    "share_basis_aggregate": "주식수 기준이 합계 행",
    "share_denominator_float_based": "유통주식수 기준 산출",
    "ambiguous_share_class": "주식 종류 구분 불명확",
    "missing_share_count": "공시 주식수 부족",
    "non_positive_net_income": "순이익 비양수",
    "non_positive_equity": "자본 비양수",
}


def _parse_payload(row: pd.Series) -> dict[str, Any]:
    raw = row.get("payload_json")
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fmt(value: object, *, decimals: int = 2, suffix: str = "") -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
    except TypeError:
        pass
    return f"{float(value):,.{decimals}f}{suffix}"


def _metric(payload: dict[str, Any], name: str) -> dict[str, Any]:
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return {}
    metric = metrics.get(name)
    return metric if isinstance(metric, dict) else {}


def _reason_text(reasons: list[object]) -> str:
    if not reasons:
        return "핵심 신뢰 게이트 통과"
    return ", ".join(REASON_LABELS.get(str(reason), str(reason)) for reason in reasons[:5])


def _has_internal_calculation(payload: dict[str, Any]) -> bool:
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return False
    return any(
        isinstance(metric, dict)
        and metric.get("source_type") == "internal_calculated_from_disclosure"
        for metric in metrics.values()
    )


def _render_candidate_list(query: str, rows: pd.DataFrame) -> str:
    lines = [f"`{query}` 기준 가치평가 후보가 여러 개입니다. 6자리 코드로 다시 조회하세요."]
    for row in rows.head(5).itertuples(index=False):
        lines.append(f"- {getattr(row, 'title', '-')}")
    return "\n".join(lines)


def render_stock_valuation(settings: Settings, *, query: str) -> str:
    rows = fetch_discord_bot_snapshot_rows(
        settings,
        snapshot_type="stock_valuation",
        query=query,
        limit=5,
    )
    if rows.empty:
        return f"`{query}` 기준 가치평가 스냅샷이 아직 준비되지 않았습니다."
    if len(rows) > 1:
        return _render_candidate_list(query, rows)

    row = rows.iloc[0]
    payload = _parse_payload(row)
    label = str(payload.get("valuation_label") or row.get("summary") or "판단 보류")
    reasons = payload.get("hard_gate_reasons") or []
    if not isinstance(reasons, list):
        reasons = []
    peer = payload.get("peer") if isinstance(payload.get("peer"), dict) else {}
    industry_group = (
        payload.get("industry_group") if isinstance(payload.get("industry_group"), dict) else {}
    )
    quality = (
        payload.get("financial_quality")
        if isinstance(payload.get("financial_quality"), dict)
        else {}
    )
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    evaluation_basis = str(
        payload.get("evaluation_basis")
        or describe_evaluation_basis(
            metrics=metrics,
            peer=peer,
            hard_gate_reasons=reasons,
            financial_quality=quality,
        )
    )
    display_industry = industry_group.get("label") or payload.get("industry") or "-"
    lines = [
        f"**{row['title']} 가치평가**",
        f"최종 판단: {label}",
        f"평가기반: {evaluation_basis}",
        (
            f"기준: {row.get('as_of_date') or '-'} · "
            f"업종 {display_industry} / 상위분류 {payload.get('sector') or '-'}"
        ),
        (
            "핵심지표: "
            f"PER {_fmt(_metric(payload, 'per').get('value'), suffix='배')} · "
            f"PBR {_fmt(_metric(payload, 'pbr').get('value'), suffix='배')} · "
            f"EPS {_fmt(_metric(payload, 'eps').get('value'), suffix='원')} · "
            f"BPS {_fmt(_metric(payload, 'bps').get('value'), suffix='원')}"
        ),
        (
            "동종비교: "
            f"{peer.get('group_type') or '-'} {peer.get('group_value') or '-'} · "
            f"표본 {peer.get('peer_count') or 0}개 · "
            f"PER 중앙 {_fmt(peer.get('median_per'), suffix='배')} · "
            f"PBR 중앙 {_fmt(peer.get('median_pbr'), suffix='배')}"
        ),
        (
            "재무품질: "
            f"ROE {_fmt(_metric(payload, 'roe').get('value'), suffix='%')} · "
            f"영업이익률 {_fmt(_metric(payload, 'operating_margin').get('value'), suffix='%')} · "
            f"순이익률 {_fmt(_metric(payload, 'net_margin').get('value'), suffix='%')} · "
            f"부채비율 {_fmt(_metric(payload, 'debt_ratio').get('value'), suffix='%')}"
        ),
        f"신뢰도: {_reason_text([str(reason) for reason in reasons])}",
    ]
    if _has_internal_calculation(payload):
        lines.append(INTERNAL_CALCULATION_DISCLOSURE)
    if quality.get("net_income") is not None and float(quality.get("net_income") or 0.0) <= 0:
        lines.append("주의: 순이익이 비양수라 최종 가치 라벨은 보류됩니다.")
    return "\n".join(lines[:10])
