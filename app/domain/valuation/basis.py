from __future__ import annotations

from collections.abc import Iterable, Mapping

CORE_METRICS = ("eps", "bps", "per", "pbr")
SHARE_COUNT_REASON_CODES = {
    "share_basis_aggregate",
    "share_denominator_float_based",
    "ambiguous_share_class",
    "missing_share_count",
}


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric(metrics: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = metrics.get(name)
    return value if isinstance(value, Mapping) else {}


def _available(metric: Mapping[str, object]) -> bool:
    return _as_float(metric.get("value")) is not None and metric.get("source_type") != "unavailable"


def _metric_reason(metrics: Mapping[str, object], name: str) -> str:
    reason = _metric(metrics, name).get("reason")
    return "" if reason is None else str(reason)


def _has_peer_percentiles(peer: Mapping[str, object]) -> bool:
    return (
        _as_float(peer.get("pbr_percentile")) is not None
        and _as_float(peer.get("per_percentile")) is not None
    )


def describe_evaluation_basis(
    *,
    metrics: Mapping[str, object] | None,
    peer: Mapping[str, object] | None,
    hard_gate_reasons: Iterable[object] | None,
    financial_quality: Mapping[str, object] | None,
) -> str:
    """Return a short user-facing basis note for the valuation label.

    The note intentionally explains the *basis* rather than changing the final label.  It is
    suitable for Discord read-store payloads and render-time fallback for older snapshots.
    """

    metrics = metrics or {}
    peer = peer or {}
    financial_quality = financial_quality or {}
    reason_set = {str(reason) for reason in (hard_gate_reasons or []) if str(reason).strip()}

    net_income = _as_float(financial_quality.get("net_income"))
    equity = _as_float(financial_quality.get("equity"))
    per_available = _available(_metric(metrics, "per"))
    pbr_available = _available(_metric(metrics, "pbr"))

    if "non_positive_net_income" in reason_set or (net_income is not None and net_income <= 0):
        if pbr_available:
            return "적자/비양수 순이익: PER 제외, PBR·재무품질 참고"
        return "적자/비양수 순이익: PER 제외, 공시 재무위험 중심 보류"

    if "non_positive_equity" in reason_set or (equity is not None and equity <= 0):
        return "자본 비양수/부족: PBR 제외, 이익·재무위험 중심 보류"

    if reason_set & SHARE_COUNT_REASON_CODES:
        return "공시 주식수 기준 불확실: 주당지표 참고 제한"

    if "missing_core_metric" in reason_set:
        metric_reasons = {_metric_reason(metrics, name) for name in CORE_METRICS}
        if "missing_or_zero_shares" in metric_reasons:
            return "공시 주식수 부족: EPS/BPS/PER/PBR 산출 제한"
        if {"negative_or_zero_eps", "missing_eps", "missing_net_income"} & metric_reasons:
            return "PER 산출 제한: PBR·재무품질 참고"
        if {"negative_or_zero_bps", "missing_bps", "missing_equity"} & metric_reasons:
            return "PBR 산출 제한: PER·재무품질 참고"
        return "핵심 가치지표 부족: 공시 재무품질 중심 보류"

    if reason_set & {"insufficient_peer_sample", "low_peer_metric_coverage"}:
        return "PER/PBR 산출값은 있으나 동종비교 표본·커버리지 부족"

    if per_available and pbr_available and _has_peer_percentiles(peer):
        return "PER/PBR 동종업계 percentile 비교"
    if per_available and pbr_available:
        return "PER/PBR 절대배수·재무품질 참고"
    if pbr_available:
        return "PBR 자산가치·재무품질 참고"
    if per_available:
        return "PER 이익가치·재무품질 참고"
    return "공시 재무품질 중심 보류"
