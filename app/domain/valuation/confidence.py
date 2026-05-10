from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.valuation.metrics import MetricValue

HARD_SHARE_REASON_CODES = {
    "share_basis_aggregate",
    "share_denominator_float_based",
    "ambiguous_share_class",
    "missing_share_count",
}


@dataclass(frozen=True, slots=True)
class ConfidenceInput:
    disclosure_age_days: int | None
    metrics: dict[str, MetricValue]
    peer_count: int
    pbr_peer_coverage: float
    per_peer_coverage: float
    source_lineage_present: bool
    share_basis_reasons: tuple[str, ...] = field(default_factory=tuple)
    net_income: float | None = None
    equity: float | None = None


@dataclass(frozen=True, slots=True)
class ConfidenceResult:
    passed: bool
    hard_gate_reasons: tuple[str, ...]
    annotations: tuple[str, ...]

    @property
    def final_label_allowed(self) -> bool:
        return self.passed


def evaluate_confidence(inputs: ConfidenceInput) -> ConfidenceResult:
    reasons: list[str] = []
    annotations: list[str] = []
    if inputs.disclosure_age_days is None or inputs.disclosure_age_days > 540:
        reasons.append("stale_disclosure")
    for metric_name in ("eps", "bps", "per", "pbr"):
        metric = inputs.metrics.get(metric_name)
        if metric is None or not metric.available:
            reasons.append("missing_core_metric")
            break
    if inputs.peer_count < 8:
        reasons.append("insufficient_peer_sample")
    if inputs.pbr_peer_coverage < 0.60 or inputs.per_peer_coverage < 0.60:
        reasons.append("low_peer_metric_coverage")
    if not inputs.source_lineage_present:
        reasons.append("missing_lineage")
    for reason in inputs.share_basis_reasons:
        if reason in HARD_SHARE_REASON_CODES:
            reasons.append(reason)
    if inputs.net_income is None or inputs.net_income <= 0:
        reasons.append("non_positive_net_income")
    if inputs.equity is None or inputs.equity <= 0:
        reasons.append("non_positive_equity")
    if any(
        metric.source_type == "internal_calculated_from_disclosure"
        for metric in inputs.metrics.values()
        if metric.available
    ):
        annotations.append("internal_calculated_from_disclosure")
    unique_reasons = tuple(dict.fromkeys(reasons))
    return ConfidenceResult(
        passed=not unique_reasons,
        hard_gate_reasons=unique_reasons,
        annotations=tuple(dict.fromkeys(annotations)),
    )
