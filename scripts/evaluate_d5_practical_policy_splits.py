# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ranking.risk_taxonomy import BUYABILITY_BLOCKING_RISK_FLAGS
from app.recommendation.buyability import (
    BUYABILITY_DISAGREEMENT_PENALTY,
    BUYABILITY_EXPECTED_RETURN_WEIGHT,
    BUYABILITY_MIN_EXPECTED_EXCESS_RETURN,
    BUYABILITY_MIN_FINAL_SELECTION_VALUE,
    BUYABILITY_MIN_PRIORITY_SCORE,
    BUYABILITY_UNCERTAINTY_PENALTY,
)

HIGH_DISAGREEMENT_FLAG = "model_disagreement_high"
DEFAULT_TRANSACTION_COST_BPS = 15.0


@dataclass(frozen=True, slots=True)
class BasketGateThresholds:
    min_coverage_ratio: float = 0.70
    min_median_ratio: float = 0.80
    max_single_date_edge_share: float = 0.40
    top5_avg_net_tolerance: float = -0.001
    top5_median_net_tolerance: float = -0.001
    top5_p10_net_tolerance: float = -0.005
    top5_hit_net_tolerance: float = -0.05
    top5_max_drawdown_net_tolerance: float = -0.05
    top5_max_sector_concentration: float = 0.60
    top3_avg_net_tolerance: float = -0.002
    top3_p10_net_tolerance: float = -0.0075
    top3_hit_net_tolerance: float = -0.07


@dataclass(frozen=True, slots=True)
class GateResult:
    policy_id: str
    gate_decision: str
    passed: bool
    fail_reasons: list[str]
    warning_reasons: list[str]

    def as_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "gate_decision": self.gate_decision,
            "passed": bool(self.passed),
            "primary_top_n": 5,
            "secondary_top_n": 3,
            "diagnostic_top_n": 1,
            "objective_hierarchy": {
                "primary": "top5",
                "secondary": "top3",
                "diagnostic": "top1",
            },
            "fail_reasons": list(self.fail_reasons),
            "warning_reasons": list(self.warning_reasons),
        }


@dataclass(frozen=True, slots=True)
class PolicySpec:
    policy_id: str
    model_key: str
    description: str
    high_disagreement_score_penalty: float = 0.0
    block_high_disagreement: bool = False
    conditional_high_disagreement: bool = False
    conditional_expected_floor: float = 0.02
    conditional_uncertainty_ceiling: float = 20.0
    conditional_priority_floor: float = 0.0


ACTIVE_ONLY_POLICY_SPECS: tuple[PolicySpec, ...] = (
    PolicySpec(
        policy_id="active_current",
        model_key="active",
        description="Current active D5 practical surface only; no challenger policies.",
    ),
)
ABC_POLICY_SPECS: tuple[PolicySpec, ...] = (
    PolicySpec(
        policy_id="active_current",
        model_key="active",
        description="Current active alpha_swing_d5_v2 practical surface.",
    ),
    PolicySpec(
        policy_id="A_hard_disagreement_quarantine",
        model_key="practical",
        description="Keep v1 practical target; exclude high model-disagreement candidates.",
        block_high_disagreement=True,
    ),
    PolicySpec(
        policy_id="B_soft_disagreement_penalty_30",
        model_key="practical",
        description=(
            "Keep v1 practical target; apply a fixed -30 score penalty to "
            "high disagreement."
        ),
        high_disagreement_score_penalty=30.0,
    ),
    PolicySpec(
        policy_id="C_conditional_disagreement_escape",
        model_key="practical",
        description=(
            "Keep v1 practical target; quarantine high disagreement unless expected return, "
            "uncertainty, and priority clear stricter floors."
        ),
        high_disagreement_score_penalty=45.0,
        conditional_high_disagreement=True,
        conditional_expected_floor=0.02,
        conditional_uncertainty_ceiling=20.0,
        conditional_priority_floor=0.0,
    ),
)
CURRENT_POLICY_SPECS: tuple[PolicySpec, ...] = (
    PolicySpec(
        policy_id="active_current",
        model_key="active",
        description="Current active alpha_swing_d5_v2 practical surface.",
    ),
    PolicySpec(
        policy_id="practical_v1_current",
        model_key="practical_v1",
        description="Current alpha_practical_d5_v1 practical surface without extra policy tuning.",
    ),
    PolicySpec(
        policy_id="practical_v2_current",
        model_key="practical_v2",
        description="Current alpha_practical_d5_v2 practical surface without extra policy tuning.",
    ),
)
STABLE_POLICY_SPECS: tuple[PolicySpec, ...] = (
    PolicySpec(
        policy_id="active_current",
        model_key="active",
        description="Current active alpha_swing_d5_v2 practical surface.",
    ),
    PolicySpec(
        policy_id="stable_buyable_current",
        model_key="stable",
        description=(
            "Experimental alpha_stable_buyable_d5_v1 surface; news-free, "
            "regime-aware, and trained on stable practical D5 utility."
        ),
    ),
)
POLICY_SETS: dict[str, tuple[PolicySpec, ...]] = {
    "active_only": ACTIVE_ONLY_POLICY_SPECS,
    "abc": ABC_POLICY_SPECS,
    "current": CURRENT_POLICY_SPECS,
    "stable": STABLE_POLICY_SPECS,
    "all": (*ABC_POLICY_SPECS, *CURRENT_POLICY_SPECS[1:]),
}


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_key_path(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected KEY=PATH")
    key, path = value.split("=", 1)
    key = key.strip()
    if not key:
        raise argparse.ArgumentTypeError("Outcome key must not be empty")
    return key, Path(path)


def _parse_window(value: str) -> dict[str, str]:
    parts = value.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("Expected NAME:START_DATE:END_DATE")
    name, start, end = parts
    _parse_date(start)
    _parse_date(end)
    return {"name": name, "start_date": start, "end_date": end}


def _json_list(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _safe_numeric(frame: pd.DataFrame, column: str, *, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _transaction_cost_rate(transaction_cost_bps: float) -> float:
    return max(float(transaction_cost_bps), 0.0) / 10_000.0


def _net_realized_return(frame: pd.DataFrame, *, transaction_cost_bps: float) -> pd.Series:
    return _safe_numeric(frame, "excess_forward_return") - _transaction_cost_rate(
        transaction_cost_bps
    )


def _max_drawdown(returns: pd.Series) -> float | None:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return None
    equity = (1.0 + values).cumprod()
    drawdown = equity.div(equity.cummax()).sub(1.0)
    return float(drawdown.min())


def _max_group_concentration(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns or frame.empty:
        return None
    values = frame[column].fillna("UNKNOWN").astype(str)
    if values.empty:
        return None
    return float(values.value_counts(normalize=True).max())


def _bootstrap_mean_ci(
    values: pd.Series,
    *,
    reps: int,
    block_size: int,
    seed: int,
) -> tuple[float | None, float | None]:
    clean = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    if len(clean) == 0 or reps <= 0:
        return None, None
    if len(clean) == 1:
        single = float(clean[0])
        return single, single
    block = max(1, min(int(block_size), len(clean)))
    rng = np.random.default_rng(int(seed))
    means: list[float] = []
    for _ in range(int(reps)):
        sampled: list[float] = []
        while len(sampled) < len(clean):
            start = int(rng.integers(0, len(clean)))
            sampled.extend(float(clean[(start + offset) % len(clean)]) for offset in range(block))
        means.append(float(np.mean(sampled[: len(clean)])))
    return float(np.quantile(means, 0.05)), float(np.quantile(means, 0.95))


def _load_outcome_frame(path: Path, *, model_key: str) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
    frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    frame["model_key"] = model_key
    frame["risk_flag_list"] = frame.get("risk_flags_json", "[]").map(_json_list)
    frame["base_buyability_blocker"] = frame["risk_flag_list"].map(
        lambda values: bool(set(values) & set(BUYABILITY_BLOCKING_RISK_FLAGS))
    )
    frame["high_model_disagreement"] = frame["risk_flag_list"].map(
        lambda values: HIGH_DISAGREEMENT_FLAG in values
    )
    frame["buyability_priority_score"] = (
        _safe_numeric(frame, "expected_excess_return")
        * BUYABILITY_EXPECTED_RETURN_WEIGHT
        - _safe_numeric(frame, "uncertainty_score") * BUYABILITY_UNCERTAINTY_PENALTY
        - _safe_numeric(frame, "disagreement_score") * BUYABILITY_DISAGREEMENT_PENALTY
    )
    return frame


def _date_split(as_of_date: date, *, tune_end_date: date, holdout_start_date: date) -> str:
    if as_of_date <= tune_end_date:
        return "tune"
    if as_of_date >= holdout_start_date:
        return "holdout"
    return "gap"


def _policy_frame(base: pd.DataFrame, policy: PolicySpec) -> pd.DataFrame:
    frame = base.copy()
    adjusted_score = _safe_numeric(frame, "final_selection_value")
    high_disagreement = frame["high_model_disagreement"].fillna(False).astype(bool)
    if policy.high_disagreement_score_penalty:
        adjusted_score = adjusted_score - (
            high_disagreement.astype(float) * float(policy.high_disagreement_score_penalty)
        )
    frame["policy_adjusted_selection_value"] = adjusted_score.clip(lower=0.0, upper=100.0)

    blocked = frame["base_buyability_blocker"].fillna(False).astype(bool)
    if policy.block_high_disagreement:
        blocked = blocked | high_disagreement
    if policy.conditional_high_disagreement:
        conditional_escape = (
            high_disagreement
            & _safe_numeric(frame, "expected_excess_return").ge(policy.conditional_expected_floor)
            & _safe_numeric(frame, "uncertainty_score").le(policy.conditional_uncertainty_ceiling)
            & _safe_numeric(frame, "buyability_priority_score").ge(
                policy.conditional_priority_floor
            )
        )
        blocked = blocked | (high_disagreement & ~conditional_escape)
    frame["policy_blocked"] = blocked

    candidate_mask = (
        ~frame["policy_blocked"]
        & _safe_numeric(frame, "expected_excess_return").gt(BUYABILITY_MIN_EXPECTED_EXCESS_RETURN)
        & frame["policy_adjusted_selection_value"].ge(BUYABILITY_MIN_FINAL_SELECTION_VALUE)
        & _safe_numeric(frame, "buyability_priority_score").ge(BUYABILITY_MIN_PRIORITY_SCORE)
    )
    candidates = frame.loc[candidate_mask].copy()
    if candidates.empty:
        return candidates
    candidates["policy_id"] = policy.policy_id
    candidates["policy_description"] = policy.description
    return candidates


def _select_top_by_date(
    frame: pd.DataFrame,
    *,
    top_n: int,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    grouping = group_cols or ["as_of_date"]
    return (
        frame.sort_values(
            ["as_of_date", "buyability_priority_score", "symbol"],
            ascending=[True, False, True],
        )
        .groupby(grouping, as_index=False, group_keys=False)
        .head(int(top_n))
        .copy()
    )


def _positive_edge_concentration(
    frame: pd.DataFrame,
    *,
    transaction_cost_bps: float,
) -> tuple[float | None, str | None]:
    if frame.empty:
        return None, None
    date_edge = (
        frame.assign(
            _positive_edge=_net_realized_return(
                frame,
                transaction_cost_bps=transaction_cost_bps,
            ).clip(lower=0.0)
        )
        .groupby("as_of_date", sort=True)["_positive_edge"]
        .sum()
    )
    total = float(date_edge.sum())
    if total <= 0.0 or date_edge.empty:
        return None, None
    max_date = date_edge.idxmax()
    return float(date_edge.loc[max_date] / total), pd.Timestamp(max_date).date().isoformat()


def _positive_edge_concentration_from_returns(
    frame: pd.DataFrame,
    *,
    date_column: str,
    return_column: str,
) -> tuple[float | None, str | None]:
    if frame.empty or date_column not in frame.columns or return_column not in frame.columns:
        return None, None
    working = frame[[date_column, return_column]].copy()
    working["_positive_edge"] = pd.to_numeric(
        working[return_column],
        errors="coerce",
    ).clip(lower=0.0)
    date_edge = working.groupby(date_column, sort=True)["_positive_edge"].sum()
    total = float(date_edge.sum())
    if total <= 0.0 or date_edge.empty:
        return None, None
    max_date = date_edge.idxmax()
    return float(date_edge.loc[max_date] / total), pd.Timestamp(max_date).date().isoformat()


def _metric_row(
    frame: pd.DataFrame,
    *,
    policy_id: str,
    split_name: str,
    top_n: int,
    transaction_cost_bps: float = 0.0,
) -> dict[str, object]:
    if frame.empty:
        return {
            "policy_id": policy_id,
            "split": split_name,
            "top_n": int(top_n),
            "n": 0,
            "dates": 0,
            "avg": None,
            "median": None,
            "hit": None,
            "p25": None,
            "p10": None,
            "p5": None,
            "avg_net": None,
            "median_net": None,
            "p10_net": None,
            "p5_net": None,
            "hit_net": None,
            "blocker_rate": None,
            "high_disagreement_rate": None,
            "avg_pred": None,
            "max_positive_edge_share": None,
            "top_positive_edge_date": None,
        }
    realized = _safe_numeric(frame, "excess_forward_return")
    net_realized = _net_realized_return(frame, transaction_cost_bps=transaction_cost_bps)
    max_edge_share, top_edge_date = _positive_edge_concentration(
        frame,
        transaction_cost_bps=transaction_cost_bps,
    )
    return {
        "policy_id": policy_id,
        "split": split_name,
        "top_n": int(top_n),
        "n": int(len(frame)),
        "dates": int(frame["as_of_date"].nunique()),
        "avg": float(realized.mean()),
        "median": float(realized.median()),
        "hit": float(realized.gt(0.0).mean()),
        "p25": float(realized.quantile(0.25)),
        "p10": float(realized.quantile(0.10)),
        "p5": float(realized.quantile(0.05)),
        "avg_net": float(net_realized.mean()),
        "median_net": float(net_realized.median()),
        "p10_net": float(net_realized.quantile(0.10)),
        "p5_net": float(net_realized.quantile(0.05)),
        "hit_net": float(net_realized.gt(0.0).mean()),
        "blocker_rate": float(frame["policy_blocked"].mean()),
        "high_disagreement_rate": float(frame["high_model_disagreement"].mean()),
        "avg_pred": float(_safe_numeric(frame, "expected_excess_return").mean()),
        "max_positive_edge_share": max_edge_share,
        "top_positive_edge_date": top_edge_date,
    }


def _summarise(
    selected: pd.DataFrame,
    *,
    top_ns: Iterable[int],
    transaction_cost_bps: float = 0.0,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for policy_id, policy_frame in selected.groupby("policy_id", sort=False):
        for split_name in ("all", "tune", "holdout"):
            split_frame = policy_frame if split_name == "all" else policy_frame.loc[
                policy_frame["split"].eq(split_name)
            ]
            for top_n in top_ns:
                top = _select_top_by_date(split_frame, top_n=int(top_n))
                rows.append(
                    _metric_row(
                        top,
                        policy_id=str(policy_id),
                        split_name=split_name,
                        top_n=int(top_n),
                        transaction_cost_bps=transaction_cost_bps,
                    )
                )
    return pd.DataFrame(rows)


def _top1_wide(selected: pd.DataFrame) -> pd.DataFrame:
    top1 = _select_top_by_date(selected, top_n=1, group_cols=["policy_id", "as_of_date"])
    if top1.empty:
        return pd.DataFrame()
    return top1.pivot_table(
        index=["split", "as_of_date"],
        columns="policy_id",
        values="excess_forward_return",
        aggfunc="first",
    ).reset_index()


def _outer_fold_summary(
    selected: pd.DataFrame,
    *,
    top_ns: Iterable[int],
    outer_fold_size: int,
    transaction_cost_bps: float = 0.0,
) -> pd.DataFrame:
    if selected.empty or outer_fold_size <= 0:
        return pd.DataFrame()
    ordered_dates = sorted(pd.Timestamp(value).date() for value in selected["as_of_date"].unique())
    rows: list[dict[str, object]] = []
    for fold_index, start in enumerate(range(0, len(ordered_dates), int(outer_fold_size)), start=1):
        fold_dates = set(ordered_dates[start : start + int(outer_fold_size)])
        if not fold_dates:
            continue
        fold_name = f"outer_fold_{fold_index:02d}"
        prior_dates = ordered_dates[:start]
        fold_frame = selected.loc[selected["as_of_date"].isin(fold_dates)]
        for policy_id, policy_frame in fold_frame.groupby("policy_id", sort=False):
            for top_n in top_ns:
                top = _select_top_by_date(policy_frame, top_n=int(top_n))
                row = _metric_row(
                    top,
                    policy_id=str(policy_id),
                    split_name=fold_name,
                    top_n=int(top_n),
                    transaction_cost_bps=transaction_cost_bps,
                )
                row["fold_start_date"] = min(fold_dates).isoformat()
                row["fold_end_date"] = max(fold_dates).isoformat()
                row["prior_date_count"] = len(prior_dates)
                rows.append(row)
    return pd.DataFrame(rows)


def _portfolio_by_date(
    selected: pd.DataFrame,
    *,
    top_ns: Iterable[int],
    transaction_cost_bps: float,
) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for (policy_id, split_name, as_of_date), group in selected.groupby(
        ["policy_id", "split", "as_of_date"],
        sort=True,
        dropna=False,
    ):
        for top_n in top_ns:
            top = _select_top_by_date(group, top_n=int(top_n))
            if top.empty:
                continue
            gross = _safe_numeric(top, "excess_forward_return")
            net = _net_realized_return(top, transaction_cost_bps=transaction_cost_bps)
            rows.append(
                {
                    "policy_id": str(policy_id),
                    "split": str(split_name),
                    "as_of_date": pd.Timestamp(as_of_date).date().isoformat(),
                    "top_n": int(top_n),
                    "n_names": int(len(top)),
                    "portfolio_gross_excess_return": float(gross.mean()),
                    "portfolio_net_excess_return": float(net.mean()),
                    "portfolio_hit_rate": float(net.gt(0.0).mean()),
                    "market_concentration": _max_group_concentration(top, "market"),
                    "sector_concentration": _max_group_concentration(top, "sector"),
                }
            )
    return pd.DataFrame(rows)


def _portfolio_summary(
    portfolio_by_date: pd.DataFrame,
    *,
    bootstrap_reps: int,
    bootstrap_block_size: int,
    bootstrap_seed: int,
) -> pd.DataFrame:
    if portfolio_by_date.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for (policy_id, split_name, top_n), group in portfolio_by_date.groupby(
        ["policy_id", "split", "top_n"],
        sort=True,
        dropna=False,
    ):
        ordered = group.sort_values("as_of_date")
        gross = pd.to_numeric(ordered["portfolio_gross_excess_return"], errors="coerce")
        net = pd.to_numeric(ordered["portfolio_net_excess_return"], errors="coerce")
        ci_low, ci_high = _bootstrap_mean_ci(
            net,
            reps=int(bootstrap_reps),
            block_size=int(bootstrap_block_size),
            seed=int(bootstrap_seed),
        )
        max_edge_share, top_edge_date = _positive_edge_concentration_from_returns(
            ordered,
            date_column="as_of_date",
            return_column="portfolio_net_excess_return",
        )
        rows.append(
            {
                "policy_id": str(policy_id),
                "split": str(split_name),
                "top_n": int(top_n),
                "dates": int(ordered["as_of_date"].nunique()),
                "avg_gross": float(gross.mean()),
                "avg_net": float(net.mean()),
                "median_net": float(net.median()),
                "p25_net": float(net.quantile(0.25)),
                "p10_net": float(net.quantile(0.10)),
                "p5_net": float(net.quantile(0.05)),
                "hit_net": float(net.gt(0.0).mean()),
                "cumulative_net": float((1.0 + net).prod() - 1.0),
                "max_drawdown_net": _max_drawdown(net),
                "mean_net_ci05": ci_low,
                "mean_net_ci95": ci_high,
                "avg_names": float(pd.to_numeric(ordered["n_names"], errors="coerce").mean()),
                "max_positive_edge_share": max_edge_share,
                "top_positive_edge_date": top_edge_date,
                "avg_market_concentration": None
                if "market_concentration" not in ordered.columns
                else float(pd.to_numeric(ordered["market_concentration"], errors="coerce").mean()),
                "max_market_concentration": None
                if "market_concentration" not in ordered.columns
                else float(pd.to_numeric(ordered["market_concentration"], errors="coerce").max()),
                "avg_sector_concentration": None
                if "sector_concentration" not in ordered.columns
                else float(pd.to_numeric(ordered["sector_concentration"], errors="coerce").mean()),
                "max_sector_concentration": None
                if "sector_concentration" not in ordered.columns
                else float(pd.to_numeric(ordered["sector_concentration"], errors="coerce").max()),
            }
        )
    return pd.DataFrame(rows)


def _gate_row(
    portfolio_summary: pd.DataFrame,
    *,
    policy_id: str,
    split_name: str,
    top_n: int,
) -> pd.Series | None:
    if portfolio_summary.empty:
        return None
    rows = portfolio_summary.loc[
        portfolio_summary["policy_id"].eq(policy_id)
        & portfolio_summary["split"].eq(split_name)
        & portfolio_summary["top_n"].eq(int(top_n))
    ]
    if rows.empty:
        return None
    return rows.iloc[0]


def _field_float(row: pd.Series, field: str) -> float | None:
    value = row.get(field)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _top1_diagnostic_warnings(
    portfolio_summary: pd.DataFrame,
    *,
    policy_id: str,
    baseline_policy_id: str,
) -> list[str]:
    candidate = _gate_row(
        portfolio_summary,
        policy_id=policy_id,
        split_name="holdout",
        top_n=1,
    )
    baseline = _gate_row(
        portfolio_summary,
        policy_id=baseline_policy_id,
        split_name="holdout",
        top_n=1,
    )
    if candidate is None or baseline is None:
        return ["top1_diagnostic_missing"]
    warnings: list[str] = []
    candidate_avg = _field_float(candidate, "avg_net")
    baseline_avg = _field_float(baseline, "avg_net")
    candidate_hit = _field_float(candidate, "hit_net")
    baseline_hit = _field_float(baseline, "hit_net")
    edge_share = _field_float(candidate, "max_positive_edge_share")
    if candidate_avg is not None and candidate_avg < 0.0:
        warnings.append("top1_negative")
    if (
        candidate_avg is not None
        and baseline_avg is not None
        and candidate_avg < baseline_avg
    ):
        warnings.append("top1_avg_net_below_baseline")
    if (
        candidate_hit is not None
        and baseline_hit is not None
        and candidate_hit < baseline_hit
    ):
        warnings.append("top1_hit_net_below_baseline")
    if edge_share is not None and edge_share > 0.40:
        warnings.append("top1_concentrated")
    return warnings


def _passes_gate(
    portfolio_summary: pd.DataFrame,
    *,
    policy_id: str,
    baseline_policy_id: str,
    min_coverage_ratio: float,
    min_median_ratio: float,
    max_high_disagreement_rate: float | None = None,
    max_single_date_edge_share: float,
    safety_fail_reasons: list[str] | None = None,
) -> GateResult:
    del max_high_disagreement_rate  # Top5 basket gates no longer use top1 disagreement rate.
    thresholds = BasketGateThresholds(
        min_coverage_ratio=float(min_coverage_ratio),
        min_median_ratio=float(min_median_ratio),
        max_single_date_edge_share=float(max_single_date_edge_share),
    )
    fail_reasons = list(safety_fail_reasons or [])
    warning_reasons: list[str] = []

    candidate_top5 = _gate_row(
        portfolio_summary,
        policy_id=policy_id,
        split_name="holdout",
        top_n=5,
    )
    baseline_top5 = _gate_row(
        portfolio_summary,
        policy_id=baseline_policy_id,
        split_name="holdout",
        top_n=5,
    )
    if candidate_top5 is None or baseline_top5 is None:
        fail_reasons.append("missing_top5_holdout_metrics")
    else:
        c_dates = int(candidate_top5.get("dates") or 0)
        b_dates = int(baseline_top5.get("dates") or 0)
        if c_dates < b_dates * thresholds.min_coverage_ratio:
            fail_reasons.append("top5_holdout_coverage_below_floor")

        checks = (
            ("avg_net", thresholds.top5_avg_net_tolerance, "top5_holdout_avg_net_below_tolerance"),
            ("p10_net", thresholds.top5_p10_net_tolerance, "top5_holdout_p10_net_below_tolerance"),
            ("hit_net", thresholds.top5_hit_net_tolerance, "top5_holdout_hit_net_below_tolerance"),
            (
                "max_drawdown_net",
                thresholds.top5_max_drawdown_net_tolerance,
                "top5_holdout_max_drawdown_net_below_tolerance",
            ),
        )
        for field, tolerance, reason in checks:
            candidate_value = _field_float(candidate_top5, field)
            baseline_value = _field_float(baseline_top5, field)
            if (
                candidate_value is None
                or baseline_value is None
                or candidate_value < baseline_value + tolerance
            ):
                fail_reasons.append(reason)

        candidate_median = _field_float(candidate_top5, "median_net")
        baseline_median = _field_float(baseline_top5, "median_net")
        if candidate_median is None or baseline_median is None:
            fail_reasons.append("top5_holdout_median_net_missing")
        else:
            median_floor = (
                baseline_median * thresholds.min_median_ratio
                if baseline_median > 0.0
                else baseline_median + thresholds.top5_median_net_tolerance
            )
            if candidate_median < median_floor:
                fail_reasons.append("top5_holdout_median_net_below_floor")

        edge_share = _field_float(candidate_top5, "max_positive_edge_share")
        if edge_share is None:
            warning_reasons.append("top5_positive_edge_concentration_missing")
        elif edge_share > thresholds.max_single_date_edge_share:
            fail_reasons.append("top5_positive_edge_concentration_above_floor")

        sector_concentration = _field_float(candidate_top5, "max_sector_concentration")
        if sector_concentration is None:
            warning_reasons.append("top5_sector_concentration_missing")
        elif sector_concentration > thresholds.top5_max_sector_concentration:
            fail_reasons.append("top5_sector_concentration_above_floor")

    candidate_top3 = _gate_row(
        portfolio_summary,
        policy_id=policy_id,
        split_name="holdout",
        top_n=3,
    )
    baseline_top3 = _gate_row(
        portfolio_summary,
        policy_id=baseline_policy_id,
        split_name="holdout",
        top_n=3,
    )
    if candidate_top3 is None or baseline_top3 is None:
        warning_reasons.append("missing_top3_holdout_metrics")
    else:
        for field, tolerance, reason in (
            ("avg_net", thresholds.top3_avg_net_tolerance, "top3_holdout_avg_net_below_tolerance"),
            ("p10_net", thresholds.top3_p10_net_tolerance, "top3_holdout_p10_net_below_tolerance"),
            ("hit_net", thresholds.top3_hit_net_tolerance, "top3_holdout_hit_net_below_tolerance"),
        ):
            candidate_value = _field_float(candidate_top3, field)
            baseline_value = _field_float(baseline_top3, field)
            if (
                candidate_value is None
                or baseline_value is None
                or candidate_value < baseline_value + tolerance
            ):
                warning_reasons.append(reason)

    warning_reasons.extend(
        _top1_diagnostic_warnings(
            portfolio_summary,
            policy_id=policy_id,
            baseline_policy_id=baseline_policy_id,
        )
    )
    warning_reasons = sorted(dict.fromkeys(warning_reasons))
    fail_reasons = sorted(dict.fromkeys(fail_reasons))

    if fail_reasons:
        decision = "stop_lane"
    elif warning_reasons:
        decision = "needs_review"
    else:
        decision = "pass_to_ltr"
    return GateResult(
        policy_id=policy_id,
        gate_decision=decision,
        passed=decision == "pass_to_ltr",
        fail_reasons=fail_reasons,
        warning_reasons=warning_reasons,
    )


def _resolve_policy_specs(policy_set: str) -> tuple[PolicySpec, ...]:
    try:
        return POLICY_SETS[policy_set]
    except KeyError as exc:
        raise ValueError(f"Unknown policy set: {policy_set}") from exc


def _resolve_outcome_paths(args: argparse.Namespace) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for key, path in args.outcome:
        paths[key] = path
    if args.active_outcomes is not None:
        paths["active"] = args.active_outcomes
    if args.practical_outcomes is not None:
        paths.setdefault("practical_v1", args.practical_outcomes)
        paths.setdefault("practical", args.practical_outcomes)
    if "practical_v1" in paths:
        paths.setdefault("practical", paths["practical_v1"])
    return paths


def run(args: argparse.Namespace) -> int:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    policy_specs = _resolve_policy_specs(args.policy_set)
    candidate_policy_count = len(
        [policy for policy in policy_specs if policy.policy_id != args.baseline_policy_id]
    )
    if (
        args.max_candidate_policy_count >= 0
        and candidate_policy_count > args.max_candidate_policy_count
    ):
        raise RuntimeError(
            "Candidate policy count exceeds guardrail: "
            f"{candidate_policy_count} > {args.max_candidate_policy_count}"
        )
    outcome_paths = _resolve_outcome_paths(args)
    required_keys = {policy.model_key for policy in policy_specs}
    missing_keys = sorted(key for key in required_keys if key not in outcome_paths)
    if missing_keys:
        raise RuntimeError(
            "Missing outcome path(s) for policy set "
            f"{args.policy_set}: {', '.join(missing_keys)}"
        )
    frames_by_key = {
        key: _load_outcome_frame(outcome_paths[key].resolve(), model_key=key)
        for key in sorted(required_keys)
    }
    selected_frames: list[pd.DataFrame] = []
    for policy in policy_specs:
        candidates = _policy_frame(frames_by_key[policy.model_key], policy)
        if candidates.empty:
            continue
        candidates["split"] = candidates["as_of_date"].map(
            lambda value: _date_split(
                value,
                tune_end_date=args.tune_end_date,
                holdout_start_date=args.holdout_start_date,
            )
        )
        candidates = candidates.loc[candidates["split"].ne("gap")].copy()
        selected_frames.append(candidates)

    selected = (
        pd.concat(selected_frames, ignore_index=True, sort=False)
        if selected_frames
        else pd.DataFrame()
    )
    selected_path = output_dir / "policy_candidate_rows.csv"
    selected.to_csv(selected_path, index=False)

    summary = (
        _summarise(
            selected,
            top_ns=args.top_ns,
            transaction_cost_bps=args.transaction_cost_bps,
        )
        if not selected.empty
        else pd.DataFrame()
    )
    summary_path = output_dir / "policy_split_summary.csv"
    summary.to_csv(summary_path, index=False)

    wide = _top1_wide(selected)
    wide_path = output_dir / "policy_top1_by_date.csv"
    wide.to_csv(wide_path, index=False)

    fold_summary = _outer_fold_summary(
        selected,
        top_ns=args.top_ns,
        outer_fold_size=args.outer_fold_size,
        transaction_cost_bps=args.transaction_cost_bps,
    )
    fold_summary_path = output_dir / "policy_outer_fold_summary.csv"
    fold_summary.to_csv(fold_summary_path, index=False)

    portfolio_daily = _portfolio_by_date(
        selected,
        top_ns=args.top_ns,
        transaction_cost_bps=args.transaction_cost_bps,
    )
    portfolio_daily_path = output_dir / "policy_portfolio_by_date.csv"
    portfolio_daily.to_csv(portfolio_daily_path, index=False)
    portfolio_summary = _portfolio_summary(
        portfolio_daily,
        bootstrap_reps=args.bootstrap_reps,
        bootstrap_block_size=args.bootstrap_block_size,
        bootstrap_seed=args.bootstrap_seed,
    )
    portfolio_summary_path = output_dir / "policy_portfolio_summary.csv"
    portfolio_summary.to_csv(portfolio_summary_path, index=False)

    repo_sha = _git_sha()
    server_sha = os.environ.get("SERVER_SHA") or repo_sha
    safety_fail_reasons: list[str] = []
    if not repo_sha:
        safety_fail_reasons.append("missing_repo_sha")
    if not server_sha:
        safety_fail_reasons.append("missing_server_sha")

    gates: list[dict[str, object]] = []
    for policy in policy_specs:
        if policy.policy_id == args.baseline_policy_id:
            continue
        gate_result = _passes_gate(
            portfolio_summary,
            policy_id=policy.policy_id,
            baseline_policy_id=args.baseline_policy_id,
            min_coverage_ratio=args.min_coverage_ratio,
            min_median_ratio=args.min_median_ratio,
            max_high_disagreement_rate=args.max_high_disagreement_rate,
            max_single_date_edge_share=args.max_single_date_edge_share,
            safety_fail_reasons=safety_fail_reasons,
        )
        gates.append(gate_result.as_dict())
    manifest = {
        "kind": "d5_practical_policy_split_evaluation",
        "outcomes": {key: str(path.resolve()) for key, path in sorted(outcome_paths.items())},
        "tune_end_date": args.tune_end_date.isoformat(),
        "holdout_start_date": args.holdout_start_date.isoformat(),
        "outer_fold_size": args.outer_fold_size,
        "contaminated_windows": args.contaminated_window,
        "baseline_policy_id": args.baseline_policy_id,
        "policy_set": args.policy_set,
        "candidate_policy_count": candidate_policy_count,
        "max_candidate_policy_count": args.max_candidate_policy_count,
        "policies": [asdict(policy) for policy in policy_specs],
        "gates": gates,
        "outputs": [
            str(summary_path),
            str(wide_path),
            str(fold_summary_path),
            str(selected_path),
            str(portfolio_daily_path),
            str(portfolio_summary_path),
        ],
        "transaction_cost_bps": args.transaction_cost_bps,
        "bootstrap_reps": args.bootstrap_reps,
        "bootstrap_block_size": args.bootstrap_block_size,
        "bootstrap_seed": args.bootstrap_seed,
        "gate_thresholds": asdict(
            BasketGateThresholds(
                min_coverage_ratio=args.min_coverage_ratio,
                min_median_ratio=args.min_median_ratio,
                max_single_date_edge_share=args.max_single_date_edge_share,
            )
        ),
        "repo_sha": repo_sha,
        "server_sha": server_sha,
        "promotion_disabled": True,
        "artifact_only": True,
        "read_only": True,
        "db_read_only": True,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")
    print(f"top1_by_date={wide_path}")
    print(f"outer_folds={fold_summary_path}")
    print(f"portfolio_by_date={portfolio_daily_path}")
    print(f"portfolio_summary={portfolio_summary_path}")
    print(f"manifest={manifest_path}")
    print(json.dumps({"gates": gates}, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate a small, predeclared D5 practical policy set across tune/holdout splits. "
            "This is artifact-only: it reads walk-forward outcome CSVs and never touches DB state."
        )
    )
    parser.add_argument("--active-outcomes", type=Path)
    parser.add_argument("--practical-outcomes", type=Path)
    parser.add_argument(
        "--outcome",
        action="append",
        default=[],
        type=_parse_key_path,
        metavar="KEY=PATH",
        help=(
            "Outcome CSV keyed by model/policy source. Required keys depend on --policy-set; "
            "for current use active=..., practical_v1=..., practical_v2=..."
        ),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--policy-set",
        choices=sorted(POLICY_SETS),
        default="abc",
        help="Predeclared policy set to evaluate. Use 'current' for baseline-only reruns.",
    )
    parser.add_argument("--tune-end-date", type=_parse_date, default=date(2026, 3, 31))
    parser.add_argument("--holdout-start-date", type=_parse_date, default=date(2026, 4, 1))
    parser.add_argument("--top-ns", nargs="+", type=int, default=[1, 3, 5])
    parser.add_argument("--baseline-policy-id", default="active_current")
    parser.add_argument("--outer-fold-size", type=int, default=0)
    parser.add_argument(
        "--contaminated-window",
        action="append",
        default=[],
        type=_parse_window,
        metavar="NAME:START:END",
    )
    parser.add_argument("--min-coverage-ratio", type=float, default=0.70)
    parser.add_argument("--min-median-ratio", type=float, default=0.80)
    parser.add_argument("--max-high-disagreement-rate", type=float, default=0.10)
    parser.add_argument("--max-single-date-edge-share", type=float, default=0.40)
    parser.add_argument(
        "--transaction-cost-bps",
        type=float,
        default=DEFAULT_TRANSACTION_COST_BPS,
        help=(
            "Round-trip transaction cost deducted from each selected name when "
            "computing net metrics."
        ),
    )
    parser.add_argument(
        "--max-candidate-policy-count",
        type=int,
        default=2,
        help="Fail fast if more non-baseline policies are evaluated; use -1 to disable.",
    )
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    parser.add_argument("--bootstrap-block-size", type=int, default=5)
    parser.add_argument("--bootstrap-seed", type=int, default=20260427)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
