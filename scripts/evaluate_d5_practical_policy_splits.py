# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

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


POLICY_SPECS: tuple[PolicySpec, ...] = (
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
        description="Keep v1 practical target; apply a fixed -30 score penalty to high disagreement.",
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


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


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


def _select_top_by_date(frame: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return (
        frame.sort_values(
            ["as_of_date", "buyability_priority_score", "symbol"],
            ascending=[True, False, True],
        )
        .groupby("as_of_date", as_index=False, group_keys=False)
        .head(int(top_n))
        .copy()
    )


def _metric_row(frame: pd.DataFrame, *, policy_id: str, split_name: str, top_n: int) -> dict[str, object]:
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
            "blocker_rate": None,
            "high_disagreement_rate": None,
            "avg_pred": None,
        }
    realized = _safe_numeric(frame, "excess_forward_return")
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
        "blocker_rate": float(frame["policy_blocked"].mean()),
        "high_disagreement_rate": float(frame["high_model_disagreement"].mean()),
        "avg_pred": float(_safe_numeric(frame, "expected_excess_return").mean()),
    }


def _summarise(selected: pd.DataFrame, *, top_ns: Iterable[int]) -> pd.DataFrame:
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
                    )
                )
    return pd.DataFrame(rows)


def _top1_wide(selected: pd.DataFrame) -> pd.DataFrame:
    top1 = _select_top_by_date(selected, top_n=1)
    if top1.empty:
        return pd.DataFrame()
    return top1.pivot_table(
        index=["split", "as_of_date"],
        columns="policy_id",
        values="excess_forward_return",
        aggfunc="first",
    ).reset_index()


def _passes_gate(
    summary: pd.DataFrame,
    *,
    policy_id: str,
    baseline_policy_id: str,
    min_coverage_ratio: float,
    min_median_ratio: float,
    max_high_disagreement_rate: float,
) -> tuple[bool, list[str]]:
    focus = summary.loc[summary["top_n"].eq(1)]
    holdout = focus.loc[focus["split"].eq("holdout")]
    tune = focus.loc[focus["split"].eq("tune")]
    candidate_h = holdout.loc[holdout["policy_id"].eq(policy_id)]
    baseline_h = holdout.loc[holdout["policy_id"].eq(baseline_policy_id)]
    candidate_t = tune.loc[tune["policy_id"].eq(policy_id)]
    baseline_t = tune.loc[tune["policy_id"].eq(baseline_policy_id)]
    reasons: list[str] = []
    if candidate_h.empty or baseline_h.empty:
        return False, ["missing_holdout_metrics"]
    c = candidate_h.iloc[0]
    b = baseline_h.iloc[0]
    if int(c["dates"] or 0) < int(b["dates"] or 0) * min_coverage_ratio:
        reasons.append("holdout_coverage_below_floor")
    if pd.isna(c["avg"]) or pd.isna(b["avg"]) or float(c["avg"]) < float(b["avg"]):
        reasons.append("holdout_avg_below_active")
    baseline_median = float(b["median"]) if pd.notna(b["median"]) else 0.0
    candidate_median = float(c["median"]) if pd.notna(c["median"]) else float("-inf")
    median_floor = baseline_median * min_median_ratio if baseline_median > 0 else baseline_median
    if candidate_median < median_floor:
        reasons.append("holdout_median_below_floor")
    if pd.isna(c["p10"]) or pd.isna(b["p10"]) or float(c["p10"]) < float(b["p10"]):
        reasons.append("holdout_p10_worse_than_active")
    if pd.isna(c["hit"]) or pd.isna(b["hit"]) or float(c["hit"]) < float(b["hit"]):
        reasons.append("holdout_hit_below_active")
    if float(c["high_disagreement_rate"] or 0.0) > max_high_disagreement_rate:
        reasons.append("holdout_high_disagreement_above_floor")
    if float(c["blocker_rate"] or 0.0) > 0.0:
        reasons.append("holdout_blocker_not_zero")

    if not candidate_t.empty and not baseline_t.empty:
        ct = candidate_t.iloc[0]
        bt = baseline_t.iloc[0]
        if pd.notna(ct["p10"]) and pd.notna(bt["p10"]) and float(ct["p10"]) < float(bt["p10"]):
            reasons.append("tune_p10_worse_than_active")
    return not reasons, reasons


def run(args: argparse.Namespace) -> int:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    frames_by_key = {
        "active": _load_outcome_frame(args.active_outcomes.resolve(), model_key="active"),
        "practical": _load_outcome_frame(args.practical_outcomes.resolve(), model_key="practical"),
    }
    selected_frames: list[pd.DataFrame] = []
    for policy in POLICY_SPECS:
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

    selected = pd.concat(selected_frames, ignore_index=True, sort=False) if selected_frames else pd.DataFrame()
    selected_path = output_dir / "policy_candidate_rows.csv"
    selected.to_csv(selected_path, index=False)

    summary = _summarise(selected, top_ns=args.top_ns) if not selected.empty else pd.DataFrame()
    summary_path = output_dir / "policy_split_summary.csv"
    summary.to_csv(summary_path, index=False)

    wide = _top1_wide(selected)
    wide_path = output_dir / "policy_top1_by_date.csv"
    wide.to_csv(wide_path, index=False)

    gates: list[dict[str, object]] = []
    for policy in POLICY_SPECS:
        if policy.policy_id == args.baseline_policy_id:
            continue
        passed, reasons = _passes_gate(
            summary,
            policy_id=policy.policy_id,
            baseline_policy_id=args.baseline_policy_id,
            min_coverage_ratio=args.min_coverage_ratio,
            min_median_ratio=args.min_median_ratio,
            max_high_disagreement_rate=args.max_high_disagreement_rate,
        )
        gates.append(
            {
                "policy_id": policy.policy_id,
                "passed": passed,
                "fail_reasons": reasons,
            }
        )
    manifest = {
        "kind": "d5_practical_policy_split_evaluation",
        "active_outcomes": str(args.active_outcomes.resolve()),
        "practical_outcomes": str(args.practical_outcomes.resolve()),
        "tune_end_date": args.tune_end_date.isoformat(),
        "holdout_start_date": args.holdout_start_date.isoformat(),
        "baseline_policy_id": args.baseline_policy_id,
        "policies": [asdict(policy) for policy in POLICY_SPECS],
        "gates": gates,
        "outputs": [str(summary_path), str(wide_path), str(selected_path)],
        "promotion_disabled": True,
        "db_read_only": True,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")
    print(f"top1_by_date={wide_path}")
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
    parser.add_argument("--active-outcomes", required=True, type=Path)
    parser.add_argument("--practical-outcomes", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--tune-end-date", type=_parse_date, default=date(2026, 3, 31))
    parser.add_argument("--holdout-start-date", type=_parse_date, default=date(2026, 4, 1))
    parser.add_argument("--top-ns", nargs="+", type=int, default=[1, 3, 5])
    parser.add_argument("--baseline-policy-id", default="active_current")
    parser.add_argument("--min-coverage-ratio", type=float, default=0.70)
    parser.add_argument("--min-median-ratio", type=float, default=0.80)
    parser.add_argument("--max-high-disagreement-rate", type=float, default=0.10)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
