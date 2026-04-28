# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True, slots=True)
class GateSpec:
    lookback_days: int
    min_history: int
    min_trailing_mean: float
    min_trailing_hit: float
    max_markets: int

    @property
    def gate_id(self) -> str:
        return (
            f"lb{self.lookback_days}_mh{self.min_history}_"
            f"mean{self.min_trailing_mean:+.4f}_hit{self.min_trailing_hit:.2f}_"
            f"m{self.max_markets}"
        )


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


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


def load_query_group_top5(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {
        "as_of_date",
        "market",
        "top_n",
        "n_names",
        "avg_stable_utility",
        "avg_excess_return",
        "hit_stable_utility",
        "symbols",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{path} missing required columns: {', '.join(missing)}")
    result = frame.loc[pd.to_numeric(frame["top_n"], errors="coerce").eq(5)].copy()
    result["as_of_date"] = pd.to_datetime(result["as_of_date"]).dt.date
    for column in ("avg_stable_utility", "avg_excess_return", "hit_stable_utility"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["market"] = result["market"].fillna("UNKNOWN").astype(str)
    return result.sort_values(["as_of_date", "market"]).reset_index(drop=True)


def _trailing_stats(history: pd.DataFrame, *, lookback_days: int) -> tuple[int, float, float]:
    if history.empty:
        return 0, 0.0, 0.0
    tail = history.tail(int(lookback_days))
    returns = pd.to_numeric(tail["avg_stable_utility"], errors="coerce").dropna()
    if returns.empty:
        return 0, 0.0, 0.0
    return int(len(returns)), float(returns.mean()), float(returns.gt(0.0).mean())


def evaluate_gate(frame: pd.DataFrame, spec: GateSpec) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    market_history: dict[str, pd.DataFrame] = {}
    for as_of_date, day_group in frame.groupby("as_of_date", sort=True):
        candidates: list[dict[str, object]] = []
        for _, row in day_group.iterrows():
            market = str(row["market"])
            history = market_history.get(market, pd.DataFrame())
            count, trailing_mean, trailing_hit = _trailing_stats(
                history,
                lookback_days=spec.lookback_days,
            )
            passes = (
                count >= spec.min_history
                and trailing_mean >= spec.min_trailing_mean
                and trailing_hit >= spec.min_trailing_hit
            )
            candidates.append(
                {
                    "as_of_date": as_of_date,
                    "market": market,
                    "passed_gate": bool(passes),
                    "history_count": count,
                    "trailing_mean": trailing_mean,
                    "trailing_hit": trailing_hit,
                    "avg_stable_utility": float(row["avg_stable_utility"]),
                    "avg_excess_return": float(row["avg_excess_return"]),
                    "hit_stable_utility": float(row["hit_stable_utility"]),
                    "symbols": str(row["symbols"]),
                }
            )
        selected = sorted(
            [candidate for candidate in candidates if candidate["passed_gate"]],
            key=lambda item: (float(item["trailing_mean"]), float(item["trailing_hit"])),
            reverse=True,
        )[: spec.max_markets]
        if selected:
            stable_values = [float(item["avg_stable_utility"]) for item in selected]
            excess_values = [float(item["avg_excess_return"]) for item in selected]
            symbols = ",".join(str(item["symbols"]) for item in selected)
            rows.append(
                {
                    "gate_id": spec.gate_id,
                    "as_of_date": as_of_date.isoformat(),
                    "selected_market_count": len(selected),
                    "selected_markets": ",".join(str(item["market"]) for item in selected),
                    "n_names": int(sum(len(str(item["symbols"]).split(",")) for item in selected)),
                    "portfolio_stable_utility": float(pd.Series(stable_values).mean()),
                    "portfolio_excess_return": float(pd.Series(excess_values).mean()),
                    "symbols": symbols,
                }
            )
        else:
            rows.append(
                {
                    "gate_id": spec.gate_id,
                    "as_of_date": as_of_date.isoformat(),
                    "selected_market_count": 0,
                    "selected_markets": "CASH",
                    "n_names": 0,
                    "portfolio_stable_utility": 0.0,
                    "portfolio_excess_return": 0.0,
                    "symbols": "",
                }
            )
        for _, row in day_group.iterrows():
            market = str(row["market"])
            market_history[market] = pd.concat(
                [market_history.get(market, pd.DataFrame()), pd.DataFrame([row])],
                ignore_index=True,
                sort=False,
            )
    daily = pd.DataFrame(rows)
    details = pd.DataFrame(
        [item for as_of_date, day in frame.groupby("as_of_date") for item in []]
    )
    del details  # Details are intentionally omitted for compact artifact size.
    return daily, summarize_daily(daily)


def summarize_daily(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    returns = pd.to_numeric(daily["portfolio_stable_utility"], errors="coerce").fillna(0.0)
    active = daily.loc[pd.to_numeric(daily["selected_market_count"], errors="coerce").gt(0)]
    active_returns = pd.to_numeric(active["portfolio_stable_utility"], errors="coerce")
    cumulative = (1.0 + returns).cumprod()
    drawdown = cumulative / cumulative.cummax() - 1.0
    return pd.DataFrame(
        [
            {
                "gate_id": str(daily["gate_id"].iloc[0]),
                "dates": int(daily["as_of_date"].nunique()),
                "active_dates": int(len(active)),
                "coverage": float(len(active) / len(daily)) if len(daily) else 0.0,
                "avg_daily_stable_utility_cash0": float(returns.mean()),
                "median_daily_stable_utility_cash0": float(returns.median()),
                "p10_daily_stable_utility_cash0": float(returns.quantile(0.10)),
                "hit_daily_stable_utility_cash0": float(returns.gt(0.0).mean()),
                "avg_active_stable_utility": None
                if active_returns.empty
                else float(active_returns.mean()),
                "median_active_stable_utility": None
                if active_returns.empty
                else float(active_returns.median()),
                "hit_active_stable_utility": None
                if active_returns.empty
                else float(active_returns.gt(0.0).mean()),
                "cumulative_stable_utility_cash0": float(cumulative.iloc[-1] - 1.0),
                "max_drawdown_stable_utility_cash0": float(drawdown.min()),
                "avg_names": float(pd.to_numeric(daily["n_names"], errors="coerce").mean()),
            }
        ]
    )


def build_gate_grid(
    *,
    lookbacks: Iterable[int],
    min_histories: Iterable[int],
    min_means: Iterable[float],
    min_hits: Iterable[float],
    max_markets: Iterable[int],
) -> list[GateSpec]:
    return [
        GateSpec(
            lookback_days=int(lookback),
            min_history=int(min_history),
            min_trailing_mean=float(min_mean),
            min_trailing_hit=float(min_hit),
            max_markets=int(max_market),
        )
        for lookback in lookbacks
        for min_history in min_histories
        for min_mean in min_means
        for min_hit in min_hits
        for max_market in max_markets
    ]


def _split_daily(
    daily: pd.DataFrame,
    *,
    tune_end_date: date,
    holdout_start_date: date,
) -> pd.DataFrame:
    result = daily.copy()
    dates = pd.to_datetime(result["as_of_date"]).dt.date
    result["split"] = "gap"
    result.loc[dates <= tune_end_date, "split"] = "tune"
    result.loc[dates >= holdout_start_date, "split"] = "holdout"
    return result.loc[result["split"].ne("gap")].copy()


def summarize_split_daily(
    daily: pd.DataFrame,
    *,
    tune_end_date: date,
    holdout_start_date: date,
) -> pd.DataFrame:
    split_daily = _split_daily(
        daily,
        tune_end_date=tune_end_date,
        holdout_start_date=holdout_start_date,
    )
    rows: list[pd.DataFrame] = []
    for split_name, group in split_daily.groupby("split", sort=True):
        summary = summarize_daily(group.drop(columns=["split"]))
        if summary.empty:
            continue
        summary.insert(1, "split", split_name)
        rows.append(summary)
    cleaned = [row.dropna(axis=1, how="all") for row in rows if not row.empty]
    return pd.concat(cleaned, ignore_index=True, sort=False) if cleaned else pd.DataFrame()


def run(args: argparse.Namespace) -> int:
    if not args.artifact_only or not args.promotion_disabled:
        raise RuntimeError("Adaptive gate evaluation must be artifact-only and promotion-disabled")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = load_query_group_top5(args.query_group_top5.resolve())
    specs = build_gate_grid(
        lookbacks=args.lookback_days,
        min_histories=args.min_history,
        min_means=args.min_trailing_mean,
        min_hits=args.min_trailing_hit,
        max_markets=args.max_markets,
    )
    daily_frames: list[pd.DataFrame] = []
    split_summaries: list[pd.DataFrame] = []
    for spec in specs:
        daily, _ = evaluate_gate(frame, spec)
        daily_frames.append(daily)
        split_summary = summarize_split_daily(
            daily,
            tune_end_date=args.tune_end_date,
            holdout_start_date=args.holdout_start_date,
        )
        if not split_summary.empty:
            for field, value in asdict(spec).items():
                split_summary[field] = value
            split_summaries.append(split_summary)
    all_daily = pd.concat(daily_frames, ignore_index=True, sort=False)
    all_split = (
        pd.concat(split_summaries, ignore_index=True, sort=False)
        if split_summaries
        else pd.DataFrame()
    )
    tune = all_split.loc[all_split["split"].eq("tune")].copy()
    if tune.empty:
        raise RuntimeError("No tune rows available for champion selection")
    eligible = tune.loc[
        tune["coverage"].ge(args.min_tune_coverage)
        & tune["active_dates"].ge(args.min_tune_active_dates)
    ].copy()
    if eligible.empty:
        eligible = tune.copy()
    eligible = eligible.sort_values(
        [
            "avg_daily_stable_utility_cash0",
            "median_daily_stable_utility_cash0",
            "p10_daily_stable_utility_cash0",
        ],
        ascending=[False, False, False],
    )
    champion_gate_id = str(eligible.iloc[0]["gate_id"])
    champion_daily = all_daily.loc[all_daily["gate_id"].eq(champion_gate_id)].copy()
    champion_split = all_split.loc[all_split["gate_id"].eq(champion_gate_id)].copy()

    outputs = {
        "grid_split_summary": output_dir / "adaptive_market_gate_grid_split_summary.csv",
        "all_daily": output_dir / "adaptive_market_gate_all_daily.csv",
        "champion_daily": output_dir / "adaptive_market_gate_champion_daily.csv",
        "champion_split_summary": output_dir / "adaptive_market_gate_champion_split_summary.csv",
        "manifest": output_dir / "adaptive_market_gate_manifest.json",
    }
    all_split.to_csv(outputs["grid_split_summary"], index=False)
    all_daily.to_csv(outputs["all_daily"], index=False)
    champion_daily.to_csv(outputs["champion_daily"], index=False)
    champion_split.to_csv(outputs["champion_split_summary"], index=False)
    manifest = {
        "kind": "d5_ltr_adaptive_market_gate_evaluation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "query_group_top5": str(args.query_group_top5.resolve()),
        "repo_sha": _git_sha(),
        "server_sha": args.server_sha or _git_sha(),
        "promotion_disabled": True,
        "artifact_only": True,
        "read_only": True,
        "db_read_only": True,
        "tune_end_date": args.tune_end_date.isoformat(),
        "holdout_start_date": args.holdout_start_date.isoformat(),
        "grid_size": len(specs),
        "champion_gate_id": champion_gate_id,
        "selection_rule": (
            "choose best tune avg_daily_stable_utility_cash0 among coverage-eligible "
            "gates; evaluate holdout unchanged"
        ),
        "outputs": {key: str(path) for key, path in outputs.items()},
    }
    outputs["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {"manifest": str(outputs["manifest"]), "champion_gate_id": champion_gate_id},
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate prior-performance adaptive market gates over D5 LTR "
            "query-group Top5 artifacts."
        )
    )
    parser.add_argument("--query-group-top5", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--tune-end-date", type=_parse_date, default=date(2026, 3, 31))
    parser.add_argument("--holdout-start-date", type=_parse_date, default=date(2026, 4, 1))
    parser.add_argument("--lookback-days", nargs="+", type=int, default=[5, 10, 20])
    parser.add_argument("--min-history", nargs="+", type=int, default=[3, 5])
    parser.add_argument("--min-trailing-mean", nargs="+", type=float, default=[-0.002, 0.0, 0.002])
    parser.add_argument("--min-trailing-hit", nargs="+", type=float, default=[0.45, 0.50, 0.55])
    parser.add_argument("--max-markets", nargs="+", type=int, default=[1, 2])
    parser.add_argument("--min-tune-coverage", type=float, default=0.15)
    parser.add_argument("--min-tune-active-dates", type=int, default=8)
    parser.add_argument("--artifact-only", action="store_true")
    parser.add_argument("--promotion-disabled", action="store_true")
    parser.add_argument("--server-sha")
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
