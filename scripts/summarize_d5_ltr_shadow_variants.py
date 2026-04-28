# ruff: noqa: E402
from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _parse_variant(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected VARIANT=DIR")
    name, path = value.split("=", 1)
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("Variant name must not be empty")
    return name, Path(path)


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


def _load_variant_topn(variant: str, directory: Path) -> pd.DataFrame:
    path = directory / "lane2_ltr_lightgbm_top5_by_date.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing topN file for {variant}: {path}")
    frame = pd.read_csv(path)
    required = {
        "as_of_date",
        "top_n",
        "avg_stable_utility",
        "avg_excess_return",
        "hit_stable_utility",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{path} missing required columns: {', '.join(missing)}")
    result = frame.copy()
    result["variant"] = variant
    result["as_of_date"] = pd.to_datetime(result["as_of_date"]).dt.date
    result["top_n"] = pd.to_numeric(result["top_n"], errors="coerce").astype("Int64")
    for column in ("avg_stable_utility", "avg_excess_return", "hit_stable_utility"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def load_variant_frames(variants: Iterable[tuple[str, Path]]) -> pd.DataFrame:
    frames = [_load_variant_topn(name, path.resolve()) for name, path in variants]
    if not frames:
        raise ValueError("At least one --variant is required")
    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined.dropna(subset=["as_of_date", "top_n"])


def summarize_variants(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (variant, top_n), group in frame.groupby(["variant", "top_n"], sort=True):
        stable = pd.to_numeric(group["avg_stable_utility"], errors="coerce")
        excess = pd.to_numeric(group["avg_excess_return"], errors="coerce")
        positive = stable[stable.gt(0.0)]
        rows.append(
            {
                "variant": str(variant),
                "top_n": int(top_n),
                "dates": int(group["as_of_date"].nunique()),
                "avg_stable_utility": float(stable.mean()),
                "median_stable_utility": float(stable.median()),
                "p10_stable_utility": float(stable.quantile(0.10)),
                "hit_stable_utility": float(stable.gt(0.0).mean()),
                "avg_excess_return": float(excess.mean()),
                "max_positive_edge_share": None
                if positive.sum() <= 0.0
                else float(positive.max() / positive.sum()),
            }
        )
    return pd.DataFrame(rows)


def _assign_blocks(dates: list[object], block_count: int) -> dict[object, int]:
    if block_count < 2:
        raise ValueError("block_count must be >= 2")
    ordered = sorted(dates)
    blocks = np.array_split(np.array(ordered, dtype=object), min(block_count, len(ordered)))
    mapping: dict[object, int] = {}
    for block_index, values in enumerate(blocks):
        for value in values.tolist():
            mapping[value] = block_index
    return mapping


def pbo_style_summary(
    frame: pd.DataFrame,
    *,
    block_count: int = 6,
    min_common_dates: int = 20,
) -> pd.DataFrame:
    """Return a combinatorial-purged/PBO-style stability summary.

    This does not retrain the ranker. It treats each supplied LTR run as a predeclared
    variant and asks: if we selected the best variant in a training block combination,
    how often did that selected variant rank in the lower half out-of-sample?
    """

    rows: list[dict[str, object]] = []
    for top_n, top_group in frame.groupby("top_n", sort=True):
        pivot = top_group.pivot_table(
            index="as_of_date",
            columns="variant",
            values="avg_stable_utility",
            aggfunc="mean",
        ).dropna(axis=0, how="any")
        variants = list(pivot.columns)
        if len(variants) < 2 or len(pivot) < min_common_dates:
            rows.append(
                {
                    "top_n": int(top_n),
                    "variant_count": int(len(variants)),
                    "common_dates": int(len(pivot)),
                    "block_count": int(block_count),
                    "combination_count": 0,
                    "pbo_lower_half_rate": None,
                    "selected_oos_avg": None,
                    "selected_oos_median": None,
                    "selected_oos_hit": None,
                    "selected_oos_p10": None,
                    "best_train_variant_counts_json": json.dumps({}, ensure_ascii=False),
                    "status": "insufficient_common_dates_or_variants",
                }
            )
            continue

        block_map = _assign_blocks(list(pivot.index), block_count)
        blocks = sorted(set(block_map.values()))
        train_block_size = max(1, len(blocks) // 2)
        records: list[dict[str, object]] = []
        for train_blocks_tuple in itertools.combinations(blocks, train_block_size):
            train_blocks = set(train_blocks_tuple)
            train_dates = [date for date in pivot.index if block_map[date] in train_blocks]
            test_dates = [date for date in pivot.index if block_map[date] not in train_blocks]
            if not train_dates or not test_dates:
                continue
            train_scores = pivot.loc[train_dates].mean(axis=0)
            test_scores = pivot.loc[test_dates].mean(axis=0)
            best_variant = str(train_scores.sort_values(ascending=False).index[0])
            test_rank = test_scores.rank(method="average", ascending=False)[best_variant]
            variant_count = len(test_scores)
            # 1.0 is best, 0.0 is worst. Lower half implies overfit risk.
            test_rank_percentile = 1.0 - ((float(test_rank) - 1.0) / max(variant_count - 1, 1))
            records.append(
                {
                    "best_train_variant": best_variant,
                    "best_train_avg": float(train_scores[best_variant]),
                    "selected_oos_avg": float(test_scores[best_variant]),
                    "selected_oos_rank_percentile": float(test_rank_percentile),
                    "lower_half_oos": bool(test_rank_percentile <= 0.5),
                }
            )
        result = pd.DataFrame(records)
        variant_counts = (
            result["best_train_variant"].value_counts().sort_index().to_dict()
            if not result.empty
            else {}
        )
        rows.append(
            {
                "top_n": int(top_n),
                "variant_count": int(len(variants)),
                "common_dates": int(len(pivot)),
                "block_count": int(len(blocks)),
                "combination_count": int(len(result)),
                "pbo_lower_half_rate": None
                if result.empty
                else float(result["lower_half_oos"].mean()),
                "selected_oos_avg": None
                if result.empty
                else float(result["selected_oos_avg"].mean()),
                "selected_oos_median": None
                if result.empty
                else float(result["selected_oos_avg"].median()),
                "selected_oos_hit": None
                if result.empty
                else float(result["selected_oos_avg"].gt(0.0).mean()),
                "selected_oos_p10": None
                if result.empty
                else float(result["selected_oos_avg"].quantile(0.10)),
                "best_train_variant_counts_json": json.dumps(variant_counts, ensure_ascii=False),
                "status": "ok" if not result.empty else "no_combinations",
            }
        )
    return pd.DataFrame(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> int:
    if not args.artifact_only or not args.promotion_disabled:
        raise RuntimeError(
            "LTR variant stability summaries must be artifact-only and promotion-disabled"
        )
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    variants = list(args.variant)
    combined = load_variant_frames(variants)
    variant_summary = summarize_variants(combined)
    pbo_summary = pbo_style_summary(
        combined,
        block_count=args.block_count,
        min_common_dates=args.min_common_dates,
    )
    repo_sha = _git_sha()
    server_sha = args.server_sha or repo_sha
    manifest = {
        "kind": "d5_ltr_shadow_variant_stability_summary",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": repo_sha,
        "server_sha": server_sha,
        "promotion_disabled": True,
        "artifact_only": True,
        "read_only": True,
        "db_read_only": True,
        "variant_count": len(variants),
        "variants": {name: str(path.resolve()) for name, path in variants},
        "block_count": int(args.block_count),
        "min_common_dates": int(args.min_common_dates),
        "method": "predeclared_variant_cpcv_pbo_style_no_retraining",
        "limitations": [
            "Does not retrain LTR inside each CPCV split.",
            "Ranks predeclared artifact variants by average stable utility.",
            "Production-candidate language still requires retraining-level CPCV if variants pass.",
        ],
        "outputs": [
            str(output_dir / "lane2_ltr_variant_summary.csv"),
            str(output_dir / "lane2_ltr_pbo_style_summary.csv"),
            str(output_dir / "lane2_ltr_variant_stability_manifest.json"),
        ],
    }
    variant_summary.to_csv(output_dir / "lane2_ltr_variant_summary.csv", index=False)
    pbo_summary.to_csv(output_dir / "lane2_ltr_pbo_style_summary.csv", index=False)
    _write_json(output_dir / "lane2_ltr_variant_stability_manifest.json", manifest)
    print(
        json.dumps(
            {"manifest": manifest["outputs"][-1], "pbo_summary": manifest["outputs"][1]},
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize D5 LTR shadow variants with CPCV/PBO-style stability diagnostics."
    )
    parser.add_argument(
        "--variant",
        action="append",
        default=[],
        type=_parse_variant,
        metavar="NAME=DIR",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--block-count", type=int, default=6)
    parser.add_argument("--min-common-dates", type=int, default=20)
    parser.add_argument("--artifact-only", action="store_true")
    parser.add_argument("--promotion-disabled", action="store_true")
    parser.add_argument("--server-sha")
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
