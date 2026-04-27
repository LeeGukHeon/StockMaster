# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ml.dataset import load_training_dataset
from app.ml.ltr_training import (
    D5_LTR_CANDIDATE_POOLS,
    D5_LTR_CONTRACT,
    build_temporal_folds,
    clean_feature_matrix,
    group_sizes,
    mean_ndcg_at_k,
    prepare_ltr_frame,
    summarize_topn,
    topn_by_rank_score,
)
from app.settings import load_settings
from app.storage.duckdb import connect_duckdb

WRITER_PATTERN = "stockmaster|daily|materialize|selection|prediction|train|duckdb"
WRITER_COMMAND_TOKENS = (
    "run_daily",
    "materialize_",
    "train_",
    "build_model_training_dataset",
    "build_forward_labels",
    "duckdb",
    "selection",
    "prediction",
)
NON_WRITER_TOKENS = (
    "run_discord_bot.py",
    "pgrep -af",
    "run_d5_ltr_shadow_experiment.py",
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


def _package_version(package: str) -> str | None:
    try:
        module = __import__(package)
    except ImportError:
        return None
    return str(getattr(module, "__version__", "unknown"))


def _active_writer_lines() -> list[str]:
    try:
        output = subprocess.check_output(
            ["pgrep", "-af", WRITER_PATTERN],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return []
    current_pid = str(os.getpid())
    lines: list[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid = line.split(maxsplit=1)[0]
        lowered = line.lower()
        if pid == current_pid or any(token in lowered for token in NON_WRITER_TOKENS):
            continue
        if not any(token in lowered for token in WRITER_COMMAND_TOKENS):
            continue
        lines.append(line)
    return lines


def _load_dataset(args: argparse.Namespace) -> pd.DataFrame:
    if args.dataset_path is not None:
        path = args.dataset_path.resolve()
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, low_memory=False)

    settings = load_settings(project_root=PROJECT_ROOT)
    connection = connect_duckdb(settings.paths.duckdb_path, read_only=True)
    try:
        return load_training_dataset(
            connection,
            train_end_date=args.train_end_date,
            horizons=[int(args.horizon)],
            symbols=None,
            limit_symbols=args.limit_symbols,
            market=args.market,
        )
    finally:
        connection.close()


def _fit_lightgbm_fold(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    *,
    feature_columns: list[str],
    relevance_column: str,
    hyperparameters: dict[str, Any],
) -> pd.DataFrame:
    try:
        from lightgbm import LGBMRanker
    except ImportError as exc:  # pragma: no cover - local env may not have lightgbm
        raise RuntimeError("LightGBM is required for --engine lightgbm") from exc

    train_sorted = train.sort_values(["query_group_key", "symbol"]).copy()
    valid_sorted = validation.sort_values(["query_group_key", "symbol"]).copy()
    y_train = pd.to_numeric(
        train_sorted[relevance_column],
        errors="coerce",
    ).fillna(0).astype(int)
    if y_train.nunique() < 2:
        raise RuntimeError("LTR training fold has fewer than two relevance classes")
    model = LGBMRanker(**hyperparameters)
    model.fit(
        clean_feature_matrix(train_sorted, feature_columns),
        y_train,
        group=group_sizes(train_sorted),
    )
    predictions = valid_sorted.copy()
    predictions["rank_score"] = model.predict(clean_feature_matrix(valid_sorted, feature_columns))
    return predictions


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> int:
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    repo_sha = _git_sha()
    server_sha = os.environ.get("SERVER_SHA") or repo_sha
    writer_lines = _active_writer_lines() if args.abort_if_db_writer else []
    if writer_lines:
        manifest = {
            "kind": "d5_ltr_shadow_experiment",
            "engine": args.engine,
            "repo_sha": repo_sha,
            "server_sha": server_sha,
            "promotion_disabled": bool(args.promotion_disabled),
            "artifact_only": bool(args.artifact_only),
            "read_only": True,
            "db_read_only": True,
            "gate_decision": "stop_lane",
            "fail_reasons": ["active_duckdb_writer_detected"],
            "active_writer_lines": writer_lines,
            "ltr_contract": D5_LTR_CONTRACT,
        }
        _write_json(output_dir / "lane2_ltr_lightgbm_manifest.json", manifest)
        print(json.dumps({"status": "aborted", "fail_reasons": manifest["fail_reasons"]}))
        return 2

    if not args.artifact_only or not args.promotion_disabled:
        raise RuntimeError("LTR shadow experiments must be artifact-only and promotion-disabled")
    if args.engine != "lightgbm":
        raise RuntimeError(f"Unsupported engine for this lane: {args.engine}")

    dataset = _load_dataset(args)
    prepared, feature_columns = prepare_ltr_frame(
        dataset,
        horizon=int(args.horizon),
        query_group_key=args.query_group_key,
        target_column=args.target_column
        or f"target_stable_practical_excess_h{int(args.horizon)}",
        relevance_column=args.relevance_label,
        candidate_pool=args.candidate_pool,
    )
    dates = sorted(prepared["as_of_date"].dropna().unique())
    folds = build_temporal_folds(
        dates,
        fold_count=args.fold_count,
        purge_days=args.purge_days,
        embargo_days=args.embargo_days,
        min_train_dates=args.min_train_dates,
    )
    if not folds:
        raise RuntimeError("No valid temporal folds after purge/embargo constraints")

    hyperparameters = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "n_estimators": args.n_estimators,
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "min_child_samples": args.min_child_samples,
        "random_state": args.random_state,
        "n_jobs": args.n_jobs,
        "verbosity": -1,
    }

    prediction_frames: list[pd.DataFrame] = []
    fold_rows: list[dict[str, object]] = []
    for fold in folds:
        train = prepared.loc[
            (prepared["as_of_date"] >= fold.train_start_date)
            & (prepared["as_of_date"] <= fold.train_end_date)
        ].copy()
        validation = prepared.loc[
            (prepared["as_of_date"] >= fold.validation_start_date)
            & (prepared["as_of_date"] <= fold.validation_end_date)
        ].copy()
        predictions = _fit_lightgbm_fold(
            train,
            validation,
            feature_columns=feature_columns,
            relevance_column=args.relevance_label,
            hyperparameters=hyperparameters,
        )
        predictions["fold_id"] = fold.fold_id
        ndcg5 = mean_ndcg_at_k(
            predictions,
            score_column="rank_score",
            relevance_column=args.relevance_label,
            k=5,
        )
        fold_row = fold.as_dict()
        fold_row.update(
            {
                "train_rows": int(len(train)),
                "validation_rows": int(len(validation)),
                "feature_count": int(len(feature_columns)),
                "ndcg_at_5": ndcg5,
            }
        )
        fold_rows.append(fold_row)
        prediction_frames.append(predictions)

    predictions = pd.concat(prediction_frames, ignore_index=True, sort=False)
    fold_summary = pd.DataFrame(fold_rows)
    relevance_target_column = args.target_column or (
        f"target_stable_practical_excess_h{int(args.horizon)}"
    )
    top5_by_date = topn_by_rank_score(
        predictions,
        top_ns=[5],
        horizon=int(args.horizon),
        portfolio_group_key=args.portfolio_group_key,
        portfolio_score_mode=args.portfolio_score_mode,
        relevance_target_column=relevance_target_column,
    )
    topn_by_date = topn_by_rank_score(
        predictions,
        top_ns=[3, 5],
        horizon=int(args.horizon),
        portfolio_group_key=args.portfolio_group_key,
        portfolio_score_mode=args.portfolio_score_mode,
        relevance_target_column=relevance_target_column,
    )
    top5_by_query_group = topn_by_rank_score(
        predictions,
        top_ns=[5],
        horizon=int(args.horizon),
        portfolio_group_key="as_of_date+market",
        portfolio_score_mode="raw",
        relevance_target_column=relevance_target_column,
    )
    topn_summary = summarize_topn(topn_by_date)

    schema_boundary = {
        "kind": "d5_ltr_schema_boundary",
        "spec_payload_json": {"ltr_contract": D5_LTR_CONTRACT},
        "model_family_json": {
            "implementation": {
                "package": "lightgbm",
                "package_version": _package_version("lightgbm"),
                "hyperparameters": hyperparameters,
                "artifact_type": "shadow_rank_score_only",
            }
        },
        "forbidden_runtime_mappings": [
            "expected_excess_return",
            "selection_engine_v2",
            "active_model_registry",
        ],
    }

    gate_failures: list[str] = []
    if repo_sha is None:
        gate_failures.append("missing_repo_sha")
    if server_sha is None:
        gate_failures.append("missing_server_sha")
    if not args.promotion_disabled:
        gate_failures.append("promotion_not_disabled")
    if not args.artifact_only:
        gate_failures.append("not_artifact_only")
    if fold_summary["ndcg_at_5"].isna().all():
        gate_failures.append("ndcg_at_5_missing")

    gate_decision = {
        "gate_decision": "pass_shadow_smoke" if not gate_failures else "stop_lane",
        "passed": not gate_failures,
        "fail_reasons": gate_failures,
        "warning_reasons": [
            "rank_score_only_no_runtime_integration",
            "cpcv_pbo_required_before_production_candidate_language",
        ],
        "purge_days": int(args.purge_days),
        "embargo_days": int(args.embargo_days),
        "eval_metric": args.eval_metric,
    }

    manifest = {
        "kind": "d5_ltr_shadow_experiment",
        "engine": args.engine,
        "repo_sha": repo_sha,
        "server_sha": server_sha,
        "promotion_disabled": True,
        "artifact_only": True,
        "read_only": True,
        "db_read_only": True,
        "query_group_key": args.query_group_key,
        "portfolio_group_key": args.portfolio_group_key,
        "portfolio_score_mode": args.portfolio_score_mode,
        "candidate_pool": args.candidate_pool,
        "relevance_label": args.relevance_label,
        "target_column": relevance_target_column,
        "eval_metric": args.eval_metric,
        "purge_days": int(args.purge_days),
        "embargo_days": int(args.embargo_days),
        "feature_count": int(len(feature_columns)),
        "fold_count": int(len(folds)),
        "input_row_count": int(len(dataset)),
        "prepared_row_count": int(len(prepared)),
        "query_group_count": int(prepared["query_group_key"].nunique()),
        "min_query_group_size": int(prepared.groupby("query_group_key").size().min()),
        "median_query_group_size": float(prepared.groupby("query_group_key").size().median()),
        "ltr_contract": D5_LTR_CONTRACT,
        "implementation": schema_boundary["model_family_json"]["implementation"],
        "gate_decision": gate_decision,
        "outputs": [],
    }

    paths = {
        "schema": output_dir / "lane2_ltr_schema_boundary.json",
        "manifest": output_dir / "lane2_ltr_lightgbm_manifest.json",
        "fold_summary": output_dir / "lane2_ltr_lightgbm_fold_summary.csv",
        "top5_by_date": output_dir / "lane2_ltr_lightgbm_top5_by_date.csv",
        "top5_by_query_group": output_dir / "lane2_ltr_lightgbm_top5_by_query_group.csv",
        "vs_baselines": output_dir / "lane2_ltr_lightgbm_vs_baselines.csv",
        "gate": output_dir / "lane2_ltr_lightgbm_gate_decision.json",
        "predictions": output_dir / "lane2_ltr_lightgbm_rank_scores.csv",
    }
    predictions.to_csv(paths["predictions"], index=False)
    fold_summary.to_csv(paths["fold_summary"], index=False)
    top5_by_date.to_csv(paths["top5_by_date"], index=False)
    top5_by_query_group.to_csv(paths["top5_by_query_group"], index=False)
    topn_summary.to_csv(paths["vs_baselines"], index=False)
    _write_json(paths["schema"], schema_boundary)
    _write_json(paths["gate"], gate_decision)
    manifest["outputs"] = [str(value) for value in paths.values()]
    _write_json(paths["manifest"], manifest)
    print(
        json.dumps(
            {"manifest": str(paths["manifest"]), "gate_decision": gate_decision},
            ensure_ascii=False,
        )
    )
    return 0 if gate_decision["passed"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run artifact-only D5 LightGBM LTR shadow experiment."
    )
    parser.add_argument("--train-end-date", required=True, type=_parse_date)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--engine", choices=["lightgbm"], default="lightgbm")
    parser.add_argument("--query-group-key", default="as_of_date+horizon+market")
    parser.add_argument(
        "--portfolio-group-key",
        choices=["as_of_date", "as_of_date+market"],
        default="as_of_date+market",
        help=(
            "Grouping for topN portfolio evaluation. as_of_date+market preserves raw LTR "
            "query-score semantics; as_of_date requires --portfolio-score-mode query_rank_pct."
        ),
    )
    parser.add_argument(
        "--portfolio-score-mode",
        choices=["raw", "query_rank_pct"],
        default="raw",
        help=(
            "Score used for topN portfolio sorting. Raw is valid within the LTR query only; "
            "query_rank_pct normalizes each query to percentile ranks before cross-market "
            "daily basket pooling."
        ),
    )
    parser.add_argument(
        "--candidate-pool",
        choices=list(D5_LTR_CANDIDATE_POOLS),
        default="full",
        help=(
            "Point-in-time candidate pool for the artifact-only LTR experiment. "
            "full preserves the original universe; stable_buyable_v1/strict apply "
            "predeclared buyability filters before label ranking."
        ),
    )
    parser.add_argument("--relevance-label", default="stable_d5_utility_relevance")
    parser.add_argument(
        "--target-column",
        help=(
            "Column used to derive ordinal relevance. Defaults to "
            "target_stable_practical_excess_h<horizon>."
        ),
    )
    parser.add_argument("--eval-metric", default="ndcg@5")
    parser.add_argument("--purge-days", type=int, default=5)
    parser.add_argument("--embargo-days", type=int, default=5)
    parser.add_argument("--artifact-only", action="store_true")
    parser.add_argument("--promotion-disabled", action="store_true")
    parser.add_argument("--abort-if-db-writer", action="store_true")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--dataset-path", type=Path)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL")
    parser.add_argument("--fold-count", type=int, default=3)
    parser.add_argument("--min-train-dates", type=int, default=20)
    parser.add_argument("--n-estimators", type=int, default=120)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--num-leaves", type=int, default=31)
    parser.add_argument("--min-child-samples", type=int, default=20)
    parser.add_argument("--random-state", type=int, default=20260428)
    parser.add_argument("--n-jobs", type=int, default=2)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
