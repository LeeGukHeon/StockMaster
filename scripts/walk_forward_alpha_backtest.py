# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.features.feature_store import load_feature_matrix
from app.ml.constants import get_alpha_model_spec, supports_horizon_for_spec
from app.ml.dataset import _load_dataset_frame, augment_market_regime_features
from app.ml.inference import build_prediction_frame_from_training_run
from app.ml.registry import load_active_alpha_model
from app.ml.training import _train_single_horizon
from app.ranking.explanatory_score import _load_regime_map
from app.selection.engine_v2 import build_selection_engine_v2_rankings
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_core_tables


@dataclass(frozen=True, slots=True)
class BacktestJob:
    horizon: int
    model_spec_id: str
    active_alpha_model_id: str | None
    source_training_run_id: str | None


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_text(value: object) -> str:
    return pd.Timestamp(value).date().isoformat()


def _resolve_jobs(
    connection: duckdb.DuckDBPyConnection,
    horizons: Iterable[int],
    model_spec_ids: list[str] | None,
) -> list[BacktestJob]:
    jobs: list[BacktestJob] = []
    if model_spec_ids:
        for model_spec_id in model_spec_ids:
            spec = get_alpha_model_spec(model_spec_id)
            for horizon in horizons:
                if supports_horizon_for_spec(spec, horizon=int(horizon)):
                    jobs.append(
                        BacktestJob(
                            horizon=int(horizon),
                            model_spec_id=model_spec_id,
                            active_alpha_model_id=None,
                            source_training_run_id=None,
                        )
                    )
        if not jobs:
            raise RuntimeError("No requested model spec supports the requested horizons")
        return jobs

    for horizon in horizons:
        active = load_active_alpha_model(connection, as_of_date=date.max, horizon=int(horizon))
        if active is None:
            raise RuntimeError(f"No active alpha model is registered for horizon={int(horizon)}")
        jobs.append(
            BacktestJob(
                horizon=int(horizon),
                model_spec_id=str(active["model_spec_id"]),
                active_alpha_model_id=(
                    None
                    if active.get("active_alpha_model_id") in (None, "")
                    else str(active["active_alpha_model_id"])
                ),
                source_training_run_id=(
                    None
                    if active.get("training_run_id") in (None, "")
                    else str(active["training_run_id"])
                ),
            )
        )
    return jobs


def _resolve_backtest_dates(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
    horizon: int,
    limit_dates: int | None,
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT feature.as_of_date
        FROM fact_feature_snapshot AS feature
        JOIN fact_forward_return_label AS label
          ON feature.as_of_date = label.as_of_date
         AND feature.symbol = label.symbol
         AND label.horizon = ?
         AND label.label_available_flag
        WHERE feature.as_of_date BETWEEN ? AND ?
        ORDER BY feature.as_of_date
        """,
        [int(horizon), start_date, end_date],
    ).fetchall()
    dates = [pd.Timestamp(row[0]).date() for row in rows]
    if limit_dates is not None and limit_dates > 0:
        return dates[-int(limit_dates) :]
    return dates


def _load_labels(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: date,
    horizon: int,
) -> pd.DataFrame:
    labels = connection.execute(
        """
        SELECT
            as_of_date,
            symbol,
            horizon,
            gross_forward_return,
            excess_forward_return,
            entry_date,
            exit_date
        FROM fact_forward_return_label
        WHERE as_of_date = ?
          AND horizon = ?
          AND label_available_flag
        """,
        [as_of_date, int(horizon)],
    ).fetchdf()
    if not labels.empty:
        labels["as_of_date"] = pd.to_datetime(labels["as_of_date"]).dt.date
        labels["symbol"] = labels["symbol"].astype(str).str.zfill(6)
    return labels


def _attach_labels_and_ranking(
    connection: duckdb.DuckDBPyConnection,
    *,
    settings,
    as_of_date: date,
    horizon: int,
    prediction_frame: pd.DataFrame,
) -> pd.DataFrame:
    prediction_frame = prediction_frame.copy()
    prediction_frame["as_of_date"] = pd.to_datetime(prediction_frame["as_of_date"]).dt.date
    prediction_frame["symbol"] = prediction_frame["symbol"].astype(str).str.zfill(6)
    labels = _load_labels(connection, as_of_date=as_of_date, horizon=horizon)
    merged = prediction_frame.merge(labels, on=["as_of_date", "symbol", "horizon"], how="inner")

    feature_rank = load_feature_matrix(connection, as_of_date=as_of_date, market="ALL")
    if feature_rank.empty:
        return merged
    rankings = build_selection_engine_v2_rankings(
        feature_matrix=feature_rank,
        as_of_date=as_of_date,
        horizons=[int(horizon)],
        regime_map=_load_regime_map(connection, as_of_date=as_of_date),
        prediction_frames_by_horizon={int(horizon): prediction_frame},
        run_id="walk-forward-alpha-backtest-artifact-only",
        settings=settings,
    )
    if not rankings:
        return merged
    ranking_cols = [
        "as_of_date",
        "symbol",
        "horizon",
        "final_selection_value",
        "final_selection_rank_pct",
        "grade",
        "report_candidate_flag",
        "risk_flags_json",
        "top_reason_tags_json",
    ]
    ranking = rankings[0].loc[:, ranking_cols].copy()
    ranking["as_of_date"] = pd.to_datetime(ranking["as_of_date"]).dt.date
    ranking["symbol"] = ranking["symbol"].astype(str).str.zfill(6)
    return merged.merge(ranking, on=["as_of_date", "symbol", "horizon"], how="left")


def _add_eval_columns(frame: pd.DataFrame) -> pd.DataFrame:
    evaluated = frame.copy()
    evaluated["prediction_error"] = (
        evaluated["expected_excess_return"] - evaluated["excess_forward_return"]
    )
    evaluated["direction_hit"] = (
        evaluated["expected_excess_return"].gt(0)
        == evaluated["excess_forward_return"].gt(0)
    )
    evaluated["realized_positive"] = evaluated["excess_forward_return"].gt(0)
    evaluated["pred_rank_pct"] = evaluated.groupby("as_of_date")[
        "expected_excess_return"
    ].rank(pct=True, method="average")
    evaluated["pred_decile"] = pd.cut(
        evaluated["pred_rank_pct"],
        bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        labels=list(range(1, 11)),
        include_lowest=True,
    )
    if "final_selection_value" in evaluated.columns:
        evaluated["score_rank"] = evaluated.groupby("as_of_date")[
            "final_selection_value"
        ].rank(ascending=False, method="first")
        evaluated["score_band"] = pd.cut(
            evaluated["final_selection_value"],
            bins=[-1, 55, 65, 75, 101],
            labels=["<55", "55-65", "65-75", "75+"],
            right=False,
        )
        evaluated["rank_bucket"] = pd.cut(
            evaluated["score_rank"],
            bins=[0, 1, 5, 20, 999999],
            labels=["top1", "top2-5", "top6-20", "other"],
            include_lowest=True,
        )
    return evaluated


def _metric_frame(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby(group_cols, dropna=False, observed=False)
    return grouped.agg(
        n=("symbol", "size"),
        n_dates=("as_of_date", "nunique"),
        avg_pred=("expected_excess_return", "mean"),
        avg_realized=("excess_forward_return", "mean"),
        median_realized=("excess_forward_return", "median"),
        p25_realized=("excess_forward_return", lambda values: values.quantile(0.25)),
        p75_realized=("excess_forward_return", lambda values: values.quantile(0.75)),
        hit_rate=("realized_positive", "mean"),
        direction_hit=("direction_hit", "mean"),
        mae=("prediction_error", lambda values: values.abs().mean()),
        rmse=("prediction_error", lambda values: float((values.pow(2).mean()) ** 0.5)),
    ).reset_index()


def _build_metrics(frame: pd.DataFrame, *, job: BacktestJob) -> pd.DataFrame:
    metric_frames: list[pd.DataFrame] = []
    for cols, slice_name in [
        (["horizon"], "overall"),
        (["horizon", "pred_decile"], "pred_decile"),
        (["horizon", "score_band"], "score_band"),
        (["horizon", "rank_bucket"], "rank_bucket"),
        (["horizon", "grade"], "grade"),
    ]:
        if all(column in frame.columns for column in cols):
            metrics = _metric_frame(frame, cols)
            if not metrics.empty:
                metrics["slice"] = slice_name
                metric_frames.append(metrics)
    if not metric_frames:
        return pd.DataFrame()
    metrics = pd.concat(metric_frames, ignore_index=True, sort=False)
    metrics["model_spec_id"] = job.model_spec_id
    metrics["source_active_alpha_model_id"] = job.active_alpha_model_id
    metrics["source_training_run_id"] = job.source_training_run_id
    return metrics


def _safe_remove_tree(path: Path) -> None:
    if not path.exists():
        return
    # Scratch roots are caller-provided, but must still be recognizably walk-forward
    # temporary model artifacts before recursive cleanup is allowed.
    if "walk_forward" not in str(path):
        raise RuntimeError(f"Refusing to remove unexpected artifact path: {path}")
    shutil.rmtree(path)


def run_backtest(args: argparse.Namespace) -> int:
    settings = load_settings(project_root=PROJECT_ROOT)
    db_path = settings.paths.duckdb_path
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    scratch_root = args.scratch_dir.resolve()
    scratch_root.mkdir(parents=True, exist_ok=True)

    # Hard safety gate: this script never calls promotion/freezing/upsert helpers and opens DuckDB
    # read-only. Model artifacts are transient files under scratch_root and are removed per date.
    connection = duckdb.connect(str(db_path), read_only=True)
    bootstrap_core_tables(connection)
    jobs = _resolve_jobs(connection, args.horizons, args.model_spec_ids)
    manifest: dict[str, object] = {
        "kind": "walk_forward_alpha_backtest_artifact_only",
        "db_path": str(db_path),
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "horizons": args.horizons,
        "promotion_disabled": True,
        "db_read_only": True,
        "delete_temp_models_per_date": not args.keep_temp_models,
        "outputs": [],
        "jobs": [asdict(job) for job in jobs],
    }

    summary_frames: list[pd.DataFrame] = []
    for job in jobs:
        model_spec = get_alpha_model_spec(job.model_spec_id)
        if not supports_horizon_for_spec(model_spec, horizon=job.horizon):
            raise RuntimeError(
                f"Active spec {job.model_spec_id} does not support horizon={job.horizon}"
            )
        dates = _resolve_backtest_dates(
            connection,
            start_date=args.start_date,
            end_date=args.end_date,
            horizon=job.horizon,
            limit_dates=args.limit_dates,
        )
        horizon_frames: list[pd.DataFrame] = []
        print(
            f"h{job.horizon}: dates={len(dates)} "
            "range="
            f"{dates[0].isoformat() if dates else '-'}.."
            f"{dates[-1].isoformat() if dates else '-'} "
            f"model_spec={job.model_spec_id}",
            flush=True,
        )
        for idx, as_of_date in enumerate(dates, start=1):
            run_id = f"walk_forward_alpha_backtest-{as_of_date.isoformat()}-h{job.horizon}"
            date_scratch = (
                scratch_root
                / f"as_of_date={as_of_date.isoformat()}"
                / f"horizon={job.horizon}"
            )
            date_scratch.mkdir(parents=True, exist_ok=True)
            dataset = _load_dataset_frame(
                connection,
                train_end_date=as_of_date,
                horizons=[job.horizon],
                symbols=None,
                limit_symbols=args.limit_symbols,
                market=args.market,
            )
            if dataset.empty:
                print(f"h{job.horizon} {as_of_date}: skipped empty training dataset", flush=True)
                _safe_remove_tree(date_scratch)
                continue
            training_run_row, _members, _metric_rows, artifact_path = _train_single_horizon(
                dataset,
                run_id=run_id,
                train_end_date=as_of_date,
                horizon=job.horizon,
                min_train_days=args.min_train_days,
                validation_days=args.validation_days,
                artifact_root=date_scratch,
                model_spec=model_spec,
            )
            if artifact_path is None or not str(training_run_row.get("artifact_uri") or ""):
                print(f"h{job.horizon} {as_of_date}: skipped no model artifact", flush=True)
                _safe_remove_tree(date_scratch)
                continue
            feature_frame = load_feature_matrix(
                connection,
                as_of_date=as_of_date,
                limit_symbols=args.limit_symbols,
                market=args.market,
                include_rank_features=False,
                include_zscore_features=False,
            )
            feature_frame = augment_market_regime_features(connection, feature_frame)
            prediction_frame, _ = build_prediction_frame_from_training_run(
                run_id=run_id,
                as_of_date=as_of_date,
                horizon=job.horizon,
                feature_frame=feature_frame,
                training_run=training_run_row,
                training_run_source="walk_forward_backtest_point_in_time",
                active_alpha_model_id=None,
                persist_member_predictions=False,
            )
            merged = _attach_labels_and_ranking(
                connection,
                settings=settings,
                as_of_date=as_of_date,
                horizon=job.horizon,
                prediction_frame=prediction_frame,
            )
            merged["backtest_run_id"] = run_id
            merged["backtest_train_end_date"] = as_of_date
            merged["backtest_training_window_start"] = training_run_row.get(
                "training_window_start"
            )
            merged["backtest_training_window_end"] = training_run_row.get(
                "training_window_end"
            )
            merged["backtest_validation_window_start"] = training_run_row.get(
                "validation_window_start"
            )
            merged["backtest_validation_window_end"] = training_run_row.get(
                "validation_window_end"
            )
            merged["backtest_model_spec_id"] = job.model_spec_id
            merged["backtest_training_run_id"] = str(training_run_row["training_run_id"])
            horizon_frames.append(merged)
            if not args.keep_temp_models:
                _safe_remove_tree(date_scratch)
            if idx % args.progress_every == 0 or idx == len(dates):
                print(f"h{job.horizon}: processed {idx}/{len(dates)}", flush=True)

        if not horizon_frames:
            continue
        evaluated = _add_eval_columns(pd.concat(horizon_frames, ignore_index=True))
        predictions_path = output_dir / f"h{job.horizon}_walk_forward_predictions_outcomes.csv"
        metrics_path = output_dir / f"h{job.horizon}_walk_forward_metrics.csv"
        evaluated.to_csv(predictions_path, index=False)
        metrics = _build_metrics(evaluated, job=job)
        metrics.to_csv(metrics_path, index=False)
        summary = metrics.loc[metrics["slice"].eq("overall")].copy()
        summary_frames.append(summary)
        manifest["outputs"].extend([str(predictions_path), str(metrics_path)])

    summary_path = output_dir / "summary.csv"
    if summary_frames:
        pd.concat(summary_frames, ignore_index=True, sort=False).to_csv(summary_path, index=False)
    else:
        pd.DataFrame().to_csv(summary_path, index=False)
    manifest["outputs"].insert(0, str(summary_path))
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")
    print(f"manifest={manifest_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an artifact-only point-in-time alpha walk-forward backtest. "
            "DuckDB is opened read-only; no promotion/freezing/upsert path is called."
        )
    )
    parser.add_argument("--start-date", required=True, type=_parse_date)
    parser.add_argument("--end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument(
        "--model-spec-ids",
        nargs="+",
        help="Optional experimental specs to backtest instead of currently active specs.",
    )
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-dates", type=int)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--market", default="ALL", choices=["ALL", "KOSPI", "KOSDAQ"])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/ad_hoc/walk_forward_alpha_backtest"),
    )
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=Path("artifacts/tmp/walk_forward_alpha_backtest"),
    )
    parser.add_argument("--keep-temp-models", action="store_true")
    parser.add_argument("--progress-every", type=int, default=5)
    return parser


def main() -> int:
    return run_backtest(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
