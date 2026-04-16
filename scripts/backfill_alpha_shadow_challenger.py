# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_outcomes,
)
from app.logging import configure_logging, get_logger
from app.ml.constants import MODEL_DOMAIN, MODEL_VERSION, get_alpha_model_spec
from app.ml.registry import load_latest_training_run
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.shadow_report import render_alpha_shadow_comparison_report
from app.ml.training import (
    AlphaTrainingResult,
    prune_training_result_artifacts,
    train_alpha_candidate_models,
)
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill alpha shadow candidates for a challenger model while pruning "
            "training artifacts after each successful date."
        )
    )
    parser.add_argument("--start-selection-date", required=True, type=_parse_date)
    parser.add_argument("--end-selection-date", required=True, type=_parse_date)
    parser.add_argument("--model-spec-id", default="alpha_rank_rolling_120_v1")
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument("--rolling-windows", nargs="+", type=int, default=[20, 60])
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-symbols", type=int, default=None)
    parser.add_argument("--market", default="ALL")
    parser.add_argument(
        "--keep-training-artifacts",
        action="store_true",
        help="Keep per-date training artifacts instead of pruning them after shadow materialization.",
    )
    parser.add_argument(
        "--skip-completed-dates",
        action="store_true",
        default=True,
        help="Skip dates where the requested challenger already has shadow rankings for every horizon.",
    )
    parser.add_argument(
        "--no-skip-completed-dates",
        dest="skip_completed_dates",
        action="store_false",
        help="Force retraining and rematerialization even if shadow rows already exist.",
    )
    parser.add_argument(
        "--prune-existing-completed-artifacts",
        action="store_true",
        default=True,
        help="When skipping a completed date, also prune the latest matching training artifacts if they still exist.",
    )
    parser.add_argument(
        "--no-prune-existing-completed-artifacts",
        dest="prune_existing_completed_artifacts",
        action="store_false",
    )
    return parser.parse_args()


def _load_trading_dates(settings, *, start_selection_date: date, end_selection_date: date) -> list[date]:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        rows = connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE is_trading_day = TRUE
              AND trading_date BETWEEN ? AND ?
            ORDER BY trading_date
            """,
            [start_selection_date, end_selection_date],
        ).fetchall()
    return [row[0] for row in rows]


def _shadow_date_complete(
    settings,
    *,
    selection_date: date,
    horizons: list[int],
    model_spec_id: str,
) -> bool:
    placeholders = ", ".join("?" for _ in horizons)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        row = connection.execute(
            f"""
            SELECT COUNT(DISTINCT horizon)
            FROM fact_alpha_shadow_ranking
            WHERE selection_date = ?
              AND model_spec_id = ?
              AND horizon IN ({placeholders})
            """,
            [selection_date, model_spec_id, *horizons],
        ).fetchone()
    return int(row[0] or 0) == len({int(horizon) for horizon in horizons})


def _build_existing_training_result(
    settings,
    *,
    train_end_date: date,
    model_spec_id: str,
    horizons: list[int],
) -> AlphaTrainingResult | None:
    artifact_paths: list[str] = []
    run_ids: list[str] = []
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        for horizon in horizons:
            training_run = load_latest_training_run(
                connection,
                horizon=int(horizon),
                model_version=MODEL_VERSION,
                train_end_date=train_end_date,
                model_domain=MODEL_DOMAIN,
                model_spec_id=model_spec_id,
            )
            if training_run is None or training_run.get("train_end_date") != train_end_date:
                return None
            artifact_uri = str(training_run.get("artifact_uri") or "").strip()
            if not artifact_uri:
                return None
            run_id = str(training_run.get("run_id") or "").strip()
            if not run_id:
                return None
            run_ids.append(run_id)
            artifact_paths.append(artifact_uri)
    if not run_ids:
        return None
    run_id = run_ids[0]
    if any(candidate != run_id for candidate in run_ids):
        return None
    return AlphaTrainingResult(
        run_id=run_id,
        train_end_date=train_end_date,
        row_count=0,
        training_run_count=len(artifact_paths),
        artifact_paths=artifact_paths,
        notes="existing successful training run",
        model_version=MODEL_VERSION,
    )


def main() -> int:
    args = _parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    model_spec = get_alpha_model_spec(args.model_spec_id)
    trading_dates = _load_trading_dates(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
    )
    if not trading_dates:
        print("No trading dates found for the requested range.")
        return 0

    print(
        "SHADOW_BACKFILL_RANGE",
        trading_dates[0],
        trading_dates[-1],
        "COUNT",
        len(trading_dates),
        "MODEL_SPEC",
        model_spec.model_spec_id,
        flush=True,
    )

    for index, as_of_date in enumerate(trading_dates, start=1):
        print(f"[shadow-backfill] {index}/{len(trading_dates)} as_of_date={as_of_date}", flush=True)
        if args.skip_completed_dates and _shadow_date_complete(
            settings,
            selection_date=as_of_date,
            horizons=list(args.horizons),
            model_spec_id=model_spec.model_spec_id,
        ):
            print(
                f"[shadow-backfill] skip existing shadow rows for {as_of_date} {model_spec.model_spec_id}",
                flush=True,
            )
            if (
                not args.keep_training_artifacts
                and args.prune_existing_completed_artifacts
            ):
                existing_training = _build_existing_training_result(
                    settings,
                    train_end_date=as_of_date,
                    model_spec_id=model_spec.model_spec_id,
                    horizons=list(args.horizons),
                )
                if existing_training is not None:
                    prune_result = prune_training_result_artifacts(
                        settings,
                        training_result=existing_training,
                    )
                    print(prune_result.notes, flush=True)
            continue

        train_result = train_alpha_candidate_models(
            settings,
            train_end_date=as_of_date,
            horizons=list(args.horizons),
            min_train_days=int(args.min_train_days),
            validation_days=int(args.validation_days),
            limit_symbols=args.limit_symbols,
            market=str(args.market),
            model_specs=(model_spec,),
        )
        print(train_result.notes, flush=True)

        shadow_result = materialize_alpha_shadow_candidates(
            settings,
            as_of_date=as_of_date,
            horizons=list(args.horizons),
            limit_symbols=args.limit_symbols,
            market=str(args.market),
        )
        print(shadow_result.notes, flush=True)
        logger.info(
            "Alpha shadow challenger materialized for date.",
            extra={
                "run_id_value": shadow_result.run_id,
                "as_of_date_value": as_of_date.isoformat(),
                "model_spec_id_value": model_spec.model_spec_id,
            },
        )

        if not args.keep_training_artifacts:
            prune_result = prune_training_result_artifacts(
                settings,
                training_result=train_result,
            )
            print(prune_result.notes, flush=True)

    outcome_result = materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=trading_dates[0],
        end_selection_date=trading_dates[-1],
        horizons=list(args.horizons),
    )
    print(outcome_result.notes, flush=True)

    summary_result = materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=trading_dates[0],
        end_selection_date=trading_dates[-1],
        horizons=list(args.horizons),
        rolling_windows=list(args.rolling_windows),
    )
    print(summary_result.notes, flush=True)

    report_result = render_alpha_shadow_comparison_report(
        settings,
        start_selection_date=trading_dates[0],
        end_selection_date=trading_dates[-1],
        horizons=list(args.horizons),
    )
    print(
        f"Alpha shadow comparison report rendered. run_id={report_result.run_id} rows={report_result.row_count}",
        flush=True,
    )
    for artifact_path in report_result.artifact_paths:
        print(f"ARTIFACT {artifact_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
