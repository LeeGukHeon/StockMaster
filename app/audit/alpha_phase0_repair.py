from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import build_feature_store
from app.ml.constants import PREDICTION_VERSION as ALPHA_PREDICTION_VERSION
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.ml.inference import materialize_alpha_predictions_v1
from app.ml.training import train_alpha_model_v1
from app.ranking.explanatory_score import materialize_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.selection.engine_v2 import materialize_selection_engine_v2
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class AlphaPhase0RepairResult:
    run_id: str
    affected_train_end_dates: list[date]
    affected_as_of_dates: list[date]
    repaired_training_run_count: int
    repaired_prediction_dates: int
    artifact_paths: list[str]
    notes: str


def detect_overlap_train_end_dates(
    connection,
    *,
    start_date: date,
    end_date: date,
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT train_end_date
        FROM (
            SELECT t.train_end_date
            FROM fact_prediction AS p
            JOIN fact_model_training_run AS t
              ON p.training_run_id = t.training_run_id
            WHERE p.ranking_version = ?
              AND p.prediction_version = ?
              AND p.as_of_date BETWEEN ? AND ?
              AND COALESCE(t.validation_window_end, t.training_window_end) >= p.as_of_date
        )
        ORDER BY train_end_date
        """,
        [SELECTION_ENGINE_VERSION, ALPHA_PREDICTION_VERSION, start_date, end_date],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows if row[0] is not None]


def detect_prelisting_dates(
    connection,
    *,
    start_date: date,
    end_date: date,
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT r.as_of_date
        FROM fact_ranking AS r
        JOIN dim_symbol AS s
          ON r.symbol = s.symbol
        WHERE r.as_of_date BETWEEN ? AND ?
          AND s.listing_date IS NOT NULL
          AND s.listing_date > r.as_of_date
        ORDER BY r.as_of_date
        """,
        [start_date, end_date],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows if row[0] is not None]


def detect_phase0_repair_scope(
    connection,
    *,
    start_date: date,
    end_date: date,
) -> tuple[list[date], list[date]]:
    overlap_train_end_dates = detect_overlap_train_end_dates(
        connection,
        start_date=start_date,
        end_date=end_date,
    )
    prelisting_dates = detect_prelisting_dates(
        connection,
        start_date=start_date,
        end_date=end_date,
    )
    affected_as_of_dates = sorted(set(overlap_train_end_dates) | set(prelisting_dates))
    return overlap_train_end_dates, affected_as_of_dates


def purge_prediction_snapshot(
    connection,
    *,
    as_of_date: date,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM fact_prediction
        WHERE as_of_date = ?
          AND prediction_version = ?
          AND ranking_version = ?
        """,
        [as_of_date, ALPHA_PREDICTION_VERSION, SELECTION_ENGINE_VERSION],
    ).fetchone()
    count = int(row[0] or 0) if row is not None else 0
    connection.execute(
        """
        DELETE FROM fact_prediction
        WHERE as_of_date = ?
          AND prediction_version = ?
          AND ranking_version = ?
        """,
        [as_of_date, ALPHA_PREDICTION_VERSION, SELECTION_ENGINE_VERSION],
    )
    return count


def rebuild_single_prediction_date(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
) -> None:
    build_feature_store(
        settings,
        as_of_date=as_of_date,
        force=True,
        cutoff_time="17:30",
    )
    build_market_regime_snapshot(settings, as_of_date=as_of_date)
    materialize_explanatory_ranking(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        force=True,
    )
    materialize_selection_engine_v1(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        force=True,
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        purge_prediction_snapshot(connection, as_of_date=as_of_date)
    materialize_alpha_predictions_v1(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
    )
    materialize_selection_engine_v2(
        settings,
        as_of_date=as_of_date,
        horizons=horizons,
        force=True,
    )


def run_alpha_phase0_repair(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    horizons: list[int],
    min_train_days: int = 120,
    validation_days: int = 20,
) -> AlphaPhase0RepairResult:
    ensure_storage_layout(settings)
    with activate_run_context("run_alpha_phase0_repair", as_of_date=end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_prediction",
                    "fact_ranking",
                    "fact_model_training_run",
                    "dim_symbol",
                    "build_feature_store",
                    "train_alpha_model_v1",
                    "materialize_alpha_predictions_v1",
                    "materialize_selection_engine_v2",
                ],
                notes=(
                    "Repair historical phase0 contamination. "
                    f"range={start_date.isoformat()}..{end_date.isoformat()} horizons={horizons}"
                ),
            )
            try:
                overlap_train_end_dates, affected_as_of_dates = detect_phase0_repair_scope(
                    connection,
                    start_date=start_date,
                    end_date=end_date,
                )

                repaired_training_run_count = 0
                for train_end_date in overlap_train_end_dates:
                    result = train_alpha_model_v1(
                        settings,
                        train_end_date=train_end_date,
                        horizons=horizons,
                        min_train_days=min_train_days,
                        validation_days=validation_days,
                    )
                    repaired_training_run_count += int(result.training_run_count)

                repaired_prediction_dates = 0
                for as_of_date in affected_as_of_dates:
                    rebuild_single_prediction_date(
                        settings,
                        as_of_date=as_of_date,
                        horizons=horizons,
                    )
                    repaired_prediction_dates += 1

                notes = (
                    "Alpha phase0 repair completed. "
                    f"train_end_dates={len(overlap_train_end_dates)} "
                    f"prediction_dates={len(affected_as_of_dates)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                )
                return AlphaPhase0RepairResult(
                    run_id=run_context.run_id,
                    affected_train_end_dates=overlap_train_end_dates,
                    affected_as_of_dates=affected_as_of_dates,
                    repaired_training_run_count=repaired_training_run_count,
                    repaired_prediction_dates=repaired_prediction_dates,
                    artifact_paths=[],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Alpha phase0 repair failed.",
                    error_message=str(exc),
                )
                raise
