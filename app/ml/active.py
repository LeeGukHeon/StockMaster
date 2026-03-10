from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import MODEL_DOMAIN, MODEL_SPEC_ID, MODEL_VERSION
from app.ml.registry import load_latest_training_run, upsert_alpha_active_models
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class AlphaActiveModelFreezeResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


def freeze_alpha_active_model(
    settings: Settings,
    *,
    as_of_date: date,
    source: str,
    note: str | None = None,
    horizons: list[int] | None = None,
    model_spec_id: str = MODEL_SPEC_ID,
    train_end_date: date | None = None,
    promotion_type: str = "MANUAL_FREEZE",
) -> AlphaActiveModelFreezeResult:
    ensure_storage_layout(settings)
    target_horizons = list(dict.fromkeys(int(value) for value in (horizons or [1, 5])))
    reference_train_end_date = min(train_end_date or as_of_date, as_of_date)
    with activate_run_context("freeze_alpha_active_model", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_model_training_run", "fact_alpha_active_model"],
                notes=(
                    "Freeze active alpha model. "
                    f"as_of_date={as_of_date.isoformat()} horizons={target_horizons}"
                ),
            )
            try:
                selected_rows: list[dict[str, object]] = []
                for horizon in target_horizons:
                    training_row = load_latest_training_run(
                        connection,
                        horizon=int(horizon),
                        model_version=MODEL_VERSION,
                        model_domain=MODEL_DOMAIN,
                        model_spec_id=model_spec_id,
                        train_end_date=reference_train_end_date,
                    )
                    if training_row is None or not training_row.get("artifact_uri"):
                        continue
                    selected_rows.append(training_row)
                if not selected_rows:
                    notes = (
                        "Alpha active model freeze was a no-op. "
                        "model_spec_id="
                        f"{model_spec_id} "
                        "reference_train_end_date="
                        f"{reference_train_end_date.isoformat()}"
                    )
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        model_version=MODEL_VERSION,
                    )
                    return AlphaActiveModelFreezeResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                now_ts = now_local(settings.app.timezone)
                active_rows: list[dict[str, object]] = []
                for row in selected_rows:
                    connection.execute(
                        """
                        UPDATE fact_alpha_active_model
                        SET effective_to_date = ?, active_flag = FALSE, updated_at = ?
                        WHERE horizon = ?
                          AND effective_from_date <= ?
                          AND (effective_to_date IS NULL OR effective_to_date >= ?)
                          AND active_flag = TRUE
                        """,
                        [
                            as_of_date - timedelta(days=1),
                            now_ts,
                            int(row["horizon"]),
                            as_of_date,
                            as_of_date,
                        ],
                    )
                    active_rows.append(
                        {
                            "active_alpha_model_id": (
                                f"{run_context.run_id}-h{int(row['horizon'])}-{model_spec_id}"
                            ),
                            "horizon": int(row["horizon"]),
                            "model_spec_id": str(row.get("model_spec_id") or model_spec_id),
                            "training_run_id": str(row["training_run_id"]),
                            "model_version": str(row["model_version"]),
                            "source_type": source,
                            "promotion_type": promotion_type,
                            "promotion_report_json": None,
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "rollback_of_active_alpha_model_id": None,
                            "note": note,
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )
                upsert_alpha_active_models(connection, pd.DataFrame(active_rows))
                notes = (
                    "Alpha active model freeze completed. "
                    f"rows={len(active_rows)} model_spec_id={model_spec_id} "
                    f"reference_train_end_date={reference_train_end_date.isoformat()}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    model_version=MODEL_VERSION,
                )
                return AlphaActiveModelFreezeResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(active_rows),
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
                    notes="Alpha active model freeze failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise
