from __future__ import annotations

# ruff: noqa: E501
from dataclasses import dataclass
from datetime import date

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .meta_common import INTRADAY_META_MODEL_DOMAIN, INTRADAY_META_MODEL_VERSION


@dataclass(slots=True)
class IntradayMetaModelFrameworkValidationResult:
    run_id: str
    as_of_date: date
    check_count: int
    warning_count: int
    notes: str


def validate_intraday_meta_model_framework(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
) -> IntradayMetaModelFrameworkValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "validate_intraday_meta_model_framework",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=[
                    "fact_model_training_run",
                    "fact_model_metric_summary",
                    "fact_intraday_meta_prediction",
                    "fact_intraday_meta_decision",
                    "fact_intraday_active_meta_model",
                ],
                notes=f"Validate intraday meta-model framework for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                counts = connection.execute(
                    f"""
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM fact_model_training_run
                            WHERE model_domain = ?
                              AND model_version = ?
                              AND horizon IN ({placeholders})
                        ) AS training_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_model_metric_summary
                            WHERE model_domain = ?
                              AND model_version = ?
                              AND horizon IN ({placeholders})
                        ) AS metric_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_meta_prediction
                            WHERE session_date <= ?
                              AND horizon IN ({placeholders})
                        ) AS prediction_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_meta_decision
                            WHERE session_date <= ?
                              AND horizon IN ({placeholders})
                        ) AS decision_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_active_meta_model
                            WHERE effective_from_date <= ?
                              AND (effective_to_date IS NULL OR effective_to_date >= ?)
                              AND horizon IN ({placeholders})
                        ) AS active_count
                    """,
                    [
                        INTRADAY_META_MODEL_DOMAIN,
                        INTRADAY_META_MODEL_VERSION,
                        *horizons,
                        INTRADAY_META_MODEL_DOMAIN,
                        INTRADAY_META_MODEL_VERSION,
                        *horizons,
                        as_of_date,
                        *horizons,
                        as_of_date,
                        *horizons,
                        as_of_date,
                        as_of_date,
                        *horizons,
                    ],
                ).fetchone()
                overlap_count = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            horizon,
                            panel_name,
                            COUNT(*) AS row_count
                        FROM fact_intraday_active_meta_model
                        WHERE effective_from_date <= ?
                          AND (effective_to_date IS NULL OR effective_to_date >= ?)
                          AND active_flag = TRUE
                          AND horizon IN ({placeholders})
                        GROUP BY horizon, panel_name
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [as_of_date, as_of_date, *horizons],
                ).fetchone()[0]
                warnings = sum(int(value or 0) == 0 for value in counts)
                if int(overlap_count or 0) > 0:
                    warnings += 1
                notes = (
                    "Intraday meta-model framework validated. "
                    f"training={int(counts[0] or 0)} metrics={int(counts[1] or 0)} "
                    f"predictions={int(counts[2] or 0)} decisions={int(counts[3] or 0)} "
                    f"active={int(counts[4] or 0)} overlap={int(overlap_count or 0)} "
                    f"warnings={warnings}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayMetaModelFrameworkValidationResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    check_count=6,
                    warning_count=warnings,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday meta-model framework validation failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
