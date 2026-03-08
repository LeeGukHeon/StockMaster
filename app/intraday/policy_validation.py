from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class IntradayPolicyFrameworkValidationResult:
    run_id: str
    as_of_date: date
    check_count: int
    warning_count: int
    notes: str


def validate_intraday_policy_framework(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
) -> IntradayPolicyFrameworkValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "validate_intraday_policy_framework",
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
                    "fact_intraday_policy_candidate",
                    "fact_intraday_policy_evaluation",
                    "fact_intraday_policy_ablation_result",
                    "fact_intraday_policy_selection_recommendation",
                    "fact_intraday_active_policy",
                ],
                notes=f"Validate intraday policy framework for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                counts = connection.execute(
                    f"""
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_policy_candidate
                            WHERE horizon IN ({placeholders})
                        ) AS candidate_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_policy_evaluation
                            WHERE horizon IN ({placeholders})
                        ) AS evaluation_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_policy_ablation_result
                            WHERE horizon IN ({placeholders})
                        ) AS ablation_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_policy_selection_recommendation
                            WHERE horizon IN ({placeholders})
                              AND recommendation_date <= ?
                        ) AS recommendation_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_active_policy
                            WHERE horizon IN ({placeholders})
                              AND effective_from_date <= ?
                              AND (
                                  effective_to_date IS NULL
                                  OR effective_to_date >= ?
                              )
                        ) AS active_count
                    """,
                    [
                        *horizons,
                        *horizons,
                        *horizons,
                        *horizons,
                        as_of_date,
                        *horizons,
                        as_of_date,
                        as_of_date,
                    ],
                ).fetchone()
                overlap_count = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            horizon,
                            scope_type,
                            scope_key,
                            COUNT(*) AS row_count
                        FROM fact_intraday_active_policy
                        WHERE horizon IN ({placeholders})
                          AND effective_from_date <= ?
                          AND (effective_to_date IS NULL OR effective_to_date >= ?)
                          AND active_flag = TRUE
                        GROUP BY horizon, scope_type, scope_key
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [*horizons, as_of_date, as_of_date],
                ).fetchone()[0]
                warnings = 0
                if int(counts[0] or 0) == 0:
                    warnings += 1
                if int(counts[1] or 0) == 0:
                    warnings += 1
                if int(counts[2] or 0) == 0:
                    warnings += 1
                if int(counts[3] or 0) == 0:
                    warnings += 1
                if int(overlap_count or 0) > 0:
                    warnings += 1
                notes = (
                    "Intraday policy framework validated. "
                    f"candidates={int(counts[0] or 0)} "
                    f"evaluations={int(counts[1] or 0)} "
                    f"ablations={int(counts[2] or 0)} "
                    f"recommendations={int(counts[3] or 0)} "
                    f"active={int(counts[4] or 0)} "
                    f"overlap={int(overlap_count or 0)} "
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
                return IntradayPolicyFrameworkValidationResult(
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
                    notes="Intraday policy framework validation failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
