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
class IntradayStrategyValidationResult:
    run_id: str
    session_date: date
    check_count: int
    warning_count: int
    notes: str


def validate_intraday_strategy_pipeline(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int],
    ranking_version: str = SELECTION_ENGINE_VERSION,
) -> IntradayStrategyValidationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "validate_intraday_strategy_pipeline",
        as_of_date=session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=session_date,
                input_sources=[
                    "fact_intraday_market_context_snapshot",
                    "fact_intraday_regime_adjustment",
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_strategy_result",
                    "fact_intraday_strategy_comparison",
                    "fact_intraday_timing_calibration",
                ],
                notes=f"Validate intraday strategy pipeline for {session_date.isoformat()}",
                ranking_version=ranking_version,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                counts = connection.execute(
                    f"""
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_market_context_snapshot
                            WHERE session_date = ?
                        ) AS context_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_regime_adjustment
                            WHERE session_date = ?
                              AND ranking_version = ?
                        ) AS adjustment_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_adjusted_entry_decision
                            WHERE session_date = ?
                              AND ranking_version = ?
                        ) AS adjusted_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_strategy_result
                            WHERE session_date = ?
                              AND horizon IN ({placeholders})
                        ) AS strategy_result_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_strategy_comparison
                            WHERE end_session_date = ?
                              AND horizon IN ({placeholders})
                        ) AS comparison_count,
                        (
                            SELECT COUNT(*)
                            FROM fact_intraday_timing_calibration
                            WHERE window_end_date = ?
                              AND horizon IN ({placeholders})
                        ) AS calibration_count
                    """,
                    [
                        session_date,
                        session_date,
                        ranking_version,
                        session_date,
                        ranking_version,
                        session_date,
                        *horizons,
                        session_date,
                        *horizons,
                        session_date,
                        *horizons,
                    ],
                ).fetchone()
                forbidden_transition = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_intraday_adjusted_entry_decision
                    WHERE session_date = ?
                      AND raw_action = 'DATA_INSUFFICIENT'
                      AND adjusted_action = 'ENTER_NOW'
                    """,
                    [session_date],
                ).fetchone()[0]
                aggressive_transition = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM fact_intraday_adjusted_entry_decision
                    WHERE session_date = ?
                      AND raw_action = 'AVOID_TODAY'
                      AND adjusted_action = 'ENTER_NOW'
                    """,
                    [session_date],
                ).fetchone()[0]
                strategy_rows = connection.execute(
                    f"""
                    SELECT strategy_id, COUNT(*) AS row_count
                    FROM fact_intraday_strategy_result
                    WHERE session_date = ?
                      AND horizon IN ({placeholders})
                    GROUP BY strategy_id
                    ORDER BY strategy_id
                    """,
                    [session_date, *horizons],
                ).fetchdf()

                warnings = 0
                if int(counts[0] or 0) == 0:
                    warnings += 1
                if int(counts[1] or 0) == 0 or int(counts[2] or 0) == 0:
                    warnings += 1
                if int(counts[3] or 0) == 0:
                    warnings += 1
                if int(forbidden_transition or 0) > 0 or int(aggressive_transition or 0) > 0:
                    warnings += 1
                if strategy_rows.empty:
                    warnings += 1

                notes = (
                    "Intraday strategy pipeline validated. "
                    f"context={int(counts[0] or 0)} "
                    f"adjustment={int(counts[1] or 0)} "
                    f"adjusted={int(counts[2] or 0)} "
                    f"strategy={int(counts[3] or 0)} "
                    f"comparison={int(counts[4] or 0)} "
                    f"calibration={int(counts[5] or 0)} "
                    f"warnings={warnings}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=ranking_version,
                )
                return IntradayStrategyValidationResult(
                    run_id=run_context.run_id,
                    session_date=session_date,
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
                    notes=f"Intraday strategy validation failed for {session_date.isoformat()}",
                    error_message=str(exc),
                    ranking_version=ranking_version,
                )
                raise
