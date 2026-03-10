# ruff: noqa: E501

from __future__ import annotations

from datetime import date

import duckdb

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection, duckdb_snapshot_connection
from app.storage.manifests import record_run_finish, record_run_start

from .common import PortfolioValidationResult


def validate_portfolio_framework(
    settings: Settings,
    *,
    as_of_date: date,
) -> PortfolioValidationResult:
    ensure_storage_layout(settings)

    def _collect_counts(connection):
        counts = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM fact_portfolio_policy_registry) AS policy_count,
                (SELECT COUNT(*) FROM fact_portfolio_candidate) AS candidate_count,
                (SELECT COUNT(*) FROM fact_portfolio_target_book) AS target_count,
                (SELECT COUNT(*) FROM fact_portfolio_rebalance_plan) AS rebalance_count,
                (SELECT COUNT(*) FROM fact_portfolio_position_snapshot) AS position_count,
                (SELECT COUNT(*) FROM fact_portfolio_nav_snapshot) AS nav_count,
                (SELECT COUNT(*) FROM fact_portfolio_evaluation_summary) AS evaluation_count
            """
        ).fetchone()
        overlap_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT COUNT(*) AS row_count
                FROM fact_portfolio_policy_registry
                WHERE effective_from_date <= ?
                  AND (effective_to_date IS NULL OR effective_to_date >= ?)
                  AND active_flag = TRUE
                HAVING COUNT(*) > 1
            )
            """,
            [as_of_date, as_of_date],
        ).fetchone()[0]
        negative_target_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_portfolio_target_book
            WHERE target_weight < 0
            """,
        ).fetchone()[0]
        gross_exposure_breach = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    snapshot_date,
                    execution_mode,
                    SUM(CASE WHEN symbol <> '__CASH__' THEN actual_weight ELSE 0 END) AS exposure
                FROM fact_portfolio_position_snapshot
                GROUP BY snapshot_date, execution_mode
                HAVING SUM(CASE WHEN symbol <> '__CASH__' THEN actual_weight ELSE 0 END) > 1.000001
            )
            """
        ).fetchone()[0]
        return counts, overlap_count, negative_target_count, gross_exposure_breach

    def _build_result(run_id: str, counts, overlap_count, negative_target_count, gross_exposure_breach, *, suffix: str = ""):
        warnings = 0
        if int(counts[1] or 0) == 0:
            warnings += 1
        if int(counts[2] or 0) == 0:
            warnings += 1
        if int(counts[3] or 0) == 0:
            warnings += 1
        if int(counts[4] or 0) == 0:
            warnings += 1
        if int(counts[5] or 0) == 0:
            warnings += 1
        if int(negative_target_count or 0) > 0:
            warnings += 1
        if int(gross_exposure_breach or 0) > 0:
            warnings += 1
        if int(overlap_count or 0) > 0:
            warnings += 1
        notes = (
            "Portfolio framework validated. "
            f"policy={int(counts[0] or 0)} "
            f"candidate={int(counts[1] or 0)} "
            f"target={int(counts[2] or 0)} "
            f"rebalance={int(counts[3] or 0)} "
            f"position={int(counts[4] or 0)} "
            f"nav={int(counts[5] or 0)} "
            f"evaluation={int(counts[6] or 0)} "
            f"overlap={int(overlap_count or 0)} warnings={warnings}"
        )
        if suffix:
            notes = f"{notes} {suffix}"
        return PortfolioValidationResult(
            run_id=run_id,
            as_of_date=as_of_date,
            check_count=10,
            artifact_paths=[],
            notes=notes,
        ), notes

    with activate_run_context("validate_portfolio_framework", as_of_date=as_of_date) as run_context:
        try:
            with duckdb_connection(settings.paths.duckdb_path) as connection:
                bootstrap_core_tables(connection)
                record_run_start(
                    connection,
                    run_id=run_context.run_id,
                    run_type=run_context.run_type,
                    started_at=run_context.started_at,
                    as_of_date=as_of_date,
                    input_sources=[
                        "fact_portfolio_policy_registry",
                        "fact_portfolio_candidate",
                        "fact_portfolio_target_book",
                        "fact_portfolio_rebalance_plan",
                        "fact_portfolio_position_snapshot",
                        "fact_portfolio_nav_snapshot",
                        "fact_portfolio_evaluation_summary",
                    ],
                    notes=f"Validate portfolio framework for {as_of_date.isoformat()}",
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                try:
                    counts, overlap_count, negative_target_count, gross_exposure_breach = _collect_counts(connection)
                    result, notes = _build_result(
                        run_context.run_id,
                        counts,
                        overlap_count,
                        negative_target_count,
                        gross_exposure_breach,
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
                    return result
                except Exception as exc:
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="failed",
                        output_artifacts=[],
                        notes="Portfolio framework validation failed.",
                        error_message=str(exc),
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    raise
        except duckdb.IOException as exc:
            message = str(exc).lower()
            if (
                "could not set lock on file" not in message
                and "cannot open file" not in message
                and "다른 프로세스" not in message
            ):
                raise
            with duckdb_snapshot_connection(settings.paths.duckdb_path) as connection:
                counts, overlap_count, negative_target_count, gross_exposure_breach = _collect_counts(connection)
                result, _ = _build_result(
                    run_context.run_id,
                    counts,
                    overlap_count,
                    negative_target_count,
                    gross_exposure_breach,
                    suffix="(snapshot read-only fallback due active writer lock)",
                )
            return result
