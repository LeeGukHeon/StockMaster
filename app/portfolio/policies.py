# ruff: noqa: E501

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

from .common import (
    PortfolioPolicyFreezeResult,
    load_portfolio_policy,
    select_active_portfolio_policy_row,
)


def freeze_active_portfolio_policy(
    settings: Settings,
    *,
    as_of_date: date,
    policy_config_path: str | Path,
    promotion_type: str = "MANUAL_FREEZE",
    note: str | None = None,
) -> PortfolioPolicyFreezeResult:
    ensure_storage_layout(settings)
    policy, path = load_portfolio_policy(settings, policy_config_path)
    with activate_run_context("freeze_active_portfolio_policy", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_policy_registry"],
                notes=f"Freeze active portfolio policy from {path}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                connection.execute(
                    """
                    UPDATE fact_portfolio_policy_registry
                    SET effective_to_date = ?, active_flag = FALSE, updated_at = ?
                    WHERE active_flag = TRUE
                    """,
                    [as_of_date - pd.Timedelta(days=1), now_local(settings.app.timezone)],
                )
                active_id = (
                    f"{run_context.run_id}-{policy.portfolio_policy_id}-{policy.portfolio_policy_version}"
                )
                frame = pd.DataFrame(
                    [
                        {
                            "active_portfolio_policy_id": active_id,
                            "portfolio_policy_id": policy.portfolio_policy_id,
                            "portfolio_policy_version": policy.portfolio_policy_version,
                            "display_name": policy.display_name,
                            "config_path": str(path),
                            "config_hash": policy.config_hash(),
                            "policy_payload_json": json.dumps(
                                policy.payload(),
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            "source_type": "config_file",
                            "promotion_type": promotion_type,
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "rollback_of_active_portfolio_policy_id": None,
                            "note": note,
                            "created_at": now_local(settings.app.timezone),
                            "updated_at": now_local(settings.app.timezone),
                        }
                    ]
                )
                connection.register("portfolio_policy_registry_stage", frame)
                connection.execute(
                    """
                    INSERT INTO fact_portfolio_policy_registry
                    SELECT * FROM portfolio_policy_registry_stage
                    """
                )
                connection.unregister("portfolio_policy_registry_stage")
                notes = (
                    "Active portfolio policy frozen. "
                    f"policy={policy.portfolio_policy_id}:{policy.portfolio_policy_version}"
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
                return PortfolioPolicyFreezeResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=1,
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
                    notes="Active portfolio policy freeze failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def rollback_active_portfolio_policy(
    settings: Settings,
    *,
    as_of_date: date,
    note: str | None = None,
) -> PortfolioPolicyFreezeResult:
    ensure_storage_layout(settings)
    with activate_run_context("rollback_active_portfolio_policy", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_portfolio_policy_registry"],
                notes=f"Rollback active portfolio policy at {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                current = select_active_portfolio_policy_row(connection, as_of_date=as_of_date)
                previous = connection.execute(
                    """
                    SELECT *
                    FROM fact_portfolio_policy_registry
                    WHERE active_flag = FALSE
                      AND effective_from_date < ?
                    ORDER BY effective_from_date DESC, created_at DESC
                    LIMIT 1
                    """,
                    [as_of_date],
                ).fetchdf()
                if current is None or previous.empty:
                    notes = "Portfolio policy rollback was a no-op."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return PortfolioPolicyFreezeResult(
                        run_id=run_context.run_id,
                        as_of_date=as_of_date,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                previous_row = previous.iloc[0]
                connection.execute(
                    """
                    UPDATE fact_portfolio_policy_registry
                    SET effective_to_date = ?, active_flag = FALSE, updated_at = ?
                    WHERE active_portfolio_policy_id = ?
                    """,
                    [
                        as_of_date - pd.Timedelta(days=1),
                        now_local(settings.app.timezone),
                        current["active_portfolio_policy_id"],
                    ],
                )
                frame = pd.DataFrame(
                    [
                        {
                            "active_portfolio_policy_id": f"{run_context.run_id}-rollback",
                            "portfolio_policy_id": previous_row["portfolio_policy_id"],
                            "portfolio_policy_version": previous_row["portfolio_policy_version"],
                            "display_name": previous_row["display_name"],
                            "config_path": previous_row["config_path"],
                            "config_hash": previous_row["config_hash"],
                            "policy_payload_json": previous_row["policy_payload_json"],
                            "source_type": "rollback",
                            "promotion_type": "MANUAL_ROLLBACK",
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "rollback_of_active_portfolio_policy_id": current[
                                "active_portfolio_policy_id"
                            ],
                            "note": note,
                            "created_at": now_local(settings.app.timezone),
                            "updated_at": now_local(settings.app.timezone),
                        }
                    ]
                )
                connection.register("portfolio_policy_rollback_stage", frame)
                connection.execute(
                    """
                    INSERT INTO fact_portfolio_policy_registry
                    SELECT * FROM portfolio_policy_rollback_stage
                    """
                )
                connection.unregister("portfolio_policy_rollback_stage")
                notes = "Active portfolio policy rolled back."
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return PortfolioPolicyFreezeResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=1,
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
                    notes="Active portfolio policy rollback failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise
