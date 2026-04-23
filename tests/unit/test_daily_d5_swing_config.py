from __future__ import annotations

from datetime import date

import duckdb

from app.ml.constants import D5_PRIMARY_FOCUS_MODEL_SPEC_ID
from app.scheduler.jobs import (
    _candidate_model_specs_for_daily_pipeline,
    _d5_swing_bootstrap_required,
)
from app.storage.duckdb import bootstrap_core_tables


def test_daily_pipeline_includes_d5_primary_spec_only_when_active_swing_enabled() -> None:
    default_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=False)
    ]
    active_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=True)
    ]

    assert D5_PRIMARY_FOCUS_MODEL_SPEC_ID not in default_ids
    assert active_ids.count(D5_PRIMARY_FOCUS_MODEL_SPEC_ID) == 1


def _insert_active_model(
    connection: duckdb.DuckDBPyConnection,
    *,
    active_alpha_model_id: str,
    model_spec_id: str,
    effective_from_date: date,
    effective_to_date: date | None = None,
    active_flag: bool = True,
    source_type: str = "test",
) -> None:
    connection.execute(
        """
        INSERT INTO fact_alpha_active_model (
            active_alpha_model_id,
            horizon,
            model_spec_id,
            training_run_id,
            model_version,
            source_type,
            promotion_type,
            promotion_report_json,
            effective_from_date,
            effective_to_date,
            active_flag,
            rollback_of_active_alpha_model_id,
            note,
            created_at,
            updated_at
        )
        VALUES (?, 5, ?, ?, 'alpha_model_v1', ?, 'MANUAL_FREEZE', NULL, ?, ?, ?, NULL, 'seed', now(), now())
        """,
        [
            active_alpha_model_id,
            model_spec_id,
            f"train-{active_alpha_model_id}",
            source_type,
            effective_from_date,
            effective_to_date,
            active_flag,
        ],
    )


def test_d5_swing_bootstrap_runs_once_then_auto_promotion_owns_h5() -> None:
    connection = duckdb.connect(":memory:")
    bootstrap_core_tables(connection)

    assert _d5_swing_bootstrap_required(
        connection,
        as_of_date=date(2026, 4, 15),
    ) is True

    _insert_active_model(
        connection,
        active_alpha_model_id="d5-bootstrap",
        model_spec_id=D5_PRIMARY_FOCUS_MODEL_SPEC_ID,
        effective_from_date=date(2026, 4, 15),
        effective_to_date=date(2026, 4, 15),
        active_flag=False,
        source_type="daily_close_d5_swing_bootstrap",
    )
    _insert_active_model(
        connection,
        active_alpha_model_id="auto-promoted-comparator",
        model_spec_id="alpha_swing_d5_v1",
        effective_from_date=date(2026, 4, 16),
        active_flag=True,
        source_type="alpha_auto_promotion",
    )

    assert _d5_swing_bootstrap_required(
        connection,
        as_of_date=date(2026, 4, 16),
    ) is False
