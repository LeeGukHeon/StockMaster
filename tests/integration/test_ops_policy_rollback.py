from __future__ import annotations

from datetime import datetime

from app.ops.policy import freeze_active_ops_policy, rollback_active_ops_policy
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_ops_policy_rollback_restores_previous_policy(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    freeze_active_ops_policy(
        settings,
        as_of_at=datetime(2026, 3, 8, 9, 0, 0),
        policy_config_path="config/ops/default_ops_policy.yaml",
        promotion_type="MANUAL_FREEZE",
        note="default freeze",
    )
    freeze_active_ops_policy(
        settings,
        as_of_at=datetime(2026, 3, 8, 10, 0, 0),
        policy_config_path="config/ops/conservative_ops_policy.yaml",
        promotion_type="MANUAL_FREEZE",
        note="conservative freeze",
    )
    rollback = rollback_active_ops_policy(
        settings,
        as_of_at=datetime(2026, 3, 8, 11, 0, 0),
        note="rollback test",
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        active = connection.execute(
            """
            SELECT policy_id, rollback_of_registry_id
            FROM fact_active_ops_policy
            WHERE active_flag = TRUE
            ORDER BY effective_from_at DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert rollback.policy_id == "default_ops_policy"
    assert active == ("default_ops_policy", "ops-policy-conservative_ops_policy-20260308T100000")
