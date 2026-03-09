from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from app.common.paths import project_root
from app.ops.maintenance import enforce_retention_policies
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_retention_prunes_intraday_bar_but_keeps_core_curated(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    settings.paths.project_root = tmp_path

    old_bar = (
        settings.paths.curated_dir
        / "intraday"
        / "bar_1m"
        / "session_date=2026-01-01"
        / "bar_1m.parquet"
    )
    old_feature = (
        settings.paths.curated_dir
        / "features"
        / "as_of_date=2026-01-01"
        / "feature_snapshot.parquet"
    )
    old_bar.parent.mkdir(parents=True, exist_ok=True)
    old_feature.parent.mkdir(parents=True, exist_ok=True)
    old_bar.write_text("bar", encoding="utf-8")
    old_feature.write_text("feature", encoding="utf-8")

    stale_at = datetime.now(tz=timezone.utc) - timedelta(days=90)
    timestamp = stale_at.timestamp()
    os.utime(old_bar, (timestamp, timestamp))
    os.utime(old_feature, (timestamp, timestamp))

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        result = enforce_retention_policies(
            settings,
            connection=connection,
            dry_run=False,
            policy_config_path=project_root() / "config" / "ops" / "default_ops_policy.yaml",
        )

    assert result.row_count == 1
    assert not old_bar.exists()
    assert old_feature.exists()
