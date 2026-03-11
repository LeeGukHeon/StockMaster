from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from app.common.paths import project_root
from app.ops.common import JobStatus
from app.ops.maintenance import (
    _parse_reclaimed_bytes,
    cleanup_docker_build_cache,
    enforce_retention_policies,
)
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


def test_parse_reclaimed_bytes_supports_human_units() -> None:
    assert _parse_reclaimed_bytes("Total reclaimed space: 54.73GB") == int(54.73 * (1024**3))
    assert _parse_reclaimed_bytes("Total reclaimed space: 512MB") == 512 * (1024**2)
    assert _parse_reclaimed_bytes("no reclaimed summary") == 0


def test_cleanup_docker_build_cache_skips_when_docker_missing(monkeypatch, tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    monkeypatch.setattr("app.ops.maintenance.shutil.which", lambda _name: None)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        result = cleanup_docker_build_cache(
            settings,
            connection=connection,
            dry_run=False,
        )
    assert result.status == JobStatus.SKIPPED
    assert "Docker CLI is not available" in result.notes


def test_cleanup_docker_build_cache_dry_run_writes_artifact(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        result = cleanup_docker_build_cache(
            settings,
            connection=connection,
            dry_run=True,
        )
    assert result.status == JobStatus.SUCCESS
    assert result.artifact_paths
