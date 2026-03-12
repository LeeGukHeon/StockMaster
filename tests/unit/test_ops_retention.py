from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from app.common.paths import project_root
from app.ops.common import JobStatus
from app.ops.maintenance import (
    _parse_reclaimed_bytes,
    cleanup_docker_build_cache,
    cleanup_model_artifacts,
    cleanup_stale_job_runs,
    enforce_retention_policies,
    reconcile_failed_runs,
)
from app.ops.repository import insert_recovery_action
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


def test_reconcile_failed_runs_does_not_requeue_targets_with_prior_recovery_action(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_job_run (
                run_id, job_name, trigger_type, status, as_of_date, started_at, finished_at,
                root_run_id, parent_run_id, recovery_of_run_id, lock_name, policy_id,
                policy_version, dry_run, step_count, failed_step_count, artifact_count,
                notes, error_message, details_json, created_at
            ) VALUES (
                'failed-run-1', 'run_daily_close_bundle', 'SCHEDULED', 'FAILED', DATE '2026-03-11',
                now(), now(), 'failed-run-1', NULL, NULL, 'scheduler_global_write', NULL, NULL,
                FALSE, 0, 0, 0, 'failed', 'error', NULL, now()
            )
            """
        )
        insert_recovery_action(
            connection,
            recovery_action_id="recovery-1",
            created_at=datetime.now(tz=timezone.utc),
            action_type="QUEUE_RECOVERY",
            status="SKIPPED",
            target_job_run_id="failed-run-1",
            triggered_by_run_id="test-job",
            recovery_run_id=None,
            lock_name="run_daily_close_bundle",
            notes="already reviewed",
            details={},
            finished_at=datetime.now(tz=timezone.utc),
        )
        result = reconcile_failed_runs(
            settings,
            connection=connection,
            job_run_id="test-job",
            limit=20,
        )
        count = connection.execute(
            "SELECT COUNT(*) FROM fact_recovery_action WHERE target_job_run_id = 'failed-run-1'"
        ).fetchone()[0]
    assert result.row_count == 0
    assert count == 1


def test_cleanup_model_artifacts_keeps_active_and_latest_runs(tmp_path) -> None:
    settings = build_test_settings(tmp_path)

    stale_artifact = settings.paths.artifacts_dir / "models" / "alpha" / "stale.pkl"
    active_artifact = settings.paths.artifacts_dir / "models" / "alpha" / "active.pkl"
    latest_artifact = settings.paths.artifacts_dir / "models" / "alpha" / "latest.pkl"
    stale_artifact.parent.mkdir(parents=True, exist_ok=True)
    stale_artifact.write_bytes(b"stale")
    active_artifact.write_bytes(b"active")
    latest_artifact.write_bytes(b"latest")

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_model_training_run (
                training_run_id, run_id, model_domain, model_version, model_spec_id,
                estimation_scheme, rolling_window_days, horizon, panel_name, train_end_date,
                training_window_start, training_window_end, validation_window_start, validation_window_end,
                train_row_count, validation_row_count, train_session_count, validation_session_count,
                feature_count, ensemble_weight_json, model_family_json, threshold_payload_json,
                diagnostic_artifact_uri, metadata_json, fallback_flag, fallback_reason, artifact_uri,
                notes, status, created_at
            ) VALUES
            (
                'stale-run', 'run-1', 'alpha', 'alpha_model_v1', 'alpha_recursive_expanding_v1',
                'recursive', NULL, 1, NULL, DATE '2026-03-09',
                NULL, NULL, NULL, NULL, 100, 20, NULL, NULL,
                10, '{}', '{}', NULL, NULL, NULL, FALSE, NULL, ?, 'stale', 'success', TIMESTAMPTZ '2026-03-09 10:00:00+00'
            ),
            (
                'active-run', 'run-2', 'alpha', 'alpha_model_v1', 'alpha_recursive_expanding_v1',
                'recursive', NULL, 1, NULL, DATE '2026-03-10',
                NULL, NULL, NULL, NULL, 100, 20, NULL, NULL,
                10, '{}', '{}', NULL, NULL, NULL, FALSE, NULL, ?, 'active', 'success', TIMESTAMPTZ '2026-03-10 10:00:00+00'
            ),
            (
                'latest-run', 'run-3', 'alpha', 'alpha_model_v1', 'alpha_recursive_expanding_v1',
                'recursive', NULL, 1, NULL, DATE '2026-03-11',
                NULL, NULL, NULL, NULL, 100, 20, NULL, NULL,
                10, '{}', '{}', NULL, NULL, NULL, FALSE, NULL, ?, 'latest', 'success', TIMESTAMPTZ '2026-03-11 10:00:00+00'
            )
            """,
            [str(stale_artifact), str(active_artifact), str(latest_artifact)],
        )
        connection.execute(
            """
            INSERT INTO fact_alpha_active_model (
                active_alpha_model_id, horizon, model_spec_id, training_run_id, model_version,
                source_type, promotion_type, promotion_report_json, effective_from_date, effective_to_date,
                active_flag, rollback_of_active_alpha_model_id, note, created_at, updated_at
            ) VALUES (
                'active-model-1', 1, 'alpha_recursive_expanding_v1', 'active-run', 'alpha_model_v1',
                'manual', 'MANUAL_FREEZE', NULL, DATE '2026-03-10', NULL,
                TRUE, NULL, NULL, now(), now()
            )
            """
        )

        result = cleanup_model_artifacts(
            settings,
            connection=connection,
            dry_run=False,
            policy_config_path=project_root() / "config" / "ops" / "default_ops_policy.yaml",
        )
        cleanup_rows = connection.execute(
            """
            SELECT removed_file_count, reclaimed_bytes
            FROM fact_retention_cleanup_run
            WHERE cleanup_scope = 'MODEL_ARTIFACTS'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    assert result.status == JobStatus.SUCCESS
    assert result.row_count == 1
    assert not stale_artifact.exists()
    assert active_artifact.exists()
    assert latest_artifact.exists()
    assert cleanup_rows[0] == 1
    assert cleanup_rows[1] >= 5


def test_cleanup_stale_job_runs_updates_postgres_when_enabled(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    captured_calls: list[tuple[str, list[object]]] = []
    monkeypatch.setattr("app.ops.maintenance.execute_postgres_sql", lambda _settings, query, params: captured_calls.append((query, list(params or []))))

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        started_at = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
        connection.execute(
            """
            INSERT INTO fact_job_run (
                run_id, job_name, trigger_type, status, as_of_date, started_at, finished_at,
                root_run_id, parent_run_id, recovery_of_run_id, lock_name, policy_id,
                policy_version, dry_run, step_count, failed_step_count, artifact_count,
                notes, error_message, details_json, created_at
            ) VALUES (
                'stale-run-1', 'run_daily_close_bundle', 'SCHEDULED', 'RUNNING', DATE '2026-03-12',
                ?, NULL, 'stale-run-1', NULL, NULL, 'scheduler_global_write', NULL, NULL,
                FALSE, 0, 0, 0, 'running', NULL, '{}', ?
            )
            """,
            [started_at, started_at],
        )
        connection.execute(
            """
            INSERT INTO fact_job_step_run (
                step_run_id, job_run_id, step_name, step_order, status, started_at, finished_at,
                critical_flag, notes, error_message, details_json, created_at
            ) VALUES (
                'stale-step-1', 'stale-run-1', 'daily_pipeline', 1, 'RUNNING', ?, NULL,
                TRUE, NULL, NULL, NULL, ?
            )
            """,
            [started_at, started_at],
        )

        result = cleanup_stale_job_runs(
            settings,
            connection=connection,
            stale_after_minutes=10,
        )

        job_row = connection.execute(
            "SELECT status, failed_step_count FROM fact_job_run WHERE run_id = 'stale-run-1'"
        ).fetchone()
        step_row = connection.execute(
            "SELECT status FROM fact_job_step_run WHERE job_run_id = 'stale-run-1'"
        ).fetchone()

    assert result.row_count == 1
    assert job_row == ("FAILED", 1)
    assert step_row == ("FAILED",)
    assert len(captured_calls) == 3
