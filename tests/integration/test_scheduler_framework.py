from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from app.ops.bundles import (
    _resolve_intraday_session_start_date,
    run_daily_close_bundle,
    run_daily_evaluation_bundle,
    run_docker_build_cache_cleanup_bundle,
    run_evaluation_bundle,
    run_news_sync_bundle,
    run_ops_maintenance_bundle,
    run_weekly_calibration_bundle,
    run_weekly_training_bundle,
)
from app.ops.common import JobStatus, TriggerType
from app.ops.scheduler import get_scheduled_job, read_scheduler_state
from app.ops.serial import acquire_serial_lock, release_serial_lock
from app.storage.duckdb import duckdb_connection
from scripts import _scheduler_cli
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_run_scheduled_bundle_is_idempotent_for_same_identity(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    monkeypatch.setattr(_scheduler_cli, "load_cli_settings", lambda: settings)

    calls: list[str] = []

    def runner(runtime_settings):
        calls.append(str(runtime_settings.paths.duckdb_path))
        return _scheduler_cli.bundle_result(
            job_key="daily_close",
            status=JobStatus.SUCCESS,
            notes="bundle completed",
            run_ids=["job-run-1"],
            as_of_date=date(2026, 3, 9),
            row_count=12,
        )

    identity = {"as_of_date": "2026-03-09"}

    first_code = _scheduler_cli.run_scheduled_bundle(
        job_key="daily_close",
        runner=runner,
        identity=identity,
    )
    second_code = _scheduler_cli.run_scheduled_bundle(
        job_key="daily_close",
        runner=runner,
        identity=identity,
    )
    state = read_scheduler_state(settings, "daily_close")

    assert first_code == 0
    assert second_code == 0
    assert len(calls) == 1
    assert state["status"] == JobStatus.SKIPPED_ALREADY_DONE
    assert state["identity"] == identity


def test_run_scheduled_bundle_skips_when_serial_lock_is_occupied(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    monkeypatch.setattr(_scheduler_cli, "load_cli_settings", lambda: settings)

    calls: list[str] = []

    def runner(_runtime_settings):
        calls.append("called")
        return _scheduler_cli.bundle_result(
            job_key="daily_close",
            status=JobStatus.SUCCESS,
            notes="should not run",
        )

    job = get_scheduled_job("daily_close")
    handle = acquire_serial_lock(
        settings,
        lock_key=job.serial_scope,
        owner_run_id="other-run",
        job_name="occupied",
    )
    try:
        exit_code = _scheduler_cli.run_scheduled_bundle(
            job_key="daily_close",
            runner=runner,
            identity={"as_of_date": "2026-03-10"},
        )
    finally:
        release_serial_lock(handle)

    state = read_scheduler_state(settings, "daily_close")

    assert exit_code == 0
    assert not calls
    assert state["status"] == JobStatus.SKIPPED_LOCKED


def test_news_sync_bundle_uses_calendar_day_identity_on_weekend(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    result = run_news_sync_bundle(
        settings,
        as_of_date=date(2026, 3, 7),
        profile="after_close",
        dry_run=True,
    )

    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        as_of_date = connection.execute(
            """
            SELECT as_of_date
            FROM fact_job_run
            WHERE job_name = 'run_news_sync_bundle'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()[0]

    assert result.status == JobStatus.SKIPPED
    assert result.as_of_date == date(2026, 3, 7)
    assert as_of_date == date(2026, 3, 7)


def test_daily_close_bundle_self_skips_on_non_trading_day(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    result = run_daily_close_bundle(
        settings,
        as_of_date=date(2026, 3, 7),
        dry_run=True,
        force=True,
        publish_discord=False,
    )

    assert result.status == JobStatus.SKIPPED_NON_TRADING_DAY


def test_daily_close_bundle_passes_requested_date_to_daily_pipeline(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    captured: dict[str, date | None] = {}
    noop_result = SimpleNamespace(artifact_paths=[])

    def fake_daily_pipeline_job(_settings, *, pipeline_date=None, **_kwargs):
        captured["pipeline_date"] = pipeline_date
        return noop_result

    monkeypatch.setattr("app.ops.bundles.run_daily_pipeline_job", fake_daily_pipeline_job)
    monkeypatch.setattr("app.ops.bundles.build_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.validate_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_target_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_rebalance_plan", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_position_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_daily_research_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_portfolio_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._refresh_release_views", lambda *a, **k: None)

    result = run_daily_close_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        force=True,
        publish_discord=False,
    )

    assert result.status == JobStatus.SUCCESS
    assert captured["pipeline_date"] == date(2026, 3, 9)


def test_daily_close_recovery_disables_discord_publish(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    captured: dict[str, object] = {}
    publish_calls: list[dict[str, object]] = []
    noop_result = SimpleNamespace(artifact_paths=[], published=False)

    def fake_daily_pipeline_job(_settings, *, pipeline_date=None, publish_discord=None, **_kwargs):
        captured["pipeline_date"] = pipeline_date
        captured["publish_discord"] = publish_discord
        return noop_result

    def fake_publish_discord_eod_report(_settings, *, as_of_date, dry_run, **_kwargs):
        publish_calls.append({"as_of_date": as_of_date, "dry_run": dry_run})
        return noop_result

    monkeypatch.setattr("app.ops.bundles.run_daily_pipeline_job", fake_daily_pipeline_job)
    monkeypatch.setattr("app.ops.bundles.build_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.validate_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_target_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_rebalance_plan", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_position_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_daily_research_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_portfolio_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.publish_discord_eod_report", fake_publish_discord_eod_report)
    monkeypatch.setattr("app.ops.bundles._refresh_release_views", lambda *a, **k: None)

    result = run_daily_close_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        trigger_type=TriggerType.RECOVERY,
        force=True,
        publish_discord=True,
    )

    assert result.status == JobStatus.SUCCESS
    assert captured["pipeline_date"] == date(2026, 3, 9)
    assert captured["publish_discord"] is False
    assert publish_calls == [{"as_of_date": date(2026, 3, 9), "dry_run": True}]


def test_daily_close_bundle_publishes_eod_after_post_close_steps(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    captured: dict[str, object] = {}
    publish_calls: list[dict[str, object]] = []
    noop_result = SimpleNamespace(artifact_paths=[], published=False)

    def fake_daily_pipeline_job(_settings, *, pipeline_date=None, publish_discord=None, **_kwargs):
        captured["pipeline_date"] = pipeline_date
        captured["publish_discord"] = publish_discord
        return noop_result

    def fake_publish_discord_eod_report(_settings, *, as_of_date, dry_run, **_kwargs):
        publish_calls.append({"as_of_date": as_of_date, "dry_run": dry_run})
        return noop_result

    monkeypatch.setattr("app.ops.bundles.run_daily_pipeline_job", fake_daily_pipeline_job)
    monkeypatch.setattr("app.ops.bundles.build_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.validate_portfolio_candidate_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_target_book", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_rebalance_plan", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_portfolio_position_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_daily_research_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_portfolio_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.publish_discord_eod_report", fake_publish_discord_eod_report)
    monkeypatch.setattr("app.ops.bundles._refresh_release_views", lambda *a, **k: None)

    result = run_daily_close_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        force=True,
        publish_discord=True,
    )

    assert result.status == JobStatus.SUCCESS
    assert captured["pipeline_date"] == date(2026, 3, 9)
    assert captured["publish_discord"] is False
    assert publish_calls == [{"as_of_date": date(2026, 3, 9), "dry_run": True}]


def test_news_sync_recovery_suppresses_close_brief_publish(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    calls: list[bool] = []
    noop_result = SimpleNamespace(artifact_paths=[])

    monkeypatch.setattr("app.ops.bundles.sync_news_metadata", lambda *a, **k: noop_result)

    def fake_publish_discord_close_brief(_settings, *, dry_run=False, **_kwargs):
        calls.append(bool(dry_run))
        return noop_result

    monkeypatch.setattr("app.ops.bundles.publish_discord_close_brief", fake_publish_discord_close_brief)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._refresh_release_views", lambda *a, **k: None)

    result = run_news_sync_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        profile="after_close",
        trigger_type=TriggerType.RECOVERY,
        force=True,
        dry_run=False,
    )

    assert result.status == JobStatus.SUCCESS
    assert calls == [True]


def test_ops_maintenance_scheduled_run_suppresses_discord_publish(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    calls: list[bool] = []
    noop_result = SimpleNamespace(artifact_paths=[], status=JobStatus.SUCCESS, notes="ok")

    monkeypatch.setattr("app.ops.bundles.check_pipeline_dependencies", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.summarize_storage_usage", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.cleanup_docker_build_cache", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.rotate_and_compress_logs", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.cleanup_disk_watermark", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.cleanup_stale_job_runs", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.reset_open_recovery_actions", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.reconcile_failed_runs", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.recover_incomplete_runs", lambda *a, **k: noop_result)

    def fake_publish_discord_ops_alerts(_settings, *, dry_run=False, **_kwargs):
        calls.append(bool(dry_run))
        return noop_result

    monkeypatch.setattr("app.ops.bundles.publish_discord_ops_alerts", fake_publish_discord_ops_alerts)

    result = run_ops_maintenance_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        trigger_type=TriggerType.SCHEDULED,
        dry_run=False,
    )

    assert result.status == JobStatus.SUCCESS
    assert calls == [True]


def test_docker_build_cache_cleanup_bundle_runs_cleanup_step(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    calls: list[dict[str, object]] = []
    noop_result = SimpleNamespace(artifact_paths=[], status=JobStatus.SUCCESS, notes="ok")

    def fake_cleanup_docker_build_cache(_settings, *, dry_run=False, **_kwargs):
        calls.append({"dry_run": dry_run})
        return noop_result

    monkeypatch.setattr("app.ops.bundles.cleanup_docker_build_cache", fake_cleanup_docker_build_cache)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)

    result = run_docker_build_cache_cleanup_bundle(
        settings,
        as_of_date=date(2026, 3, 9),
        dry_run=False,
    )

    assert result.status == JobStatus.SUCCESS
    assert calls == [{"dry_run": False}]


def test_evaluation_bundle_passes_requested_date_to_evaluation_job(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    captured: dict[str, date | None] = {}
    noop_result = SimpleNamespace(artifact_paths=[])

    def fake_evaluation_job(_settings, *, selection_end_date=None, **_kwargs):
        captured["selection_end_date"] = selection_end_date
        return noop_result

    monkeypatch.setattr("app.ops.bundles.run_evaluation_job", fake_evaluation_job)
    monkeypatch.setattr("app.ops.bundles.evaluate_portfolio_policies", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.render_evaluation_report", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.check_pipeline_dependencies", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._block_if_required_snapshot_missing", lambda *a, **k: None)
    monkeypatch.setattr("app.ops.bundles._refresh_release_views", lambda *a, **k: None)

    result = run_evaluation_bundle(
        settings,
        as_of_date=date(2026, 3, 10),
        force=True,
    )

    assert result.status == JobStatus.SUCCESS
    assert captured["selection_end_date"] == date(2026, 3, 10)


def test_daily_evaluation_bundle_uses_requested_date_not_latest_selection_date(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    captured: dict[str, date | None] = {}
    noop_result = SimpleNamespace(artifact_paths=[])

    def fake_evaluation_job(_settings, *, selection_end_date=None, **_kwargs):
        captured["selection_end_date"] = selection_end_date
        return noop_result

    monkeypatch.setattr("app.ops.bundles.run_evaluation_job", fake_evaluation_job)
    monkeypatch.setattr("app.ops.bundles.evaluate_portfolio_policies", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.check_pipeline_dependencies", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._block_if_required_snapshot_missing", lambda *a, **k: None)
    monkeypatch.setattr(
        "app.ops.bundles._resolve_latest_selection_date",
        lambda *_args, **_kwargs: date(2026, 3, 12),
    )

    result = run_daily_evaluation_bundle(
        settings,
        as_of_date=date(2026, 3, 13),
        dry_run=False,
    )

    assert result.status == JobStatus.SUCCESS
    assert captured["selection_end_date"] == date(2026, 3, 13)


def test_evaluation_bundle_blocks_when_daily_close_outputs_are_stale(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    monkeypatch.setattr(
        "app.ops.bundles.expected_job_reference_date",
        lambda *_args, **_kwargs: date(2026, 3, 13),
    )
    monkeypatch.setattr(
        "app.ops.bundles._latest_table_date",
        lambda *_args, **_kwargs: date(2026, 3, 12),
    )

    result = run_evaluation_bundle(
        settings,
        as_of_date=date(2026, 3, 13),
        force=True,
        dry_run=False,
    )

    assert result.status == JobStatus.BLOCKED
    assert "required snapshot 'selection_engine_v2' is stale" in result.notes


def test_weekly_calibration_bundle_does_not_pass_split_counts_to_calibration(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    expected_start = date(2026, 3, 2)
    noop_result = SimpleNamespace(
        artifact_paths=[],
        status=JobStatus.SUCCESS,
        notes="ok",
        row_count=0,
    )
    captured_calls: list[dict[str, object]] = []
    outcome_calls: list[dict[str, object]] = []

    def fake_calibration(_settings, **kwargs):
        captured_calls.append(dict(kwargs))
        return noop_result

    monkeypatch.setattr(
        "app.ops.bundles._resolve_intraday_session_start_date",
        lambda *a, **k: expected_start,
    )
    monkeypatch.setattr(
        "app.ops.bundles.materialize_intraday_policy_candidates",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.materialize_intraday_decision_outcomes",
        lambda *a, **k: outcome_calls.append(dict(k)) or noop_result,
    )
    monkeypatch.setattr("app.ops.bundles.run_intraday_policy_calibration", fake_calibration)
    monkeypatch.setattr("app.ops.bundles.run_intraday_policy_walkforward", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles.evaluate_intraday_policy_ablation", lambda *a, **k: noop_result)
    monkeypatch.setattr(
        "app.ops.bundles.materialize_intraday_policy_recommendations",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr("app.ops.bundles.calibrate_intraday_meta_thresholds", lambda *a, **k: noop_result)
    monkeypatch.setattr(
        "app.ops.bundles.render_intraday_policy_research_report",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.materialize_intraday_research_capability",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._skip_if_intraday_feature_disabled", lambda *a, **k: None)

    result = run_weekly_calibration_bundle(
        settings,
        as_of_date=date(2026, 3, 13),
        force=True,
        dry_run=False,
    )

    assert result.status == JobStatus.DEGRADED_SUCCESS
    assert len(outcome_calls) == 2
    assert [call["horizons"] for call in outcome_calls] == [[1], [5]]
    assert len(captured_calls) == 2
    assert [call["horizons"] for call in captured_calls] == [[1], [5]]
    assert all(call["start_session_date"] == expected_start for call in captured_calls)
    assert all(call["split_version"] == "wf_40_10_10_step5" for call in captured_calls)
    assert all(call["refresh_decision_outcomes"] is False for call in captured_calls)
    assert all("train_sessions" not in call for call in captured_calls)
    assert all("validation_sessions" not in call for call in captured_calls)
    assert all("test_sessions" not in call for call in captured_calls)
    assert all("step_sessions" not in call for call in captured_calls)


def test_weekly_training_bundle_splits_heavy_steps_by_horizon(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    expected_start = date(2026, 3, 2)
    noop_result = SimpleNamespace(
        artifact_paths=[],
        status=JobStatus.SUCCESS,
        notes="ok",
        row_count=0,
    )
    training_calls: list[dict[str, object]] = []
    walkforward_calls: list[dict[str, object]] = []
    evaluation_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "app.ops.bundles._resolve_intraday_session_start_date",
        lambda *a, **k: expected_start,
    )
    monkeypatch.setattr(
        "app.ops.bundles.train_intraday_meta_models",
        lambda *a, **k: training_calls.append(dict(k)) or noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.run_intraday_meta_walkforward",
        lambda *a, **k: walkforward_calls.append(dict(k)) or noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.evaluate_intraday_meta_models",
        lambda *a, **k: evaluation_calls.append(dict(k)) or noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.render_intraday_meta_model_report",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr(
        "app.ops.bundles.materialize_intraday_research_capability",
        lambda *a, **k: noop_result,
    )
    monkeypatch.setattr("app.ops.bundles.materialize_health_snapshots", lambda *a, **k: noop_result)
    monkeypatch.setattr("app.ops.bundles._skip_if_intraday_feature_disabled", lambda *a, **k: None)

    result = run_weekly_training_bundle(
        settings,
        as_of_date=date(2026, 3, 13),
        force=True,
        dry_run=False,
    )

    assert result.status == JobStatus.DEGRADED_SUCCESS
    assert [call["horizons"] for call in training_calls] == [[1], [5]]
    assert [call["horizons"] for call in walkforward_calls] == [[1], [5]]
    assert [call["horizons"] for call in evaluation_calls] == [[1], [5]]
    assert all(call["start_session_date"] == expected_start for call in walkforward_calls)
    assert all(call["start_session_date"] == expected_start for call in evaluation_calls)


def test_resolve_intraday_session_start_date_uses_available_adjusted_sessions(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    session_dates = [
        date(2026, 2, 26),
        date(2026, 2, 27),
        date(2026, 3, 2),
        date(2026, 3, 4),
        date(2026, 3, 6),
        date(2026, 3, 9),
    ]

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        rows = [
            (
                "adjusted-test",
                session_date,
                session_date,
                "005930",
                1,
                "09:05",
                "selection_engine_v2",
                "RISK_ON",
                "default",
                "ENTER_NOW",
                "ENTER_NOW",
                65.0,
                65.0,
                None,
                "strong",
                True,
                False,
                None,
                None,
                None,
            )
            for session_date in session_dates
        ]
        connection.executemany(
            """
            INSERT INTO fact_intraday_adjusted_entry_decision (
                run_id,
                selection_date,
                session_date,
                symbol,
                horizon,
                checkpoint_time,
                ranking_version,
                market_regime_family,
                adjustment_profile,
                raw_action,
                adjusted_action,
                raw_timing_score,
                adjusted_timing_score,
                selection_confidence_bucket,
                signal_quality_flag,
                eligible_to_execute_flag,
                fallback_flag,
                adjustment_reason_codes_json,
                risk_flags_json,
                decision_notes_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """,
            rows,
        )
        start_date = _resolve_intraday_session_start_date(
            settings,
            end_date=date(2026, 3, 9),
            required_sessions=4,
            connection=connection,
        )

    assert start_date == date(2026, 3, 2)
