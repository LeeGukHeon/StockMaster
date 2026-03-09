from __future__ import annotations

from datetime import date

from app.ops.bundles import run_daily_close_bundle, run_news_sync_bundle
from app.ops.common import JobStatus
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
