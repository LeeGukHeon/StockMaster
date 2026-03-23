from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from app.common.disk import DiskWatermark
from app.ops.common import JobStatus
from app.ops.health import check_pipeline_dependencies, materialize_health_snapshots
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def _fake_scalar_factory(
    *,
    latest_universe: date | None,
    latest_calendar: date | None,
    latest_selection: date | None,
    latest_prediction: date | None,
    latest_portfolio_target: date | None,
    latest_nav: date | None,
    latest_evaluation: date | None,
):
    def _fake_scalar(_connection, query: str, params=None):
        normalized = " ".join(query.split())
        mapping = {
            "SELECT MAX(as_of_date) FROM dim_symbol": latest_universe,
            "SELECT MAX(trading_date) FROM dim_trading_calendar WHERE is_trading_day": latest_calendar,
            (
                "SELECT MAX(as_of_date) FROM fact_ranking "
                "WHERE ranking_version = 'selection_engine_v2'"
            ): latest_selection,
            (
                "SELECT MAX(as_of_date) FROM fact_prediction "
                "WHERE ranking_version = 'selection_engine_v2'"
            ): latest_prediction,
            "SELECT MAX(as_of_date) FROM fact_portfolio_target_book": latest_portfolio_target,
            "SELECT MAX(snapshot_date) FROM fact_portfolio_nav_snapshot": latest_nav,
            "SELECT MAX(summary_date) FROM fact_evaluation_summary": latest_evaluation,
        }
        for pattern, value in mapping.items():
            if normalized == pattern:
                return value
        return 0

    return _fake_scalar


def test_check_pipeline_dependencies_respects_scheduler_cutoff_times(tmp_path, monkeypatch) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    latest_dates = {
        "latest_universe": date(2026, 3, 12),
        "latest_calendar": date(2026, 12, 31),
        "latest_selection": date(2026, 3, 12),
        "latest_prediction": date(2026, 3, 12),
        "latest_portfolio_target": date(2026, 3, 12),
        "latest_nav": date(2026, 3, 13),
        "latest_evaluation": date(2026, 3, 13),
    }
    monkeypatch.setattr(
        "app.ops.health.measure_disk_usage",
        lambda *_args, **_kwargs: SimpleNamespace(
            status=DiskWatermark.NORMAL,
            usage_ratio=0.10,
        ),
    )

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)

        monkeypatch.setattr(
            "app.ops.health._scalar",
            _fake_scalar_factory(**latest_dates),
        )
        monkeypatch.setattr(
            "app.ops.health.now_local",
            lambda _tz: datetime(2026, 3, 13, 16, 59, tzinfo=ZoneInfo("Asia/Seoul")),
        )
        before_close = check_pipeline_dependencies(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 13),
            job_run_id="before-close",
        )
        before_rows = connection.execute(
            """
            SELECT dependency_name, status, ready_flag, observed_state
            FROM vw_latest_pipeline_dependency_state
            ORDER BY dependency_name
            """
        ).fetchdf()

        monkeypatch.setattr(
            "app.ops.health.now_local",
            lambda _tz: datetime(2026, 3, 13, 19, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )
        after_close = check_pipeline_dependencies(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 13),
            job_run_id="after-close",
        )
        after_rows = connection.execute(
            """
            SELECT dependency_name, status, ready_flag, observed_state
            FROM vw_latest_pipeline_dependency_state
            ORDER BY dependency_name
            """
        ).fetchdf()

    assert before_close.status == JobStatus.SUCCESS
    before_selection = before_rows.loc[
        before_rows["dependency_name"] == "selection_v2_ready"
    ].iloc[0]
    assert bool(before_selection["ready_flag"]) is True
    assert "required=2026-03-12" in str(before_selection["observed_state"])

    assert after_close.status == JobStatus.BLOCKED
    after_selection = after_rows.loc[
        after_rows["dependency_name"] == "selection_v2_ready"
    ].iloc[0]
    after_prediction = after_rows.loc[
        after_rows["dependency_name"] == "prediction_ready"
    ].iloc[0]
    after_target = after_rows.loc[
        after_rows["dependency_name"] == "portfolio_target_ready"
    ].iloc[0]
    assert bool(after_selection["ready_flag"]) is False
    assert bool(after_prediction["ready_flag"]) is False
    assert after_target["status"] == JobStatus.DEGRADED_SUCCESS
    assert "required=2026-03-13" in str(after_selection["observed_state"])


def test_materialize_health_snapshots_deduplicates_and_resolves_open_alerts(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    snapshot_ts = datetime.now(ZoneInfo("Asia/Seoul")).replace(microsecond=0)
    now_values = iter(
        [
            snapshot_ts,
            snapshot_ts + timedelta(minutes=1),
            snapshot_ts + timedelta(minutes=2),
        ]
    )
    monkeypatch.setattr(
        "app.ops.health.measure_disk_usage",
        lambda *_args, **_kwargs: SimpleNamespace(
            status=DiskWatermark.NORMAL,
            usage_ratio=0.10,
        ),
    )
    monkeypatch.setattr("app.ops.health.now_local", lambda _tz: next(now_values))
    as_of_date = snapshot_ts.date()

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_job_run (
                run_id,
                job_name,
                trigger_type,
                status,
                as_of_date,
                started_at,
                finished_at,
                root_run_id,
                parent_run_id,
                recovery_of_run_id,
                lock_name,
                policy_id,
                policy_version,
                dry_run,
                step_count,
                failed_step_count,
                artifact_count,
                notes,
                error_message,
                details_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "failed-job-1",
                "unit_test_bundle",
                "MANUAL",
                "FAILED",
                as_of_date,
                snapshot_ts - timedelta(hours=1),
                snapshot_ts - timedelta(minutes=30),
                "failed-job-1",
                None,
                None,
                None,
                None,
                None,
                False,
                1,
                1,
                0,
                "unit test failure",
                "boom",
                None,
                snapshot_ts - timedelta(hours=1),
            ],
        )

        first = materialize_health_snapshots(
            settings,
            connection=connection,
            as_of_date=as_of_date,
            job_run_id="health-1",
        )
        second = materialize_health_snapshots(
            settings,
            connection=connection,
            as_of_date=as_of_date,
            job_run_id="health-2",
        )
        open_failed_alerts = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE alert_type = 'FAILED_RUNS_24H'
              AND status = 'OPEN'
            """
        ).fetchone()[0]
        assert first.status == JobStatus.DEGRADED_SUCCESS
        assert second.status == JobStatus.DEGRADED_SUCCESS
        assert open_failed_alerts == 1

        connection.execute(
            """
            UPDATE fact_job_run
            SET status = 'SUCCESS',
                started_at = ?,
                finished_at = ?
            WHERE run_id = 'failed-job-1'
            """,
            [
                snapshot_ts - timedelta(days=2),
                snapshot_ts - timedelta(days=2) + timedelta(minutes=5),
            ],
        )

        third = materialize_health_snapshots(
            settings,
            connection=connection,
            as_of_date=as_of_date,
            job_run_id="health-3",
        )
        open_failed_alerts = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE alert_type = 'FAILED_RUNS_24H'
              AND status = 'OPEN'
            """
        ).fetchone()[0]
        resolved_failed_alerts = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE alert_type = 'FAILED_RUNS_24H'
              AND status = 'RESOLVED'
            """
        ).fetchone()[0]
        latest_open_alert_metric = connection.execute(
            """
            SELECT metric_value_double
            FROM vw_latest_health_snapshot
            WHERE health_scope = 'overall'
              AND component_name = 'platform'
              AND metric_name = 'open_alert_count'
            """
        ).fetchone()[0]
        assert third.status == JobStatus.SUCCESS
        assert open_failed_alerts == 0
        assert resolved_failed_alerts == 1
        assert float(latest_open_alert_metric or 0.0) == 0.0


def test_materialize_health_snapshots_ignores_cleanup_recovered_failures(
    tmp_path,
    monkeypatch,
) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    snapshot_ts = datetime.now(ZoneInfo("Asia/Seoul")).replace(microsecond=0)
    monkeypatch.setattr(
        "app.ops.health.measure_disk_usage",
        lambda *_args, **_kwargs: SimpleNamespace(
            status=DiskWatermark.NORMAL,
            usage_ratio=0.10,
        ),
    )
    monkeypatch.setattr("app.ops.health.now_local", lambda _tz: snapshot_ts)
    as_of_date = snapshot_ts.date()

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_job_run (
                run_id,
                job_name,
                trigger_type,
                status,
                as_of_date,
                started_at,
                finished_at,
                root_run_id,
                parent_run_id,
                recovery_of_run_id,
                lock_name,
                policy_id,
                policy_version,
                dry_run,
                step_count,
                failed_step_count,
                artifact_count,
                notes,
                error_message,
                details_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "failed-job-cleaned",
                "unit_test_bundle",
                "MANUAL",
                "FAILED",
                as_of_date,
                snapshot_ts - timedelta(hours=1),
                snapshot_ts - timedelta(minutes=30),
                "failed-job-cleaned",
                None,
                None,
                None,
                None,
                None,
                False,
                1,
                1,
                0,
                "unit test stale cleanup",
                "boom",
                '{"cleanup_recovered": true}',
                snapshot_ts - timedelta(hours=1),
            ],
        )

        result = materialize_health_snapshots(
            settings,
            connection=connection,
            as_of_date=as_of_date,
            job_run_id="health-cleanup-filter",
        )
        open_failed_alerts = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alert_event
            WHERE alert_type = 'FAILED_RUNS_24H'
              AND status = 'OPEN'
            """
        ).fetchone()[0]

        assert result.status == JobStatus.SUCCESS
        assert open_failed_alerts == 0
