from __future__ import annotations

from datetime import date, datetime

from app.ops.scheduler import (
    SCHEDULED_JOB_MAP,
    resolve_due_intraday_checkpoint,
    resolve_news_collection_dates,
)
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_scheduler_registry_contains_expected_jobs() -> None:
    expected_job_keys = {
        "ops_maintenance",
        "news_morning",
        "intraday_assist",
        "news_after_close",
        "evaluation",
        "daily_close",
        "daily_audit_lite",
        "weekly_training_candidate",
        "weekly_calibration",
    }

    assert set(SCHEDULED_JOB_MAP) == expected_job_keys
    assert SCHEDULED_JOB_MAP["news_morning"].run_times == ("08:30",)
    assert SCHEDULED_JOB_MAP["news_morning"].date_semantics == "calendar_day"
    assert SCHEDULED_JOB_MAP["news_morning"].trading_day_required is False
    assert SCHEDULED_JOB_MAP["news_after_close"].run_times == ("16:10",)
    assert SCHEDULED_JOB_MAP["news_after_close"].date_semantics == "calendar_day"
    assert SCHEDULED_JOB_MAP["daily_close"].run_times == ("18:40",)
    assert SCHEDULED_JOB_MAP["daily_close"].date_semantics == "trading_day"
    assert SCHEDULED_JOB_MAP["intraday_assist"].intraday_interval_minutes == 5
    assert SCHEDULED_JOB_MAP["daily_audit_lite"].date_semantics == "calendar_day"
    assert SCHEDULED_JOB_MAP["weekly_training_candidate"].date_semantics == "hybrid"
    assert SCHEDULED_JOB_MAP["weekly_training_candidate"].heavy_job is True
    assert SCHEDULED_JOB_MAP["weekly_calibration"].date_semantics == "hybrid"
    assert SCHEDULED_JOB_MAP["weekly_calibration"].heavy_job is True


def test_resolve_news_collection_dates_includes_weekend_gap(tmp_path) -> None:
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            UPDATE dim_trading_calendar
            SET prev_trading_date = DATE '2026-03-06'
            WHERE trading_date = DATE '2026-03-09'
            """
        )

    result = resolve_news_collection_dates(
        settings,
        target_date=date(2026, 3, 9),
        profile="morning",
    )

    assert result == [
        date(2026, 3, 7),
        date(2026, 3, 8),
        date(2026, 3, 9),
    ]


def test_resolve_due_intraday_checkpoint_returns_latest_due(tmp_path) -> None:
    settings = build_test_settings(tmp_path)

    due_checkpoint = resolve_due_intraday_checkpoint(
        settings,
        as_of_dt=datetime(2026, 3, 9, 10, 7),
    )

    assert due_checkpoint == "10:00"
