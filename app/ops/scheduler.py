from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from app.common.paths import ensure_directory
from app.common.time import now_local, today_local
from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection

DEFAULT_INTRADAY_CHECKPOINTS: tuple[str, ...] = ("09:05", "09:15", "09:30", "10:00", "11:00")
DATE_SEMANTICS_CALENDAR = "calendar_day"
DATE_SEMANTICS_TRADING = "trading_day"
DATE_SEMANTICS_HYBRID = "hybrid"


@dataclass(frozen=True, slots=True)
class ScheduledJobDefinition:
    job_key: str
    label: str
    description: str
    service_slug: str
    bundle_script: str
    bundle_args: tuple[str, ...]
    schedule_label: str
    on_calendar: tuple[str, ...]
    weekdays: tuple[int, ...]
    run_times: tuple[str, ...] = ()
    intraday_window_start: str | None = None
    intraday_window_end: str | None = None
    intraday_interval_minutes: int | None = None
    date_semantics: str = DATE_SEMANTICS_CALENDAR
    trading_day_required: bool = False
    serial_scope: str = "global_write"
    heavy_job: bool = False

    @property
    def service_name(self) -> str:
        return f"stockmaster-scheduler@{self.service_slug}.service"

    @property
    def timer_name(self) -> str:
        return f"stockmaster-{self.service_slug}.timer"


SCHEDULED_JOBS: tuple[ScheduledJobDefinition, ...] = (
    ScheduledJobDefinition(
        job_key="ops_maintenance",
        label="운영 유지보수",
        description="로그 회전, 디스크 점검, stale lock 정리, health 갱신",
        service_slug="ops-maintenance",
        bundle_script="scripts/run_ops_maintenance_bundle.py",
        bundle_args=(),
        schedule_label="매일 02:30",
        on_calendar=("*-*-* 02:30:00",),
        weekdays=(0, 1, 2, 3, 4, 5, 6),
        run_times=("02:30",),
        date_semantics=DATE_SEMANTICS_CALENDAR,
    ),
    ScheduledJobDefinition(
        job_key="news_morning",
        label="아침 뉴스 수집",
        description="야간·해외발 뉴스 메타데이터를 장 시작 전 반영",
        service_slug="news-morning",
        bundle_script="scripts/run_news_sync_bundle.py",
        bundle_args=("--profile", "morning"),
        schedule_label="평일 08:30",
        on_calendar=("Mon..Fri *-*-* 08:30:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("08:30",),
        date_semantics=DATE_SEMANTICS_CALENDAR,
    ),
    ScheduledJobDefinition(
        job_key="intraday_assist",
        label="장중 후보군 보조",
        description="후보군 장중 데이터 수집, timing signal, 조정 action, meta overlay 갱신",
        service_slug="intraday-assist",
        bundle_script="scripts/run_intraday_assist_bundle.py",
        bundle_args=(),
        schedule_label="평일 08:55-15:15, 5분 간격",
        on_calendar=(
            "Mon..Fri *-*-* 08:55:00",
            "Mon..Fri *-*-* 09:00/5:00",
            "Mon..Fri *-*-* 10:00/5:00",
            "Mon..Fri *-*-* 11:00/5:00",
            "Mon..Fri *-*-* 12:00/5:00",
            "Mon..Fri *-*-* 13:00/5:00",
            "Mon..Fri *-*-* 14:00/5:00",
            "Mon..Fri *-*-* 15:00/5:00",
        ),
        weekdays=(0, 1, 2, 3, 4),
        intraday_window_start="08:55",
        intraday_window_end="15:15",
        intraday_interval_minutes=5,
        date_semantics=DATE_SEMANTICS_TRADING,
        trading_day_required=True,
    ),
    ScheduledJobDefinition(
        job_key="news_after_close",
        label="장 마감 직후 뉴스 수집",
        description="당일 장중·장후 뉴스 메타데이터를 마감 직후 수집",
        service_slug="news-after-close",
        bundle_script="scripts/run_news_sync_bundle.py",
        bundle_args=("--profile", "after_close"),
        schedule_label="평일 16:10",
        on_calendar=("Mon..Fri *-*-* 16:10:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("16:10",),
        date_semantics=DATE_SEMANTICS_CALENDAR,
    ),
    ScheduledJobDefinition(
        job_key="evaluation",
        label="장후 평가",
        description="matured outcome, calibration, postmortem, portfolio 평가를 갱신",
        service_slug="evaluation",
        bundle_script="scripts/run_evaluation_bundle.py",
        bundle_args=(),
        schedule_label="평일 16:20",
        on_calendar=("Mon..Fri *-*-* 16:20:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("16:20",),
        date_semantics=DATE_SEMANTICS_TRADING,
        trading_day_required=True,
    ),
    ScheduledJobDefinition(
        job_key="daily_close",
        label="장후 추천 생성",
        description="최종 뉴스 재수집 후 selection, portfolio, 장후 리포트와 snapshot을 생성",
        service_slug="daily-close",
        bundle_script="scripts/run_daily_close_bundle.py",
        bundle_args=(),
        schedule_label="평일 18:40",
        on_calendar=("Mon..Fri *-*-* 18:40:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("18:40",),
        date_semantics=DATE_SEMANTICS_TRADING,
        trading_day_required=True,
        heavy_job=True,
    ),
    ScheduledJobDefinition(
        job_key="daily_audit_lite",
        label="일일 경량 감사",
        description="latest layer, artifact reference, freshness, release sanity를 점검",
        service_slug="daily-audit-lite",
        bundle_script="scripts/run_daily_audit_lite_bundle.py",
        bundle_args=(),
        schedule_label="평일 19:05",
        on_calendar=("Mon..Fri *-*-* 19:05:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("19:05",),
        date_semantics=DATE_SEMANTICS_CALENDAR,
    ),
    ScheduledJobDefinition(
        job_key="daily_overlay_refresh",
        label="Daily Overlay Refresh",
        description=(
            "Light overlay refresh that recalibrates policy recommendations "
            "and guarded auto-promotion against the current alpha lineage."
        ),
        service_slug="daily-overlay-refresh",
        bundle_script="scripts/run_daily_overlay_refresh_bundle.py",
        bundle_args=(),
        schedule_label="Weekdays 20:10",
        on_calendar=("Mon..Fri *-*-* 20:10:00",),
        weekdays=(0, 1, 2, 3, 4),
        run_times=("20:10",),
        date_semantics=DATE_SEMANTICS_TRADING,
        trading_day_required=True,
    ),
    ScheduledJobDefinition(
        job_key="docker_build_cache_cleanup",
        label="도커 빌드 캐시 정리",
        description="Docker builder cache만 안전하게 정리",
        service_slug="docker-build-cache-cleanup",
        bundle_script="scripts/run_docker_build_cache_cleanup_bundle.py",
        bundle_args=(),
        schedule_label="매일 23:40",
        on_calendar=("*-*-* 23:40:00",),
        weekdays=(0, 1, 2, 3, 4, 5, 6),
        run_times=("23:40",),
        date_semantics=DATE_SEMANTICS_CALENDAR,
    ),
    ScheduledJobDefinition(
        job_key="weekly_training_candidate",
        label="주간 학습 후보 생성",
        description="alpha/meta 재학습 후보와 비교 리포트만 생성하고 production 반영은 하지 않음",
        service_slug="weekly-training",
        bundle_script="scripts/run_weekly_training_bundle.py",
        bundle_args=(),
        schedule_label="토요일 03:30",
        on_calendar=("Sat *-*-* 03:30:00",),
        weekdays=(5,),
        run_times=("03:30",),
        date_semantics=DATE_SEMANTICS_HYBRID,
        heavy_job=True,
    ),
    ScheduledJobDefinition(
        job_key="weekly_calibration",
        label="주간 보정·정책 후보 생성",
        description=(
            "policy calibration, meta threshold, recommendation 후보를 만들되 "
            "자동 반영하지 않음"
        ),
        service_slug="weekly-calibration",
        bundle_script="scripts/run_weekly_calibration_bundle.py",
        bundle_args=(),
        schedule_label="토요일 06:30",
        on_calendar=("Sat *-*-* 06:30:00",),
        weekdays=(5,),
        run_times=("06:30",),
        date_semantics=DATE_SEMANTICS_HYBRID,
    ),
    ScheduledJobDefinition(
        job_key="weekly_policy_research",
        label="Weekly Policy Research",
        description=(
            "Heavy weekly policy walk-forward and ablation research. "
            "Artifacts refresh only and are never auto-activated."
        ),
        service_slug="weekly-policy-research",
        bundle_script="scripts/run_weekly_policy_research_bundle.py",
        bundle_args=(),
        schedule_label="Sat 07:45",
        on_calendar=("Sat *-*-* 07:45:00",),
        weekdays=(5,),
        run_times=("07:45",),
        date_semantics=DATE_SEMANTICS_HYBRID,
        heavy_job=True,
    ),
)

SCHEDULED_JOB_MAP: dict[str, ScheduledJobDefinition] = {
    item.job_key: item for item in SCHEDULED_JOBS
}
SCHEDULED_SERVICE_SLUG_MAP: dict[str, ScheduledJobDefinition] = {
    item.service_slug: item for item in SCHEDULED_JOBS
}


def scheduler_state_dir(settings: Settings) -> Path:
    return ensure_directory(settings.paths.cache_dir / "scheduler_state")


def scheduler_state_path(settings: Settings, job_key: str) -> Path:
    return scheduler_state_dir(settings) / f"{job_key}.json"


def write_scheduler_state(settings: Settings, job_key: str, payload: dict[str, Any]) -> Path:
    state_path = scheduler_state_path(settings, job_key)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return state_path


def read_scheduler_state(settings: Settings, job_key: str) -> dict[str, Any]:
    state_path = scheduler_state_path(settings, job_key)
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def get_scheduled_job(job_key: str) -> ScheduledJobDefinition:
    try:
        return SCHEDULED_JOB_MAP[job_key]
    except KeyError as exc:
        raise KeyError(f"Unknown scheduled job: {job_key}") from exc


def get_scheduled_job_by_service_slug(service_slug: str) -> ScheduledJobDefinition:
    try:
        return SCHEDULED_SERVICE_SLUG_MAP[service_slug]
    except KeyError as exc:
        raise KeyError(f"Unknown scheduled service slug: {service_slug}") from exc


def local_manual_command(job: ScheduledJobDefinition) -> str:
    args = " ".join(job.bundle_args)
    return f"python {job.bundle_script}" + (f" {args}" if args else "")


def server_manual_command(job: ScheduledJobDefinition) -> str:
    return f"sudo systemctl start {job.service_name}"


def schedule_job_catalog_frame(
    settings: Settings,
    *,
    now_ts: datetime | None = None,
) -> pd.DataFrame:
    effective_now = now_ts or now_local(settings.app.timezone)
    rows: list[dict[str, Any]] = []
    for item in SCHEDULED_JOBS:
        state = read_scheduler_state(settings, item.job_key)
        rows.append(
            {
                "job_key": item.job_key,
                "label": item.label,
                "description": item.description,
                "schedule_label": item.schedule_label,
                "next_run_at": _next_run_at(item, settings=settings, now_ts=effective_now),
                "last_status": state.get("status"),
                "last_finished_at": state.get("finished_at"),
                "last_notes": state.get("notes"),
                "last_run_id": state.get("run_id"),
                "date_semantics": item.date_semantics,
                "trading_day_required": item.trading_day_required,
                "heavy_job": item.heavy_job,
                "manual_local_command": local_manual_command(item),
                "manual_server_command": server_manual_command(item),
                "timer_name": item.timer_name,
                "service_name": item.service_name,
                "on_calendar": "; ".join(item.on_calendar),
            }
        )
    return pd.DataFrame(rows)


def scheduler_state_frame(settings: Settings, *, limit: int = 50) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in SCHEDULED_JOBS:
        state = read_scheduler_state(settings, item.job_key)
        if not state:
            continue
        rows.append(
            {
                "job_key": item.job_key,
                "label": item.label,
                "status": state.get("status"),
                "finished_at": state.get("finished_at"),
                "run_id": state.get("run_id"),
                "notes": state.get("notes"),
                "identity_json": json.dumps(state.get("identity", {}), ensure_ascii=False),
            }
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows).sort_values("finished_at", ascending=False)
    return frame.head(int(limit)).reset_index(drop=True)


def resolve_reference_trading_date(
    settings: Settings,
    *,
    target_date: date | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> date:
    effective_target = target_date or today_local(settings.app.timezone)
    if connection is not None:
        row = connection.execute(
            """
            SELECT trading_date
            FROM dim_trading_calendar
            WHERE trading_date <= ?
              AND is_trading_day = TRUE
            ORDER BY trading_date DESC
            LIMIT 1
            """,
            [effective_target],
        ).fetchone()
        return row[0] if row and row[0] is not None else effective_target
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return resolve_reference_trading_date(
            settings,
            target_date=effective_target,
            connection=read_connection,
        )


def resolve_previous_trading_date(
    settings: Settings,
    *,
    target_date: date,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> date | None:
    if connection is not None:
        row = connection.execute(
            """
            SELECT prev_trading_date
            FROM dim_trading_calendar
            WHERE trading_date = ?
            """,
            [target_date],
        ).fetchone()
        return row[0] if row and row[0] is not None else None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return resolve_previous_trading_date(
            settings,
            target_date=target_date,
            connection=read_connection,
        )


def resolve_next_trading_date(
    settings: Settings,
    *,
    target_date: date,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> date | None:
    if connection is not None:
        row = connection.execute(
            """
            SELECT next_trading_date
            FROM dim_trading_calendar
            WHERE trading_date = ?
            """,
            [target_date],
        ).fetchone()
        return row[0] if row and row[0] is not None else None
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return resolve_next_trading_date(
            settings,
            target_date=target_date,
            connection=read_connection,
        )


def is_trading_day(
    settings: Settings,
    *,
    target_date: date,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> bool:
    if connection is not None:
        row = connection.execute(
            """
            SELECT is_trading_day
            FROM dim_trading_calendar
            WHERE trading_date = ?
            """,
            [target_date],
        ).fetchone()
        return bool(row[0]) if row and row[0] is not None else False
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as read_connection:
        bootstrap_core_tables(read_connection)
        return is_trading_day(settings, target_date=target_date, connection=read_connection)


def resolve_news_collection_dates(
    settings: Settings,
    *,
    target_date: date,
    profile: str,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> list[date]:
    normalized = profile.strip().lower()
    if normalized == "morning":
        previous_trading = resolve_previous_trading_date(
            settings,
            target_date=target_date,
            connection=connection,
        )
        if previous_trading is None:
            return [target_date]
        output: list[date] = []
        current = previous_trading + timedelta(days=1)
        while current <= target_date:
            output.append(current)
            current += timedelta(days=1)
        return output or [target_date]
    return [target_date]


def resolve_due_intraday_checkpoint(
    settings: Settings,
    *,
    as_of_dt: datetime | None = None,
    checkpoints: tuple[str, ...] = DEFAULT_INTRADAY_CHECKPOINTS,
) -> str | None:
    effective_now = as_of_dt or now_local(settings.app.timezone)
    due: str | None = None
    for checkpoint in checkpoints:
        hours, minutes = (int(part) for part in checkpoint.split(":", 1))
        if effective_now.timetz().replace(tzinfo=None) >= time(hours, minutes):
            due = checkpoint
    return due


def bundle_already_completed(
    connection: duckdb.DuckDBPyConnection,
    *,
    job_name: str,
    as_of_date: date | None,
    bundle_phase: str | None = None,
    checkpoint_time: str | None = None,
    profile: str | None = None,
) -> bool:
    filters = [
        "job_name = ?",
        "status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'DEGRADED_SUCCESS')",
    ]
    parameters: list[Any] = [job_name]
    if as_of_date is not None:
        filters.append("as_of_date = ?")
        parameters.append(as_of_date)
    if bundle_phase is not None:
        filters.append("json_extract_string(details_json, '$.bundle_phase') = ?")
        parameters.append(bundle_phase)
    if checkpoint_time is not None:
        filters.append("json_extract_string(details_json, '$.checkpoint_time') = ?")
        parameters.append(checkpoint_time)
    if profile is not None:
        filters.append("json_extract_string(details_json, '$.profile') = ?")
        parameters.append(profile)
    row = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM fact_job_run
        WHERE {" AND ".join(filters)}
        """,
        parameters,
    ).fetchone()
    return bool(row and row[0])


def bundle_last_result_frame(settings: Settings, *, limit: int = 50) -> pd.DataFrame:
    bundle_names = [
        "run_daily_close_bundle",
        "run_daily_overlay_refresh_bundle",
        "run_evaluation_bundle",
        "run_docker_build_cache_cleanup_bundle",
        "run_intraday_assist_bundle",
        "run_weekly_training_bundle",
        "run_weekly_calibration_bundle",
        "run_weekly_policy_research_bundle",
        "run_ops_maintenance_bundle",
        "run_daily_audit_lite_bundle",
        "run_news_sync_bundle",
    ]
    placeholders = ", ".join("?" for _ in bundle_names)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(
            f"""
            SELECT
                run_id,
                job_name,
                status,
                as_of_date,
                started_at,
                finished_at,
                notes,
                details_json
            FROM fact_job_run
            WHERE job_name IN ({placeholders})
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [*bundle_names, int(limit)],
        ).fetchdf()


def _iter_daily_times(job: ScheduledJobDefinition) -> list[time]:
    if job.run_times:
        return [time.fromisoformat(value) for value in job.run_times]
    if job.intraday_window_start and job.intraday_window_end and job.intraday_interval_minutes:
        start_dt = datetime.combine(date.today(), time.fromisoformat(job.intraday_window_start))
        end_dt = datetime.combine(date.today(), time.fromisoformat(job.intraday_window_end))
        current = start_dt
        output: list[time] = []
        while current <= end_dt:
            output.append(current.time())
            current += timedelta(minutes=int(job.intraday_interval_minutes))
        return output
    return []


def _effective_schedule_reference_time(
    settings: Settings,
    *,
    as_of_date: date | None,
    now_ts: datetime | None = None,
) -> datetime:
    timezone = ZoneInfo(settings.app.timezone)
    effective_now = (now_ts or now_local(settings.app.timezone)).astimezone(timezone)
    if as_of_date is None or as_of_date == effective_now.date():
        return effective_now
    return datetime.combine(as_of_date, time(23, 59, 59), tzinfo=timezone)


def latest_scheduled_run_at(
    job: ScheduledJobDefinition,
    *,
    settings: Settings,
    as_of_date: date | None = None,
    now_ts: datetime | None = None,
) -> datetime | None:
    effective_now = _effective_schedule_reference_time(
        settings,
        as_of_date=as_of_date,
        now_ts=now_ts,
    )
    daily_times = sorted(_iter_daily_times(job))
    if not daily_times:
        return None
    for day_offset in range(0, 35):
        candidate_day = effective_now.date() - timedelta(days=day_offset)
        if candidate_day.weekday() not in job.weekdays:
            continue
        for candidate_time in reversed(daily_times):
            candidate_dt = datetime.combine(candidate_day, candidate_time, tzinfo=effective_now.tzinfo)
            if candidate_dt <= effective_now:
                return candidate_dt
    return None


def expected_job_reference_date(
    settings: Settings,
    *,
    job_key: str,
    as_of_date: date | None = None,
    now_ts: datetime | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> date | None:
    job = get_scheduled_job(job_key)
    latest_run_at = latest_scheduled_run_at(
        job,
        settings=settings,
        as_of_date=as_of_date,
        now_ts=now_ts,
    )
    if latest_run_at is None:
        return None
    reference_date = latest_run_at.date()
    if job.date_semantics == DATE_SEMANTICS_CALENDAR:
        return reference_date
    return resolve_reference_trading_date(
        settings,
        target_date=reference_date,
        connection=connection,
    )


def _next_run_at(
    job: ScheduledJobDefinition,
    *,
    settings: Settings,
    now_ts: datetime,
) -> datetime | None:
    timezone = ZoneInfo(settings.app.timezone)
    effective_now = now_ts.astimezone(timezone)
    daily_times = _iter_daily_times(job)
    for day_offset in range(0, 14):
        candidate_day = effective_now.date() + timedelta(days=day_offset)
        if candidate_day.weekday() not in job.weekdays:
            continue
        for candidate_time in daily_times:
            candidate_dt = datetime.combine(candidate_day, candidate_time, tzinfo=timezone)
            if candidate_dt > effective_now:
                return candidate_dt
    return None
