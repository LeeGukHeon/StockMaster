# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.time import today_local
from app.ops.bundles import (
    run_daily_audit_lite_bundle,
    run_daily_close_bundle,
    run_daily_overlay_refresh_bundle,
    run_docker_build_cache_cleanup_bundle,
    run_evaluation_bundle,
    run_intraday_assist_bundle,
    run_news_sync_bundle,
    run_ops_maintenance_bundle,
    run_weekly_calibration_bundle,
    run_weekly_policy_research_bundle,
    run_weekly_training_bundle,
)
from app.ops.common import JobStatus, TriggerType
from app.ops.scheduler import (
    get_scheduled_follow_up_jobs,
    get_scheduled_job,
    get_scheduled_job_by_service_slug,
    read_scheduler_state,
    resolve_due_intraday_checkpoint,
)
from scripts._ops_cli import load_cli_settings, log_and_print, parse_date
from scripts._scheduler_cli import SchedulerSkip, bundle_result
from scripts._scheduler_cli import run_scheduled_bundle as run_with_scheduler

CHAINABLE_JOB_STATUSES = {
    "SUCCESS",
    "PARTIAL_SUCCESS",
    "DEGRADED_SUCCESS",
    "SKIPPED_ALREADY_DONE",
}
SMART_BACKSTOP_UPSTREAM_MAP: dict[str, tuple[str, int]] = {
    "daily_overlay_refresh": ("daily_close", 0),
    "daily_audit_lite": ("daily_overlay_refresh", -1),
    "weekly_calibration": ("weekly_training_candidate", 0),
    "weekly_policy_research": ("weekly_calibration", 0),
}


def _resolve_job(args) -> tuple[str, object]:
    if args.service_slug:
        job = get_scheduled_job_by_service_slug(args.service_slug)
    else:
        job = get_scheduled_job(args.job_key)
    return job.job_key, job


def _resolve_as_of_date(settings, explicit_date: date | None) -> date:
    return explicit_date or today_local(settings.app.timezone)


def _resolve_profile(job_key: str) -> str | None:
    if job_key == "news_morning":
        return "morning"
    if job_key == "news_after_close":
        return "after_close"
    return None


def _resolve_checkpoint_time(
    settings,
    *,
    job_key: str,
    explicit_checkpoint_time: str | None,
) -> str | None:
    if job_key != "intraday_assist":
        return explicit_checkpoint_time
    if explicit_checkpoint_time is not None:
        return explicit_checkpoint_time
    return resolve_due_intraday_checkpoint(settings) or "PREP"


def _identity_for_job(
    *,
    target_date: date,
    profile: str | None,
    checkpoint_time: str | None,
) -> dict[str, str]:
    identity = {"as_of_date": target_date.isoformat()}
    if profile is not None:
        identity["profile"] = profile
    if checkpoint_time is not None:
        identity["checkpoint_time"] = checkpoint_time
    return identity


def _status_for_scheduler_identity(
    settings,
    *,
    job_key: str,
    as_of_date: date,
) -> str | None:
    state = read_scheduler_state(settings, job_key)
    identity = state.get("identity")
    if not isinstance(identity, dict):
        return None
    if identity.get("as_of_date") != as_of_date.isoformat():
        return None
    status = state.get("status")
    return str(status).upper() if status else None


def _smart_backstop_skip(
    settings,
    *,
    job_key: str,
    target_date: date,
    force: bool,
    scheduler_run: bool,
) -> SchedulerSkip | None:
    if force or not scheduler_run:
        return None
    upstream = SMART_BACKSTOP_UPSTREAM_MAP.get(job_key)
    if upstream is None:
        return None
    upstream_job_key, day_offset = upstream
    upstream_date = target_date + timedelta(days=int(day_offset))
    upstream_status = _status_for_scheduler_identity(
        settings,
        job_key=upstream_job_key,
        as_of_date=upstream_date,
    )
    if upstream_status in CHAINABLE_JOB_STATUSES:
        return None
    if upstream_status is None:
        return SchedulerSkip(
            JobStatus.SKIPPED,
            (
                f"Smart backstop skipped: upstream {upstream_job_key} "
                f"has no successful state for {upstream_date.isoformat()}."
            ),
            details={
                "smart_backstop": True,
                "upstream_job_key": upstream_job_key,
                "upstream_as_of_date": upstream_date.isoformat(),
                "upstream_status": None,
            },
        )
    return SchedulerSkip(
        JobStatus.SKIPPED,
        (
            f"Smart backstop skipped: upstream {upstream_job_key} "
            f"status={upstream_status} for {upstream_date.isoformat()}."
        ),
        details={
            "smart_backstop": True,
            "upstream_job_key": upstream_job_key,
            "upstream_as_of_date": upstream_date.isoformat(),
            "upstream_status": upstream_status,
        },
    )


def _execute_job(
    settings,
    *,
    job_key: str,
    target_date: date,
    checkpoint_time: str | None,
    dry_run: bool,
    force: bool,
    skip_discord: bool,
    scheduler_run: bool,
    policy_config_path: str | None,
) -> int:
    profile = _resolve_profile(job_key)
    resolved_checkpoint_time = _resolve_checkpoint_time(
        settings,
        job_key=job_key,
        explicit_checkpoint_time=checkpoint_time,
    )
    identity = _identity_for_job(
        target_date=target_date,
        profile=profile,
        checkpoint_time=resolved_checkpoint_time,
    )

    def runner(runtime_settings):
        smart_skip = _smart_backstop_skip(
            runtime_settings,
            job_key=job_key,
            target_date=target_date,
            force=force,
            scheduler_run=scheduler_run,
        )
        if smart_skip is not None:
            raise smart_skip
        trigger_type = TriggerType.SCHEDULED if scheduler_run else TriggerType.MANUAL
        if job_key in {"news_morning", "news_after_close"}:
            result = run_news_sync_bundle(
                runtime_settings,
                as_of_date=target_date,
                profile=profile or "after_close",
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "daily_close":
            result = run_daily_close_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                publish_discord=not skip_discord,
                policy_config_path=policy_config_path,
            )
        elif job_key == "daily_overlay_refresh":
            result = run_daily_overlay_refresh_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "docker_build_cache_cleanup":
            result = run_docker_build_cache_cleanup_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                policy_config_path=policy_config_path,
            )
        elif job_key == "evaluation":
            result = run_evaluation_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "intraday_assist":
            result = run_intraday_assist_bundle(
                runtime_settings,
                as_of_date=target_date,
                checkpoint_time=(
                    None if resolved_checkpoint_time == "PREP" else resolved_checkpoint_time
                ),
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "weekly_training_candidate":
            result = run_weekly_training_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "weekly_calibration":
            result = run_weekly_calibration_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "weekly_policy_research":
            result = run_weekly_policy_research_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        elif job_key == "ops_maintenance":
            result = run_ops_maintenance_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                policy_config_path=policy_config_path,
            )
        elif job_key == "daily_audit_lite":
            result = run_daily_audit_lite_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=dry_run,
                force=force,
                policy_config_path=policy_config_path,
            )
        else:
            raise RuntimeError(f"Unsupported scheduler job: {job_key}")
        return bundle_result(
            job_key=job_key,
            status=result.status,
            notes=result.notes,
            run_ids=[result.run_id],
            artifact_paths=list(result.artifact_paths),
            as_of_date=result.as_of_date,
            row_count=result.row_count,
            details={"bundle_job_name": result.job_name},
        )

    return run_with_scheduler(
        job_key=job_key,
        runner=runner,
        identity=identity,
        force=force,
    )


def _run_follow_up_chain(
    settings,
    *,
    job_key: str,
    target_date: date,
    force: bool,
    skip_discord: bool,
    policy_config_path: str | None,
    visited: set[str],
) -> int:
    exit_code = 0
    for next_job_key, day_offset in get_scheduled_follow_up_jobs(job_key):
        if next_job_key in visited:
            continue
        visited.add(next_job_key)
        next_target_date = target_date + timedelta(days=int(day_offset))
        log_and_print(
            f"Scheduler chaining follow-up job: {job_key} -> {next_job_key} "
            f"as_of_date={next_target_date.isoformat()}"
        )
        exit_code = max(
            exit_code,
            _execute_job(
                settings,
                job_key=next_job_key,
                target_date=next_target_date,
                checkpoint_time=None,
                dry_run=False,
                force=force,
                skip_discord=skip_discord,
                scheduler_run=True,
                policy_config_path=policy_config_path,
            ),
        )
        state = read_scheduler_state(settings, next_job_key)
        status = str(state.get("status") or "").upper()
        if status in CHAINABLE_JOB_STATUSES:
            exit_code = max(
                exit_code,
                _run_follow_up_chain(
                    settings,
                    job_key=next_job_key,
                    target_date=next_target_date,
                    force=force,
                    skip_discord=skip_discord,
                    policy_config_path=policy_config_path,
                    visited=visited,
                ),
            )
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a scheduler job with serial lock discipline.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--job-key")
    target.add_argument("--service-slug")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument("--checkpoint-time")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-discord", action="store_true")
    parser.add_argument("--scheduler-run", action="store_true")
    parser.add_argument("--policy-config-path")
    parser.add_argument("--skip-chain", action="store_true")
    args = parser.parse_args()

    settings = load_cli_settings()
    job_key, _job = _resolve_job(args)
    target_date = _resolve_as_of_date(settings, args.as_of_date)
    exit_code = _execute_job(
        settings,
        job_key=job_key,
        target_date=target_date,
        checkpoint_time=args.checkpoint_time,
        dry_run=args.dry_run,
        force=args.force,
        skip_discord=args.skip_discord,
        scheduler_run=args.scheduler_run,
        policy_config_path=args.policy_config_path,
    )
    state = read_scheduler_state(settings, job_key)
    status = str(state.get("status") or "").upper()
    if (
        args.scheduler_run
        and not args.dry_run
        and not args.skip_chain
        and status in CHAINABLE_JOB_STATUSES
    ):
        exit_code = max(
            exit_code,
            _run_follow_up_chain(
                settings,
                job_key=job_key,
                target_date=target_date,
                force=args.force,
                skip_discord=args.skip_discord,
                policy_config_path=args.policy_config_path,
                visited={job_key},
            ),
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
