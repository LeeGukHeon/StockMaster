# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
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
from app.ops.common import TriggerType
from app.ops.scheduler import (
    get_scheduled_job,
    get_scheduled_job_by_service_slug,
    resolve_due_intraday_checkpoint,
)
from scripts._ops_cli import load_cli_settings, parse_date
from scripts._scheduler_cli import bundle_result
from scripts._scheduler_cli import run_scheduled_bundle as run_with_scheduler


def _resolve_job(args) -> tuple[str, object]:
    if args.service_slug:
        job = get_scheduled_job_by_service_slug(args.service_slug)
    else:
        job = get_scheduled_job(args.job_key)
    return job.job_key, job


def _resolve_as_of_date(settings, explicit_date: date | None) -> date:
    return explicit_date or today_local(settings.app.timezone)


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
    args = parser.parse_args()

    settings = load_cli_settings()
    job_key, job = _resolve_job(args)
    target_date = _resolve_as_of_date(settings, args.as_of_date)
    profile = None
    if job_key == "news_morning":
        profile = "morning"
    elif job_key == "news_after_close":
        profile = "after_close"
    checkpoint_time = args.checkpoint_time
    if job_key == "intraday_assist" and checkpoint_time is None:
        checkpoint_time = resolve_due_intraday_checkpoint(settings) or "PREP"

    identity = {"as_of_date": target_date.isoformat()}
    if profile is not None:
        identity["profile"] = profile
    if checkpoint_time is not None:
        identity["checkpoint_time"] = checkpoint_time

    def runner(runtime_settings):
        trigger_type = TriggerType.SCHEDULED if args.scheduler_run else TriggerType.MANUAL
        if job_key in {"news_morning", "news_after_close"}:
            result = run_news_sync_bundle(
                runtime_settings,
                as_of_date=target_date,
                profile=profile or "after_close",
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "daily_close":
            result = run_daily_close_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                publish_discord=not args.skip_discord,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "daily_overlay_refresh":
            result = run_daily_overlay_refresh_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "docker_build_cache_cleanup":
            result = run_docker_build_cache_cleanup_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "evaluation":
            result = run_evaluation_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "intraday_assist":
            result = run_intraday_assist_bundle(
                runtime_settings,
                as_of_date=target_date,
                checkpoint_time=None if checkpoint_time == "PREP" else checkpoint_time,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "weekly_training_candidate":
            result = run_weekly_training_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "weekly_calibration":
            result = run_weekly_calibration_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "weekly_policy_research":
            result = run_weekly_policy_research_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "ops_maintenance":
            result = run_ops_maintenance_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                policy_config_path=args.policy_config_path,
            )
        elif job_key == "daily_audit_lite":
            result = run_daily_audit_lite_bundle(
                runtime_settings,
                as_of_date=target_date,
                trigger_type=trigger_type,
                dry_run=args.dry_run,
                force=args.force,
                policy_config_path=args.policy_config_path,
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
        force=args.force,
    )


if __name__ == "__main__":
    raise SystemExit(main())
