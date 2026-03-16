# ruff: noqa: E402

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
from scripts._ops_cli import load_cli_settings


def main() -> int:
    settings = load_cli_settings()
    probe_date = date(2026, 3, 9)
    results = [
        run_news_sync_bundle(settings, as_of_date=probe_date, profile="morning", dry_run=True),
        run_news_sync_bundle(settings, as_of_date=probe_date, profile="after_close", dry_run=True),
        run_intraday_assist_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_evaluation_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_daily_close_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_daily_audit_lite_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_daily_overlay_refresh_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_docker_build_cache_cleanup_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_weekly_training_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_weekly_calibration_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_weekly_policy_research_bundle(settings, as_of_date=probe_date, dry_run=True),
        run_ops_maintenance_bundle(settings, as_of_date=probe_date, dry_run=True),
    ]
    for item in results:
        print(f"{item.job_name}: {item.status} run_id={item.run_id}")
    failed = [item for item in results if str(item.status).upper() in {"FAILED", "BLOCKED"}]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
