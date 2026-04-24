# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ops.scheduler import schedule_job_catalog_frame
from scripts._ops_cli import load_cli_settings

REQUIRED_FILES = [
    "deploy/systemd/stockmaster-scheduler@.service",
    "deploy/systemd/stockmaster-ops-maintenance.timer",
    "deploy/systemd/stockmaster-news-morning.timer",
    "deploy/systemd/stockmaster-news-after-close.timer",
    "deploy/systemd/stockmaster-evaluation.timer",
    "deploy/systemd/stockmaster-daily-close.timer",
    "scripts/server/run_scheduler_job.sh",
    "scripts/server/install_scheduler_units.sh",
    "scripts/server/uninstall_scheduler_units.sh",
    "scripts/server/status_scheduler_units.sh",
]


def main() -> int:
    settings = load_cli_settings()
    missing = [relative for relative in REQUIRED_FILES if not (PROJECT_ROOT / relative).exists()]
    catalog = schedule_job_catalog_frame(settings)
    expected_jobs = {
        "ops_maintenance",
        "news_morning",
        "news_after_close",
        "evaluation",
        "daily_close",
    }
    catalog_jobs = set(catalog["job_key"].tolist()) if not catalog.empty else set()
    missing_jobs = sorted(expected_jobs - catalog_jobs)
    print(
        "Scheduler validation: "
        f"files_missing={len(missing)} jobs_missing={len(missing_jobs)} catalog_rows={len(catalog)}"
    )
    if missing:
        print("Missing files:")
        for item in missing:
            print(f"- {item}")
    if missing_jobs:
        print("Missing scheduled jobs:")
        for item in missing_jobs:
            print(f"- {item}")
    return 1 if missing or missing_jobs else 0


if __name__ == "__main__":
    raise SystemExit(main())
