# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.scheduler.jobs import run_daily_pipeline_job
from app.settings import load_settings
from scripts._ops_cli import parse_date


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daily pipeline job.")
    parser.add_argument("--as-of-date", type=parse_date)
    parser.add_argument(
        "--skip-active-d5-swing",
        action="store_true",
        help="Do not bootstrap alpha_swing_d5_v2 into the H5 active/auto-promotion cycle.",
    )
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_daily_pipeline_job(
        settings,
        pipeline_date=args.as_of_date,
        active_d5_swing=not args.skip_active_d5_swing,
    )
    logger.info(
        "Daily pipeline finished.",
        extra={"run_id_value": result.run_id, "status": result.status},
    )
    print(f"Daily pipeline completed. run_id={result.run_id} status={result.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
