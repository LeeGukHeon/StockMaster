# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.scheduler.jobs import run_evaluation_job
from app.settings import load_settings
from scripts._ops_cli import parse_date


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the evaluation job.")
    parser.add_argument("--as-of-date", type=parse_date)
    args = parser.parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_evaluation_job(settings, selection_end_date=args.as_of_date)
    logger.info(
        "Evaluation finished.",
        extra={"run_id_value": result.run_id, "status": result.status},
    )
    print(f"Evaluation completed. run_id={result.run_id} status={result.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
