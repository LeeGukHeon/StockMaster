# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.alpha_phase0 import run_alpha_phase0_audit
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run phase 0 alpha chasing/PIT audit.")
    parser.add_argument("--start-date", required=True, type=_parse_date)
    parser.add_argument("--end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument("--cutoff-time", default="17:30")
    parser.add_argument("--top-k", type=int, default=10)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)

    result = run_alpha_phase0_audit(
        settings,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=list(args.horizons),
        cutoff_time=args.cutoff_time,
        top_k=int(args.top_k),
    )
    logger.info(
        "Alpha phase0 audit completed.",
        extra={
            "run_id_value": result.run_id,
            "branch_recommendation": result.branch_recommendation,
            "pit_status": result.pit_status,
            "row_count": result.row_count,
        },
    )
    print(
        "Alpha phase0 audit completed. "
        f"run_id={result.run_id} branch={result.branch_recommendation} "
        f"pit_status={result.pit_status} metric_rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
