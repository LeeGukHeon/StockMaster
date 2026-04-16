# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.ml.shadow_report import render_alpha_shadow_comparison_report
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Render alpha shadow comparison report.")
    parser.add_argument("--start-selection-date", required=True, type=_parse_date)
    parser.add_argument("--end-selection-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    args = parser.parse_args()

    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = render_alpha_shadow_comparison_report(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=list(args.horizons),
    )
    logger.info(
        "Alpha shadow comparison report rendered.",
        extra={"run_id_value": result.run_id, "row_count": result.row_count},
    )
    print(
        f"Alpha shadow comparison report rendered. run_id={result.run_id} rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
