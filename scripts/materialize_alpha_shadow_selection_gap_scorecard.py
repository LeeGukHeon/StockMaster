#!/usr/bin/env python3
# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.evaluation.alpha_shadow import materialize_alpha_shadow_selection_gap_scorecard
from app.settings import load_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-selection-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-selection-date", type=date.fromisoformat, required=True)
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5])
    parser.add_argument("--rolling-windows", type=int, nargs="+", default=[20, 60])
    parser.add_argument("--model-spec-ids", nargs="*", default=None)
    parser.add_argument(
        "--skip-outcome-refresh",
        action="store_true",
        help="Reuse existing fact_alpha_shadow_selection_outcome rows instead of rematerializing them.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    result = materialize_alpha_shadow_selection_gap_scorecard(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=list(args.horizons),
        model_spec_ids=list(args.model_spec_ids) if args.model_spec_ids else None,
        rolling_windows=list(args.rolling_windows),
        ensure_shadow_selection_outcomes=not args.skip_outcome_refresh,
    )
    print(result.notes)


if __name__ == "__main__":
    main()
