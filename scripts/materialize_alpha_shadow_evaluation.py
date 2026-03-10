from __future__ import annotations

import argparse
from datetime import date

from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_outcomes,
)
from app.settings import load_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-selection-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-selection-date", type=date.fromisoformat, required=True)
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5])
    parser.add_argument("--rolling-windows", type=int, nargs="+", default=[20, 60])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_settings()
    outcome_result = materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=list(args.horizons),
    )
    summary_result = materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=args.start_selection_date,
        end_selection_date=args.end_selection_date,
        horizons=list(args.horizons),
        rolling_windows=list(args.rolling_windows),
    )
    print(outcome_result.notes)
    print(summary_result.notes)


if __name__ == "__main__":
    main()
