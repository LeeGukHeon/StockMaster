from __future__ import annotations

import argparse
from datetime import date

from app.ml.shadow import materialize_alpha_shadow_candidates
from app.settings import load_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of-date", type=date.fromisoformat, required=True)
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5])
    parser.add_argument("--limit-symbols", type=int, default=None)
    parser.add_argument("--market", default="ALL")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_settings()
    result = materialize_alpha_shadow_candidates(
        settings,
        as_of_date=args.as_of_date,
        horizons=list(args.horizons),
        limit_symbols=args.limit_symbols,
        market=args.market,
    )
    print(result.notes)


if __name__ == "__main__":
    main()
