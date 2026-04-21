from __future__ import annotations

import argparse
from datetime import date

from app.ml.constants import CHALLENGER_ALPHA_MODEL_SPECS, get_alpha_model_spec
from app.ml.training import train_alpha_candidate_models
from app.settings import load_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5])
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    parser.add_argument("--limit-symbols", type=int, default=None)
    parser.add_argument("--market", default="ALL")
    parser.add_argument("--model-spec-ids", nargs="+", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_settings()
    result = train_alpha_candidate_models(
        settings,
        train_end_date=args.train_end_date,
        horizons=list(args.horizons),
        min_train_days=args.min_train_days,
        validation_days=args.validation_days,
        limit_symbols=args.limit_symbols,
        market=args.market,
        model_specs=(
            tuple(get_alpha_model_spec(model_spec_id) for model_spec_id in args.model_spec_ids)
            if args.model_spec_ids
            else CHALLENGER_ALPHA_MODEL_SPECS
        ),
    )
    print(result.notes)


if __name__ == "__main__":
    main()
