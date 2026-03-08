# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.policy import materialize_intraday_policy_candidates
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize intraday policy candidates.")
    parser.add_argument("--search-space-version", required=True)
    parser.add_argument("--horizons", nargs="+", type=int, required=True)
    parser.add_argument("--checkpoints", nargs="+", required=True)
    parser.add_argument("--scopes", nargs="+", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = materialize_intraday_policy_candidates(
        settings,
        search_space_version=args.search_space_version,
        horizons=args.horizons,
        checkpoints=args.checkpoints,
        scopes=args.scopes,
    )
    logger.info("Intraday policy candidates completed.", extra={"run_id_value": result.run_id})
    print(f"Intraday policy candidates completed. run_id={result.run_id} rows={result.row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
