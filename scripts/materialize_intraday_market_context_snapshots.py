# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.context import materialize_intraday_market_context_snapshots
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize intraday market context snapshots.")
    parser.add_argument("--session-date", required=True, type=_parse_date)
    parser.add_argument("--checkpoints", nargs="+", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = materialize_intraday_market_context_snapshots(
        settings,
        session_date=args.session_date,
        checkpoints=args.checkpoints,
    )
    logger.info(
        "Intraday market context snapshots completed.",
        extra={"run_id_value": result.run_id},
    )
    print(
        "Intraday market context snapshots completed. "
        f"session_date={args.session_date.isoformat()} "
        f"run_id={result.run_id} rows={result.row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
