# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.intraday.collector import run_intraday_candidate_collector
from app.logging import configure_logging
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run intraday candidate collector.")
    parser.add_argument("--session-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-cycles", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    result = run_intraday_candidate_collector(
        settings,
        session_date=args.session_date,
        horizons=args.horizons,
        poll_seconds=args.poll_seconds,
        dry_run=args.dry_run,
        max_cycles=args.max_cycles,
    )
    print(
        f"Intraday candidate collector completed. session_date={args.session_date.isoformat()} "
        f"cycles={result.cycle_count} checkpoints={','.join(result.processed_checkpoints)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
