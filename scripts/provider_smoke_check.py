# ruff: noqa: E402

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ingestion.provider_smoke import run_provider_smoke_check
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run KIS/DART provider smoke checks.")
    parser.add_argument("--symbol", default="005930", help="KR stock symbol for the probe.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_provider_smoke_check(settings, symbol=args.symbol)
    logger.info(
        "Provider smoke check completed.",
        extra={
            "run_id_value": result.run_id,
            "symbol": result.symbol,
            "kis_status": result.kis_status,
            "dart_status": result.dart_status,
            "corp_code": result.corp_code,
        },
    )
    print(
        "Provider smoke check completed. "
        f"run_id={result.run_id} symbol={result.symbol} "
        f"kis={result.kis_status} dart={result.dart_status} corp_code={result.corp_code}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
