# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ingestion.universe_sync import sync_universe
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def main() -> int:
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = sync_universe(settings)
    logger.info(
        "Universe sync completed.",
        extra={
            "run_id_value": result.run_id,
            "row_count": result.row_count,
            "active_common_stock_count": result.active_common_stock_count,
            "dart_mapped_count": result.dart_mapped_count,
        },
    )
    print(
        "Universe sync completed. "
        f"run_id={result.run_id} rows={result.row_count} "
        f"active_common={result.active_common_stock_count} "
        f"dart_mapped={result.dart_mapped_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
