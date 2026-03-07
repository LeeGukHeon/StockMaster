# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage


def main() -> int:
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = bootstrap_storage(settings)
    logger.info(
        "Bootstrap completed.",
        extra={
            "bootstrap_run_id": result.run_id,
            "duckdb_path": str(result.duckdb_path),
            "created_directories": [str(path) for path in result.created_directories],
        },
    )
    print(f"Bootstrap completed. run_id={result.run_id} duckdb={result.duckdb_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
