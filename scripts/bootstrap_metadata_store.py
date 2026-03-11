# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.settings import load_settings
from app.storage.metadata_postgres import (
    bootstrap_postgres_metadata_store,
    metadata_postgres_enabled,
)


def main() -> int:
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    if not metadata_postgres_enabled(settings):
        print("Metadata bootstrap skipped. Enable postgres metadata store first.")
        return 0
    bootstrap_postgres_metadata_store(settings)
    get_logger(__name__).info(
        "Metadata store bootstrapped.",
        extra={"metadata_schema": settings.metadata.db_schema},
    )
    print(f"Metadata store bootstrapped. schema={settings.metadata.db_schema}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
