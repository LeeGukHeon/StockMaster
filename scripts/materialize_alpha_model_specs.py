#!/usr/bin/env python3
# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging import configure_logging, get_logger
from app.ml.constants import ALPHA_CANDIDATE_MODEL_SPECS
from app.ml.registry import upsert_alpha_model_specs
from app.ml.training import build_alpha_model_spec_registry_frame
from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection


def main() -> None:
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    frame = build_alpha_model_spec_registry_frame(ALPHA_CANDIDATE_MODEL_SPECS)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        upsert_alpha_model_specs(connection, frame)
    logger.info(
        "Alpha model specs materialized.",
        extra={"row_count": len(frame)},
    )
    print(f"Alpha model specs materialized. rows={len(frame)}")


if __name__ == "__main__":
    main()
