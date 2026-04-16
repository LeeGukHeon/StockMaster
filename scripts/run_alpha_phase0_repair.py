# ruff: noqa: E402

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.audit.alpha_phase0_repair import run_alpha_phase0_repair
from app.logging import configure_logging, get_logger
from app.settings import load_settings


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _apply_host_runtime_overrides() -> None:
    runtime_root = Path(os.environ.get("STOCKMASTER_RUNTIME_ROOT", "/opt/stockmaster/runtime"))
    host_duckdb = runtime_root / "data" / "marts" / "main.duckdb"
    current_duckdb = os.environ.get("APP_DUCKDB_PATH", "")
    if current_duckdb.startswith("/workspace/") and host_duckdb.exists():
        os.environ["APP_DATA_DIR"] = str(runtime_root / "data")
        os.environ["APP_DUCKDB_PATH"] = str(host_duckdb)
        os.environ["APP_ARTIFACTS_DIR"] = str(runtime_root / "artifacts")
    if (
        os.environ.get("METADATA_DB_ENABLED", "false").lower() == "true"
        and os.environ.get("METADATA_DB_BACKEND", "duckdb").lower() == "postgres"
    ):
        current_url = os.environ.get("METADATA_DB_URL", "")
        if (not current_url) or ("@metadata_db" in current_url):
            user = os.environ.get("METADATA_DB_POSTGRES_USER", "stockmaster")
            password = os.environ.get("METADATA_DB_POSTGRES_PASSWORD", "change_me")
            database = os.environ.get("METADATA_DB_POSTGRES_DB", "stockmaster_meta")
            port = os.environ.get("METADATA_DB_HOST_PORT", "5433")
            os.environ["METADATA_DB_URL"] = (
                f"postgresql://{user}:{password}@127.0.0.1:{port}/{database}"
            )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Repair historical phase0 contamination.")
    parser.add_argument("--start-date", required=True, type=_parse_date)
    parser.add_argument("--end-date", required=True, type=_parse_date)
    parser.add_argument("--horizons", nargs="+", type=int, default=[1, 5])
    parser.add_argument("--min-train-days", type=int, default=120)
    parser.add_argument("--validation-days", type=int, default=20)
    args = parser.parse_args()

    _apply_host_runtime_overrides()
    settings = load_settings(project_root=PROJECT_ROOT)
    configure_logging(settings)
    logger = get_logger(__name__)
    result = run_alpha_phase0_repair(
        settings,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=list(args.horizons),
        min_train_days=int(args.min_train_days),
        validation_days=int(args.validation_days),
    )
    logger.info(
        "Alpha phase0 repair completed.",
        extra={
            "run_id_value": result.run_id,
            "repaired_training_run_count": result.repaired_training_run_count,
            "repaired_prediction_dates": result.repaired_prediction_dates,
        },
    )
    print(
        "Alpha phase0 repair completed. "
        f"run_id={result.run_id} "
        f"train_dates={len(result.affected_train_end_dates)} "
        f"prediction_dates={len(result.affected_as_of_dates)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
