from __future__ import annotations

import argparse

from app.settings import get_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.ui.read_model import materialize_ui_read_model_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize the UI read-model snapshot.")
    parser.add_argument("--job-run-id", default="manual-ui-read-model", help="Run id label for the snapshot artifact path.")
    args = parser.parse_args()

    settings = get_settings()
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        result = materialize_ui_read_model_snapshot(
            settings,
            connection=connection,
            as_of_date=None,
            job_run_id=str(args.job_run_id),
        )
    print(
        f"UI read-model snapshot completed. run_id={result.run_id} "
        f"rows={result.row_count} artifacts={len(result.artifact_paths)}"
    )


if __name__ == "__main__":
    main()
