from __future__ import annotations

import argparse
from pathlib import Path

from app.settings import load_settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.ui.read_model import materialize_ui_read_model_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize the UI read-model snapshot.")
    parser.add_argument("--job-run-id", default="manual-ui-read-model", help="Run id label for the snapshot artifact path.")
    parser.add_argument("--env-file", default=None, help="Optional env file. Defaults to deploy/env/.env.server when available.")
    parser.add_argument("--scope", default="all", choices=["all", "core", "stock_intraday"], help="Dataset scope to materialize.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    default_env = project_root / "deploy" / "env" / ".env.server"
    env_file = args.env_file or (default_env if default_env.exists() else None)
    settings = load_settings(project_root=project_root, env_file=env_file)
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        result = materialize_ui_read_model_snapshot(
            settings,
            connection=connection,
            as_of_date=None,
            job_run_id=str(args.job_run_id),
            scope=str(args.scope),
        )
    print(
        f"UI read-model snapshot completed. run_id={result.run_id} "
        f"rows={result.row_count} artifacts={len(result.artifact_paths)}"
    )


if __name__ == "__main__":
    main()
