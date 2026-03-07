from __future__ import annotations

from app.common.paths import project_root
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


def test_bootstrap_creates_storage_layout_and_manifest(tmp_path):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "integration.duckdb"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"APP_DATA_DIR={data_dir.as_posix()}",
                f"APP_DUCKDB_PATH={duckdb_path.as_posix()}",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)
    result = bootstrap_storage(settings)

    assert result.duckdb_path.exists()
    assert settings.paths.raw_dir.exists()
    assert settings.paths.curated_dir.exists()
    assert settings.paths.logs_dir.exists()

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {
            "dim_symbol",
            "dim_trading_calendar",
            "ops_run_manifest",
            "ops_disk_usage_log",
        } <= tables
        latest_run = connection.execute(
            """
            SELECT run_type, status
            FROM ops_run_manifest
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert latest_run == ("bootstrap", "success")
