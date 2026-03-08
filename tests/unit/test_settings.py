from __future__ import annotations

from pathlib import Path

import pytest

from app.common.paths import project_root
from app.settings import load_settings


def test_load_settings_applies_env_overrides(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=prod",
                "APP_DATA_DIR=./runtime-data",
                "APP_DUCKDB_PATH=./runtime-data/marts/test.duckdb",
                "STORAGE_WARNING_RATIO=0.55",
                "MODEL_DEFAULT_HORIZONS=D1,D5,D10",
                "DISCORD_REPORT_ENABLED=true",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)

    assert settings.app.env == "prod"
    assert settings.paths.data_dir == (project_root() / "runtime-data").resolve()
    assert (
        settings.paths.duckdb_path
        == (project_root() / "runtime-data" / "marts" / "test.duckdb").resolve()
    )
    assert settings.storage.warning_ratio == 0.55
    assert settings.model.default_horizons == ["D1", "D5", "D10"]
    assert settings.discord.enabled is True


def test_load_settings_raises_for_missing_explicit_env_file():
    with pytest.raises(FileNotFoundError):
        load_settings(project_root=project_root(), env_file=Path("missing.env"))


def test_load_settings_accepts_server_environment_profile(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=server",
                "APP_DATA_DIR=./server-data",
                "APP_DUCKDB_PATH=./server-data/marts/server.duckdb",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)

    assert settings.app.env == "server"
    assert settings.paths.data_dir == (project_root() / "server-data").resolve()
    assert (
        settings.paths.duckdb_path
        == (project_root() / "server-data" / "marts" / "server.duckdb").resolve()
    )
