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
                "DASHBOARD_ACCESS_ENABLED=true",
                "DASHBOARD_ACCESS_USERNAME=mobile",
                "DASHBOARD_ACCESS_PASSWORD=secret-pass",
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
    assert settings.dashboard_access.enabled is True
    assert settings.dashboard_access.username == "mobile"
    assert settings.dashboard_access.password == "secret-pass"


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
    assert settings.intraday_research.enabled is True
    assert settings.intraday_research.assist_enabled is True
    assert settings.intraday_research.postmortem_enabled is True
    assert settings.intraday_research.policy_adjustment_enabled is True
    assert settings.intraday_research.meta_model_enabled is True
    assert settings.intraday_research.research_reports_enabled is True
    assert settings.intraday_research.rollout_mode == "RESEARCH_NON_TRADING"


def test_load_settings_keeps_intraday_research_disabled_in_local_profile(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "APP_ENV=local",
                "APP_DATA_DIR=./local-data",
                "APP_DUCKDB_PATH=./local-data/marts/local.duckdb",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)

    assert settings.app.env == "local"
    assert settings.intraday_research.enabled is False
    assert settings.intraday_research.assist_enabled is False
    assert settings.intraday_research.meta_model_enabled is False


def test_load_settings_enables_krx_live_with_allowlist(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ENABLE_KRX_LIVE=true",
                "KRX_API_KEY=test-key",
                "KRX_ALLOWED_SERVICES=etf_daily_trade,index_kospi_daily",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)

    assert settings.providers.krx.enabled_live is True
    assert settings.providers.krx.api_key == "test-key"
    assert settings.providers.krx.allowed_services == ["etf_daily_trade", "index_kospi_daily"]
    assert settings.providers.krx.service_urls["etf_daily_trade"].endswith(
        "/svc/apis/etp/etf_bydd_trd"
    )


def test_load_settings_accepts_postgres_metadata_store(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "METADATA_DB_ENABLED=true",
                "METADATA_DB_BACKEND=postgres",
                "METADATA_DB_URL=postgresql://stockmaster:secret@127.0.0.1:5432/stockmaster_meta",
                "METADATA_DB_SCHEMA=stockmaster_meta",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(project_root=project_root(), env_file=env_file)

    assert settings.metadata.enabled is True
    assert settings.metadata.backend == "postgres"
    assert settings.metadata.db_schema == "stockmaster_meta"
    assert settings.metadata.db_url.startswith("postgresql://stockmaster:")


def test_load_settings_requires_dashboard_password_when_access_enabled(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DASHBOARD_ACCESS_ENABLED=true",
                "DASHBOARD_ACCESS_USERNAME=stockmaster",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="DASHBOARD_ACCESS_PASSWORD"):
        load_settings(project_root=project_root(), env_file=env_file)
