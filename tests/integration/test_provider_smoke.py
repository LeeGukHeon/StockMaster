from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.common.paths import project_root
from app.ingestion.provider_smoke import run_provider_smoke_check
from app.settings import load_settings
from app.storage.duckdb import duckdb_connection


class FakeSmokeKISProvider:
    def __init__(self, raw_path: Path) -> None:
        self.raw_path = raw_path

    def fetch_current_quote(self, *, symbol: str):
        self.raw_path.write_text('{"symbol":"005930"}', encoding="utf-8")
        return {"symbol": symbol, "_raw_path": str(self.raw_path)}


class FakeSmokeDartProvider:
    def __init__(self, cache_path: Path, raw_path: Path) -> None:
        self.cache_path = cache_path
        self.raw_path = raw_path

    def load_corp_code_map(self, *, force: bool = False) -> pd.DataFrame:
        self.cache_path.write_text("cached", encoding="utf-8")
        return pd.DataFrame(
            [
                {
                    "corp_code": "00126380",
                    "corp_name": "삼성전자",
                    "stock_code": "005930",
                    "modify_date": pd.Timestamp("2024-01-01").date(),
                }
            ]
        )

    def fetch_company_overview(self, *, corp_code: str):
        self.raw_path.write_text('{"corp_code":"00126380"}', encoding="utf-8")
        return {"corp_code": corp_code, "_raw_path": str(self.raw_path)}


def test_provider_smoke_check_records_manifest(tmp_path):
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
    result = run_provider_smoke_check(
        settings,
        symbol="005930",
        kis_provider=FakeSmokeKISProvider(tmp_path / "quote.json"),
        dart_provider=FakeSmokeDartProvider(
            tmp_path / "corp_cache.txt", tmp_path / "overview.json"
        ),
    )

    assert result.kis_status == "ok"
    assert result.dart_status == "ok"
    assert result.corp_code == "00126380"
    assert len(result.artifact_paths) == 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        latest = connection.execute(
            """
            SELECT run_type, status
            FROM ops_run_manifest
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert latest == ("provider_smoke_check", "success")
