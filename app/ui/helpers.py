from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd

from app.common.disk import DiskUsageReport, measure_disk_usage
from app.providers.base import ProviderHealth
from app.providers.dart.client import DartProvider
from app.providers.kis.client import KISProvider
from app.providers.krx.client import KrxProvider
from app.providers.naver_news.client import NaverNewsProvider
from app.settings import Settings, load_settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import fetch_recent_runs


def load_ui_settings(project_root: Path) -> Settings:
    settings = load_settings(project_root=project_root)
    ensure_storage_layout(settings)
    if not settings.paths.duckdb_path.exists():
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
    return settings


def recent_runs_frame(settings: Settings, *, limit: int = 10) -> pd.DataFrame:
    if not settings.paths.duckdb_path.exists():
        return pd.DataFrame()
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        frame = fetch_recent_runs(connection, limit=limit)
    return frame


def disk_report(settings: Settings) -> DiskUsageReport:
    return measure_disk_usage(
        settings.paths.data_dir,
        warning_ratio=settings.storage.warning_ratio,
        prune_ratio=settings.storage.prune_ratio,
        limit_ratio=settings.storage.limit_ratio,
    )


def provider_health_frame(settings: Settings) -> pd.DataFrame:
    providers = [
        KISProvider(settings),
        DartProvider(settings),
        KrxProvider(settings),
        NaverNewsProvider(settings),
    ]
    try:
        rows: list[ProviderHealth] = [provider.health_check() for provider in providers]
    finally:
        for provider in providers:
            provider.close()
    return pd.DataFrame([asdict(row) for row in rows])


def watermark_frame(settings: Settings) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"threshold": "warning", "ratio": settings.storage.warning_ratio},
            {"threshold": "prune", "ratio": settings.storage.prune_ratio},
            {"threshold": "limit", "ratio": settings.storage.limit_ratio},
        ]
    )
