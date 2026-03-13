from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from app.common.paths import project_root as detect_project_root
from app.common.paths import resolve_path
from app.providers.krx.registry import canonicalize_krx_service_slugs, krx_default_service_urls


class AppConfig(BaseModel):
    name: str = "stockmaster"
    display_name: str = "KR Stock Research Platform v1"
    env: Literal["local", "server", "prod", "prod_like"] = "local"
    timezone: str = "Asia/Seoul"


class PathConfig(BaseModel):
    project_root: Path
    data_dir: Path
    duckdb_path: Path
    raw_dir: Path | None = None
    curated_dir: Path | None = None
    marts_dir: Path | None = None
    cache_dir: Path | None = None
    logs_dir: Path | None = None
    artifacts_dir: Path | None = None

    @model_validator(mode="after")
    def normalize_paths(self) -> "PathConfig":
        self.project_root = self.project_root.resolve()
        self.data_dir = resolve_path(self.data_dir, self.project_root)
        self.duckdb_path = resolve_path(self.duckdb_path, self.project_root)
        self.raw_dir = resolve_path(self.raw_dir or self.data_dir / "raw", self.project_root)
        self.curated_dir = resolve_path(
            self.curated_dir or self.data_dir / "curated",
            self.project_root,
        )
        self.marts_dir = resolve_path(self.marts_dir or self.data_dir / "marts", self.project_root)
        self.cache_dir = resolve_path(self.cache_dir or self.data_dir / "cache", self.project_root)
        self.logs_dir = resolve_path(self.logs_dir or self.data_dir / "logs", self.project_root)
        self.artifacts_dir = resolve_path(
            self.artifacts_dir or self.data_dir / "artifacts",
            self.project_root,
        )
        return self

    def data_directories(self) -> list[Path]:
        return [
            self.data_dir,
            self.raw_dir,
            self.curated_dir,
            self.marts_dir,
            self.cache_dir,
            self.logs_dir,
            self.artifacts_dir,
        ]


class LoggingConfig(BaseModel):
    level: str = "INFO"
    filename: str = "app.log"

    @field_validator("level")
    @classmethod
    def normalize_level(cls, value: str) -> str:
        return value.upper()


class StorageConfig(BaseModel):
    warning_ratio: float = 0.70
    prune_ratio: float = 0.80
    limit_ratio: float = 0.90

    @model_validator(mode="after")
    def validate_thresholds(self) -> "StorageConfig":
        if not 0 < self.warning_ratio < 1:
            raise ValueError("warning_ratio must be between 0 and 1.")
        if not 0 < self.prune_ratio < 1:
            raise ValueError("prune_ratio must be between 0 and 1.")
        if not 0 < self.limit_ratio <= 1:
            raise ValueError("limit_ratio must be between 0 and 1.")
        if not self.warning_ratio < self.prune_ratio < self.limit_ratio:
            raise ValueError("Storage thresholds must be ordered warning < prune < limit.")
        return self


class RetentionConfig(BaseModel):
    raw_api_days: int = 7
    intraday_5m_days: int = 90
    intraday_1m_days: int = 60
    orderbook_summary_days: int = 30
    report_cache_days: int = 7
    log_days: int = 30


class KisProviderConfig(BaseModel):
    base_url: str
    app_key: str | None = None
    app_secret: str | None = None
    account_no: str | None = None
    product_code: str | None = None
    use_mock: bool = False


class DartProviderConfig(BaseModel):
    base_url: str
    api_key: str | None = None


class KrxProviderConfig(BaseModel):
    base_url: str
    api_key: str | None = None
    enabled_live: bool = False
    allowed_services: list[str] = Field(default_factory=list)
    daily_request_budget: int = 1000
    request_timeout_seconds: float = 20.0
    source_attribution_label: str = "한국거래소 통계정보"
    service_urls: dict[str, str] = Field(default_factory=krx_default_service_urls)

    @field_validator("allowed_services", mode="before")
    @classmethod
    def normalize_allowed_services(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            values = [item.strip() for item in value.split(",") if item.strip()]
        else:
            values = list(value)
        return canonicalize_krx_service_slugs(values)

    @model_validator(mode="after")
    def validate_live_configuration(self) -> "KrxProviderConfig":
        if self.daily_request_budget <= 0:
            raise ValueError("KRX daily request budget must be positive.")
        if self.request_timeout_seconds <= 0:
            raise ValueError("KRX request timeout must be positive.")
        cleaned_urls = {
            slug: url
            for slug, url in self.service_urls.items()
            if slug in set(self.allowed_services) and str(url).strip()
        }
        self.service_urls = cleaned_urls
        if not self.enabled_live:
            return self
        if not self.api_key:
            raise ValueError("KRX_API_KEY is required when ENABLE_KRX_LIVE=true.")
        if not self.allowed_services:
            raise ValueError("KRX allowed service list is required when ENABLE_KRX_LIVE=true.")
        return self


class NaverNewsProviderConfig(BaseModel):
    base_url: str
    client_id: str | None = None
    client_secret: str | None = None


class ProviderConfig(BaseModel):
    kis: KisProviderConfig
    dart: DartProviderConfig
    krx: KrxProviderConfig
    naver_news: NaverNewsProviderConfig


class DiscordConfig(BaseModel):
    enabled: bool = False
    webhook_url: str | None = None
    username: str = "KR Stock Research Bot"


class DashboardAccessConfig(BaseModel):
    enabled: bool = False
    username: str = "stockmaster"
    password: str | None = None

    @model_validator(mode="after")
    def validate_access(self) -> "DashboardAccessConfig":
        if self.enabled and not self.password:
            raise ValueError(
                "DASHBOARD_ACCESS_PASSWORD is required when dashboard access is enabled."
            )
        return self


class MetadataStoreConfig(BaseModel):
    enabled: bool = False
    backend: Literal["duckdb", "postgres"] = "duckdb"
    db_url: str | None = None
    db_schema: str = "public"

    @model_validator(mode="after")
    def validate_backend(self) -> "MetadataStoreConfig":
        if self.enabled and self.backend == "postgres" and not self.db_url:
            raise ValueError("METADATA_DB_URL is required when metadata backend is postgres.")
        return self


class ModelConfig(BaseModel):
    default_horizons: list[str] = Field(default_factory=lambda: ["D1", "D5"])
    uncertainty_lambda: float = 1.0
    disagreement_eta: float = 1.0
    implementation_kappa: float = 1.0
    regime_rho: float = 1.0

    @field_validator("default_horizons", mode="before")
    @classmethod
    def normalize_horizons(cls, value: Any) -> list[str]:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return list(value)


class IntradayResearchConfig(BaseModel):
    enabled: bool = False
    assist_enabled: bool = False
    postmortem_enabled: bool = False
    policy_adjustment_enabled: bool = False
    meta_model_enabled: bool = False
    research_reports_enabled: bool = False
    discord_summary_enabled: bool = False
    writeback_enabled: bool = False
    rollout_mode: str = "RESEARCH_NON_TRADING"


class Settings(BaseModel):
    app: AppConfig
    paths: PathConfig
    logging: LoggingConfig
    storage: StorageConfig
    retention: RetentionConfig
    providers: ProviderConfig
    discord: DiscordConfig
    dashboard_access: DashboardAccessConfig = Field(default_factory=DashboardAccessConfig)
    metadata: MetadataStoreConfig
    model: ModelConfig
    intraday_research: IntradayResearchConfig


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing configuration file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Configuration file must contain a mapping: {path}")
    return data


def _resolve_env_file(project_root: Path, env_file: str | Path | None) -> Path | None:
    if env_file is None:
        candidate = project_root / ".env"
        return candidate if candidate.exists() else None
    candidate = Path(env_file)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    if not candidate.exists():
        raise FileNotFoundError(f"Environment file not found: {candidate}")
    return candidate.resolve()


def _read_env_values(env_path: Path | None) -> dict[str, str]:
    values: dict[str, str] = {}
    if env_path is not None:
        values.update(
            {key: value for key, value in dotenv_values(env_path).items() if value is not None}
        )
    for key, value in os.environ.items():
        if value:
            values[key] = value
    return values


def _parse_bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _apply_env_overrides(config: dict[str, Any], env_values: dict[str, str]) -> dict[str, Any]:
    paths = config.setdefault("paths", {})
    app = config.setdefault("app", {})
    logging_cfg = config.setdefault("logging", {})
    storage = config.setdefault("storage", {})
    providers = config.setdefault("providers", {})
    discord = config.setdefault("discord", {})
    dashboard_access = config.setdefault("dashboard_access", {})
    metadata = config.setdefault("metadata", {})
    model = config.setdefault("model", {})
    retention = config.setdefault("retention", {})
    intraday_research = config.setdefault("intraday_research", {})

    app["env"] = env_values.get("APP_ENV", app.get("env"))
    app["timezone"] = env_values.get("APP_TIMEZONE", app.get("timezone"))
    env_name = str(app.get("env") or "local")
    research_default = env_name in {"server", "prod_like"}

    logging_cfg["level"] = env_values.get("APP_LOG_LEVEL", logging_cfg.get("level"))

    paths["data_dir"] = env_values.get("APP_DATA_DIR", paths.get("data_dir"))
    paths["duckdb_path"] = env_values.get("APP_DUCKDB_PATH", paths.get("duckdb_path"))

    storage["warning_ratio"] = float(
        env_values.get("STORAGE_WARNING_RATIO", storage.get("warning_ratio"))
    )
    storage["prune_ratio"] = float(
        env_values.get("STORAGE_PRUNE_RATIO", storage.get("prune_ratio"))
    )
    storage["limit_ratio"] = float(
        env_values.get("STORAGE_LIMIT_RATIO", storage.get("limit_ratio"))
    )

    retention["raw_api_days"] = int(
        env_values.get("RETENTION_RAW_API_DAYS", retention.get("raw_api_days"))
    )
    retention["intraday_5m_days"] = int(
        env_values.get("RETENTION_INTRADAY_5M_DAYS", retention.get("intraday_5m_days"))
    )
    retention["intraday_1m_days"] = int(
        env_values.get("RETENTION_INTRADAY_1M_DAYS", retention.get("intraday_1m_days"))
    )
    retention["orderbook_summary_days"] = int(
        env_values.get(
            "RETENTION_ORDERBOOK_SUMMARY_DAYS",
            retention.get("orderbook_summary_days"),
        )
    )
    retention["report_cache_days"] = int(
        env_values.get("RETENTION_REPORT_CACHE_DAYS", retention.get("report_cache_days"))
    )
    retention["log_days"] = int(env_values.get("RETENTION_LOG_DAYS", retention.get("log_days")))

    kis = providers.setdefault("kis", {})
    kis["app_key"] = env_values.get("KIS_APP_KEY")
    kis["app_secret"] = env_values.get("KIS_APP_SECRET")
    kis["account_no"] = env_values.get("KIS_ACCOUNT_NO")
    kis["product_code"] = env_values.get("KIS_PRODUCT_CODE")
    kis["use_mock"] = _parse_bool(env_values.get("KIS_USE_MOCK"), kis.get("use_mock", False))

    dart = providers.setdefault("dart", {})
    dart["api_key"] = env_values.get("DART_API_KEY")

    krx = providers.setdefault("krx", {})
    krx["api_key"] = env_values.get("KRX_API_KEY")
    krx["enabled_live"] = _parse_bool(
        env_values.get("ENABLE_KRX_LIVE"),
        krx.get("enabled_live", False),
    )
    allowed_services = env_values.get(
        "KRX_ALLOWED_SERVICES",
        env_values.get("KRX_APPROVED_SERVICE_SLUGS"),
    )
    if allowed_services is not None:
        krx["allowed_services"] = allowed_services
    krx["daily_request_budget"] = int(
        env_values.get("KRX_DAILY_REQUEST_BUDGET", krx.get("daily_request_budget", 1000))
    )
    krx["request_timeout_seconds"] = float(
        env_values.get(
            "KRX_REQUEST_TIMEOUT_SECONDS",
            krx.get("request_timeout_seconds", 20.0),
        )
    )
    krx["source_attribution_label"] = env_values.get(
        "KRX_SOURCE_ATTRIBUTION_LABEL",
        krx.get("source_attribution_label", "한국거래소 통계정보"),
    )
    configured_service_urls = dict(krx.get("service_urls", krx_default_service_urls()))
    for env_key, env_value in env_values.items():
        if not env_key.startswith("KRX_SERVICE_URL_") or not env_value:
            continue
        raw_slug = env_key.removeprefix("KRX_SERVICE_URL_").strip().lower()
        service_slug = raw_slug.replace("__", "-").replace("_", "_")
        configured_service_urls[service_slug] = env_value
    if configured_service_urls:
        krx["service_urls"] = configured_service_urls

    naver_news = providers.setdefault("naver_news", {})
    naver_news["client_id"] = env_values.get("NAVER_CLIENT_ID")
    naver_news["client_secret"] = env_values.get("NAVER_CLIENT_SECRET")

    discord["enabled"] = _parse_bool(
        env_values.get("DISCORD_REPORT_ENABLED"),
        discord.get("enabled", False),
    )
    discord["webhook_url"] = env_values.get("DISCORD_WEBHOOK_URL")
    discord["username"] = env_values.get("DISCORD_USERNAME", discord.get("username"))

    dashboard_access["enabled"] = _parse_bool(
        env_values.get("DASHBOARD_ACCESS_ENABLED"),
        dashboard_access.get("enabled", False),
    )
    dashboard_access["username"] = env_values.get(
        "DASHBOARD_ACCESS_USERNAME",
        dashboard_access.get("username", "stockmaster"),
    )
    dashboard_access["password"] = env_values.get(
        "DASHBOARD_ACCESS_PASSWORD",
        dashboard_access.get("password"),
    )

    metadata["enabled"] = _parse_bool(
        env_values.get("METADATA_DB_ENABLED"),
        metadata.get("enabled", False),
    )
    metadata["backend"] = env_values.get("METADATA_DB_BACKEND", metadata.get("backend", "duckdb"))
    metadata["db_url"] = env_values.get("METADATA_DB_URL", metadata.get("db_url"))
    metadata["db_schema"] = env_values.get(
        "METADATA_DB_SCHEMA",
        metadata.get("db_schema", "public"),
    )

    model["default_horizons"] = env_values.get(
        "MODEL_DEFAULT_HORIZONS",
        model.get("default_horizons"),
    )
    model["uncertainty_lambda"] = float(
        env_values.get("MODEL_UNCERTAINTY_LAMBDA", model.get("uncertainty_lambda"))
    )
    model["disagreement_eta"] = float(
        env_values.get("MODEL_DISAGREEMENT_ETA", model.get("disagreement_eta"))
    )
    model["implementation_kappa"] = float(
        env_values.get("MODEL_IMPLEMENTATION_KAPPA", model.get("implementation_kappa"))
    )
    model["regime_rho"] = float(env_values.get("MODEL_REGIME_RHO", model.get("regime_rho")))

    intraday_research["enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_RESEARCH"),
        intraday_research.get("enabled", research_default),
    )
    intraday_default = bool(intraday_research["enabled"])
    intraday_research["assist_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_ASSIST"),
        intraday_research.get("assist_enabled", intraday_default),
    )
    intraday_research["postmortem_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_POSTMORTEM"),
        intraday_research.get("postmortem_enabled", intraday_default),
    )
    intraday_research["policy_adjustment_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_POLICY_ADJUSTMENT"),
        intraday_research.get("policy_adjustment_enabled", intraday_default),
    )
    intraday_research["meta_model_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_META_MODEL"),
        intraday_research.get("meta_model_enabled", intraday_default),
    )
    intraday_research["research_reports_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_RESEARCH_REPORTS"),
        intraday_research.get("research_reports_enabled", intraday_default),
    )
    intraday_research["discord_summary_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_DISCORD_SUMMARY"),
        intraday_research.get("discord_summary_enabled", False),
    )
    intraday_research["writeback_enabled"] = _parse_bool(
        env_values.get("ENABLE_INTRADAY_WRITEBACK"),
        intraday_research.get("writeback_enabled", intraday_default),
    )
    intraday_research["rollout_mode"] = env_values.get(
        "INTRADAY_RESEARCH_ROLLOUT_MODE",
        intraday_research.get("rollout_mode", "RESEARCH_NON_TRADING"),
    )

    return config


def _merge_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_settings(
    *,
    project_root: Path | None = None,
    env_file: str | Path | None = None,
) -> Settings:
    root = (project_root or detect_project_root()).resolve()
    base_config = _load_yaml(root / "config" / "settings.yaml")
    base_config["retention"] = _load_yaml(root / "config" / "retention.yaml")
    base_config["paths"]["project_root"] = root

    env_path = _resolve_env_file(root, env_file)
    env_values = _read_env_values(env_path)
    env_name = env_values.get("APP_ENV", base_config.get("app", {}).get("env", "local"))
    env_config_path = root / "config" / "app" / f"environment.{env_name}.yaml"
    if env_config_path.exists():
        base_config = _merge_dicts(base_config, _load_yaml(env_config_path))
        base_config["paths"]["project_root"] = root
    config = _apply_env_overrides(base_config, env_values)

    try:
        return Settings.model_validate(config)
    except ValidationError as exc:
        raise RuntimeError(f"Settings validation failed:\n{exc}") from exc


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
