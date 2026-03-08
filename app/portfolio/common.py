from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from app.settings import Settings

PORTFOLIO_POLICY_DOMAIN = "portfolio_policy"
DEFAULT_PORTFOLIO_POLICY_PATH = Path("config/portfolio_policies/balanced_long_only_v1.yaml")
DEFENSIVE_PORTFOLIO_POLICY_PATH = Path("config/portfolio_policies/defensive_long_only_v1.yaml")
EXECUTION_MODES: tuple[str, ...] = ("OPEN_ALL", "TIMING_ASSISTED")
REBALANCE_ACTIONS: tuple[str, ...] = (
    "BUY_NEW",
    "ADD",
    "HOLD",
    "TRIM",
    "EXIT",
    "SKIP",
    "NO_ACTION",
)
CANDIDATE_STATES: tuple[str, ...] = (
    "NEW_ENTRY_CANDIDATE",
    "HOLD_CANDIDATE",
    "TRIM_CANDIDATE",
    "EXIT_CANDIDATE",
    "WATCH_ONLY",
    "BLOCKED",
)
CASH_SYMBOL = "__CASH__"
REGIME_SEQUENCE: tuple[str, ...] = ("panic", "risk_off", "neutral", "risk_on", "euphoria")


def json_text(value: dict[str, Any] | list[Any] | None) -> str | None:
    if value is None or value == {} or value == []:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def ordered_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    existing = [column for column in columns if column in frame.columns]
    remainder = [column for column in frame.columns if column not in existing]
    return frame.loc[:, [*existing, *remainder]].copy()


def normalize_score_100(value: object, *, neutral: float = 50.0) -> float:
    if pd.isna(value):
        return neutral
    numeric = float(value)
    if abs(numeric) <= 1.5:
        numeric *= 100.0
    return max(0.0, min(100.0, numeric))


def normalize_decimal(value: object, *, neutral: float = 0.0) -> float:
    if pd.isna(value):
        return neutral
    numeric = float(value)
    if abs(numeric) > 2.0:
        numeric /= 100.0
    return numeric


def compute_policy_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


class RegimeCashConfig(BaseModel):
    panic: float = 0.30
    risk_off: float = 0.20
    neutral: float = 0.10
    risk_on: float = 0.05
    euphoria: float = 0.08

    @field_validator("*")
    @classmethod
    def validate_ratio(cls, value: float) -> float:
        if not 0.0 <= float(value) <= 1.0:
            raise ValueError("cash ratio must be between 0 and 1")
        return float(value)

    def as_dict(self) -> dict[str, float]:
        return {name: float(getattr(self, name)) for name in REGIME_SEQUENCE}


class PortfolioWeightConfig(BaseModel):
    tactical_alpha: float = 0.25
    lower_band: float = 0.20
    flow: float = 0.15
    regime: float = 0.10
    current_holding_bonus: float = 0.05
    data_confidence_bonus: float = 0.05
    uncertainty_penalty: float = 0.35
    disagreement_penalty: float = 0.25
    implementation_penalty: float = 0.30


class PortfolioPolicy(BaseModel):
    portfolio_policy_id: str
    portfolio_policy_version: str
    display_name: str
    description: str = ""
    primary_horizon: int = 5
    tactical_horizon: int = 1
    virtual_capital_krw: float = 100_000_000.0
    execution_modes: list[str] = Field(default_factory=lambda: list(EXECUTION_MODES))
    rank_universe_limit: int = 30
    min_names: int = 4
    max_names: int = 10
    entry_score_floor: float = 0.010
    hold_score_floor: float = 0.000
    hard_exit_score_floor: float = -0.015
    lower_band_floor: float = -0.020
    vol_floor: float = 0.020
    max_single_weight: float = 0.18
    max_sector_weight: float = 0.35
    max_kosdaq_weight: float = 0.35
    max_turnover_ratio: float = 0.35
    adv20_participation_limit: float = 0.02
    liquidity_min_adv20_krw: float = 2_000_000_000.0
    hold_hysteresis: float = 0.70
    current_holding_bonus: float = 0.02
    waitlist_rank_limit: int = 5
    target_cash_floor_by_regime: RegimeCashConfig = Field(default_factory=RegimeCashConfig)
    target_cash_ceiling_by_regime: RegimeCashConfig = Field(default_factory=RegimeCashConfig)
    weights: PortfolioWeightConfig = Field(default_factory=PortfolioWeightConfig)

    @field_validator("execution_modes", mode="before")
    @classmethod
    def normalize_execution_modes(cls, value: Any) -> list[str]:
        if value is None:
            return list(EXECUTION_MODES)
        if isinstance(value, str):
            return [value]
        return [str(item).upper() for item in value]

    @model_validator(mode="after")
    def validate_policy(self) -> "PortfolioPolicy":
        invalid_modes = [mode for mode in self.execution_modes if mode not in EXECUTION_MODES]
        if invalid_modes:
            raise ValueError(f"Unsupported execution modes: {invalid_modes}")
        if not 0 < self.max_single_weight <= 1:
            raise ValueError("max_single_weight must be between 0 and 1")
        if not 0 < self.max_sector_weight <= 1:
            raise ValueError("max_sector_weight must be between 0 and 1")
        if not 0 < self.max_kosdaq_weight <= 1:
            raise ValueError("max_kosdaq_weight must be between 0 and 1")
        if not 0 < self.max_turnover_ratio <= 1:
            raise ValueError("max_turnover_ratio must be between 0 and 1")
        if not 0 < self.adv20_participation_limit <= 1:
            raise ValueError("adv20_participation_limit must be between 0 and 1")
        if self.min_names <= 0 or self.max_names < self.min_names:
            raise ValueError("min_names/max_names are invalid")
        return self

    def regime_cash_target(self, regime_state: str) -> float:
        floors = self.target_cash_floor_by_regime.as_dict()
        ceilings = self.target_cash_ceiling_by_regime.as_dict()
        key = str(regime_state or "neutral").lower()
        floor = floors.get(key, floors["neutral"])
        ceiling = ceilings.get(key, ceilings["neutral"])
        return (floor + ceiling) / 2.0

    def payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def config_hash(self) -> str:
        return compute_policy_hash(self.payload())


def resolve_portfolio_policy_path(
    settings: Settings,
    policy_config_path: str | Path | None = None,
) -> Path:
    if policy_config_path is None:
        return (settings.paths.project_root / DEFAULT_PORTFOLIO_POLICY_PATH).resolve()
    candidate = Path(policy_config_path)
    if not candidate.is_absolute():
        candidate = settings.paths.project_root / candidate
    return candidate.resolve()


def load_portfolio_policy(
    settings: Settings,
    policy_config_path: str | Path | None = None,
) -> tuple[PortfolioPolicy, Path]:
    path = resolve_portfolio_policy_path(settings, policy_config_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValidationError.from_exception_data("PortfolioPolicy", [])
    policy = PortfolioPolicy.model_validate(payload)
    return policy, path


def select_active_portfolio_policy_row(connection, *, as_of_date: date) -> pd.Series | None:
    frame = connection.execute(
        """
        SELECT *
        FROM fact_portfolio_policy_registry
        WHERE effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
          AND active_flag = TRUE
        ORDER BY effective_from_date DESC, created_at DESC
        LIMIT 1
        """,
        [as_of_date, as_of_date],
    ).fetchdf()
    if frame.empty:
        return None
    return frame.iloc[0]


def load_active_or_default_portfolio_policy(
    settings: Settings,
    connection,
    *,
    as_of_date: date,
    policy_config_path: str | Path | None = None,
) -> tuple[PortfolioPolicy, str | None, str | None]:
    if policy_config_path is not None:
        policy, path = load_portfolio_policy(settings, policy_config_path)
        return policy, None, str(path)

    active_row = select_active_portfolio_policy_row(connection, as_of_date=as_of_date)
    if active_row is not None and pd.notna(active_row.get("policy_payload_json")):
        payload = json.loads(str(active_row["policy_payload_json"]))
        policy = PortfolioPolicy.model_validate(payload)
        return policy, str(active_row["active_portfolio_policy_id"]), str(
            active_row.get("config_path") or ""
        )

    policy, path = load_portfolio_policy(settings, DEFAULT_PORTFOLIO_POLICY_PATH)
    return policy, None, str(path)


@dataclass(slots=True)
class PortfolioCandidateBookResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioValidationResult:
    run_id: str
    as_of_date: date
    check_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioTargetBookResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioRebalancePlanResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioPositionSnapshotResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioNavResult:
    run_id: str
    end_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioWalkforwardResult:
    run_id: str
    start_as_of_date: date
    end_as_of_date: date
    processed_dates: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioEvaluationResult:
    run_id: str
    end_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioPolicyFreezeResult:
    run_id: str
    as_of_date: date
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioReportResult:
    run_id: str
    as_of_date: date
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class PortfolioPublishResult:
    run_id: str
    as_of_date: date
    dry_run: bool
    published: bool
    artifact_paths: list[str]
    notes: str
