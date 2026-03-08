from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.constants import SELECTION_ENGINE_VERSION
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start
from app.storage.parquet_io import write_parquet

from .common import DEFAULT_CHECKPOINTS, INTRADAY_REGIME_FAMILIES, json_text, rank_list
from .strategy import materialize_intraday_decision_outcomes

DEFAULT_SEARCH_SPACE_VERSION = "pcal_v1"
DEFAULT_OBJECTIVE_VERSION = "ip_obj_v1"
DEFAULT_SPLIT_VERSION = "wf_40_10_10_step5"
DEFAULT_SPLIT_MODE = "ANCHORED_WALKFORWARD"

POLICY_TEMPLATE_IDS: tuple[str, ...] = (
    "BASE_DEFAULT",
    "DEFENSIVE_LIGHT",
    "DEFENSIVE_STRONG",
    "RISK_ON_LIGHT",
    "GAP_GUARD_STRICT",
    "FRICTION_GUARD_STRICT",
    "COHORT_GUARD_STRICT",
    "FULL_BALANCED",
)
POLICY_SCOPE_TYPES: tuple[str, ...] = (
    "GLOBAL",
    "HORIZON",
    "HORIZON_CHECKPOINT",
    "HORIZON_REGIME_CLUSTER",
    "HORIZON_CHECKPOINT_REGIME_FAMILY",
)
REGIME_CLUSTER_MAP: dict[str, tuple[str, ...]] = {
    "RISK_OFF": ("PANIC_OPEN", "WEAK_RISK_OFF"),
    "NEUTRAL": ("NEUTRAL_CHOP",),
    "RISK_ON": ("HEALTHY_TREND", "OVERHEATED_GAP_CHASE"),
    "DATA_WEAK": ("DATA_WEAK",),
}
REGIME_FAMILY_TO_CLUSTER: dict[str, str] = {
    family: cluster for cluster, families in REGIME_CLUSTER_MAP.items() for family in families
}

POLICY_TEMPLATES: dict[str, dict[str, object]] = {
    "BASE_DEFAULT": {
        "enter_threshold_delta": 0.0,
        "wait_threshold_delta": 0.0,
        "avoid_threshold_delta": 0.0,
        "min_selection_confidence_gate": 55.0,
        "min_signal_quality_gate": 50.0,
        "uncertainty_penalty_weight": 0.55,
        "spread_penalty_weight": 0.40,
        "friction_penalty_weight": 0.50,
        "gap_chase_penalty_weight": 0.45,
        "cohort_weakness_penalty_weight": 0.45,
        "market_shock_penalty_weight": 0.55,
        "data_weak_guard_strength": 0.70,
        "max_gap_up_allowance_pct": 4.5,
        "min_execution_strength_gate": 48.0,
        "min_orderbook_imbalance_gate": 0.47,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 30,
    },
    "DEFENSIVE_LIGHT": {
        "enter_threshold_delta": 4.0,
        "wait_threshold_delta": 2.0,
        "avoid_threshold_delta": 3.0,
        "min_selection_confidence_gate": 60.0,
        "min_signal_quality_gate": 55.0,
        "uncertainty_penalty_weight": 0.70,
        "spread_penalty_weight": 0.55,
        "friction_penalty_weight": 0.60,
        "gap_chase_penalty_weight": 0.65,
        "cohort_weakness_penalty_weight": 0.60,
        "market_shock_penalty_weight": 0.70,
        "data_weak_guard_strength": 0.85,
        "max_gap_up_allowance_pct": 3.2,
        "min_execution_strength_gate": 52.0,
        "min_orderbook_imbalance_gate": 0.50,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 24,
    },
    "DEFENSIVE_STRONG": {
        "enter_threshold_delta": 7.0,
        "wait_threshold_delta": 4.0,
        "avoid_threshold_delta": 6.0,
        "min_selection_confidence_gate": 66.0,
        "min_signal_quality_gate": 60.0,
        "uncertainty_penalty_weight": 0.90,
        "spread_penalty_weight": 0.75,
        "friction_penalty_weight": 0.75,
        "gap_chase_penalty_weight": 0.80,
        "cohort_weakness_penalty_weight": 0.85,
        "market_shock_penalty_weight": 0.90,
        "data_weak_guard_strength": 1.00,
        "max_gap_up_allowance_pct": 2.4,
        "min_execution_strength_gate": 55.0,
        "min_orderbook_imbalance_gate": 0.53,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 18,
    },
    "RISK_ON_LIGHT": {
        "enter_threshold_delta": -3.0,
        "wait_threshold_delta": -1.0,
        "avoid_threshold_delta": -2.0,
        "min_selection_confidence_gate": 50.0,
        "min_signal_quality_gate": 46.0,
        "uncertainty_penalty_weight": 0.45,
        "spread_penalty_weight": 0.25,
        "friction_penalty_weight": 0.30,
        "gap_chase_penalty_weight": 0.25,
        "cohort_weakness_penalty_weight": 0.30,
        "market_shock_penalty_weight": 0.35,
        "data_weak_guard_strength": 0.55,
        "max_gap_up_allowance_pct": 5.2,
        "min_execution_strength_gate": 45.0,
        "min_orderbook_imbalance_gate": 0.44,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": True,
        "selection_rank_cap": 35,
    },
    "GAP_GUARD_STRICT": {
        "enter_threshold_delta": 5.0,
        "wait_threshold_delta": 2.0,
        "avoid_threshold_delta": 4.0,
        "min_selection_confidence_gate": 58.0,
        "min_signal_quality_gate": 55.0,
        "uncertainty_penalty_weight": 0.55,
        "spread_penalty_weight": 0.45,
        "friction_penalty_weight": 0.50,
        "gap_chase_penalty_weight": 0.95,
        "cohort_weakness_penalty_weight": 0.45,
        "market_shock_penalty_weight": 0.60,
        "data_weak_guard_strength": 0.70,
        "max_gap_up_allowance_pct": 2.1,
        "min_execution_strength_gate": 50.0,
        "min_orderbook_imbalance_gate": 0.48,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 22,
    },
    "FRICTION_GUARD_STRICT": {
        "enter_threshold_delta": 4.0,
        "wait_threshold_delta": 2.0,
        "avoid_threshold_delta": 4.0,
        "min_selection_confidence_gate": 58.0,
        "min_signal_quality_gate": 52.0,
        "uncertainty_penalty_weight": 0.55,
        "spread_penalty_weight": 0.95,
        "friction_penalty_weight": 0.95,
        "gap_chase_penalty_weight": 0.40,
        "cohort_weakness_penalty_weight": 0.40,
        "market_shock_penalty_weight": 0.55,
        "data_weak_guard_strength": 0.70,
        "max_gap_up_allowance_pct": 4.0,
        "min_execution_strength_gate": 53.0,
        "min_orderbook_imbalance_gate": 0.55,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 24,
    },
    "COHORT_GUARD_STRICT": {
        "enter_threshold_delta": 3.0,
        "wait_threshold_delta": 1.0,
        "avoid_threshold_delta": 3.0,
        "min_selection_confidence_gate": 56.0,
        "min_signal_quality_gate": 52.0,
        "uncertainty_penalty_weight": 0.55,
        "spread_penalty_weight": 0.40,
        "friction_penalty_weight": 0.45,
        "gap_chase_penalty_weight": 0.40,
        "cohort_weakness_penalty_weight": 0.95,
        "market_shock_penalty_weight": 0.70,
        "data_weak_guard_strength": 0.75,
        "max_gap_up_allowance_pct": 4.0,
        "min_execution_strength_gate": 49.0,
        "min_orderbook_imbalance_gate": 0.46,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": False,
        "selection_rank_cap": 26,
    },
    "FULL_BALANCED": {
        "enter_threshold_delta": -1.0,
        "wait_threshold_delta": 0.0,
        "avoid_threshold_delta": -1.0,
        "min_selection_confidence_gate": 54.0,
        "min_signal_quality_gate": 48.0,
        "uncertainty_penalty_weight": 0.60,
        "spread_penalty_weight": 0.50,
        "friction_penalty_weight": 0.55,
        "gap_chase_penalty_weight": 0.55,
        "cohort_weakness_penalty_weight": 0.55,
        "market_shock_penalty_weight": 0.60,
        "data_weak_guard_strength": 0.75,
        "max_gap_up_allowance_pct": 4.2,
        "min_execution_strength_gate": 48.0,
        "min_orderbook_imbalance_gate": 0.46,
        "allow_enter_under_data_weak": False,
        "allow_wait_override": True,
        "selection_rank_cap": 28,
    },
}
ABLATION_COMPONENTS: dict[str, tuple[str, ...]] = {
    "NO_UNCERTAINTY_PENALTY": ("uncertainty_penalty_weight",),
    "NO_SPREAD_PENALTY": ("spread_penalty_weight",),
    "NO_FRICTION_GUARD": ("friction_penalty_weight", "min_execution_strength_gate"),
    "NO_GAP_CHASE_GUARD": ("gap_chase_penalty_weight", "max_gap_up_allowance_pct"),
    "NO_COHORT_GUARD": ("cohort_weakness_penalty_weight",),
    "NO_MARKET_SHOCK_GUARD": ("market_shock_penalty_weight",),
    "NO_DATA_WEAK_GUARD": ("data_weak_guard_strength",),
}


@dataclass(slots=True)
class IntradayPolicyCandidateResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPolicyCalibrationResult:
    run_id: str
    experiment_row_count: int
    evaluation_row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPolicyWalkforwardResult:
    run_id: str
    experiment_row_count: int
    evaluation_row_count: int
    split_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPolicyAblationResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayPolicyRecommendationResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class IntradayActivePolicyResult:
    run_id: str
    row_count: int
    artifact_paths: list[str]
    notes: str


POLICY_CANDIDATE_COLUMNS = [
    "policy_candidate_id",
    "search_space_version",
    "template_id",
    "scope_type",
    "scope_key",
    "horizon",
    "checkpoint_time",
    "regime_cluster",
    "regime_family",
    "candidate_label",
    "parameter_hash",
    "enter_threshold_delta",
    "wait_threshold_delta",
    "avoid_threshold_delta",
    "min_selection_confidence_gate",
    "min_signal_quality_gate",
    "uncertainty_penalty_weight",
    "spread_penalty_weight",
    "friction_penalty_weight",
    "gap_chase_penalty_weight",
    "cohort_weakness_penalty_weight",
    "market_shock_penalty_weight",
    "data_weak_guard_strength",
    "max_gap_up_allowance_pct",
    "min_execution_strength_gate",
    "min_orderbook_imbalance_gate",
    "allow_enter_under_data_weak",
    "allow_wait_override",
    "selection_rank_cap",
    "created_at",
]
POLICY_EXPERIMENT_COLUMNS = [
    "experiment_run_id",
    "experiment_name",
    "experiment_type",
    "search_space_version",
    "objective_version",
    "split_version",
    "split_mode",
    "as_of_date",
    "start_session_date",
    "end_session_date",
    "train_start_date",
    "train_end_date",
    "validation_start_date",
    "validation_end_date",
    "test_start_date",
    "test_end_date",
    "horizon",
    "checkpoint_scope",
    "regime_scope",
    "candidate_count",
    "selected_policy_candidate_id",
    "fallback_used_flag",
    "status",
    "artifact_path",
    "notes_json",
    "created_at",
    "updated_at",
]
POLICY_EVALUATION_COLUMNS = [
    "experiment_run_id",
    "experiment_type",
    "search_space_version",
    "objective_version",
    "split_version",
    "split_mode",
    "split_name",
    "split_index",
    "window_start_date",
    "window_end_date",
    "horizon",
    "policy_candidate_id",
    "template_id",
    "scope_type",
    "scope_key",
    "checkpoint_time",
    "regime_cluster",
    "regime_family",
    "window_session_count",
    "sample_count",
    "matured_count",
    "executed_count",
    "no_entry_count",
    "execution_rate",
    "mean_realized_excess_return",
    "median_realized_excess_return",
    "hit_rate",
    "mean_timing_edge_vs_open_bps",
    "positive_timing_edge_rate",
    "skip_saved_loss_rate",
    "missed_winner_rate",
    "left_tail_proxy",
    "stability_score",
    "objective_score",
    "manual_review_required_flag",
    "fallback_scope_type",
    "fallback_scope_key",
    "notes_json",
    "created_at",
]
POLICY_ABLATION_COLUMNS = [
    "experiment_run_id",
    "ablation_date",
    "start_session_date",
    "end_session_date",
    "horizon",
    "base_policy_source",
    "base_policy_candidate_id",
    "ablation_name",
    "sample_count",
    "mean_realized_excess_return_delta",
    "median_realized_excess_return_delta",
    "hit_rate_delta",
    "mean_timing_edge_vs_open_bps_delta",
    "execution_rate_delta",
    "skip_saved_loss_rate_delta",
    "missed_winner_rate_delta",
    "left_tail_proxy_delta",
    "stability_score_delta",
    "objective_score_delta",
    "notes_json",
    "created_at",
]
POLICY_RECOMMENDATION_COLUMNS = [
    "recommendation_date",
    "horizon",
    "scope_type",
    "scope_key",
    "recommendation_rank",
    "policy_candidate_id",
    "template_id",
    "source_experiment_run_id",
    "search_space_version",
    "objective_version",
    "split_version",
    "sample_count",
    "test_session_count",
    "executed_count",
    "execution_rate",
    "mean_realized_excess_return",
    "median_realized_excess_return",
    "hit_rate",
    "mean_timing_edge_vs_open_bps",
    "positive_timing_edge_rate",
    "skip_saved_loss_rate",
    "missed_winner_rate",
    "left_tail_proxy",
    "stability_score",
    "objective_score",
    "manual_review_required_flag",
    "fallback_scope_type",
    "fallback_scope_key",
    "recommendation_reason_json",
    "created_at",
]
ACTIVE_POLICY_COLUMNS = [
    "active_policy_id",
    "horizon",
    "scope_type",
    "scope_key",
    "checkpoint_time",
    "regime_cluster",
    "regime_family",
    "policy_candidate_id",
    "source_recommendation_date",
    "promotion_type",
    "source_type",
    "effective_from_date",
    "effective_to_date",
    "active_flag",
    "fallback_scope_type",
    "fallback_scope_key",
    "rollback_of_active_policy_id",
    "note",
    "created_at",
    "updated_at",
]


def _ordered_stage_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise KeyError(f"Missing required policy columns: {missing}")
    return frame.loc[:, columns].copy()


def upsert_intraday_policy_candidate(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_policy_candidate_stage",
        _ordered_stage_frame(frame, POLICY_CANDIDATE_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_policy_candidate
        WHERE policy_candidate_id IN (
            SELECT policy_candidate_id
            FROM intraday_policy_candidate_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_policy_candidate
        SELECT * FROM intraday_policy_candidate_stage
        """
    )
    connection.unregister("intraday_policy_candidate_stage")


def upsert_intraday_policy_experiment_run(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_policy_experiment_run_stage",
        _ordered_stage_frame(frame, POLICY_EXPERIMENT_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_policy_experiment_run
        WHERE experiment_run_id IN (
            SELECT experiment_run_id
            FROM intraday_policy_experiment_run_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_policy_experiment_run
        SELECT * FROM intraday_policy_experiment_run_stage
        """
    )
    connection.unregister("intraday_policy_experiment_run_stage")


def upsert_intraday_policy_evaluation(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_policy_evaluation_stage",
        _ordered_stage_frame(frame, POLICY_EVALUATION_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_policy_evaluation
        WHERE (
            experiment_run_id,
            split_name,
            split_index,
            horizon,
            policy_candidate_id
        ) IN (
            SELECT
                experiment_run_id,
                split_name,
                split_index,
                horizon,
                policy_candidate_id
            FROM intraday_policy_evaluation_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_policy_evaluation
        SELECT * FROM intraday_policy_evaluation_stage
        """
    )
    connection.unregister("intraday_policy_evaluation_stage")


def upsert_intraday_policy_ablation_result(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_policy_ablation_stage",
        _ordered_stage_frame(frame, POLICY_ABLATION_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_policy_ablation_result
        WHERE (
            experiment_run_id,
            horizon,
            ablation_name,
            base_policy_candidate_id
        ) IN (
            SELECT
                experiment_run_id,
                horizon,
                ablation_name,
                base_policy_candidate_id
            FROM intraday_policy_ablation_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_policy_ablation_result
        SELECT * FROM intraday_policy_ablation_stage
        """
    )
    connection.unregister("intraday_policy_ablation_stage")


def upsert_intraday_policy_selection_recommendation(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_policy_recommendation_stage",
        _ordered_stage_frame(frame, POLICY_RECOMMENDATION_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_policy_selection_recommendation
        WHERE (
            recommendation_date,
            horizon,
            scope_type,
            scope_key,
            recommendation_rank
        ) IN (
            SELECT
                recommendation_date,
                horizon,
                scope_type,
                scope_key,
                recommendation_rank
            FROM intraday_policy_recommendation_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_policy_selection_recommendation
        SELECT * FROM intraday_policy_recommendation_stage
        """
    )
    connection.unregister("intraday_policy_recommendation_stage")


def upsert_intraday_active_policy(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register(
        "intraday_active_policy_stage",
        _ordered_stage_frame(frame, ACTIVE_POLICY_COLUMNS),
    )
    connection.execute(
        """
        DELETE FROM fact_intraday_active_policy
        WHERE active_policy_id IN (
            SELECT active_policy_id
            FROM intraday_active_policy_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_intraday_active_policy
        SELECT * FROM intraday_active_policy_stage
        """
    )
    connection.unregister("intraday_active_policy_stage")


def _safe_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.mean())


def _safe_median(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return float(clean.median())


def _normalize_outcome_status(label_available: object, exclusion_reason: object) -> str:
    if pd.notna(label_available) and bool(label_available):
        return "matured"
    if exclusion_reason in {
        "insufficient_future_trading_days",
        "missing_entry_day_ohlcv",
        "missing_exit_day_ohlcv",
    }:
        return "pending"
    return "unavailable"


def _left_tail_proxy(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    quantile = float(clean.quantile(0.1))
    return min(0.0, quantile)


def _stability_score(frame: pd.DataFrame) -> float | None:
    matured = frame.loc[frame["outcome_status"].isin(["executed", "no_entry"])].copy()
    if matured.empty:
        return None
    by_session = matured.groupby("session_date", as_index=False)["realized_excess_return"].mean()
    if len(by_session) <= 1:
        return 55.0
    dispersion_bps = float(
        pd.to_numeric(by_session["realized_excess_return"], errors="coerce").std(ddof=0) * 10000.0
    )
    return max(0.0, 100.0 - min(100.0, dispersion_bps / 2.5))


def _objective_score(row: dict[str, object]) -> float:
    mean_excess_bps = float((row.get("mean_realized_excess_return") or 0.0) * 10000.0)
    mean_edge_bps = float(row.get("mean_timing_edge_vs_open_bps") or 0.0)
    hit_rate = float((row.get("hit_rate") or 0.0) * 100.0)
    execution_rate = float((row.get("execution_rate") or 0.0) * 100.0)
    skip_saved = float((row.get("skip_saved_loss_rate") or 0.0) * 100.0)
    missed = float((row.get("missed_winner_rate") or 0.0) * 100.0)
    left_tail_penalty = abs(float((row.get("left_tail_proxy") or 0.0) * 10000.0))
    stability = float(row.get("stability_score") or 0.0)
    score = (
        mean_excess_bps * 0.35
        + mean_edge_bps * 0.18
        + hit_rate * 0.14
        + execution_rate * 0.08
        + skip_saved * 0.10
        + stability * 0.10
        - missed * 0.10
        - left_tail_penalty * 0.12
    )
    if pd.notna(row.get("manual_review_required_flag")) and bool(
        row.get("manual_review_required_flag")
    ):
        score -= 20.0
    return float(score)


def _scope_key(
    *,
    horizon: int,
    scope_type: str,
    checkpoint_time: str | None = None,
    regime_cluster: str | None = None,
    regime_family: str | None = None,
) -> str:
    if scope_type == "GLOBAL":
        return f"H{horizon}|GLOBAL"
    if scope_type == "HORIZON":
        return f"H{horizon}"
    if scope_type == "HORIZON_CHECKPOINT":
        return f"H{horizon}|CP={checkpoint_time}"
    if scope_type == "HORIZON_REGIME_CLUSTER":
        return f"H{horizon}|RC={regime_cluster}"
    return f"H{horizon}|CP={checkpoint_time}|RF={regime_family}"


def _parameter_hash(payload: dict[str, object]) -> str:
    return hashlib.sha1(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


def _candidate_id(
    *,
    search_space_version: str,
    template_id: str,
    scope_key: str,
    parameter_hash: str,
) -> str:
    base = f"{search_space_version}|{template_id}|{scope_key}|{parameter_hash}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]


def _normalize_scope(scope: str) -> str:
    normalized = scope.strip().upper()
    if normalized not in POLICY_SCOPE_TYPES:
        raise ValueError(f"Unsupported intraday policy scope: {scope}")
    return normalized


def _normalize_checkpoint_list(checkpoints: list[str] | None) -> list[str]:
    values = checkpoints or list(DEFAULT_CHECKPOINTS)
    return sorted({value.strip() for value in values})


def _candidate_rows(
    *,
    search_space_version: str,
    horizons: list[int],
    checkpoints: list[str],
    scopes: list[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    created_at = pd.Timestamp.now(tz="UTC")
    for horizon in sorted({int(value) for value in horizons}):
        for scope in [_normalize_scope(value) for value in scopes]:
            combinations: list[tuple[str | None, str | None, str | None]] = []
            if scope in {"GLOBAL", "HORIZON"}:
                combinations = [(None, None, None)]
            elif scope == "HORIZON_CHECKPOINT":
                combinations = [(checkpoint, None, None) for checkpoint in checkpoints]
            elif scope == "HORIZON_REGIME_CLUSTER":
                combinations = [(None, cluster, None) for cluster in REGIME_CLUSTER_MAP]
            else:
                combinations = [
                    (checkpoint, None, family)
                    for checkpoint in checkpoints
                    for family in INTRADAY_REGIME_FAMILIES
                ]
            for template_id in POLICY_TEMPLATE_IDS:
                parameters = dict(POLICY_TEMPLATES[template_id])
                for checkpoint_time, regime_cluster, regime_family in combinations:
                    if regime_family and regime_cluster is None:
                        regime_cluster = REGIME_FAMILY_TO_CLUSTER.get(regime_family)
                    scope_key = _scope_key(
                        horizon=horizon,
                        scope_type=scope,
                        checkpoint_time=checkpoint_time,
                        regime_cluster=regime_cluster,
                        regime_family=regime_family,
                    )
                    hash_payload = {
                        "template_id": template_id,
                        "scope_type": scope,
                        "scope_key": scope_key,
                        "horizon": horizon,
                        "checkpoint_time": checkpoint_time,
                        "regime_cluster": regime_cluster,
                        "regime_family": regime_family,
                        **parameters,
                    }
                    parameter_hash = _parameter_hash(hash_payload)
                    rows.append(
                        {
                            "policy_candidate_id": _candidate_id(
                                search_space_version=search_space_version,
                                template_id=template_id,
                                scope_key=scope_key,
                                parameter_hash=parameter_hash,
                            ),
                            "search_space_version": search_space_version,
                            "template_id": template_id,
                            "scope_type": scope,
                            "scope_key": scope_key,
                            "horizon": horizon,
                            "checkpoint_time": checkpoint_time,
                            "regime_cluster": regime_cluster,
                            "regime_family": regime_family,
                            "candidate_label": f"{template_id} [{scope_key}]",
                            "parameter_hash": parameter_hash,
                            **parameters,
                            "created_at": created_at,
                        }
                    )
    return rows


def materialize_intraday_policy_candidates(
    settings: Settings,
    *,
    search_space_version: str,
    horizons: list[int],
    checkpoints: list[str],
    scopes: list[str],
) -> IntradayPolicyCandidateResult:
    ensure_storage_layout(settings)
    effective_checkpoints = _normalize_checkpoint_list(checkpoints)
    with activate_run_context(
        "materialize_intraday_policy_candidates",
        as_of_date=date.today(),
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[],
                notes=(
                    "Materialize intraday policy candidates. "
                    f"search_space_version={search_space_version}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                output = pd.DataFrame(
                    _candidate_rows(
                        search_space_version=search_space_version,
                        horizons=horizons,
                        checkpoints=effective_checkpoints,
                        scopes=scopes,
                    )
                )
                upsert_intraday_policy_candidate(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/policy_candidate",
                            partitions={"search_space_version": search_space_version},
                            filename="policy_candidate.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday policy candidates materialized. "
                    f"search_space_version={search_space_version} rows={len(output)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayPolicyCandidateResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday policy candidate materialization failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _load_policy_candidates(
    connection,
    *,
    search_space_version: str,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_policy_candidate
        WHERE search_space_version = ?
          AND horizon IN ({placeholders})
        ORDER BY horizon, scope_type, scope_key, template_id
        """,
        [search_space_version, *horizons],
    ).fetchdf()


def _load_policy_base_frame(
    connection,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    checkpoints: list[str] | None = None,
) -> pd.DataFrame:
    horizon_placeholders = ",".join("?" for _ in horizons)
    checkpoint_filter = ""
    parameters: list[object] = [start_session_date, end_session_date, *horizons]
    if checkpoints:
        checkpoint_placeholders = ",".join("?" for _ in checkpoints)
        checkpoint_filter = f"AND adjusted.checkpoint_time IN ({checkpoint_placeholders})"
        parameters.extend(checkpoints)
    return connection.execute(
        f"""
        WITH first_open AS (
            SELECT
                session_date,
                symbol,
                open AS baseline_open_price
            FROM fact_intraday_bar_1m
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY session_date, symbol
                ORDER BY bar_ts
            ) = 1
        )
        SELECT
            candidate.selection_date,
            candidate.session_date,
            candidate.symbol,
            candidate.market,
            candidate.company_name,
            candidate.horizon,
            candidate.ranking_version,
            candidate.candidate_rank,
            candidate.final_selection_value,
            candidate.final_selection_rank_pct,
            candidate.expected_excess_return,
            candidate.uncertainty_score,
            candidate.disagreement_score,
            candidate.fallback_flag AS selection_fallback_flag,
            adjusted.checkpoint_time,
            adjusted.raw_action,
            adjusted.adjusted_action,
            adjusted.raw_timing_score,
            adjusted.adjusted_timing_score,
            adjusted.eligible_to_execute_flag,
            adjusted.market_regime_family,
            adjusted.adjustment_profile,
            adjusted.signal_quality_flag,
            adjusted.fallback_flag AS adjusted_fallback_flag,
            adjusted.adjustment_reason_codes_json,
            adjusted.risk_flags_json,
            raw.entry_reference_price,
            raw.action_score,
            raw.signal_quality_score AS raw_signal_quality_score,
            raw.fallback_flag AS raw_fallback_flag,
            signal.gap_opening_quality_score,
            signal.micro_trend_score,
            signal.relative_activity_score,
            signal.orderbook_score,
            signal.execution_strength_score,
            signal.risk_friction_score,
            signal.signal_quality_score,
            signal.fallback_flags_json,
            quote.spread_bps,
            quote.imbalance_ratio,
            quote.quote_status,
            trade.execution_strength,
            trade.activity_ratio,
            trade.trade_summary_status,
            context.market_breadth_ratio,
            context.market_shock_proxy,
            context.candidate_mean_signal_quality,
            context.data_quality_flag,
            label.label_available_flag,
            label.exclusion_reason,
            COALESCE(first_open.baseline_open_price, label.entry_price) AS baseline_open_price,
            label.entry_price AS label_entry_price,
            label.exit_price,
            label.gross_forward_return AS baseline_open_return,
            label.excess_forward_return AS baseline_open_excess_return,
            label.baseline_forward_return,
            label.exit_date
        FROM fact_intraday_adjusted_entry_decision AS adjusted
        JOIN fact_intraday_candidate_session AS candidate
          ON adjusted.session_date = candidate.session_date
         AND adjusted.symbol = candidate.symbol
         AND adjusted.horizon = candidate.horizon
         AND adjusted.ranking_version = candidate.ranking_version
        LEFT JOIN fact_intraday_entry_decision AS raw
          ON adjusted.session_date = raw.session_date
         AND adjusted.symbol = raw.symbol
         AND adjusted.horizon = raw.horizon
         AND adjusted.checkpoint_time = raw.checkpoint_time
         AND adjusted.ranking_version = raw.ranking_version
        LEFT JOIN fact_intraday_signal_snapshot AS signal
          ON adjusted.session_date = signal.session_date
         AND adjusted.symbol = signal.symbol
         AND adjusted.horizon = signal.horizon
         AND adjusted.checkpoint_time = signal.checkpoint_time
         AND adjusted.ranking_version = signal.ranking_version
        LEFT JOIN fact_intraday_quote_summary AS quote
          ON adjusted.session_date = quote.session_date
         AND adjusted.symbol = quote.symbol
         AND adjusted.checkpoint_time = quote.checkpoint_time
        LEFT JOIN fact_intraday_trade_summary AS trade
          ON adjusted.session_date = trade.session_date
         AND adjusted.symbol = trade.symbol
         AND adjusted.checkpoint_time = trade.checkpoint_time
        LEFT JOIN fact_intraday_market_context_snapshot AS context
          ON adjusted.session_date = context.session_date
         AND adjusted.checkpoint_time = context.checkpoint_time
         AND context.context_scope = 'market'
        LEFT JOIN fact_forward_return_label AS label
          ON candidate.selection_date = label.as_of_date
         AND candidate.symbol = label.symbol
         AND candidate.horizon = label.horizon
        LEFT JOIN first_open
          ON adjusted.session_date = first_open.session_date
         AND adjusted.symbol = first_open.symbol
        WHERE adjusted.session_date BETWEEN ? AND ?
          AND adjusted.horizon IN ({horizon_placeholders})
          {checkpoint_filter}
        ORDER BY adjusted.session_date, adjusted.horizon, adjusted.symbol, adjusted.checkpoint_time
        """,
        parameters,
    ).fetchdf()


def _regime_cluster(value: object) -> str:
    return REGIME_FAMILY_TO_CLUSTER.get(str(value or ""), "UNKNOWN")


def _candidate_scope_mask(frame: pd.DataFrame, candidate: pd.Series) -> pd.Series:
    mask = frame["horizon"].eq(int(candidate["horizon"]))
    scope_type = str(candidate["scope_type"])
    if scope_type in {"GLOBAL", "HORIZON"}:
        return mask
    if scope_type == "HORIZON_CHECKPOINT":
        return mask & frame["checkpoint_time"].eq(str(candidate["checkpoint_time"]))
    if scope_type == "HORIZON_REGIME_CLUSTER":
        return mask & frame["regime_cluster"].eq(str(candidate["regime_cluster"]))
    return (
        mask
        & frame["checkpoint_time"].eq(str(candidate["checkpoint_time"]))
        & frame["market_regime_family"].eq(str(candidate["regime_family"]))
    )


def _estimated_gap_up_pct(row: pd.Series) -> float:
    if pd.notna(row.get("baseline_open_price")) and pd.notna(row.get("label_entry_price")):
        open_price = float(row["baseline_open_price"])
        reference = float(row["label_entry_price"])
        if reference:
            return max(0.0, (open_price / reference - 1.0) * 100.0)
    score = (
        50.0
        if pd.isna(row.get("gap_opening_quality_score"))
        else float(row["gap_opening_quality_score"])
    )
    return max(0.0, (55.0 - score) / 5.0)


def _policy_transition(
    row: pd.Series,
    candidate: pd.Series,
    *,
    trace_source: str,
) -> dict[str, object]:
    raw_action = str(row.get("raw_action") or "DATA_INSUFFICIENT")
    adjusted_action = str(row.get("adjusted_action") or raw_action)
    reasons = rank_list(
        [f"policy_template:{candidate['template_id']}", f"policy_scope:{candidate['scope_key']}"]
    )
    if raw_action == "DATA_INSUFFICIENT" or adjusted_action == "DATA_INSUFFICIENT":
        return {
            "tuned_action": "DATA_INSUFFICIENT",
            "tuned_score": None,
            "policy_trace": trace_source,
            "policy_candidate_id": candidate["policy_candidate_id"],
            "fallback_used_flag": trace_source != "direct_policy",
            "reason_codes": rank_list(reasons + ["policy_data_insufficient_locked"]),
        }
    if adjusted_action == "AVOID_TODAY":
        return {
            "tuned_action": "AVOID_TODAY",
            "tuned_score": float(
                row.get("adjusted_timing_score") or row.get("raw_timing_score") or 0.0
            ),
            "policy_trace": trace_source,
            "policy_candidate_id": candidate["policy_candidate_id"],
            "fallback_used_flag": trace_source != "direct_policy",
            "reason_codes": rank_list(reasons + ["adjusted_avoid_preserved"]),
        }

    selection_score = float(row.get("final_selection_value") or 0.0)
    signal_quality = float(
        row.get("raw_signal_quality_score") or row.get("signal_quality_score") or 0.0
    )
    execution_strength = float(
        row.get("execution_strength") or row.get("execution_strength_score") or 0.0
    )
    orderbook_imbalance = (
        abs(float(row["imbalance_ratio"]))
        if pd.notna(row.get("imbalance_ratio"))
        else float(row.get("orderbook_score") or 0.0) / 100.0
    )
    spread_bps = (
        float(row["spread_bps"])
        if pd.notna(row.get("spread_bps"))
        else max(0.0, (60.0 - float(row.get("orderbook_score") or 50.0)) * 0.25)
    )
    base_score = float(row.get("adjusted_timing_score") or row.get("raw_timing_score") or 50.0)
    uncertainty_score = float(row.get("uncertainty_score") or 0.0)
    market_breadth_ratio = float(row.get("market_breadth_ratio") or 0.5)
    market_shock_proxy = float(row.get("market_shock_proxy") or 0.0)
    gap_up_pct = _estimated_gap_up_pct(row)
    data_weak = (
        str(row.get("market_regime_family") or "") == "DATA_WEAK"
        or str(row.get("data_quality_flag") or "") == "weak"
    )

    penalties = {
        "uncertainty": float(candidate["uncertainty_penalty_weight"]) * uncertainty_score / 10.0,
        "spread": float(candidate["spread_penalty_weight"]) * max(0.0, spread_bps - 12.0),
        "friction": float(candidate["friction_penalty_weight"])
        * max(0.0, 55.0 - float(row.get("risk_friction_score") or 55.0))
        / 6.0,
        "gap": float(candidate["gap_chase_penalty_weight"])
        * max(0.0, gap_up_pct - float(candidate["max_gap_up_allowance_pct"]))
        * 1.8,
        "cohort": float(candidate["cohort_weakness_penalty_weight"])
        * max(0.0, 0.52 - market_breadth_ratio)
        * 100.0
        / 3.0,
        "shock": float(candidate["market_shock_penalty_weight"]) * market_shock_proxy / 12.0,
        "data_weak": float(candidate["data_weak_guard_strength"]) * (10.0 if data_weak else 0.0),
    }
    tuned_score = base_score - sum(penalties.values())

    if selection_score < float(candidate["min_selection_confidence_gate"]):
        tuned_action = "WAIT_RECHECK"
        reasons.append("selection_confidence_gate_block")
    elif signal_quality < float(candidate["min_signal_quality_gate"]):
        tuned_action = "WAIT_RECHECK"
        reasons.append("signal_quality_gate_block")
    elif execution_strength < float(candidate["min_execution_strength_gate"]):
        tuned_action = "WAIT_RECHECK"
        reasons.append("execution_strength_gate_block")
    elif orderbook_imbalance < float(candidate["min_orderbook_imbalance_gate"]):
        tuned_action = "WAIT_RECHECK"
        reasons.append("orderbook_imbalance_gate_block")
    elif int(row.get("candidate_rank") or 0) > int(candidate["selection_rank_cap"]):
        tuned_action = "WAIT_RECHECK"
        reasons.append("selection_rank_cap_block")
    elif data_weak and not bool(candidate["allow_enter_under_data_weak"]):
        tuned_action = "DATA_INSUFFICIENT"
        reasons.append("data_weak_enter_block")
    else:
        enter_threshold = 65.0 + float(candidate["enter_threshold_delta"])
        wait_threshold = 52.0 + float(candidate["wait_threshold_delta"])
        avoid_threshold = 35.0 + float(candidate["avoid_threshold_delta"])
        if tuned_score <= avoid_threshold:
            tuned_action = "AVOID_TODAY"
            reasons.append("policy_avoid_threshold_hit")
        elif tuned_score >= enter_threshold and (
            adjusted_action == "ENTER_NOW" or bool(candidate["allow_wait_override"])
        ):
            tuned_action = "ENTER_NOW"
            reasons.append("policy_enter_threshold_hit")
        elif adjusted_action == "ENTER_NOW" and tuned_score >= wait_threshold:
            tuned_action = "WAIT_RECHECK"
            reasons.append("policy_enter_downgraded_to_wait")
        else:
            tuned_action = "WAIT_RECHECK"
            reasons.append("policy_wait_zone")

    return {
        "tuned_action": tuned_action,
        "tuned_score": tuned_score,
        "policy_trace": trace_source,
        "policy_candidate_id": candidate["policy_candidate_id"],
        "fallback_used_flag": trace_source != "direct_policy",
        "reason_codes": rank_list(reasons),
    }


def _group_decision_rows(frame: pd.DataFrame) -> dict[tuple[date, str, int], pd.DataFrame]:
    grouped: dict[tuple[date, str, int], pd.DataFrame] = {}
    if frame.empty:
        return grouped
    ordered = frame.sort_values(["session_date", "symbol", "horizon", "checkpoint_time"])
    for key, partition in ordered.groupby(["session_date", "symbol", "horizon"], sort=False):
        grouped[(pd.Timestamp(key[0]).date(), str(key[1]).zfill(6), int(key[2]))] = (
            partition.reset_index(drop=True)
        )
    return grouped


def _evaluate_policy_candidate(
    candidate: pd.Series,
    decision_frame: pd.DataFrame,
    *,
    experiment_run_id: str,
    experiment_type: str,
    search_space_version: str,
    objective_version: str,
    split_version: str,
    split_mode: str,
    split_name: str,
    split_index: int,
    window_start_date: date,
    window_end_date: date,
) -> dict[str, object]:
    scoped = decision_frame.loc[_candidate_scope_mask(decision_frame, candidate)].copy()
    if scoped.empty:
        result = {
            "experiment_run_id": experiment_run_id,
            "experiment_type": experiment_type,
            "search_space_version": search_space_version,
            "objective_version": objective_version,
            "split_version": split_version,
            "split_mode": split_mode,
            "split_name": split_name,
            "split_index": split_index,
            "window_start_date": window_start_date,
            "window_end_date": window_end_date,
            "horizon": int(candidate["horizon"]),
            "policy_candidate_id": candidate["policy_candidate_id"],
            "template_id": candidate["template_id"],
            "scope_type": candidate["scope_type"],
            "scope_key": candidate["scope_key"],
            "checkpoint_time": candidate["checkpoint_time"],
            "regime_cluster": candidate["regime_cluster"],
            "regime_family": candidate["regime_family"],
            "window_session_count": 0,
            "sample_count": 0,
            "matured_count": 0,
            "executed_count": 0,
            "no_entry_count": 0,
            "execution_rate": None,
            "mean_realized_excess_return": None,
            "median_realized_excess_return": None,
            "hit_rate": None,
            "mean_timing_edge_vs_open_bps": None,
            "positive_timing_edge_rate": None,
            "skip_saved_loss_rate": None,
            "missed_winner_rate": None,
            "left_tail_proxy": None,
            "stability_score": None,
            "manual_review_required_flag": True,
            "fallback_scope_type": None,
            "fallback_scope_key": None,
            "notes_json": json_text({"reason": "no_scope_matches"}),
            "created_at": pd.Timestamp.now(tz="UTC"),
        }
        result["objective_score"] = _objective_score(result)
        return result

    group_rows: list[dict[str, object]] = []
    for _, row in scoped.iterrows():
        transition = _policy_transition(row, candidate, trace_source="direct_policy")
        group_rows.append({**row.to_dict(), **transition})
    tuned = pd.DataFrame(group_rows)
    grouped = _group_decision_rows(tuned)
    outcomes: list[dict[str, object]] = []
    for partition in grouped.values():
        partition = partition.sort_values("checkpoint_time").reset_index(drop=True)
        chosen = partition.loc[partition["tuned_action"] == "ENTER_NOW"]
        chosen_row = None if chosen.empty else chosen.iloc[0]
        last_row = partition.iloc[-1]
        outcome_status = _normalize_outcome_status(
            last_row["label_available_flag"], last_row["exclusion_reason"]
        )
        baseline_open_return = (
            None
            if pd.isna(last_row["baseline_open_return"])
            else float(last_row["baseline_open_return"])
        )
        baseline_forward_return = (
            None
            if pd.isna(last_row["baseline_forward_return"])
            else float(last_row["baseline_forward_return"])
        )
        if outcome_status != "matured":
            realized_excess_return = None
            timing_edge_bps = None
            executed_flag = False
            no_entry_flag = False
            final_status = outcome_status
            skip_saved_loss_flag = False
            missed_winner_flag = False
        elif (
            chosen_row is not None
            and pd.notna(chosen_row["entry_reference_price"])
            and pd.notna(chosen_row["exit_price"])
        ):
            entry_price = float(chosen_row["entry_reference_price"])
            exit_price = float(chosen_row["exit_price"])
            realized_return = exit_price / entry_price - 1.0 if entry_price else None
            realized_excess_return = (
                None
                if realized_return is None or baseline_forward_return is None
                else realized_return - baseline_forward_return
            )
            timing_edge = (
                None
                if realized_return is None or baseline_open_return is None
                else realized_return - baseline_open_return
            )
            timing_edge_bps = None if timing_edge is None else timing_edge * 10000.0
            executed_flag = True
            no_entry_flag = False
            final_status = "executed"
            skip_saved_loss_flag = False
            missed_winner_flag = False
        else:
            realized_excess_return = (
                None if baseline_forward_return is None else 0.0 - baseline_forward_return
            )
            timing_edge_bps = (
                None if baseline_open_return is None else (0.0 - baseline_open_return) * 10000.0
            )
            executed_flag = False
            no_entry_flag = True
            final_status = "no_entry"
            skip_saved_loss_flag = baseline_open_return is not None and baseline_open_return < 0
            missed_winner_flag = baseline_open_return is not None and baseline_open_return > 0
        outcomes.append(
            {
                "session_date": pd.Timestamp(last_row["session_date"]).date(),
                "outcome_status": final_status,
                "executed_flag": executed_flag,
                "no_entry_flag": no_entry_flag,
                "realized_excess_return": realized_excess_return,
                "timing_edge_vs_open_bps": timing_edge_bps,
                "skip_saved_loss_flag": skip_saved_loss_flag,
                "missed_winner_flag": missed_winner_flag,
            }
        )
    outcome_frame = pd.DataFrame(outcomes)
    matured = outcome_frame.loc[
        outcome_frame["outcome_status"].isin(["executed", "no_entry"])
    ].copy()
    no_entry = matured.loc[matured["no_entry_flag"] == True]  # noqa: E712
    window_session_count = int(
        pd.Series([row["session_date"] for row in outcomes]).nunique() if outcomes else 0
    )
    result = {
        "experiment_run_id": experiment_run_id,
        "experiment_type": experiment_type,
        "search_space_version": search_space_version,
        "objective_version": objective_version,
        "split_version": split_version,
        "split_mode": split_mode,
        "split_name": split_name,
        "split_index": split_index,
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "horizon": int(candidate["horizon"]),
        "policy_candidate_id": candidate["policy_candidate_id"],
        "template_id": candidate["template_id"],
        "scope_type": candidate["scope_type"],
        "scope_key": candidate["scope_key"],
        "checkpoint_time": candidate["checkpoint_time"],
        "regime_cluster": candidate["regime_cluster"],
        "regime_family": candidate["regime_family"],
        "window_session_count": window_session_count,
        "sample_count": int(len(outcome_frame)),
        "matured_count": int(len(matured)),
        "executed_count": int(matured["executed_flag"].fillna(False).sum())
        if not matured.empty
        else 0,
        "no_entry_count": int(matured["no_entry_flag"].fillna(False).sum())
        if not matured.empty
        else 0,
        "execution_rate": matured["executed_flag"].fillna(False).mean()
        if not matured.empty
        else None,
        "mean_realized_excess_return": _safe_mean(matured["realized_excess_return"]),
        "median_realized_excess_return": _safe_median(matured["realized_excess_return"]),
        "hit_rate": matured["realized_excess_return"].gt(0).mean()
        if not matured["realized_excess_return"].dropna().empty
        else None,
        "mean_timing_edge_vs_open_bps": _safe_mean(matured["timing_edge_vs_open_bps"]),
        "positive_timing_edge_rate": matured["timing_edge_vs_open_bps"].gt(0).mean()
        if not matured["timing_edge_vs_open_bps"].dropna().empty
        else None,
        "skip_saved_loss_rate": no_entry["skip_saved_loss_flag"].fillna(False).mean()
        if not no_entry.empty
        else None,
        "missed_winner_rate": no_entry["missed_winner_flag"].fillna(False).mean()
        if not no_entry.empty
        else None,
        "left_tail_proxy": _left_tail_proxy(matured["realized_excess_return"]),
        "stability_score": _stability_score(outcome_frame),
        "manual_review_required_flag": bool(window_session_count < 5 or len(matured) < 10),
        "fallback_scope_type": None,
        "fallback_scope_key": None,
        "notes_json": json_text(
            {
                "pending_count": int(outcome_frame["outcome_status"].eq("pending").sum())
                if not outcome_frame.empty
                else 0,
                "unavailable_count": int(outcome_frame["outcome_status"].eq("unavailable").sum())
                if not outcome_frame.empty
                else 0,
            }
        ),
        "created_at": pd.Timestamp.now(tz="UTC"),
    }
    result["objective_score"] = _objective_score(result)
    return result


def _parse_split_version(split_version: str) -> tuple[int, int, int, int]:
    tokens = split_version.replace("wf_", "").replace("step", "_").split("_")
    digits = [int(token) for token in tokens if token.isdigit()]
    if len(digits) >= 4:
        return digits[0], digits[1], digits[2], digits[3]
    return 40, 10, 10, 5


def _walkforward_splits(
    session_dates: list[date],
    *,
    mode: str,
    train_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
) -> list[dict[str, object]]:
    if len(session_dates) < (train_sessions + validation_sessions + test_sessions):
        return []
    splits: list[dict[str, object]] = []
    cursor = 0
    split_index = 0
    while True:
        train_start = 0 if mode == "ANCHORED_WALKFORWARD" else cursor
        train_end = train_start + train_sessions
        validation_end = train_end + validation_sessions
        test_end = validation_end + test_sessions
        if test_end > len(session_dates):
            break
        splits.append(
            {
                "split_index": split_index,
                "train_dates": session_dates[train_start:train_end],
                "validation_dates": session_dates[train_end:validation_end],
                "test_dates": session_dates[validation_end:test_end],
            }
        )
        split_index += 1
        cursor += step_sessions
    return splits


def _evaluate_candidate_set(
    *,
    candidates: pd.DataFrame,
    decision_frame: pd.DataFrame,
    run_id_prefix: str,
    experiment_type: str,
    search_space_version: str,
    objective_version: str,
    split_version: str,
    split_mode: str,
    splits: list[dict[str, object]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    evaluation_rows: list[dict[str, object]] = []
    experiment_rows: list[dict[str, object]] = []
    now_ts = pd.Timestamp.now(tz="UTC")
    if not splits:
        evaluations = [
            _evaluate_policy_candidate(
                candidate,
                decision_frame,
                experiment_run_id=f"{run_id_prefix}-h{int(candidate['horizon'])}",
                experiment_type=experiment_type,
                search_space_version=search_space_version,
                objective_version=objective_version,
                split_version=split_version,
                split_mode=split_mode,
                split_name="all",
                split_index=0,
                window_start_date=pd.Timestamp(decision_frame["session_date"].min()).date()
                if not decision_frame.empty
                else date.today(),
                window_end_date=pd.Timestamp(decision_frame["session_date"].max()).date()
                if not decision_frame.empty
                else date.today(),
            )
            for _, candidate in candidates.iterrows()
        ]
        evaluation_rows.extend(evaluations)
        for horizon in sorted({int(value) for value in candidates["horizon"].unique()}):
            experiment_rows.append(
                {
                    "experiment_run_id": f"{run_id_prefix}-h{horizon}",
                    "experiment_name": f"{experiment_type.lower()}_intraday_policy_h{horizon}",
                    "experiment_type": experiment_type,
                    "search_space_version": search_space_version,
                    "objective_version": objective_version,
                    "split_version": split_version,
                    "split_mode": split_mode,
                    "as_of_date": None
                    if decision_frame.empty
                    else pd.Timestamp(decision_frame["session_date"].max()).date(),
                    "start_session_date": None
                    if decision_frame.empty
                    else pd.Timestamp(decision_frame["session_date"].min()).date(),
                    "end_session_date": None
                    if decision_frame.empty
                    else pd.Timestamp(decision_frame["session_date"].max()).date(),
                    "train_start_date": None,
                    "train_end_date": None,
                    "validation_start_date": None,
                    "validation_end_date": None,
                    "test_start_date": None,
                    "test_end_date": None,
                    "horizon": horizon,
                    "checkpoint_scope": "MULTI",
                    "regime_scope": "MULTI",
                    "candidate_count": int(
                        candidates.loc[candidates["horizon"] == horizon].shape[0]
                    ),
                    "selected_policy_candidate_id": None,
                    "fallback_used_flag": True,
                    "status": "success",
                    "artifact_path": None,
                    "notes_json": json_text({"mode": "all_window_manual_review"}),
                    "created_at": now_ts,
                    "updated_at": now_ts,
                }
            )
        return pd.DataFrame(experiment_rows), pd.DataFrame(evaluation_rows)

    for split in splits:
        for split_name in ("train", "validation", "test"):
            split_dates = split[f"{split_name}_dates"]
            window_frame = decision_frame.loc[
                decision_frame["session_date"].isin(split_dates)
            ].copy()
            if window_frame.empty:
                continue
            window_start_date = split_dates[0]
            window_end_date = split_dates[-1]
            for _, candidate in candidates.iterrows():
                evaluation_rows.append(
                    _evaluate_policy_candidate(
                        candidate,
                        window_frame,
                        experiment_run_id=f"{run_id_prefix}-h{int(candidate['horizon'])}-s{int(split['split_index'])}",
                        experiment_type=experiment_type,
                        search_space_version=search_space_version,
                        objective_version=objective_version,
                        split_version=split_version,
                        split_mode=split_mode,
                        split_name=split_name,
                        split_index=int(split["split_index"]),
                        window_start_date=window_start_date,
                        window_end_date=window_end_date,
                    )
                )
        for horizon in sorted({int(value) for value in candidates["horizon"].unique()}):
            horizon_candidates = candidates.loc[candidates["horizon"] == horizon]
            validation_rows = [
                row
                for row in evaluation_rows
                if row["split_name"] == "validation"
                and row["split_index"] == int(split["split_index"])
                and row["horizon"] == horizon
            ]
            selected = (
                max(validation_rows, key=lambda row: float(row["objective_score"]))
                if validation_rows
                else None
            )
            experiment_rows.append(
                {
                    "experiment_run_id": f"{run_id_prefix}-h{horizon}-s{int(split['split_index'])}",
                    "experiment_name": f"{experiment_type.lower()}_intraday_policy_h{horizon}",
                    "experiment_type": experiment_type,
                    "search_space_version": search_space_version,
                    "objective_version": objective_version,
                    "split_version": split_version,
                    "split_mode": split_mode,
                    "as_of_date": split["test_dates"][-1],
                    "start_session_date": split["train_dates"][0],
                    "end_session_date": split["test_dates"][-1],
                    "train_start_date": split["train_dates"][0],
                    "train_end_date": split["train_dates"][-1],
                    "validation_start_date": split["validation_dates"][0],
                    "validation_end_date": split["validation_dates"][-1],
                    "test_start_date": split["test_dates"][0],
                    "test_end_date": split["test_dates"][-1],
                    "horizon": horizon,
                    "checkpoint_scope": "MULTI",
                    "regime_scope": "MULTI",
                    "candidate_count": int(len(horizon_candidates)),
                    "selected_policy_candidate_id": None
                    if selected is None
                    else selected["policy_candidate_id"],
                    "fallback_used_flag": bool(
                        selected is not None
                        and pd.notna(selected["manual_review_required_flag"])
                        and bool(selected["manual_review_required_flag"])
                    ),
                    "status": "success",
                    "artifact_path": None,
                    "notes_json": json_text({"split_index": int(split["split_index"])}),
                    "created_at": now_ts,
                    "updated_at": now_ts,
                }
            )
    return pd.DataFrame(experiment_rows), pd.DataFrame(evaluation_rows)


def _ensure_policy_candidates(
    settings: Settings,
    *,
    search_space_version: str,
    horizons: list[int],
    checkpoints: list[str],
    scopes: list[str],
) -> None:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        existing = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_intraday_policy_candidate
            WHERE search_space_version = ?
            """,
            [search_space_version],
        ).fetchone()[0]
    if int(existing or 0) == 0:
        materialize_intraday_policy_candidates(
            settings,
            search_space_version=search_space_version,
            horizons=horizons,
            checkpoints=checkpoints,
            scopes=scopes,
        )


def run_intraday_policy_calibration(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    checkpoints: list[str],
    objective_version: str,
    split_version: str,
    search_space_version: str,
) -> IntradayPolicyCalibrationResult:
    ensure_storage_layout(settings)
    effective_checkpoints = _normalize_checkpoint_list(checkpoints)
    scopes = ["GLOBAL", "HORIZON", "HORIZON_CHECKPOINT", "HORIZON_REGIME_CLUSTER"]
    _ensure_policy_candidates(
        settings,
        search_space_version=search_space_version,
        horizons=horizons,
        checkpoints=effective_checkpoints,
        scopes=scopes,
    )
    materialize_intraday_decision_outcomes(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
    )
    train_sessions, validation_sessions, test_sessions, step_sessions = _parse_split_version(
        split_version
    )
    with activate_run_context(
        "run_intraday_policy_calibration",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=[
                    "fact_intraday_policy_candidate",
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_strategy_result",
                ],
                notes=(
                    "Run matured-only intraday policy calibration. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                candidates = _load_policy_candidates(
                    connection,
                    search_space_version=search_space_version,
                    horizons=horizons,
                )
                decision_frame = _load_policy_base_frame(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    checkpoints=effective_checkpoints,
                )
                decision_frame["regime_cluster"] = decision_frame["market_regime_family"].map(
                    _regime_cluster
                )
                matured_dates = sorted(
                    {
                        pd.Timestamp(value).date()
                        for value in decision_frame.loc[
                            decision_frame["label_available_flag"] == True,  # noqa: E712
                            "session_date",
                        ].dropna()
                    }
                )
                splits = _walkforward_splits(
                    matured_dates,
                    mode="ANCHORED_WALKFORWARD",
                    train_sessions=train_sessions,
                    validation_sessions=validation_sessions,
                    test_sessions=test_sessions,
                    step_sessions=step_sessions,
                )
                experiment_frame, evaluation_frame = _evaluate_candidate_set(
                    candidates=candidates,
                    decision_frame=decision_frame,
                    run_id_prefix=run_context.run_id,
                    experiment_type="CALIBRATION",
                    search_space_version=search_space_version,
                    objective_version=objective_version,
                    split_version=split_version,
                    split_mode="ANCHORED_WALKFORWARD",
                    splits=splits,
                )
                upsert_intraday_policy_experiment_run(connection, experiment_frame)
                upsert_intraday_policy_evaluation(connection, evaluation_frame)
                artifact_paths = []
                if not evaluation_frame.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                evaluation_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/policy_evaluation",
                                partitions={
                                    "end_session_date": end_session_date.isoformat(),
                                    "experiment_type": "CALIBRATION",
                                },
                                filename="policy_evaluation.parquet",
                            )
                        )
                    )
                notes = (
                    "Intraday policy calibration completed. "
                    f"experiments={len(experiment_frame)} evaluations={len(evaluation_frame)} "
                    f"splits={len(splits)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayPolicyCalibrationResult(
                    run_id=run_context.run_id,
                    experiment_row_count=len(experiment_frame),
                    evaluation_row_count=len(evaluation_frame),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday policy calibration failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def run_intraday_policy_walkforward(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    mode: str,
    train_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
    horizons: list[int],
    checkpoints: list[str] | None = None,
    objective_version: str = DEFAULT_OBJECTIVE_VERSION,
    split_version: str | None = None,
    search_space_version: str = DEFAULT_SEARCH_SPACE_VERSION,
) -> IntradayPolicyWalkforwardResult:
    ensure_storage_layout(settings)
    effective_mode = mode.upper()
    effective_checkpoints = _normalize_checkpoint_list(checkpoints)
    scopes = ["GLOBAL", "HORIZON", "HORIZON_CHECKPOINT", "HORIZON_REGIME_CLUSTER"]
    _ensure_policy_candidates(
        settings,
        search_space_version=search_space_version,
        horizons=horizons,
        checkpoints=effective_checkpoints,
        scopes=scopes,
    )
    materialize_intraday_decision_outcomes(
        settings,
        start_session_date=start_session_date,
        end_session_date=end_session_date,
        horizons=horizons,
    )
    effective_split_version = (
        split_version
        or f"wf_{train_sessions}_{validation_sessions}_{test_sessions}_step{step_sessions}"
    )
    with activate_run_context(
        "run_intraday_policy_walkforward",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=[
                    "fact_intraday_policy_candidate",
                    "fact_intraday_adjusted_entry_decision",
                    "fact_intraday_strategy_result",
                ],
                notes=(
                    "Run intraday policy walk-forward. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()} "
                    f"mode={effective_mode}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                candidates = _load_policy_candidates(
                    connection,
                    search_space_version=search_space_version,
                    horizons=horizons,
                )
                decision_frame = _load_policy_base_frame(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    checkpoints=effective_checkpoints,
                )
                decision_frame["regime_cluster"] = decision_frame["market_regime_family"].map(
                    _regime_cluster
                )
                matured_dates = sorted(
                    {
                        pd.Timestamp(value).date()
                        for value in decision_frame.loc[
                            decision_frame["label_available_flag"] == True,  # noqa: E712
                            "session_date",
                        ].dropna()
                    }
                )
                splits = _walkforward_splits(
                    matured_dates,
                    mode=effective_mode,
                    train_sessions=train_sessions,
                    validation_sessions=validation_sessions,
                    test_sessions=test_sessions,
                    step_sessions=step_sessions,
                )
                experiment_frame, evaluation_frame = _evaluate_candidate_set(
                    candidates=candidates,
                    decision_frame=decision_frame,
                    run_id_prefix=run_context.run_id,
                    experiment_type="WALKFORWARD",
                    search_space_version=search_space_version,
                    objective_version=objective_version,
                    split_version=effective_split_version,
                    split_mode=effective_mode,
                    splits=splits,
                )
                upsert_intraday_policy_experiment_run(connection, experiment_frame)
                upsert_intraday_policy_evaluation(connection, evaluation_frame)
                artifact_paths = []
                if not evaluation_frame.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                evaluation_frame,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/policy_walkforward",
                                partitions={
                                    "end_session_date": end_session_date.isoformat(),
                                    "mode": effective_mode.lower(),
                                },
                                filename="policy_walkforward.parquet",
                            )
                        )
                    )
                notes = (
                    "Intraday policy walk-forward completed. "
                    f"experiments={len(experiment_frame)} evaluations={len(evaluation_frame)} "
                    f"splits={len(splits)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayPolicyWalkforwardResult(
                    run_id=run_context.run_id,
                    experiment_row_count=len(experiment_frame),
                    evaluation_row_count=len(evaluation_frame),
                    split_count=len(splits),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday policy walk-forward failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _latest_recommendation_rows(
    connection, *, as_of_date: date, horizons: list[int]
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    return connection.execute(
        f"""
        SELECT *
        FROM fact_intraday_policy_selection_recommendation
        WHERE recommendation_date = (
            SELECT MAX(recommendation_date)
            FROM fact_intraday_policy_selection_recommendation
            WHERE recommendation_date <= ?
        )
          AND horizon IN ({placeholders})
          AND recommendation_rank = 1
        ORDER BY horizon, scope_type, scope_key
        """,
        [as_of_date, *horizons],
    ).fetchdf()


def _evaluation_recommendation_seed_rows(
    connection,
    *,
    as_of_date: date,
    horizons: list[int],
) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in horizons)
    frame = connection.execute(
        f"""
        SELECT
            horizon,
            scope_type,
            scope_key,
            policy_candidate_id,
            template_id,
            MAX(search_space_version) AS search_space_version,
            MAX(objective_version) AS objective_version,
            MAX(split_version) AS split_version,
            AVG(objective_score) FILTER (WHERE split_name = 'test') AS objective_score,
            AVG(stability_score) FILTER (WHERE split_name = 'test') AS stability_score
        FROM fact_intraday_policy_evaluation
        WHERE horizon IN ({placeholders})
          AND window_end_date <= ?
        GROUP BY horizon, scope_type, scope_key, policy_candidate_id, template_id
        ORDER BY horizon, scope_type, scope_key, objective_score DESC NULLS LAST
        """,
        [*horizons, as_of_date],
    ).fetchdf()
    if frame.empty:
        return frame
    seed_rows: list[pd.Series] = []
    for _, partition in frame.groupby(["horizon", "scope_type", "scope_key"], sort=False):
        ordered = partition.sort_values(
            ["objective_score", "stability_score"],
            ascending=[False, False],
            na_position="last",
        )
        seed_rows.append(ordered.iloc[0])
    return pd.DataFrame(seed_rows).reset_index(drop=True)


def _fallback_scope_candidates(
    scope_type: str, scope_key: str, horizon: int
) -> list[tuple[str, str]]:
    if scope_type == "HORIZON_CHECKPOINT_REGIME_FAMILY":
        checkpoint = scope_key.split("|")[1].split("=")[1]
        return [
            ("HORIZON_CHECKPOINT", f"H{horizon}|CP={checkpoint}"),
            ("HORIZON", f"H{horizon}"),
            ("GLOBAL", f"H{horizon}|GLOBAL"),
        ]
    if scope_type in {"HORIZON_REGIME_CLUSTER", "HORIZON_CHECKPOINT"}:
        return [("HORIZON", f"H{horizon}"), ("GLOBAL", f"H{horizon}|GLOBAL")]
    if scope_type == "HORIZON":
        return [("GLOBAL", f"H{horizon}|GLOBAL")]
    return []


def materialize_intraday_policy_recommendations(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    minimum_test_sessions: int,
) -> IntradayPolicyRecommendationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "materialize_intraday_policy_recommendations",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_intraday_policy_evaluation"],
                notes=f"Materialize intraday policy recommendations for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                evaluation_frame = connection.execute(
                    f"""
                    SELECT
                        horizon,
                        scope_type,
                        scope_key,
                        policy_candidate_id,
                        template_id,
                        MAX(experiment_run_id) AS source_experiment_run_id,
                        MAX(search_space_version) AS search_space_version,
                        MAX(objective_version) AS objective_version,
                        MAX(split_version) AS split_version,
                        SUM(window_session_count)
                            FILTER (WHERE split_name = 'test') AS test_session_count,
                        SUM(sample_count) FILTER (WHERE split_name = 'test') AS sample_count,
                        SUM(executed_count) FILTER (WHERE split_name = 'test') AS executed_count,
                        AVG(execution_rate) FILTER (WHERE split_name = 'test') AS execution_rate,
                        AVG(mean_realized_excess_return)
                            FILTER (WHERE split_name = 'test') AS mean_realized_excess_return,
                        AVG(median_realized_excess_return)
                            FILTER (WHERE split_name = 'test') AS median_realized_excess_return,
                        AVG(hit_rate) FILTER (WHERE split_name = 'test') AS hit_rate,
                        AVG(mean_timing_edge_vs_open_bps)
                            FILTER (WHERE split_name = 'test') AS mean_timing_edge_vs_open_bps,
                        AVG(positive_timing_edge_rate)
                            FILTER (WHERE split_name = 'test') AS positive_timing_edge_rate,
                        AVG(skip_saved_loss_rate)
                            FILTER (WHERE split_name = 'test') AS skip_saved_loss_rate,
                        AVG(missed_winner_rate)
                            FILTER (WHERE split_name = 'test') AS missed_winner_rate,
                        AVG(left_tail_proxy) FILTER (WHERE split_name = 'test') AS left_tail_proxy,
                        AVG(stability_score) FILTER (WHERE split_name = 'test') AS stability_score,
                        AVG(objective_score) FILTER (WHERE split_name = 'test') AS objective_score,
                        BOOL_OR(manual_review_required_flag)
                            FILTER (WHERE split_name = 'test') AS manual_review_required_flag,
                        AVG(objective_score)
                            FILTER (WHERE split_name IN ('validation', 'all'))
                            AS fallback_objective_score,
                        SUM(window_session_count)
                            FILTER (WHERE split_name IN ('validation', 'all'))
                            AS fallback_session_count
                    FROM fact_intraday_policy_evaluation
                    WHERE horizon IN ({placeholders})
                      AND window_end_date <= ?
                    GROUP BY horizon, scope_type, scope_key, policy_candidate_id, template_id
                    ORDER BY horizon, scope_type, scope_key, objective_score DESC NULLS LAST
                    """,
                    [*horizons, as_of_date],
                ).fetchdf()
                if evaluation_frame.empty:
                    notes = "No intraday policy evaluation rows were available for recommendations."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayPolicyRecommendationResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )

                grouped_best: dict[tuple[int, str, str], pd.DataFrame] = {}
                for key, partition in evaluation_frame.groupby(
                    ["horizon", "scope_type", "scope_key"], sort=False
                ):
                    ordered = partition.copy()
                    ordered["effective_objective_score"] = ordered["objective_score"].fillna(
                        ordered["fallback_objective_score"]
                    )
                    ordered = ordered.sort_values(
                        [
                            "manual_review_required_flag",
                            "effective_objective_score",
                            "stability_score",
                        ],
                        ascending=[True, False, False],
                        na_position="last",
                    )
                    grouped_best[(int(key[0]), str(key[1]), str(key[2]))] = ordered.reset_index(
                        drop=True
                    )

                now_ts = pd.Timestamp.now(tz="UTC")
                rows: list[dict[str, object]] = []
                for horizon, scope_type, scope_key in sorted(grouped_best.keys()):
                    ordered = grouped_best[(horizon, scope_type, scope_key)]
                    recommendation_rows: list[pd.Series] = []
                    fallback_scope_type = None
                    fallback_scope_key = None
                    lead_test_session_count = (
                        int(ordered.iloc[0]["test_session_count"])
                        if not ordered.empty and pd.notna(ordered.iloc[0]["test_session_count"])
                        else 0
                    )
                    if not ordered.empty and lead_test_session_count >= minimum_test_sessions:
                        recommendation_rows = [
                            ordered.iloc[index] for index in range(min(3, len(ordered)))
                        ]
                    else:
                        for candidate_scope_type, candidate_scope_key in _fallback_scope_candidates(
                            scope_type, scope_key, horizon
                        ):
                            fallback = grouped_best.get(
                                (horizon, candidate_scope_type, candidate_scope_key)
                            )
                            if fallback is None or fallback.empty:
                                continue
                            recommendation_rows = [
                                fallback.iloc[index] for index in range(min(3, len(fallback)))
                            ]
                            fallback_scope_type = candidate_scope_type
                            fallback_scope_key = candidate_scope_key
                            break
                        if not recommendation_rows and not ordered.empty:
                            recommendation_rows = [ordered.iloc[0]]

                    for rank, row in enumerate(recommendation_rows, start=1):
                        test_session_count = (
                            int(row["test_session_count"])
                            if pd.notna(row["test_session_count"])
                            else (
                                int(row["fallback_session_count"])
                                if pd.notna(row["fallback_session_count"])
                                else 0
                            )
                        )
                        manual_review_required = (
                            pd.notna(row.get("manual_review_required_flag"))
                            and bool(row.get("manual_review_required_flag"))
                        ) or (test_session_count < minimum_test_sessions)
                        rows.append(
                            {
                                "recommendation_date": as_of_date,
                                "horizon": horizon,
                                "scope_type": scope_type,
                                "scope_key": scope_key,
                                "recommendation_rank": rank,
                                "policy_candidate_id": row["policy_candidate_id"],
                                "template_id": row["template_id"],
                                "source_experiment_run_id": row["source_experiment_run_id"],
                                "search_space_version": row["search_space_version"],
                                "objective_version": row["objective_version"],
                                "split_version": row["split_version"],
                                "sample_count": int(
                                    row["sample_count"]
                                    if pd.notna(row["sample_count"])
                                    else (
                                        row["fallback_session_count"]
                                        if pd.notna(row["fallback_session_count"])
                                        else 0
                                    )
                                ),
                                "test_session_count": test_session_count,
                                "executed_count": int(row["executed_count"])
                                if pd.notna(row["executed_count"])
                                else 0,
                                "execution_rate": row["execution_rate"],
                                "mean_realized_excess_return": row["mean_realized_excess_return"],
                                "median_realized_excess_return": row[
                                    "median_realized_excess_return"
                                ],
                                "hit_rate": row["hit_rate"],
                                "mean_timing_edge_vs_open_bps": row["mean_timing_edge_vs_open_bps"],
                                "positive_timing_edge_rate": row["positive_timing_edge_rate"],
                                "skip_saved_loss_rate": row["skip_saved_loss_rate"],
                                "missed_winner_rate": row["missed_winner_rate"],
                                "left_tail_proxy": row["left_tail_proxy"],
                                "stability_score": row["stability_score"],
                                "objective_score": row["effective_objective_score"],
                                "manual_review_required_flag": manual_review_required,
                                "fallback_scope_type": fallback_scope_type,
                                "fallback_scope_key": fallback_scope_key,
                                "recommendation_reason_json": json_text(
                                    {
                                        "minimum_test_sessions": minimum_test_sessions,
                                        "fallback_scope_type": fallback_scope_type,
                                        "fallback_scope_key": fallback_scope_key,
                                    }
                                ),
                                "created_at": now_ts,
                            }
                        )
                output = pd.DataFrame(rows)
                upsert_intraday_policy_selection_recommendation(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/policy_recommendation",
                            partitions={"recommendation_date": as_of_date.isoformat()},
                            filename="policy_recommendation.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday policy recommendations materialized. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(output)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayPolicyRecommendationResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday policy recommendation materialization failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def evaluate_intraday_policy_ablation(
    settings: Settings,
    *,
    start_session_date: date,
    end_session_date: date,
    horizons: list[int],
    base_policy_source: str,
) -> IntradayPolicyAblationResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "evaluate_intraday_policy_ablation",
        as_of_date=end_session_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=end_session_date,
                input_sources=[
                    "fact_intraday_policy_selection_recommendation",
                    "fact_intraday_adjusted_entry_decision",
                ],
                notes=(
                    "Evaluate intraday policy ablation. "
                    f"range={start_session_date.isoformat()}..{end_session_date.isoformat()}"
                ),
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                decision_frame = _load_policy_base_frame(
                    connection,
                    start_session_date=start_session_date,
                    end_session_date=end_session_date,
                    horizons=horizons,
                    checkpoints=list(DEFAULT_CHECKPOINTS),
                )
                decision_frame["regime_cluster"] = decision_frame["market_regime_family"].map(
                    _regime_cluster
                )
                base_rows = _latest_recommendation_rows(
                    connection,
                    as_of_date=end_session_date,
                    horizons=horizons,
                )
                if base_rows.empty and base_policy_source == "latest_recommendation":
                    base_rows = _evaluation_recommendation_seed_rows(
                        connection,
                        as_of_date=end_session_date,
                        horizons=horizons,
                    )
                if base_rows.empty:
                    notes = "No latest recommendation rows were available for ablation."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayPolicyAblationResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                candidate_ids = [
                    str(candidate_id)
                    for candidate_id in base_rows["policy_candidate_id"].dropna().unique().tolist()
                ]
                if not candidate_ids:
                    notes = "No policy candidate ids were available for ablation."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayPolicyAblationResult(
                        run_id=run_context.run_id,
                        row_count=0,
                        artifact_paths=[],
                        notes=notes,
                    )
                candidate_placeholders = ",".join("?" for _ in candidate_ids)
                candidate_frame = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_intraday_policy_candidate
                    WHERE policy_candidate_id IN ({candidate_placeholders})
                    """,
                    candidate_ids,
                ).fetchdf()
                candidate_map = candidate_frame.set_index("policy_candidate_id", drop=False)
                rows: list[dict[str, object]] = []
                now_ts = pd.Timestamp.now(tz="UTC")
                for _, recommendation in base_rows.iterrows():
                    if recommendation["policy_candidate_id"] not in candidate_map.index:
                        continue
                    base_candidate = candidate_map.loc[recommendation["policy_candidate_id"]]
                    base_metrics = _evaluate_policy_candidate(
                        base_candidate,
                        decision_frame,
                        experiment_run_id=run_context.run_id,
                        experiment_type="ABLATION",
                        search_space_version=str(recommendation["search_space_version"]),
                        objective_version=str(recommendation["objective_version"]),
                        split_version=str(recommendation["split_version"]),
                        split_mode="ABLATION",
                        split_name="all",
                        split_index=0,
                        window_start_date=start_session_date,
                        window_end_date=end_session_date,
                    )
                    for ablation_name, fields in ABLATION_COMPONENTS.items():
                        mutated = base_candidate.copy()
                        for field in fields:
                            if field == "max_gap_up_allowance_pct":
                                mutated[field] = 999.0
                            elif field.endswith("_gate"):
                                mutated[field] = 0.0
                            elif field == "selection_rank_cap":
                                mutated[field] = 999
                            else:
                                mutated[field] = 0.0
                        ablated_metrics = _evaluate_policy_candidate(
                            mutated,
                            decision_frame,
                            experiment_run_id=run_context.run_id,
                            experiment_type="ABLATION",
                            search_space_version=str(recommendation["search_space_version"]),
                            objective_version=str(recommendation["objective_version"]),
                            split_version=str(recommendation["split_version"]),
                            split_mode="ABLATION",
                            split_name="all",
                            split_index=0,
                            window_start_date=start_session_date,
                            window_end_date=end_session_date,
                        )
                        rows.append(
                            {
                                "experiment_run_id": run_context.run_id,
                                "ablation_date": end_session_date,
                                "start_session_date": start_session_date,
                                "end_session_date": end_session_date,
                                "horizon": int(base_candidate["horizon"]),
                                "base_policy_source": base_policy_source,
                                "base_policy_candidate_id": base_candidate["policy_candidate_id"],
                                "ablation_name": ablation_name,
                                "sample_count": int(base_metrics["sample_count"]),
                                "mean_realized_excess_return_delta": (
                                    ablated_metrics["mean_realized_excess_return"] or 0.0
                                )
                                - (base_metrics["mean_realized_excess_return"] or 0.0),
                                "median_realized_excess_return_delta": (
                                    ablated_metrics["median_realized_excess_return"] or 0.0
                                )
                                - (base_metrics["median_realized_excess_return"] or 0.0),
                                "hit_rate_delta": (ablated_metrics["hit_rate"] or 0.0)
                                - (base_metrics["hit_rate"] or 0.0),
                                "mean_timing_edge_vs_open_bps_delta": (
                                    ablated_metrics["mean_timing_edge_vs_open_bps"] or 0.0
                                )
                                - (base_metrics["mean_timing_edge_vs_open_bps"] or 0.0),
                                "execution_rate_delta": (ablated_metrics["execution_rate"] or 0.0)
                                - (base_metrics["execution_rate"] or 0.0),
                                "skip_saved_loss_rate_delta": (
                                    ablated_metrics["skip_saved_loss_rate"] or 0.0
                                )
                                - (base_metrics["skip_saved_loss_rate"] or 0.0),
                                "missed_winner_rate_delta": (
                                    ablated_metrics["missed_winner_rate"] or 0.0
                                )
                                - (base_metrics["missed_winner_rate"] or 0.0),
                                "left_tail_proxy_delta": (ablated_metrics["left_tail_proxy"] or 0.0)
                                - (base_metrics["left_tail_proxy"] or 0.0),
                                "stability_score_delta": (ablated_metrics["stability_score"] or 0.0)
                                - (base_metrics["stability_score"] or 0.0),
                                "objective_score_delta": (ablated_metrics["objective_score"] or 0.0)
                                - (base_metrics["objective_score"] or 0.0),
                                "notes_json": json_text(
                                    {
                                        "template_id": base_candidate["template_id"],
                                        "scope_key": base_candidate["scope_key"],
                                    }
                                ),
                                "created_at": now_ts,
                            }
                        )
                output = pd.DataFrame(rows)
                upsert_intraday_policy_ablation_result(connection, output)
                artifact_paths = []
                if not output.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/policy_ablation",
                                partitions={"end_session_date": end_session_date.isoformat()},
                                filename="policy_ablation.parquet",
                            )
                        )
                    )
                notes = (
                    "Intraday policy ablation evaluated. "
                    f"rows={len(output)} base_policy_source={base_policy_source}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayPolicyAblationResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday policy ablation evaluation failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def freeze_intraday_active_policy(
    settings: Settings,
    *,
    as_of_date: date,
    promotion_type: str,
    source: str,
    note: str,
) -> IntradayActivePolicyResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "freeze_intraday_active_policy",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_intraday_policy_selection_recommendation"],
                notes=f"Freeze intraday active policy for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                recommendation_rows = _latest_recommendation_rows(
                    connection, as_of_date=as_of_date, horizons=[1, 5]
                )
                if recommendation_rows.empty:
                    notes = "No recommendation rows were available to freeze."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayActivePolicyResult(
                        run_id=run_context.run_id, row_count=0, artifact_paths=[], notes=notes
                    )
                candidate_frame = connection.execute(
                    """
                    SELECT *
                    FROM fact_intraday_policy_candidate
                    WHERE policy_candidate_id IN (
                        SELECT policy_candidate_id
                        FROM fact_intraday_policy_selection_recommendation
                        WHERE recommendation_date = (
                            SELECT MAX(recommendation_date)
                            FROM fact_intraday_policy_selection_recommendation
                            WHERE recommendation_date <= ?
                        )
                          AND recommendation_rank = 1
                    )
                    """,
                    [as_of_date],
                ).fetchdf()
                candidate_map = candidate_frame.set_index("policy_candidate_id", drop=False)
                now_ts = pd.Timestamp.now(tz="UTC")
                new_rows: list[dict[str, object]] = []
                for _, recommendation in recommendation_rows.iterrows():
                    candidate = candidate_map.loc[recommendation["policy_candidate_id"]]
                    connection.execute(
                        """
                        UPDATE fact_intraday_active_policy
                        SET active_flag = FALSE,
                            effective_to_date = ?,
                            updated_at = ?
                        WHERE horizon = ?
                          AND scope_type = ?
                          AND scope_key = ?
                          AND active_flag = TRUE
                          AND effective_from_date <= ?
                          AND (effective_to_date IS NULL OR effective_to_date >= ?)
                        """,
                        [
                            as_of_date - timedelta(days=1),
                            now_ts,
                            int(recommendation["horizon"]),
                            recommendation["scope_type"],
                            recommendation["scope_key"],
                            as_of_date,
                            as_of_date,
                        ],
                    )
                    new_rows.append(
                        {
                            "active_policy_id": hashlib.sha1(
                                (
                                    f"{run_context.run_id}|{recommendation['horizon']}|{recommendation['scope_type']}|{recommendation['scope_key']}"
                                ).encode("utf-8")
                            ).hexdigest()[:18],
                            "horizon": int(recommendation["horizon"]),
                            "scope_type": recommendation["scope_type"],
                            "scope_key": recommendation["scope_key"],
                            "checkpoint_time": candidate["checkpoint_time"],
                            "regime_cluster": candidate["regime_cluster"],
                            "regime_family": candidate["regime_family"],
                            "policy_candidate_id": recommendation["policy_candidate_id"],
                            "source_recommendation_date": recommendation["recommendation_date"],
                            "promotion_type": promotion_type,
                            "source_type": source,
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "fallback_scope_type": recommendation["fallback_scope_type"],
                            "fallback_scope_key": recommendation["fallback_scope_key"],
                            "rollback_of_active_policy_id": None,
                            "note": note,
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )
                output = pd.DataFrame(new_rows)
                upsert_intraday_active_policy(connection, output)
                artifact_paths = [
                    str(
                        write_parquet(
                            output,
                            base_dir=settings.paths.curated_dir,
                            dataset="intraday/active_policy",
                            partitions={"effective_from_date": as_of_date.isoformat()},
                            filename="active_policy.parquet",
                        )
                    )
                ]
                notes = (
                    "Intraday active policy frozen. "
                    f"effective_from_date={as_of_date.isoformat()} rows={len(output)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayActivePolicyResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Freezing intraday active policy failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def rollback_intraday_active_policy(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    note: str,
) -> IntradayActivePolicyResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "rollback_intraday_active_policy",
        as_of_date=as_of_date,
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=as_of_date,
                input_sources=["fact_intraday_active_policy"],
                notes=f"Rollback intraday active policy for {as_of_date.isoformat()}",
                ranking_version=SELECTION_ENGINE_VERSION,
            )
            try:
                placeholders = ",".join("?" for _ in horizons)
                active_rows = connection.execute(
                    f"""
                    SELECT *
                    FROM fact_intraday_active_policy
                    WHERE active_flag = TRUE
                      AND horizon IN ({placeholders})
                      AND effective_from_date <= ?
                      AND (effective_to_date IS NULL OR effective_to_date >= ?)
                    ORDER BY horizon, scope_type, scope_key, effective_from_date DESC
                    """,
                    [*horizons, as_of_date, as_of_date],
                ).fetchdf()
                if active_rows.empty:
                    notes = "No active policy rows were available to roll back."
                    record_run_finish(
                        connection,
                        run_id=run_context.run_id,
                        finished_at=now_local(settings.app.timezone),
                        status="success",
                        output_artifacts=[],
                        notes=notes,
                        ranking_version=SELECTION_ENGINE_VERSION,
                    )
                    return IntradayActivePolicyResult(
                        run_id=run_context.run_id, row_count=0, artifact_paths=[], notes=notes
                    )
                now_ts = pd.Timestamp.now(tz="UTC")
                restored_rows: list[dict[str, object]] = []
                for _, active in active_rows.iterrows():
                    connection.execute(
                        """
                        UPDATE fact_intraday_active_policy
                        SET active_flag = FALSE,
                            effective_to_date = ?,
                            updated_at = ?
                        WHERE active_policy_id = ?
                        """,
                        [as_of_date - timedelta(days=1), now_ts, active["active_policy_id"]],
                    )
                    previous = connection.execute(
                        """
                        SELECT *
                        FROM fact_intraday_active_policy
                        WHERE horizon = ?
                          AND scope_type = ?
                          AND scope_key = ?
                          AND effective_from_date < ?
                        ORDER BY effective_from_date DESC, created_at DESC
                        LIMIT 1
                        """,
                        [
                            int(active["horizon"]),
                            active["scope_type"],
                            active["scope_key"],
                            active["effective_from_date"],
                        ],
                    ).fetchdf()
                    if previous.empty:
                        continue
                    previous_row = previous.iloc[0]
                    restored_rows.append(
                        {
                            "active_policy_id": hashlib.sha1(
                                (
                                    f"{run_context.run_id}|rollback|{active['active_policy_id']}"
                                ).encode("utf-8")
                            ).hexdigest()[:18],
                            "horizon": int(previous_row["horizon"]),
                            "scope_type": previous_row["scope_type"],
                            "scope_key": previous_row["scope_key"],
                            "checkpoint_time": previous_row["checkpoint_time"],
                            "regime_cluster": previous_row["regime_cluster"],
                            "regime_family": previous_row["regime_family"],
                            "policy_candidate_id": previous_row["policy_candidate_id"],
                            "source_recommendation_date": previous_row[
                                "source_recommendation_date"
                            ],
                            "promotion_type": "ROLLBACK_RESTORE",
                            "source_type": "rollback_intraday_active_policy",
                            "effective_from_date": as_of_date,
                            "effective_to_date": None,
                            "active_flag": True,
                            "fallback_scope_type": previous_row["fallback_scope_type"],
                            "fallback_scope_key": previous_row["fallback_scope_key"],
                            "rollback_of_active_policy_id": active["active_policy_id"],
                            "note": note,
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                    )
                output = pd.DataFrame(restored_rows)
                upsert_intraday_active_policy(connection, output)
                artifact_paths = []
                if not output.empty:
                    artifact_paths.append(
                        str(
                            write_parquet(
                                output,
                                base_dir=settings.paths.curated_dir,
                                dataset="intraday/active_policy_rollback",
                                partitions={"effective_from_date": as_of_date.isoformat()},
                                filename="active_policy_rollback.parquet",
                            )
                        )
                    )
                notes = (
                    "Intraday active policy rollback completed. "
                    f"effective_from_date={as_of_date.isoformat()} rows={len(output)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                return IntradayActivePolicyResult(
                    run_id=run_context.run_id,
                    row_count=len(output),
                    artifact_paths=artifact_paths,
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Intraday active policy rollback failed.",
                    error_message=str(exc),
                    ranking_version=SELECTION_ENGINE_VERSION,
                )
                raise


def _load_active_policy_registry(connection, *, as_of_date: date) -> pd.DataFrame:
    return connection.execute(
        """
        SELECT
            active.*,
            candidate.template_id,
            candidate.search_space_version,
            candidate.enter_threshold_delta,
            candidate.wait_threshold_delta,
            candidate.avoid_threshold_delta,
            candidate.min_selection_confidence_gate,
            candidate.min_signal_quality_gate,
            candidate.uncertainty_penalty_weight,
            candidate.spread_penalty_weight,
            candidate.friction_penalty_weight,
            candidate.gap_chase_penalty_weight,
            candidate.cohort_weakness_penalty_weight,
            candidate.market_shock_penalty_weight,
            candidate.data_weak_guard_strength,
            candidate.max_gap_up_allowance_pct,
            candidate.min_execution_strength_gate,
            candidate.min_orderbook_imbalance_gate,
            candidate.allow_enter_under_data_weak,
            candidate.allow_wait_override,
            candidate.selection_rank_cap
        FROM fact_intraday_active_policy AS active
        JOIN fact_intraday_policy_candidate AS candidate
          ON active.policy_candidate_id = candidate.policy_candidate_id
        WHERE active.active_flag = TRUE
          AND active.effective_from_date <= ?
          AND (active.effective_to_date IS NULL OR active.effective_to_date >= ?)
        ORDER BY
            active.horizon,
            active.scope_type,
            active.scope_key,
            active.effective_from_date DESC
        """,
        [as_of_date, as_of_date],
    ).fetchdf()


def apply_active_intraday_policy_frame(
    settings: Settings,
    *,
    session_date: date,
    horizons: list[int] | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    connection=None,
) -> pd.DataFrame:
    ensure_storage_layout(settings)
    effective_horizons = horizons or [1, 5]
    owns_connection = connection is None
    if owns_connection:
        connection_context = duckdb_connection(settings.paths.duckdb_path, read_only=True)
        connection = connection_context.__enter__()
    try:
        bootstrap_core_tables(connection)
        frame = _load_policy_base_frame(
            connection,
            start_session_date=session_date,
            end_session_date=session_date,
            horizons=effective_horizons,
            checkpoints=list(DEFAULT_CHECKPOINTS),
        )
        active_policies = _load_active_policy_registry(connection, as_of_date=session_date)
    finally:
        if owns_connection:
            connection_context.__exit__(None, None, None)
    if frame.empty:
        return pd.DataFrame()
    frame["regime_cluster"] = frame["market_regime_family"].map(_regime_cluster)
    if symbol:
        frame = frame.loc[frame["symbol"] == symbol.zfill(6)].copy()
    if frame.empty:
        return frame

    specificity_order = {
        "HORIZON_CHECKPOINT_REGIME_FAMILY": 0,
        "HORIZON_REGIME_CLUSTER": 1,
        "HORIZON_CHECKPOINT": 2,
        "HORIZON": 3,
        "GLOBAL": 4,
    }
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        applicable = active_policies.loc[active_policies["horizon"] == int(row["horizon"])].copy()
        if not applicable.empty:
            row_frame = pd.DataFrame([row]).assign(
                regime_cluster=_regime_cluster(row["market_regime_family"])
            )
            candidate_mask = pd.Series(
                [
                    bool(_candidate_scope_mask(row_frame, policy).iloc[0])
                    for _, policy in applicable.iterrows()
                ],
                index=applicable.index,
            )
            applicable = applicable.loc[candidate_mask].copy()
        if applicable.empty:
            rows.append(
                {
                    **row.to_dict(),
                    "tuned_action": row["adjusted_action"],
                    "tuned_score": row["adjusted_timing_score"],
                    "active_policy_candidate_id": None,
                    "active_policy_template_id": None,
                    "active_policy_scope_type": None,
                    "active_policy_scope_key": None,
                    "policy_trace": "default_adjusted_policy",
                    "policy_reason_codes_json": json_text(["default_adjusted_policy"]),
                    "fallback_used_flag": True,
                }
            )
            continue
        applicable["specificity_rank"] = applicable["scope_type"].map(specificity_order)
        selected_policy = applicable.sort_values(
            ["specificity_rank", "effective_from_date"],
            ascending=[True, False],
        ).iloc[0]
        trace_source = (
            "direct_policy"
            if str(selected_policy["scope_key"])
            == _scope_key(
                horizon=int(row["horizon"]),
                scope_type=str(selected_policy["scope_type"]),
                checkpoint_time=row["checkpoint_time"],
                regime_cluster=_regime_cluster(row["market_regime_family"]),
                regime_family=row["market_regime_family"],
            )
            else "fallback_policy"
        )
        transition = _policy_transition(row, selected_policy, trace_source=trace_source)
        rows.append(
            {
                **row.to_dict(),
                "tuned_action": transition["tuned_action"],
                "tuned_score": transition["tuned_score"],
                "active_policy_candidate_id": selected_policy["policy_candidate_id"],
                "active_policy_template_id": selected_policy["template_id"],
                "active_policy_scope_type": selected_policy["scope_type"],
                "active_policy_scope_key": selected_policy["scope_key"],
                "policy_trace": transition["policy_trace"],
                "policy_reason_codes_json": json_text(transition["reason_codes"]),
                "fallback_used_flag": transition["fallback_used_flag"],
            }
        )
    output = pd.DataFrame(rows)
    output = output.sort_values(["horizon", "symbol", "checkpoint_time"]).reset_index(drop=True)
    if limit is not None:
        output = output.head(limit).copy()
    return output
