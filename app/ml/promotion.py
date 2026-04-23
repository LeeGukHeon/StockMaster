from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.feature_store import REQUIRED_QUALITY_FEATURE_NAMES
from app.ml.active import freeze_alpha_active_model
from app.ml.constants import (
    MCS_ALPHA,
    MCS_BLOCK_LENGTH,
    MCS_BOOTSTRAP_REPS,
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PROMOTION_LOOKBACK_SELECTION_DATES,
    get_alpha_model_spec,
    resolve_promotion_primary_loss_for_spec,
)
from app.ml.registry import (
    load_active_alpha_model,
    load_alpha_model_specs,
    load_latest_training_run,
)
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start

PRIMARY_LOSS_NAME = "loss_top10"
AUDIT_LOSS_NAMES: tuple[str, ...] = ("loss_top20", "loss_point", "loss_rank")
ALL_LOSS_NAMES: tuple[str, ...] = (PRIMARY_LOSS_NAME, *AUDIT_LOSS_NAMES)
MODEL_SPEC_LABELS: dict[str, str] = {
    MODEL_SPEC_ID: "recursive",
    "alpha_rolling_120_v1": "rolling 120d",
    "alpha_rolling_250_v1": "rolling 250d",
    "alpha_rank_rolling_120_v1": "rank rolling 120d",
    "alpha_topbucket_h1_rolling_120_v1": "topbucket h1 rolling 120d",
    "alpha_lead_d1_v1": "lead d1 v1",
    "alpha_swing_d5_v1": "swing d5 v1",
    "alpha_swing_d5_v2": "swing d5 top5 v2",
    "alpha_recursive_rolling_combo": "recursive+rolling combo",
}
DECISION_LABELS: dict[str, str] = {
    "KEEP_ACTIVE": "Active kept",
    "PROMOTE_CHALLENGER": "Challenger promoted",
    "NO_AUTO_PROMOTION": "No auto-promotion",
}
DECISION_REASON_LABELS: dict[str, str] = {
    "incumbent_in_superior_set": "incumbent remained in the superior set",
    "single_challenger_survived": "one challenger survived the superior set",
    "combo_survived_in_superior_set": "combo candidate survived the superior set",
    "ambiguous_superior_set": "multiple challengers survived without a clear winner",
    "no_matured_shadow_history": "matured shadow self-backtest history is not available",
    "no_complete_loss_matrix": "shadow self-backtest matrix is incomplete",
    "shadow_validation_failed": "shadow validation failed for the promoted challenger",
}


@dataclass(slots=True)
class AlphaPromotionResult:
    run_id: str
    as_of_date: date
    row_count: int
    promoted_horizon_count: int
    artifact_paths: list[str]
    notes: str


def format_alpha_model_spec_id(model_spec_id: str | None) -> str:
    if model_spec_id is None:
        return "-"
    return MODEL_SPEC_LABELS.get(str(model_spec_id), str(model_spec_id))


def format_alpha_promotion_decision(decision: str | None) -> str:
    if decision is None:
        return "-"
    return DECISION_LABELS.get(str(decision), str(decision))


def format_alpha_promotion_reason(reason: str | None) -> str:
    if reason is None:
        return "-"
    return DECISION_REASON_LABELS.get(str(reason), str(reason))


def _safe_json_load(value: object) -> dict[str, object]:
    if value in (None, "", "{}"):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _float_or_none(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _loss_to_return(loss_value: object) -> float | None:
    value = _float_or_none(loss_value)
    if value is None:
        return None
    return -value


def _loss_to_rank_ic(loss_value: object) -> float | None:
    value = _float_or_none(loss_value)
    if value is None:
        return None
    return -value


def _resolve_primary_loss_name_for_model(
    model_spec_id: str | None,
    *,
    horizon: int,
) -> str:
    if model_spec_id in (None, ""):
        return PRIMARY_LOSS_NAME
    try:
        model_spec = get_alpha_model_spec(str(model_spec_id))
    except KeyError:
        return PRIMARY_LOSS_NAME
    return resolve_promotion_primary_loss_for_spec(model_spec, horizon=horizon)


def _resolve_loss_names_for_model(
    model_spec_id: str | None,
    *,
    horizon: int,
) -> tuple[str, ...]:
    primary_loss_name = _resolve_primary_loss_name_for_model(model_spec_id, horizon=horizon)
    return (primary_loss_name, *AUDIT_LOSS_NAMES)


def _load_latest_promotion_rows(
    connection,
    *,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    where_clause = "WHERE 1 = 1"
    params: list[object] = []
    if as_of_date is not None:
        where_clause += " AND promotion_date <= ?"
        params.append(as_of_date)
    return connection.execute(
        f"""
        WITH latest AS (
            SELECT *
            FROM fact_alpha_promotion_test
            {where_clause}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY horizon, incumbent_model_spec_id, challenger_model_spec_id, loss_name
                ORDER BY promotion_date DESC, created_at DESC
            ) = 1
        )
        SELECT *
        FROM latest
        ORDER BY promotion_date DESC, horizon, challenger_model_spec_id
        """,
        params,
    ).fetchdf()


def _load_active_alpha_registry_frame(
    connection,
    *,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    if as_of_date is None:
        return connection.execute(
            """
            SELECT
                horizon,
                model_spec_id AS active_model_spec_id,
                effective_from_date AS active_effective_from_date,
                source_type AS active_source_type,
                promotion_type AS active_promotion_type
            FROM fact_alpha_active_model
            WHERE active_flag = TRUE
              AND effective_from_date <= CURRENT_DATE
              AND (effective_to_date IS NULL OR effective_to_date >= CURRENT_DATE)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY horizon
                ORDER BY effective_from_date DESC, created_at DESC
            ) = 1
            """
        ).fetchdf()
    return connection.execute(
        """
        SELECT
            horizon,
            model_spec_id AS active_model_spec_id,
            effective_from_date AS active_effective_from_date,
            source_type AS active_source_type,
            promotion_type AS active_promotion_type
        FROM fact_alpha_active_model
        WHERE active_flag = TRUE
          AND effective_from_date <= ?
          AND (effective_to_date IS NULL OR effective_to_date >= ?)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY horizon
            ORDER BY effective_from_date DESC, created_at DESC
        ) = 1
        """,
        [as_of_date, as_of_date],
    ).fetchdf()


def _best_comparison_spec_id(
    *,
    horizon: int,
    incumbent_model_spec_id: str,
    active_model_spec_id: str,
    chosen_model_spec_id: str | None,
    mean_losses: dict[str, object],
    primary_loss_name_by_model_spec: dict[str, str] | None = None,
) -> str | None:
    if (
        chosen_model_spec_id is not None
        and active_model_spec_id == chosen_model_spec_id
        and active_model_spec_id != incumbent_model_spec_id
    ):
        return incumbent_model_spec_id
    if chosen_model_spec_id is not None and chosen_model_spec_id != active_model_spec_id:
        return chosen_model_spec_id

    candidates: list[tuple[float, str]] = []
    for model_spec_id, loss_payload in mean_losses.items():
        if str(model_spec_id) == active_model_spec_id or not isinstance(loss_payload, dict):
            continue
        primary_loss_name = (
            (primary_loss_name_by_model_spec or {}).get(str(model_spec_id))
            or _resolve_primary_loss_name_for_model(str(model_spec_id), horizon=horizon)
        )
        loss_value = _float_or_none(loss_payload.get(primary_loss_name))
        if loss_value is None:
            continue
        candidates.append((loss_value, str(model_spec_id)))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


def load_alpha_promotion_summary(
    connection,
    *,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    promotion_rows = _load_latest_promotion_rows(connection, as_of_date=as_of_date)
    if promotion_rows.empty:
        return pd.DataFrame()
    active_rows = _load_active_alpha_registry_frame(connection, as_of_date=as_of_date)
    active_lookup = {
        int(row.horizon): row._asdict()
        for row in active_rows.itertuples(index=False)
    }

    summary_rows: list[dict[str, object]] = []
    for horizon, group in promotion_rows.groupby("horizon", sort=True):
        latest_promotion_date = pd.Timestamp(group["promotion_date"].max()).date()
        horizon_group = group.loc[
            pd.to_datetime(group["promotion_date"]).dt.date == latest_promotion_date
        ].copy()
        if horizon_group.empty:
            continue

        parsed_rows: list[dict[str, object]] = []
        for row in horizon_group.itertuples(index=False):
            detail_payload = _safe_json_load(row.detail_json)
            primary_loss_name = str(
                detail_payload.get("primary_loss_name")
                or _resolve_primary_loss_name_for_model(
                    str(row.challenger_model_spec_id),
                    horizon=int(horizon),
                )
            )
            if str(row.loss_name) != primary_loss_name:
                continue
            parsed_rows.append(
                {
                    "row": row,
                    "detail_payload": detail_payload,
                    "mean_losses": detail_payload.get("mean_losses", {}),
                    "chosen_model_spec_id": detail_payload.get("chosen_model_spec_id"),
                    "decision_reason": detail_payload.get("decision_reason"),
                    "superior_set": detail_payload.get("superior_set", []),
                    "primary_loss_name": primary_loss_name,
                    "primary_loss_name_by_model_spec": detail_payload.get(
                        "primary_loss_name_by_model_spec",
                        {},
                    ),
                }
            )

        if not parsed_rows:
            continue

        first = parsed_rows[0]
        incumbent_model_spec_id = str(first["row"].incumbent_model_spec_id)
        decision = str(first["row"].decision)
        chosen_model_spec_id = (
            str(first["chosen_model_spec_id"])
            if first["chosen_model_spec_id"] not in (None, "")
            else None
        )
        active_row = active_lookup.get(int(horizon), {})
        active_model_spec_id = str(
            active_row.get("active_model_spec_id") or incumbent_model_spec_id
        )
        mean_losses = (
            first["mean_losses"] if isinstance(first["mean_losses"], dict) else {}
        )
        primary_loss_name_by_model_spec = (
            first["primary_loss_name_by_model_spec"]
            if isinstance(first["primary_loss_name_by_model_spec"], dict)
            else {}
        )
        comparison_model_spec_id = _best_comparison_spec_id(
            horizon=int(horizon),
            incumbent_model_spec_id=incumbent_model_spec_id,
            active_model_spec_id=active_model_spec_id,
            chosen_model_spec_id=chosen_model_spec_id,
            mean_losses=mean_losses,
            primary_loss_name_by_model_spec=primary_loss_name_by_model_spec,
        )
        representative = next(
            (
                item
                for item in parsed_rows
                if str(item["row"].challenger_model_spec_id)
                == str(comparison_model_spec_id or chosen_model_spec_id or incumbent_model_spec_id)
            ),
            first,
        )
        active_losses = mean_losses.get(active_model_spec_id, {})
        comparison_losses = mean_losses.get(comparison_model_spec_id, {})
        active_primary_loss_name = (
            primary_loss_name_by_model_spec.get(active_model_spec_id)
            or _resolve_primary_loss_name_for_model(active_model_spec_id, horizon=int(horizon))
        )
        comparison_primary_loss_name = (
            primary_loss_name_by_model_spec.get(str(comparison_model_spec_id))
            or _resolve_primary_loss_name_for_model(
                str(comparison_model_spec_id) if comparison_model_spec_id is not None else None,
                horizon=int(horizon),
            )
        )
        if (
            comparison_model_spec_id == incumbent_model_spec_id
            and active_model_spec_id != incumbent_model_spec_id
        ):
            comparison_role_label = "prior incumbent"
        elif comparison_model_spec_id is None:
            comparison_role_label = "-"
        else:
            comparison_role_label = "best challenger"

        superior_set = [
            format_alpha_model_spec_id(str(model_spec_id))
            for model_spec_id in first["superior_set"]
        ]
        active_primary_return = _loss_to_return(
            active_losses.get(active_primary_loss_name) if isinstance(active_losses, dict) else None
        )
        comparison_primary_return = _loss_to_return(
            comparison_losses.get(comparison_primary_loss_name)
            if isinstance(comparison_losses, dict)
            else None
        )
        active_top10 = _loss_to_return(
            active_losses.get(PRIMARY_LOSS_NAME) if isinstance(active_losses, dict) else None
        )
        comparison_top10 = _loss_to_return(
            comparison_losses.get(PRIMARY_LOSS_NAME)
            if isinstance(comparison_losses, dict)
            else None
        )
        summary_rows.append(
            {
                "promotion_date": latest_promotion_date,
                "horizon": int(horizon),
                "summary_title": f"H{int(horizon)} {format_alpha_promotion_decision(decision)}",
                "decision": decision,
                "decision_label": format_alpha_promotion_decision(decision),
                "decision_reason": first["decision_reason"],
                "decision_reason_label": format_alpha_promotion_reason(first["decision_reason"]),
                "active_model_spec_id": active_model_spec_id,
                "active_model_label": format_alpha_model_spec_id(active_model_spec_id),
                "active_role_label": "active serving spec",
                "comparison_model_spec_id": comparison_model_spec_id,
                "comparison_model_label": format_alpha_model_spec_id(comparison_model_spec_id),
                "comparison_role_label": "legacy comparison baseline"
                if comparison_model_spec_id is not None
                else comparison_role_label,
                "incumbent_model_spec_id": incumbent_model_spec_id,
                "incumbent_model_label": format_alpha_model_spec_id(incumbent_model_spec_id),
                "chosen_model_spec_id": chosen_model_spec_id,
                "chosen_model_label": format_alpha_model_spec_id(chosen_model_spec_id),
                "fallback_model_spec_id": MODEL_SPEC_ID,
                "fallback_model_label": format_alpha_model_spec_id(MODEL_SPEC_ID),
                "fallback_role_label": "fallback baseline",
                "window_start": representative["row"].window_start,
                "window_end": representative["row"].window_end,
                "sample_count": int(representative["row"].sample_count or 0),
                "p_value": _float_or_none(representative["row"].p_value),
                "superior_set_label": ", ".join(superior_set) if superior_set else "-",
                "active_primary_loss_name": active_primary_loss_name,
                "comparison_primary_loss_name": comparison_primary_loss_name,
                "active_primary_mean_excess_return": active_primary_return,
                "comparison_primary_mean_excess_return": comparison_primary_return,
                "active_top10_mean_excess_return": active_top10,
                "comparison_top10_mean_excess_return": comparison_top10,
                "promotion_gap": (
                    None
                    if (
                        active_primary_return is None
                        or comparison_primary_return is None
                        or active_primary_loss_name != comparison_primary_loss_name
                    )
                    else comparison_primary_return - active_primary_return
                ),
                "active_top20_mean_excess_return": _loss_to_return(
                    active_losses.get("loss_top20") if isinstance(active_losses, dict) else None
                ),
                "comparison_top20_mean_excess_return": _loss_to_return(
                    comparison_losses.get("loss_top20")
                    if isinstance(comparison_losses, dict)
                    else None
                ),
                "active_point_loss": _float_or_none(
                    active_losses.get("loss_point") if isinstance(active_losses, dict) else None
                ),
                "comparison_point_loss": _float_or_none(
                    comparison_losses.get("loss_point")
                    if isinstance(comparison_losses, dict)
                    else None
                ),
                "active_rank_ic": _loss_to_rank_ic(
                    active_losses.get("loss_rank") if isinstance(active_losses, dict) else None
                ),
                "comparison_rank_ic": _loss_to_rank_ic(
                    comparison_losses.get("loss_rank")
                    if isinstance(comparison_losses, dict)
                    else None
                ),
                "active_effective_from_date": active_row.get("active_effective_from_date"),
                "active_source_type": active_row.get("active_source_type"),
                "active_promotion_type": active_row.get("active_promotion_type"),
            }
        )

    if not summary_rows:
        return pd.DataFrame()
    return pd.DataFrame(summary_rows).sort_values(
        ["promotion_date", "horizon"],
        ascending=[False, True],
    )


def upsert_alpha_promotion_tests(connection, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    connection.register("alpha_promotion_test_stage", frame)
    connection.execute(
        """
        DELETE FROM fact_alpha_promotion_test
        WHERE (
            promotion_date,
            horizon,
            incumbent_model_spec_id,
            challenger_model_spec_id,
            loss_name
        ) IN (
            SELECT
                promotion_date,
                horizon,
                incumbent_model_spec_id,
                challenger_model_spec_id,
                loss_name
            FROM alpha_promotion_test_stage
        )
        """
    )
    connection.execute(
        """
        INSERT INTO fact_alpha_promotion_test (
            promotion_date,
            horizon,
            incumbent_model_spec_id,
            challenger_model_spec_id,
            loss_name,
            window_start,
            window_end,
            sample_count,
            mcs_member_flag,
            incumbent_mcs_member_flag,
            p_value,
            decision,
            detail_json,
            created_at
        )
        SELECT
            promotion_date,
            horizon,
            incumbent_model_spec_id,
            challenger_model_spec_id,
            loss_name,
            window_start,
            window_end,
            sample_count,
            mcs_member_flag,
            incumbent_mcs_member_flag,
            p_value,
            decision,
            detail_json,
            created_at
        FROM alpha_promotion_test_stage
        """
    )
    connection.unregister("alpha_promotion_test_stage")


def _moving_block_bootstrap_indices(
    sample_count: int,
    *,
    block_length: int,
    rng: np.random.Generator,
) -> np.ndarray:
    indices: list[int] = []
    effective_block_length = max(1, min(int(block_length), sample_count))
    while len(indices) < sample_count:
        start = int(rng.integers(0, sample_count))
        indices.extend((start + offset) % sample_count for offset in range(effective_block_length))
    return np.asarray(indices[:sample_count], dtype=np.int64)


def _compute_mcs_tmax(
    loss_frame: pd.DataFrame,
    *,
    bootstrap_reps: int,
    block_length: int,
    seed: int,
) -> dict[str, object]:
    model_spec_ids = [str(column) for column in loss_frame.columns]
    values = loss_frame.to_numpy(dtype="float64", copy=True)
    sample_count, model_count = values.shape
    if sample_count < 2 or model_count < 2:
        return {
            "p_value": 1.0,
            "observed_tmax": 0.0,
            "elimination_scores": {model_spec_id: 0.0 for model_spec_id in model_spec_ids},
            "pairwise_t_stats": {},
        }

    pairwise_diffs = values[:, :, None] - values[:, None, :]
    mean_diffs = pairwise_diffs.mean(axis=0)
    rng = np.random.default_rng(seed)
    boot_mean_diffs = np.zeros((bootstrap_reps, model_count, model_count), dtype="float64")
    for bootstrap_index in range(bootstrap_reps):
        boot_indices = _moving_block_bootstrap_indices(
            sample_count,
            block_length=block_length,
            rng=rng,
        )
        boot_sample = values[boot_indices]
        boot_diffs = boot_sample[:, :, None] - boot_sample[:, None, :]
        boot_mean_diffs[bootstrap_index] = boot_diffs.mean(axis=0)

    centered_boot_diffs = boot_mean_diffs - mean_diffs
    scale = centered_boot_diffs.std(axis=0, ddof=1)
    scale[scale <= 1e-12] = np.nan

    observed_t = np.divide(
        mean_diffs,
        scale,
        out=np.zeros_like(mean_diffs, dtype="float64"),
        where=~np.isnan(scale),
    )
    centered_boot_t = np.divide(
        centered_boot_diffs,
        scale,
        out=np.zeros_like(centered_boot_diffs, dtype="float64"),
        where=~np.isnan(scale),
    )
    observed_tmax = float(np.abs(observed_t).max(initial=0.0))
    boot_tmax = np.abs(centered_boot_t).reshape(bootstrap_reps, -1).max(axis=1)
    p_value = float((1 + np.sum(boot_tmax >= observed_tmax)) / (bootstrap_reps + 1))

    elimination_scores: dict[str, float] = {}
    pairwise_t_stats: dict[str, dict[str, float]] = {}
    for row_index, model_spec_id in enumerate(model_spec_ids):
        row_stats = {
            other_spec_id: float(observed_t[row_index, col_index])
            for col_index, other_spec_id in enumerate(model_spec_ids)
            if col_index != row_index
        }
        pairwise_t_stats[model_spec_id] = row_stats
        elimination_scores[model_spec_id] = max(row_stats.values(), default=0.0)

    return {
        "p_value": p_value,
        "observed_tmax": observed_tmax,
        "elimination_scores": elimination_scores,
        "pairwise_t_stats": pairwise_t_stats,
    }


def _run_model_confidence_set(
    loss_frame: pd.DataFrame,
    *,
    alpha: float,
    bootstrap_reps: int,
    block_length: int,
    seed: int = 42,
) -> dict[str, object]:
    survivors = [str(column) for column in loss_frame.columns]
    history: list[dict[str, object]] = []
    latest_test = {
        "p_value": 1.0,
        "observed_tmax": 0.0,
        "elimination_scores": {},
        "pairwise_t_stats": {},
    }
    iteration = 0
    while len(survivors) > 1:
        current_loss_frame = loss_frame.loc[:, survivors].copy()
        latest_test = _compute_mcs_tmax(
            current_loss_frame,
            bootstrap_reps=bootstrap_reps,
            block_length=block_length,
            seed=seed + iteration,
        )
        history.append(
            {
                "survivors_before_test": list(survivors),
                "p_value": latest_test["p_value"],
                "observed_tmax": latest_test["observed_tmax"],
                "elimination_scores": latest_test["elimination_scores"],
            }
        )
        if float(latest_test["p_value"]) >= float(alpha):
            break
        worst_model = max(
            latest_test["elimination_scores"].items(),
            key=lambda item: (float(item[1]), item[0]),
        )[0]
        history[-1]["eliminated_model_spec_id"] = worst_model
        survivors.remove(worst_model)
        iteration += 1
    return {
        "superior_set": survivors,
        "history": history,
        "p_value": float(latest_test["p_value"]),
        "observed_tmax": float(latest_test["observed_tmax"]),
        "pairwise_t_stats": latest_test["pairwise_t_stats"],
    }


def _load_candidate_model_spec_ids(
    connection,
    *,
    as_of_date: date,
    horizon: int,
    incumbent_model_spec_id: str,
) -> list[str]:
    model_spec_ids = {
        str(spec["model_spec_id"])
        for spec in load_alpha_model_specs(
            connection,
            model_domain=MODEL_DOMAIN,
            active_only=True,
        )
        if load_latest_training_run(
            connection,
            horizon=int(horizon),
            model_version=MODEL_VERSION,
            train_end_date=as_of_date,
            model_domain=MODEL_DOMAIN,
            model_spec_id=str(spec["model_spec_id"]),
        )
        is not None
    }
    incumbent_training_run = load_latest_training_run(
        connection,
        horizon=int(horizon),
        model_version=MODEL_VERSION,
        train_end_date=as_of_date,
        model_domain=MODEL_DOMAIN,
        model_spec_id=incumbent_model_spec_id,
    )
    if incumbent_training_run is not None:
        model_spec_ids.add(incumbent_model_spec_id)
    return sorted(model_spec_ids)


def _validate_shadow_promotion_candidate(
    connection,
    *,
    selection_dates: list[date],
    horizon: int,
    model_spec_id: str,
) -> dict[str, object]:
    if not selection_dates:
        return {"ok": True, "reason": None, "checks": {}}

    placeholders = ",".join("?" for _ in selection_dates)
    common_params = [int(horizon), model_spec_id, *selection_dates]
    counts_row = connection.execute(
        f"""
        WITH pred AS (
            SELECT selection_date, symbol, training_run_id
            FROM fact_alpha_shadow_prediction
            WHERE horizon = ?
              AND model_spec_id = ?
              AND selection_date IN ({placeholders})
        ),
        rank_rows AS (
            SELECT selection_date, symbol, training_run_id
            FROM fact_alpha_shadow_ranking
            WHERE horizon = ?
              AND model_spec_id = ?
              AND selection_date IN ({placeholders})
        ),
        outcome_rows AS (
            SELECT selection_date, symbol, training_run_id
            FROM fact_alpha_shadow_selection_outcome
            WHERE horizon = ?
              AND model_spec_id = ?
              AND selection_date IN ({placeholders})
        )
        SELECT
            (SELECT COUNT(*) FROM pred) AS prediction_rows,
            (SELECT COUNT(*) FROM rank_rows) AS ranking_rows,
            (SELECT COUNT(*) FROM outcome_rows) AS outcome_rows,
            (
                SELECT COUNT(*)
                FROM outcome_rows AS outcome
                LEFT JOIN pred
                  ON outcome.selection_date = pred.selection_date
                 AND outcome.symbol = pred.symbol
                WHERE pred.symbol IS NULL
            ) AS missing_prediction_rows,
            (
                SELECT COUNT(*)
                FROM outcome_rows AS outcome
                LEFT JOIN rank_rows AS ranking
                  ON outcome.selection_date = ranking.selection_date
                 AND outcome.symbol = ranking.symbol
                WHERE ranking.symbol IS NULL
            ) AS missing_ranking_rows,
            (
                SELECT COUNT(*)
                FROM outcome_rows AS outcome
                JOIN pred
                  ON outcome.selection_date = pred.selection_date
                 AND outcome.symbol = pred.symbol
                JOIN rank_rows AS ranking
                  ON outcome.selection_date = ranking.selection_date
                 AND outcome.symbol = ranking.symbol
                WHERE COALESCE(outcome.training_run_id, '') <> COALESCE(pred.training_run_id, '')
                   OR COALESCE(outcome.training_run_id, '') <> COALESCE(ranking.training_run_id, '')
                   OR COALESCE(pred.training_run_id, '') <> COALESCE(ranking.training_run_id, '')
            ) AS lineage_mismatch_rows
        """,
        [
            *common_params,
            *common_params,
            *common_params,
        ],
    ).fetchone()
    (
        prediction_rows,
        ranking_rows,
        outcome_rows,
        missing_prediction_rows,
        missing_ranking_rows,
        lineage_mismatch_rows,
    ) = (int(value or 0) for value in counts_row or (0, 0, 0, 0, 0, 0))

    quality_row = connection.execute(
        f"""
        WITH quality AS (
            SELECT
                as_of_date,
                COUNT(DISTINCT feature_name) AS feature_count,
                SUM(CASE WHEN feature_value IS NULL THEN 1 ELSE 0 END) AS null_count
            FROM fact_feature_snapshot
            WHERE as_of_date IN ({placeholders})
              AND feature_name IN ({", ".join("?" for _ in REQUIRED_QUALITY_FEATURE_NAMES)})
            GROUP BY as_of_date
        )
        SELECT
            COUNT(*) AS observed_feature_dates,
            SUM(
                CASE
                    WHEN feature_count < {len(REQUIRED_QUALITY_FEATURE_NAMES)} OR null_count > 0
                    THEN 1
                    ELSE 0
                END
            ) AS bad_feature_dates
        FROM quality
        """,
        [*selection_dates, *REQUIRED_QUALITY_FEATURE_NAMES],
    ).fetchone()
    observed_feature_dates = int((quality_row or (0, 0))[0] or 0)
    bad_feature_dates = int((quality_row or (0, 0))[1] or 0)

    lineage_inputs_present = prediction_rows > 0 or ranking_rows > 0
    ok = True
    if lineage_inputs_present and (
        missing_prediction_rows > 0 or missing_ranking_rows > 0 or lineage_mismatch_rows > 0
    ):
        ok = False
    if observed_feature_dates > 0 and bad_feature_dates > 0:
        ok = False

    return {
        "ok": ok,
        "reason": None if ok else "shadow_validation_failed",
        "checks": {
            "prediction_rows": prediction_rows,
            "ranking_rows": ranking_rows,
            "outcome_rows": outcome_rows,
            "missing_prediction_rows": missing_prediction_rows,
            "missing_ranking_rows": missing_ranking_rows,
            "lineage_mismatch_rows": lineage_mismatch_rows,
            "observed_feature_dates": observed_feature_dates,
            "bad_feature_dates": bad_feature_dates,
        },
    }


def _load_promotion_loss_summary(
    connection,
    *,
    as_of_date: date,
    horizon: int,
    model_spec_ids: list[str],
) -> pd.DataFrame:
    if not model_spec_ids:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in model_spec_ids)
    frame = connection.execute(
        f"""
        SELECT
            selection_date,
            symbol,
            model_spec_id,
            final_selection_value,
            selection_percentile,
            realized_excess_return,
            prediction_error
        FROM fact_alpha_shadow_selection_outcome
        WHERE selection_date < ?
          AND horizon = ?
          AND outcome_status = 'matured'
          AND model_spec_id IN ({placeholders})
        ORDER BY selection_date, model_spec_id, final_selection_value DESC, symbol
        """,
        [as_of_date, int(horizon), *model_spec_ids],
    ).fetchdf()
    if frame.empty:
        return frame

    summary_rows: list[dict[str, object]] = []
    for (selection_date, model_spec_id), group in frame.groupby(
        ["selection_date", "model_spec_id"],
        sort=True,
    ):
        ordered = group.sort_values(
            ["final_selection_value", "symbol"],
            ascending=[False, True],
        )
        top5 = ordered.head(5)
        top10 = ordered.head(10)
        top20 = ordered.head(20)
        rank_ic = pd.to_numeric(
            ordered["selection_percentile"],
            errors="coerce",
        ).corr(pd.to_numeric(ordered["realized_excess_return"], errors="coerce"))
        summary_rows.append(
            {
                "selection_date": pd.Timestamp(selection_date).date(),
                "model_spec_id": str(model_spec_id),
                "loss_top5": -float(
                    pd.to_numeric(top5["realized_excess_return"], errors="coerce").mean()
                ),
                "loss_top10": -float(
                    pd.to_numeric(top10["realized_excess_return"], errors="coerce").mean()
                ),
                "loss_top20": -float(
                    pd.to_numeric(top20["realized_excess_return"], errors="coerce").mean()
                ),
                "loss_point": float(
                    pd.to_numeric(ordered["prediction_error"], errors="coerce").pow(2).mean()
                ),
                "loss_rank": (
                    None
                    if pd.isna(rank_ic)
                    else -float(rank_ic)
                ),
            }
        )
    return pd.DataFrame(summary_rows)


def _resolve_decision(
    *,
    incumbent_model_spec_id: str,
    superior_set: list[str],
) -> tuple[str, str | None, str]:
    if incumbent_model_spec_id in superior_set:
        return "KEEP_ACTIVE", incumbent_model_spec_id, "incumbent_in_superior_set"
    challenger_set = [
        model_spec_id
        for model_spec_id in superior_set
        if model_spec_id != incumbent_model_spec_id
    ]
    if len(challenger_set) == 1:
        return "PROMOTE_CHALLENGER", challenger_set[0], "single_challenger_survived"
    combo_candidates = [
        model_spec_id for model_spec_id in challenger_set if "combo" in model_spec_id
    ]
    if combo_candidates:
        return "PROMOTE_CHALLENGER", sorted(combo_candidates)[0], "combo_survived_in_superior_set"
    return "NO_AUTO_PROMOTION", None, "ambiguous_superior_set"


def run_alpha_auto_promotion(
    settings: Settings,
    *,
    as_of_date: date,
    horizons: list[int],
    lookback_selection_dates: int = PROMOTION_LOOKBACK_SELECTION_DATES,
    mcs_alpha: float = MCS_ALPHA,
    bootstrap_reps: int = MCS_BOOTSTRAP_REPS,
    block_length: int = MCS_BLOCK_LENGTH,
) -> AlphaPromotionResult:
    ensure_storage_layout(settings)
    target_horizons = list(dict.fromkeys(int(value) for value in horizons))
    with activate_run_context("run_alpha_auto_promotion", as_of_date=as_of_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_alpha_shadow_selection_outcome",
                    "fact_alpha_active_model",
                    "fact_model_training_run",
                    "dim_alpha_model_spec",
                ],
                notes=(
                    "Run alpha auto-promotion from trailing shadow self-backtest. "
                    f"as_of_date={as_of_date.isoformat()} horizons={target_horizons}"
                ),
            )
            try:
                promotion_rows: list[dict[str, object]] = []
                promoted_horizon_count = 0
                for horizon in target_horizons:
                    active_model = load_active_alpha_model(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                    )
                    incumbent_model_spec_id = (
                        str(active_model["model_spec_id"])
                        if active_model is not None
                        else MODEL_SPEC_ID
                    )
                    candidate_model_spec_ids = _load_candidate_model_spec_ids(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                        incumbent_model_spec_id=incumbent_model_spec_id,
                    )
                    primary_loss_name_by_model_spec = {
                        model_spec_id: _resolve_primary_loss_name_for_model(
                            model_spec_id,
                            horizon=int(horizon),
                        )
                        for model_spec_id in candidate_model_spec_ids
                    }
                    loss_summary = _load_promotion_loss_summary(
                        connection,
                        as_of_date=as_of_date,
                        horizon=int(horizon),
                        model_spec_ids=candidate_model_spec_ids,
                    )
                    if loss_summary.empty:
                        superior_set: list[str] = []
                        decision = "NO_AUTO_PROMOTION"
                        chosen_model_spec_id = None
                        decision_reason = "no_matured_shadow_history"
                        window_start = None
                        window_end = None
                        sample_count = 0
                        primary_p_value = None
                        mcs_payload: dict[str, object] = {
                            "superior_set": superior_set,
                            "history": [],
                            "primary_loss_name_by_model_spec": primary_loss_name_by_model_spec,
                        }
                    else:
                        loss_summary = loss_summary.copy()
                        loss_summary["primary_loss_name"] = loss_summary["model_spec_id"].map(
                            primary_loss_name_by_model_spec
                        )
                        primary_loss_name_lookup = dict(primary_loss_name_by_model_spec)

                        def primary_loss_for_row(
                            row: pd.Series,
                            *,
                            loss_name_lookup: dict[str, str] = primary_loss_name_lookup,
                        ) -> object:
                            loss_name = str(
                                row["primary_loss_name"]
                                or loss_name_lookup.get(
                                    str(row["model_spec_id"]),
                                    PRIMARY_LOSS_NAME,
                                )
                            )
                            return row[loss_name]

                        loss_summary["primary_loss"] = loss_summary.apply(
                            primary_loss_for_row,
                            axis=1,
                        )
                        primary_loss_frame = (
                            loss_summary.pivot(
                                index="selection_date",
                                columns="model_spec_id",
                                values="primary_loss",
                            )
                            .reindex(columns=candidate_model_spec_ids)
                            .dropna(axis=0, how="any")
                            .sort_index()
                        )
                        if primary_loss_frame.empty:
                            superior_set = []
                            decision = "NO_AUTO_PROMOTION"
                            chosen_model_spec_id = None
                            decision_reason = "no_complete_loss_matrix"
                            window_start = None
                            window_end = None
                            sample_count = 0
                            primary_p_value = None
                            mcs_payload = {
                                "superior_set": superior_set,
                                "history": [],
                                "primary_loss_name_by_model_spec": primary_loss_name_by_model_spec,
                            }
                        else:
                            primary_loss_frame = primary_loss_frame.tail(
                                int(lookback_selection_dates)
                            )
                            aligned_dates = primary_loss_frame.index.tolist()
                            aligned_summary = loss_summary.loc[
                                loss_summary["selection_date"].isin(aligned_dates)
                            ].copy()
                            mcs_payload = _run_model_confidence_set(
                                primary_loss_frame,
                                alpha=mcs_alpha,
                                bootstrap_reps=bootstrap_reps,
                                block_length=block_length,
                            )
                            superior_set = list(mcs_payload["superior_set"])
                            decision, chosen_model_spec_id, decision_reason = _resolve_decision(
                                incumbent_model_spec_id=incumbent_model_spec_id,
                                superior_set=superior_set,
                            )
                            window_start = min(aligned_dates)
                            window_end = max(aligned_dates)
                            sample_count = int(len(aligned_dates))
                            primary_p_value = float(mcs_payload["p_value"])
                            mean_losses: dict[str, dict[str, float | None]] = {}
                            for model_spec_id, model_group in aligned_summary.groupby(
                                "model_spec_id",
                                sort=True,
                            ):
                                model_spec_id_str = str(model_spec_id)
                                primary_loss_name = primary_loss_name_by_model_spec.get(
                                    model_spec_id_str,
                                    PRIMARY_LOSS_NAME,
                                )
                                mean_losses[model_spec_id_str] = {
                                    primary_loss_name: _float_or_none(
                                        model_group["primary_loss"].mean()
                                    ),
                                    PRIMARY_LOSS_NAME: _float_or_none(
                                        model_group[PRIMARY_LOSS_NAME].mean()
                                    ),
                                    "loss_top20": _float_or_none(
                                        model_group["loss_top20"].mean()
                                    ),
                                    "loss_point": _float_or_none(
                                        model_group["loss_point"].mean()
                                    ),
                                    "loss_rank": _float_or_none(
                                        model_group["loss_rank"].mean()
                                    ),
                                }
                            mcs_payload["mean_losses"] = mean_losses
                            mcs_payload["primary_loss_name_by_model_spec"] = (
                                primary_loss_name_by_model_spec
                            )
                            mcs_payload["window_start"] = window_start.isoformat()
                            mcs_payload["window_end"] = window_end.isoformat()
                            mcs_payload["sample_count"] = sample_count
                            mcs_payload["incumbent_model_spec_id"] = incumbent_model_spec_id
                            mcs_payload["decision_reason"] = decision_reason
                            mcs_payload["chosen_model_spec_id"] = chosen_model_spec_id
                            shadow_validation = None
                            if decision == "PROMOTE_CHALLENGER" and chosen_model_spec_id:
                                shadow_validation = _validate_shadow_promotion_candidate(
                                    connection,
                                    selection_dates=aligned_dates,
                                    horizon=int(horizon),
                                    model_spec_id=chosen_model_spec_id,
                                )
                                mcs_payload["shadow_validation"] = shadow_validation
                                if not bool(shadow_validation.get("ok")):
                                    decision = "NO_AUTO_PROMOTION"
                                    chosen_model_spec_id = None
                                    decision_reason = str(shadow_validation["reason"])
                                    mcs_payload["decision_reason"] = decision_reason
                                    mcs_payload["chosen_model_spec_id"] = None

                            should_freeze = False
                            freeze_source = "alpha_auto_promotion"
                            freeze_note = None
                            if decision == "PROMOTE_CHALLENGER" and chosen_model_spec_id:
                                should_freeze = True
                                freeze_note = (
                                    "Auto-promotion from trailing shadow self-backtest. "
                                    f"incumbent={incumbent_model_spec_id} "
                                    f"challenger={chosen_model_spec_id}"
                                )
                            elif active_model is None and decision == "KEEP_ACTIVE":
                                should_freeze = True
                                freeze_source = "alpha_auto_promotion_init"
                                freeze_note = (
                                    "Initialize active alpha registry from promotion engine. "
                                    f"model_spec_id={incumbent_model_spec_id}"
                                )
                            if should_freeze:
                                freeze_result = freeze_alpha_active_model(
                                    settings,
                                    as_of_date=as_of_date,
                                    source=freeze_source,
                                    note=freeze_note,
                                    horizons=[int(horizon)],
                                    model_spec_id=chosen_model_spec_id or incumbent_model_spec_id,
                                    train_end_date=as_of_date,
                                    promotion_type="AUTO_PROMOTION",
                                    promotion_report_json=mcs_payload,
                                )
                                promoted_horizon_count += int(freeze_result.row_count > 0)

                    challenger_ids = candidate_model_spec_ids or [incumbent_model_spec_id]
                    for challenger_model_spec_id in challenger_ids:
                        primary_loss_name = _resolve_primary_loss_name_for_model(
                            challenger_model_spec_id,
                            horizon=int(horizon),
                        )
                        challenger_losses = (
                            mcs_payload.get("mean_losses", {}).get(challenger_model_spec_id, {})
                            if isinstance(mcs_payload, dict)
                            else {}
                        )
                        incumbent_losses = (
                            mcs_payload.get("mean_losses", {}).get(incumbent_model_spec_id, {})
                            if isinstance(mcs_payload, dict)
                            else {}
                        )
                        for loss_name in _resolve_loss_names_for_model(
                            challenger_model_spec_id,
                            horizon=int(horizon),
                        ):
                            detail_payload = {
                                "superior_set": superior_set,
                                "chosen_model_spec_id": chosen_model_spec_id,
                                "decision_reason": decision_reason,
                                "primary_loss_name": primary_loss_name,
                                "primary_loss_name_by_model_spec": mcs_payload.get(
                                    "primary_loss_name_by_model_spec",
                                    {},
                                )
                                if isinstance(mcs_payload, dict)
                                else {},
                                "incumbent_mean_losses": incumbent_losses,
                                "challenger_mean_losses": challenger_losses,
                                "mcs_history": mcs_payload.get("history", [])
                                if isinstance(mcs_payload, dict)
                                else [],
                                "pairwise_t_stats": mcs_payload.get("pairwise_t_stats", {})
                                if isinstance(mcs_payload, dict)
                                else {},
                                "shadow_validation": mcs_payload.get("shadow_validation", {})
                                if isinstance(mcs_payload, dict)
                                else {},
                            }
                            promotion_rows.append(
                                {
                                    "promotion_date": as_of_date,
                                    "horizon": int(horizon),
                                    "incumbent_model_spec_id": incumbent_model_spec_id,
                                    "challenger_model_spec_id": challenger_model_spec_id,
                                    "loss_name": loss_name,
                                    "window_start": window_start,
                                    "window_end": window_end,
                                    "sample_count": int(sample_count),
                                    "mcs_member_flag": challenger_model_spec_id in superior_set,
                                    "incumbent_mcs_member_flag": incumbent_model_spec_id
                                    in superior_set,
                                    "p_value": primary_p_value
                                    if loss_name == primary_loss_name
                                    else None,
                                    "decision": decision,
                                    "detail_json": json.dumps(
                                        detail_payload,
                                        ensure_ascii=False,
                                        sort_keys=True,
                                    ),
                                    "created_at": pd.Timestamp.utcnow(),
                                }
                            )

                promotion_frame = pd.DataFrame(promotion_rows)
                upsert_alpha_promotion_tests(connection, promotion_frame)
                notes = (
                    "Alpha auto-promotion completed. "
                    f"as_of_date={as_of_date.isoformat()} rows={len(promotion_frame)} "
                    f"promoted_horizons={promoted_horizon_count}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=[],
                    notes=notes,
                    model_version=MODEL_VERSION,
                )
                return AlphaPromotionResult(
                    run_id=run_context.run_id,
                    as_of_date=as_of_date,
                    row_count=len(promotion_frame),
                    promoted_horizon_count=promoted_horizon_count,
                    artifact_paths=[],
                    notes=notes,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Alpha auto-promotion failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise
