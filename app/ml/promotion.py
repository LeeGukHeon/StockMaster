from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.ml.active import freeze_alpha_active_model
from app.ml.constants import (
    MCS_ALPHA,
    MCS_BLOCK_LENGTH,
    MCS_BOOTSTRAP_REPS,
    MODEL_DOMAIN,
    MODEL_SPEC_ID,
    MODEL_VERSION,
    PROMOTION_LOOKBACK_SELECTION_DATES,
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


@dataclass(slots=True)
class AlphaPromotionResult:
    run_id: str
    as_of_date: date
    row_count: int
    promoted_horizon_count: int
    artifact_paths: list[str]
    notes: str


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
                        }
                    else:
                        primary_loss_frame = (
                            loss_summary.pivot(
                                index="selection_date",
                                columns="model_spec_id",
                                values=PRIMARY_LOSS_NAME,
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
                            mean_losses = (
                                aligned_summary.groupby("model_spec_id", sort=True)
                                .agg(
                                    {
                                        PRIMARY_LOSS_NAME: "mean",
                                        "loss_top20": "mean",
                                        "loss_point": "mean",
                                        "loss_rank": "mean",
                                    }
                                )
                                .to_dict(orient="index")
                            )
                            mcs_payload["mean_losses"] = mean_losses
                            mcs_payload["window_start"] = window_start.isoformat()
                            mcs_payload["window_end"] = window_end.isoformat()
                            mcs_payload["sample_count"] = sample_count
                            mcs_payload["incumbent_model_spec_id"] = incumbent_model_spec_id
                            mcs_payload["decision_reason"] = decision_reason
                            mcs_payload["chosen_model_spec_id"] = chosen_model_spec_id

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
                        for loss_name in ALL_LOSS_NAMES:
                            detail_payload = {
                                "superior_set": superior_set,
                                "chosen_model_spec_id": chosen_model_spec_id,
                                "decision_reason": decision_reason,
                                "incumbent_mean_losses": incumbent_losses,
                                "challenger_mean_losses": challenger_losses,
                                "mcs_history": mcs_payload.get("history", [])
                                if isinstance(mcs_payload, dict)
                                else [],
                                "pairwise_t_stats": mcs_payload.get("pairwise_t_stats", {})
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
                                    if loss_name == PRIMARY_LOSS_NAME
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
