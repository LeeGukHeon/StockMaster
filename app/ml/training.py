from __future__ import annotations

import json
import math
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, ElasticNetCV
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.common.run_context import activate_run_context
from app.common.time import now_local
from app.features.constants import FEATURE_VERSION
from app.labels.forward_returns import LABEL_VERSION
from app.ml.constants import (
    ALPHA_CANDIDATE_MODEL_SPECS,
    CALIBRATION_BIN_COUNT,
    DEFAULT_TRAIN_ALPHA_CANDIDATE_MODEL_SPECS,
    MODEL_DOMAIN,
    MODEL_VERSION,
    SELECTION_ENGINE_VERSION,
    AlphaModelSpec,
    resolve_feature_columns_for_spec,
    resolve_member_names_for_spec,
    resolve_target_column_for_spec,
    resolve_training_target_variant_for_spec,
    supports_horizon_for_spec,
)
from app.ml.dataset import (
    TRAINING_FEATURE_COLUMNS,
    build_model_training_dataset,
    load_training_dataset,
)
from app.ml.registry import (
    clear_training_run_artifact_uris,
    upsert_alpha_model_specs,
    upsert_model_member_predictions,
    upsert_model_metric_summary,
    upsert_model_training_runs,
    write_model_artifact,
)
from app.settings import Settings
from app.storage.bootstrap import ensure_storage_layout
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.manifests import record_run_finish, record_run_start


@dataclass(slots=True)
class AlphaTrainingResult:
    run_id: str
    train_end_date: date
    row_count: int
    training_run_count: int
    artifact_paths: list[str]
    notes: str
    model_version: str


@dataclass(slots=True)
class AlphaOOFBackfillResult:
    run_id: str
    start_train_end_date: date
    end_train_end_date: date
    run_count: int
    artifact_paths: list[str]
    notes: str


@dataclass(slots=True)
class AlphaTrainingArtifactPruneResult:
    run_id: str
    pruned_artifact_uri_count: int
    removed_root_count: int
    removed_roots: list[str]
    notes: str


def _resolve_training_artifact_root(path: Path) -> Path:
    if path.name == "alpha_model_v1.pkl" and len(path.parents) >= 3:
        return path.parents[2]
    if path.name == "training_summary.json":
        return path.parent
    return path.parent


def _resolve_training_artifact_roots(artifact_paths: list[str]) -> list[Path]:
    roots = sorted(
        {_resolve_training_artifact_root(Path(artifact_path)) for artifact_path in artifact_paths},
        key=lambda value: len(value.parts),
    )
    deduped: list[Path] = []
    for root in roots:
        if any(existing == root or existing in root.parents for existing in deduped):
            continue
        deduped.append(root)
    return deduped


def prune_training_result_artifacts(
    settings: Settings,
    *,
    training_result: AlphaTrainingResult,
) -> AlphaTrainingArtifactPruneResult:
    removed_roots: list[str] = []
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        pruned_artifact_uri_count = clear_training_run_artifact_uris(
            connection,
            run_id=training_result.run_id,
        )

    for root in _resolve_training_artifact_roots(training_result.artifact_paths):
        if not root.exists():
            continue
        shutil.rmtree(root)
        removed_roots.append(str(root))

    notes = (
        "Alpha training artifacts pruned after shadow materialization. "
        f"run_id={training_result.run_id} "
        f"artifact_uris={pruned_artifact_uri_count} "
        f"removed_roots={len(removed_roots)}"
    )
    return AlphaTrainingArtifactPruneResult(
        run_id=training_result.run_id,
        pruned_artifact_uri_count=pruned_artifact_uri_count,
        removed_root_count=len(removed_roots),
        removed_roots=removed_roots,
        notes=notes,
    )


def _metric_rows(
    *,
    training_run_id: str,
    horizon: int,
    member_name: str,
    split_name: str,
    actual: pd.Series,
    predicted: pd.Series,
    as_of_dates: pd.Series | None = None,
) -> list[dict[str, object]]:
    pair = pd.DataFrame(
        {
            "actual": pd.to_numeric(actual, errors="coerce"),
            "predicted": pd.to_numeric(predicted, errors="coerce"),
        }
    )
    if as_of_dates is not None:
        pair["as_of_date"] = pd.to_datetime(as_of_dates, errors="coerce").dt.date
    pair = pair.dropna(subset=["actual", "predicted"])
    if pair.empty:
        values = {
            "mae": None,
            "rmse": None,
            "corr": None,
            "rank_ic": None,
            "directional_hit_rate": None,
            "top5_mean_excess_return": None,
            "top10_mean_excess_return": None,
            "top20_mean_excess_return": None,
        }
    else:
        if (
            len(pair) < 2
            or float(pair["actual"].std(ddof=0) or 0.0) == 0.0
            or float(pair["predicted"].std(ddof=0) or 0.0) == 0.0
        ):
            corr = None
        else:
            corr = pair["actual"].corr(pair["predicted"])
        cohort_rank_ics: list[float] = []

        if "as_of_date" in pair.columns and pair["as_of_date"].notna().any():
            cohort_top5_returns: list[float] = []
            cohort_top10_returns: list[float] = []
            cohort_top20_returns: list[float] = []
            for _, group in pair.dropna(subset=["as_of_date"]).groupby("as_of_date", sort=True):
                ordered = group.sort_values("predicted", ascending=False)
                top5 = ordered.head(min(5, len(ordered)))
                top10 = ordered.head(min(10, len(ordered)))
                top20 = ordered.head(min(20, len(ordered)))
                cohort_top5_returns.append(float(top5["actual"].mean()))
                cohort_top10_returns.append(float(top10["actual"].mean()))
                cohort_top20_returns.append(float(top20["actual"].mean()))
                if len(group) >= 2:
                    group_actual_rank = group["actual"].rank(method="average")
                    group_predicted_rank = group["predicted"].rank(method="average")
                    group_rank_ic = group_actual_rank.corr(group_predicted_rank)
                    if not pd.isna(group_rank_ic):
                        cohort_rank_ics.append(float(group_rank_ic))
            top5_mean_excess_return = (
                float(np.mean(cohort_top5_returns)) if cohort_top5_returns else None
            )
            top10_mean_excess_return = (
                float(np.mean(cohort_top10_returns)) if cohort_top10_returns else None
            )
            top20_mean_excess_return = (
                float(np.mean(cohort_top20_returns)) if cohort_top20_returns else None
            )
            rank_ic = float(np.mean(cohort_rank_ics)) if cohort_rank_ics else None
        else:
            ordered = pair.sort_values("predicted", ascending=False)
            top5 = ordered.head(min(5, len(ordered)))
            top10 = ordered.head(min(10, len(ordered)))
            top20 = ordered.head(min(20, len(ordered)))
            top5_mean_excess_return = float(top5["actual"].mean())
            top10_mean_excess_return = float(top10["actual"].mean())
            top20_mean_excess_return = float(top20["actual"].mean())
            actual_rank = pair["actual"].rank(method="average")
            predicted_rank = pair["predicted"].rank(method="average")
            rank_ic = actual_rank.corr(predicted_rank) if len(pair) >= 2 else None
        values = {
            "mae": float(mean_absolute_error(pair["actual"], pair["predicted"])),
            "rmse": float(math.sqrt(mean_squared_error(pair["actual"], pair["predicted"]))),
            "corr": None if pd.isna(corr) else float(corr),
            "rank_ic": None if pd.isna(rank_ic) else float(rank_ic),
            "directional_hit_rate": float(
                (np.sign(pair["actual"]) == np.sign(pair["predicted"])).mean()
            ),
            "top5_mean_excess_return": top5_mean_excess_return,
            "top10_mean_excess_return": top10_mean_excess_return,
            "top20_mean_excess_return": top20_mean_excess_return,
        }
    created_at = pd.Timestamp.utcnow()
    return [
        {
            "training_run_id": training_run_id,
            "model_version": MODEL_VERSION,
            "horizon": int(horizon),
            "member_name": member_name,
            "split_name": split_name,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "sample_count": int(len(pair)),
            "created_at": created_at,
        }
        for metric_name, metric_value in values.items()
    ]


def _resolve_date_split(
    dates: list[date],
    *,
    validation_days: int,
    min_train_days: int,
) -> tuple[list[date], list[date], bool, str | None]:
    if not dates:
        return [], [], True, "no_training_dates"

    unique_dates = sorted(dict.fromkeys(dates))
    if len(unique_dates) == 1:
        return unique_dates, unique_dates, True, "single_training_date"

    effective_validation_days = min(
        max(1, int(validation_days)),
        max(1, len(unique_dates) // 3),
    )
    validation_dates = unique_dates[-effective_validation_days:]
    train_dates = unique_dates[:-effective_validation_days]
    fallback_reasons: list[str] = []
    if len(train_dates) < 1:
        train_dates = unique_dates[:-1]
        validation_dates = unique_dates[-1:]
        fallback_reasons.append("narrow_history_split")
    if len(train_dates) < int(min_train_days):
        fallback_reasons.append("insufficient_train_days")
    return train_dates, validation_dates, bool(fallback_reasons), ",".join(fallback_reasons) or None


def _make_elasticnet_model(train_dates: list[date]) -> Pipeline:
    if len(train_dates) >= 3:
        n_splits = min(3, len(train_dates) - 1)
        estimator = ElasticNetCV(
            l1_ratio=[0.1, 0.5, 0.9],
            alphas=[0.001, 0.01, 0.1, 1.0],
            cv=TimeSeriesSplit(n_splits=n_splits),
            random_state=42,
            max_iter=5000,
        )
    else:
        estimator = ElasticNet(alpha=0.01, l1_ratio=0.5, random_state=42, max_iter=5000)
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", estimator),
        ]
    )


def _make_model_builders(train_dates: list[date]) -> dict[str, Any]:
    return {
        "elasticnet": _make_elasticnet_model(train_dates),
        "hist_gbm": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        max_depth=4,
                        learning_rate=0.05,
                        max_iter=200,
                        min_samples_leaf=20,
                        random_state=42,
                    ),
                ),
            ]
        ),
        "extra_trees": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    ExtraTreesRegressor(
                        n_estimators=200,
                        min_samples_leaf=4,
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }


def _select_model_builders(
    train_dates: list[date],
    *,
    member_names: tuple[str, ...],
) -> dict[str, Any]:
    all_builders = _make_model_builders(train_dates)
    selected = {
        member_name: all_builders[member_name]
        for member_name in member_names
        if member_name in all_builders
    }
    return selected or all_builders


def _normalise_weights(
    metrics: dict[str, dict[str, float | None]],
    *,
    model_spec: AlphaModelSpec | None = None,
) -> dict[str, float]:
    if not metrics:
        return {}

    training_target_variant = (
        resolve_training_target_variant_for_spec(model_spec) if model_spec is not None else None
    )

    if training_target_variant == "top20_weighted":
        metric_weights = {
            "top10_mean_excess_return": 6.0,
            "top20_mean_excess_return": 5.0,
            "top5_mean_excess_return": 2.0,
            "rank_ic": 1.0,
            "corr": 0.5,
            "mae": 0.5,
        }
    elif training_target_variant == "top5_binary":
        metric_weights = {
            "top5_mean_excess_return": 6.0,
            "top10_mean_excess_return": 4.0,
            "top20_mean_excess_return": 2.0,
            "rank_ic": 1.5,
            "corr": 1.0,
            "mae": 1.0,
        }
    elif training_target_variant == "buyable_top5":
        metric_weights = {
            "top10_mean_excess_return": 5.0,
            "top20_mean_excess_return": 4.0,
            "top5_mean_excess_return": 3.0,
            "rank_ic": 2.0,
            "corr": 1.0,
            "mae": 1.5,
        }
    elif training_target_variant in {
        "practical_excess_return",
        "practical_excess_return_v2",
        "practical_path_return_v3",
        "stable_practical_excess_return",
        "robust_buyable_excess_return",
    }:
        metric_weights = {
            "top5_mean_excess_return": 6.0,
            "top10_mean_excess_return": 4.0,
            "top20_mean_excess_return": 2.0,
            "rank_ic": 2.0,
            "corr": 1.0,
            "mae": 1.0,
        }
    else:
        metric_weights = {
            "top5_mean_excess_return": 6.0,
            "top10_mean_excess_return": 4.0,
            "top20_mean_excess_return": 2.0,
            "rank_ic": 2.0,
            "corr": 1.0,
            "mae": 1.0,
        }
    lower_is_better = {"mae"}

    score_frame = pd.DataFrame.from_dict(metrics, orient="index")
    raw_weights = {member_name: 0.0 for member_name in metrics}

    for metric_name, importance in metric_weights.items():
        if metric_name not in score_frame.columns:
            continue
        values = pd.to_numeric(score_frame[metric_name], errors="coerce")
        available = values.dropna()
        if available.empty:
            continue
        comparable = -available if metric_name in lower_is_better else available
        ranked = comparable.rank(method="average", pct=True)
        for member_name, ranked_value in ranked.items():
            raw_weights[str(member_name)] += float(ranked_value) * float(importance)

    positive_total = sum(value for value in raw_weights.values() if value > 0)
    if positive_total <= 0:
        equal_weight = 1.0 / len(raw_weights) if raw_weights else 0.0
        return {key: equal_weight for key in raw_weights}
    return {key: value / positive_total for key, value in raw_weights.items()}


def _calibration_payload(validation_frame: pd.DataFrame) -> list[dict[str, object]]:
    if validation_frame.empty:
        return []
    working = validation_frame.copy()
    working["predicted"] = pd.to_numeric(working["predicted"], errors="coerce")
    working["actual"] = pd.to_numeric(working["actual"], errors="coerce")
    working = working.dropna(subset=["predicted", "actual"])
    if working.empty:
        return []
    residual = working["actual"] - working["predicted"]
    bin_count = min(CALIBRATION_BIN_COUNT, max(1, len(working) // 20))
    if bin_count <= 1:
        return [
            {
                "bucket": "global",
                "prediction_lower": None,
                "prediction_upper": None,
                "residual_q25": float(residual.quantile(0.25)),
                "residual_median": float(residual.quantile(0.50)),
                "residual_q75": float(residual.quantile(0.75)),
                "expected_abs_error": float(residual.abs().mean()),
                "sample_count": int(len(working)),
            }
        ]

    bucket_codes, bucket_edges = pd.qcut(
        working["predicted"],
        q=bin_count,
        labels=False,
        retbins=True,
        duplicates="drop",
    )
    working["bucket_code"] = bucket_codes.astype("Int64")
    payload: list[dict[str, object]] = []
    for bucket_code, group in working.groupby("bucket_code", sort=True):
        if pd.isna(bucket_code):
            continue
        bucket_index = int(bucket_code)
        group_residual = group["actual"] - group["predicted"]
        payload.append(
            {
                "bucket": f"bucket_{bucket_index + 1:02d}",
                "prediction_lower": float(bucket_edges[bucket_index]),
                "prediction_upper": float(bucket_edges[bucket_index + 1]),
                "residual_q25": float(group_residual.quantile(0.25)),
                "residual_median": float(group_residual.quantile(0.50)),
                "residual_q75": float(group_residual.quantile(0.75)),
                "expected_abs_error": float(group_residual.abs().mean()),
                "sample_count": int(len(group)),
            }
        )
    if not payload:
        return _calibration_payload(working[["predicted", "actual"]])
    return payload


def _prepare_feature_frame(
    dataset: pd.DataFrame,
    *,
    horizon: int,
    model_spec: AlphaModelSpec,
) -> pd.DataFrame:
    working = dataset.copy()
    target_column = resolve_target_column_for_spec(model_spec, horizon=horizon)
    if target_column in working.columns:
        working = working.rename(columns={target_column: "target"})
    else:
        working["target"] = pd.NA
    working = working.dropna(subset=["target"]).copy()
    if working.empty:
        return working
    for column in TRAINING_FEATURE_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    working["as_of_date"] = pd.to_datetime(working["as_of_date"]).dt.date
    return working.sort_values(["as_of_date", "symbol"]).reset_index(drop=True)


def _filter_training_frame_for_spec(
    frame: pd.DataFrame,
    *,
    model_spec: AlphaModelSpec,
    validation_days: int,
) -> pd.DataFrame:
    if model_spec.rolling_window_days is None:
        return frame
    if frame.empty:
        return frame
    unique_dates = sorted(dict.fromkeys(frame["as_of_date"].tolist()))
    if not unique_dates:
        return frame
    total_window_days = int(model_spec.rolling_window_days) + max(1, int(validation_days))
    retained_dates = set(unique_dates[-total_window_days:])
    return frame.loc[frame["as_of_date"].isin(retained_dates)].copy()


def _model_spec_registry_row(model_spec: AlphaModelSpec) -> dict[str, object]:
    member_names = list(resolve_member_names_for_spec(model_spec))
    feature_columns = list(resolve_feature_columns_for_spec(model_spec))
    return {
        "model_spec_id": model_spec.model_spec_id,
        "model_domain": MODEL_DOMAIN,
        "model_version": MODEL_VERSION,
        "estimation_scheme": model_spec.estimation_scheme,
        "rolling_window_days": model_spec.rolling_window_days,
        "feature_version": FEATURE_VERSION,
        "label_version": LABEL_VERSION,
        "selection_engine_version": SELECTION_ENGINE_VERSION,
        "spec_payload_json": json.dumps(
            {
                "member_names": member_names,
                "feature_groups": list(model_spec.feature_groups or ()),
                "feature_columns": feature_columns,
                "target_variant": model_spec.target_variant,
                "training_target_variant": resolve_training_target_variant_for_spec(model_spec),
                "validation_primary_metric_name": model_spec.validation_primary_metric_name,
                "promotion_primary_loss_name": model_spec.promotion_primary_loss_name,
                "allowed_horizons": list(model_spec.allowed_horizons or ()),
                "lifecycle_role": model_spec.lifecycle_role,
                "lifecycle_fallback_flag": bool(model_spec.lifecycle_fallback_flag),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "active_candidate_flag": bool(model_spec.active_candidate_flag),
        "created_at": pd.Timestamp.utcnow(),
        "updated_at": pd.Timestamp.utcnow(),
    }


def build_alpha_model_spec_registry_frame(
    model_specs: tuple[AlphaModelSpec, ...] = ALPHA_CANDIDATE_MODEL_SPECS,
) -> pd.DataFrame:
    return pd.DataFrame([_model_spec_registry_row(spec) for spec in model_specs])


def _train_single_horizon(
    dataset: pd.DataFrame,
    *,
    run_id: str,
    train_end_date: date,
    horizon: int,
    min_train_days: int,
    validation_days: int,
    artifact_root: Path,
    model_spec: AlphaModelSpec,
) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, Path | None]:
    training_frame = _prepare_feature_frame(dataset, horizon=horizon, model_spec=model_spec)
    training_frame = _filter_training_frame_for_spec(
        training_frame,
        model_spec=model_spec,
        validation_days=validation_days,
    )
    training_run_id = f"{run_id}-{model_spec.model_spec_id}-h{int(horizon)}"
    if training_frame.empty:
        spec_feature_columns = list(resolve_feature_columns_for_spec(model_spec))
        spec_member_names = list(resolve_member_names_for_spec(model_spec))
        row = {
            "training_run_id": training_run_id,
            "run_id": run_id,
            "model_domain": MODEL_DOMAIN,
            "model_version": MODEL_VERSION,
            "model_spec_id": model_spec.model_spec_id,
            "estimation_scheme": model_spec.estimation_scheme,
            "rolling_window_days": model_spec.rolling_window_days,
            "horizon": int(horizon),
            "train_end_date": train_end_date,
            "training_window_start": None,
            "training_window_end": None,
            "validation_window_start": None,
            "validation_window_end": None,
            "train_row_count": 0,
            "validation_row_count": 0,
            "feature_count": len(spec_feature_columns),
            "ensemble_weight_json": json.dumps({}, ensure_ascii=False),
            "model_family_json": json.dumps(
                {
                    "members": spec_member_names,
                    "feature_groups": list(model_spec.feature_groups or ()),
                    "target_variant": model_spec.target_variant,
                    "training_target_variant": resolve_training_target_variant_for_spec(model_spec),
                },
                ensure_ascii=False,
            ),
            "fallback_flag": True,
            "fallback_reason": "empty_dataset",
            "artifact_uri": None,
            "notes": "No labeled rows were available.",
            "status": "success",
            "created_at": pd.Timestamp.utcnow(),
        }
        return row, pd.DataFrame(), pd.DataFrame(), None

    unique_dates = training_frame["as_of_date"].tolist()
    train_dates, validation_dates, fallback_flag, fallback_reason = _resolve_date_split(
        unique_dates,
        validation_days=validation_days,
        min_train_days=min_train_days,
    )

    train_mask = training_frame["as_of_date"].isin(train_dates)
    validation_mask = training_frame["as_of_date"].isin(validation_dates)
    train_frame = training_frame.loc[train_mask].copy()
    validation_frame = training_frame.loc[validation_mask].copy()
    if train_frame.empty:
        train_frame = training_frame.copy()
        validation_frame = training_frame.copy()
        fallback_flag = True
        fallback_reason = ",".join(
            value for value in [fallback_reason, "train_validation_overlap"] if value
        )

    active_feature_columns = [
        column
        for column in resolve_feature_columns_for_spec(model_spec)
        if column in train_frame.columns and train_frame[column].notna().any()
    ]
    if not active_feature_columns:
        active_feature_columns = list(resolve_feature_columns_for_spec(model_spec))

    X_train = train_frame[active_feature_columns].apply(pd.to_numeric, errors="coerce")
    y_train = pd.to_numeric(train_frame["target"], errors="coerce")
    X_validation = validation_frame[active_feature_columns].apply(
        pd.to_numeric,
        errors="coerce",
    )
    y_validation = pd.to_numeric(validation_frame["target"], errors="coerce")
    final_fit_frame = training_frame.copy()
    X_final_fit = final_fit_frame[active_feature_columns].apply(pd.to_numeric, errors="coerce")
    y_final_fit = pd.to_numeric(final_fit_frame["target"], errors="coerce")

    model_builders = _select_model_builders(
        sorted(dict.fromkeys(train_dates)),
        member_names=resolve_member_names_for_spec(model_spec),
    )
    artifact_payload: dict[str, Any] = {
        "model_version": MODEL_VERSION,
        "training_run_id": training_run_id,
        "horizon": int(horizon),
        "train_end_date": train_end_date.isoformat(),
        "feature_columns": active_feature_columns,
        "member_order": list(model_builders.keys()),
        "ensemble_weights": {},
        "fallback_flag": bool(fallback_flag),
        "fallback_reason": fallback_reason,
        "validation_holdout_refit_flag": True,
        "final_fit_window_start": (
            min(final_fit_frame["as_of_date"]).isoformat() if not final_fit_frame.empty else None
        ),
        "final_fit_window_end": (
            max(final_fit_frame["as_of_date"]).isoformat() if not final_fit_frame.empty else None
        ),
        "final_fit_row_count": int(len(final_fit_frame)),
        "calibration": [],
        "members": {},
    }

    member_prediction_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    validation_predictions: dict[str, pd.Series] = {}
    metric_summary: dict[str, dict[str, float | None]] = {}
    for member_name, model in model_builders.items():
        model.fit(X_train, y_train)
        train_pred = pd.Series(model.predict(X_train), index=train_frame.index, dtype="float64")
        valid_pred = pd.Series(
            model.predict(X_validation),
            index=validation_frame.index,
            dtype="float64",
        )
        validation_predictions[member_name] = valid_pred
        metric_rows.extend(
            _metric_rows(
                training_run_id=training_run_id,
                horizon=horizon,
                member_name=member_name,
                split_name="train",
                actual=y_train,
                predicted=train_pred,
                as_of_dates=train_frame["as_of_date"],
            )
        )
        metric_rows.extend(
            _metric_rows(
                training_run_id=training_run_id,
                horizon=horizon,
                member_name=member_name,
                split_name="validation",
                actual=y_validation,
                predicted=valid_pred,
                as_of_dates=validation_frame["as_of_date"],
            )
        )
        validation_metric_subset = {
            row["metric_name"]: row["metric_value"]
            for row in metric_rows
            if row["member_name"] == member_name and row["split_name"] == "validation"
        }
        metric_summary[member_name] = {
            "mae": validation_metric_subset.get("mae"),
            "corr": validation_metric_subset.get("corr"),
            "rank_ic": validation_metric_subset.get("rank_ic"),
            "top5_mean_excess_return": validation_metric_subset.get("top5_mean_excess_return"),
            "top10_mean_excess_return": validation_metric_subset.get("top10_mean_excess_return"),
            "top20_mean_excess_return": validation_metric_subset.get("top20_mean_excess_return"),
        }
        member_prediction_rows.extend(
            {
                "training_run_id": training_run_id,
                "as_of_date": row.as_of_date,
                "symbol": row.symbol,
                "horizon": int(horizon),
                "model_version": MODEL_VERSION,
                "prediction_role": "validation",
                "member_name": member_name,
                "predicted_excess_return": float(valid_pred.loc[index]),
                "actual_excess_return": float(row.target),
                "residual": float(row.target - valid_pred.loc[index]),
                "fallback_flag": bool(fallback_flag),
                "fallback_reason": fallback_reason,
                "created_at": pd.Timestamp.utcnow(),
            }
            for index, row in validation_frame.assign(target=y_validation).iterrows()
        )

    ensemble_weights = _normalise_weights(metric_summary, model_spec=model_spec)
    artifact_payload["ensemble_weights"] = ensemble_weights
    if validation_predictions:
        ensemble_validation = sum(
            validation_predictions[member_name] * weight
            for member_name, weight in ensemble_weights.items()
        )
    else:
        ensemble_validation = pd.Series(dtype="float64")
    metric_rows.extend(
        _metric_rows(
            training_run_id=training_run_id,
            horizon=horizon,
            member_name="ensemble",
            split_name="validation",
            actual=y_validation,
            predicted=ensemble_validation,
            as_of_dates=validation_frame["as_of_date"],
        )
    )
    member_prediction_rows.extend(
        {
            "training_run_id": training_run_id,
            "as_of_date": row.as_of_date,
            "symbol": row.symbol,
            "horizon": int(horizon),
            "model_version": MODEL_VERSION,
            "prediction_role": "validation",
            "member_name": "ensemble",
            "predicted_excess_return": float(ensemble_validation.loc[index]),
            "actual_excess_return": float(row.target),
            "residual": float(row.target - ensemble_validation.loc[index]),
            "fallback_flag": bool(fallback_flag),
            "fallback_reason": fallback_reason,
            "created_at": pd.Timestamp.utcnow(),
        }
        for index, row in validation_frame.assign(target=y_validation).iterrows()
    )
    artifact_payload["calibration"] = _calibration_payload(
        pd.DataFrame(
            {
                "predicted": ensemble_validation,
                "actual": y_validation,
            }
        )
    )
    artifact_payload["model_domain"] = MODEL_DOMAIN
    artifact_payload["model_spec_id"] = model_spec.model_spec_id
    artifact_payload["estimation_scheme"] = model_spec.estimation_scheme
    artifact_payload["rolling_window_days"] = model_spec.rolling_window_days
    artifact_payload["target_variant"] = model_spec.target_variant
    artifact_payload["training_target_variant"] = resolve_training_target_variant_for_spec(
        model_spec
    )
    for member_name, model in model_builders.items():
        model.fit(X_final_fit, y_final_fit)
        artifact_payload["members"][member_name] = model

    artifact_path = (
        artifact_root
        / f"model_spec_id={model_spec.model_spec_id}"
        / f"horizon={int(horizon)}"
        / "alpha_model_v1.pkl"
    )
    write_model_artifact(artifact_path, artifact_payload)
    training_run_row = {
        "training_run_id": training_run_id,
        "run_id": run_id,
        "model_domain": MODEL_DOMAIN,
        "model_version": MODEL_VERSION,
        "model_spec_id": model_spec.model_spec_id,
        "estimation_scheme": model_spec.estimation_scheme,
        "rolling_window_days": model_spec.rolling_window_days,
        "horizon": int(horizon),
        "train_end_date": train_end_date,
        "training_window_start": min(train_dates) if train_dates else None,
        "training_window_end": max(train_dates) if train_dates else None,
        "validation_window_start": min(validation_dates) if validation_dates else None,
        "validation_window_end": max(validation_dates) if validation_dates else None,
        "train_row_count": int(len(train_frame)),
        "validation_row_count": int(len(validation_frame)),
        "feature_count": len(active_feature_columns),
        "ensemble_weight_json": json.dumps(ensemble_weights, ensure_ascii=False, sort_keys=True),
        "model_family_json": json.dumps(
            {
                "members": list(model_builders.keys()),
                "feature_groups": list(model_spec.feature_groups or ()),
                "target_variant": model_spec.target_variant,
                "training_target_variant": resolve_training_target_variant_for_spec(model_spec),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "fallback_flag": bool(fallback_flag),
        "fallback_reason": fallback_reason,
        "artifact_uri": str(artifact_path),
        "notes": (
            f"model_spec_id={model_spec.model_spec_id} "
            f"train_rows={len(train_frame)} validation_rows={len(validation_frame)} "
            f"final_fit_rows={len(final_fit_frame)} "
            f"fallback={bool(fallback_flag)}"
        ),
        "status": "success",
        "created_at": pd.Timestamp.utcnow(),
    }
    return (
        training_run_row,
        pd.DataFrame(member_prediction_rows),
        pd.DataFrame(metric_rows),
        artifact_path,
    )


def train_alpha_model_v1(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    min_train_days: int,
    validation_days: int,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> AlphaTrainingResult:
    return _train_alpha_specs(
        settings,
        train_end_date=train_end_date,
        horizons=horizons,
        min_train_days=min_train_days,
        validation_days=validation_days,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
        model_specs=(ALPHA_CANDIDATE_MODEL_SPECS[0],),
        run_type="train_alpha_model_v1",
        note_prefix="Train sklearn alpha model v1.",
    )


def train_alpha_candidate_models(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    min_train_days: int,
    validation_days: int,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
    model_specs: tuple[AlphaModelSpec, ...] = DEFAULT_TRAIN_ALPHA_CANDIDATE_MODEL_SPECS,
) -> AlphaTrainingResult:
    return _train_alpha_specs(
        settings,
        train_end_date=train_end_date,
        horizons=horizons,
        min_train_days=min_train_days,
        validation_days=validation_days,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
        model_specs=model_specs,
        run_type="train_alpha_candidate_models",
        note_prefix="Train alpha challenger candidate models.",
    )


def _train_alpha_specs(
    settings: Settings,
    *,
    train_end_date: date,
    horizons: list[int],
    min_train_days: int,
    validation_days: int,
    symbols: list[str] | None,
    limit_symbols: int | None,
    market: str,
    model_specs: tuple[AlphaModelSpec, ...],
    run_type: str,
    note_prefix: str,
) -> AlphaTrainingResult:
    ensure_storage_layout(settings)
    build_model_training_dataset(
        settings,
        train_end_date=train_end_date,
        horizons=horizons,
        min_train_days=min_train_days,
        symbols=symbols,
        limit_symbols=limit_symbols,
        market=market,
    )

    with activate_run_context(run_type, as_of_date=train_end_date) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=[
                    "fact_feature_snapshot",
                    "fact_forward_return_label",
                    "fact_model_training_run",
                    "dim_alpha_model_spec",
                ],
                notes=(
                    f"{note_prefix} "
                    f"train_end_date={train_end_date.isoformat()} horizons={horizons} "
                    f"model_specs={[spec.model_spec_id for spec in model_specs]}"
                ),
            )
            try:
                dataset = load_training_dataset(
                    connection,
                    train_end_date=train_end_date,
                    horizons=horizons,
                    symbols=symbols,
                    limit_symbols=limit_symbols,
                    market=market,
                )
                if dataset.empty:
                    notes = (
                        "No supervised dataset rows were available for alpha training. "
                        f"train_end_date={train_end_date.isoformat()}"
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
                    return AlphaTrainingResult(
                        run_id=run_context.run_id,
                        train_end_date=train_end_date,
                        row_count=0,
                        training_run_count=0,
                        artifact_paths=[],
                        notes=notes,
                        model_version=MODEL_VERSION,
                    )

                artifact_root = (
                    settings.paths.artifacts_dir
                    / "models"
                    / MODEL_VERSION
                    / f"train_end_date={train_end_date.isoformat()}"
                    / run_context.run_id
                )
                artifact_root.mkdir(parents=True, exist_ok=True)
                training_run_rows: list[dict[str, object]] = []
                member_prediction_frames: list[pd.DataFrame] = []
                metric_frames: list[pd.DataFrame] = []
                artifact_paths: list[str] = []
                upsert_alpha_model_specs(
                    connection,
                    build_alpha_model_spec_registry_frame(model_specs),
                )
                for model_spec in model_specs:
                    for horizon in horizons:
                        if not supports_horizon_for_spec(model_spec, horizon=int(horizon)):
                            continue
                        (
                            training_run_row,
                            member_predictions,
                            metric_summary,
                            artifact_path,
                        ) = _train_single_horizon(
                            dataset,
                            run_id=run_context.run_id,
                            train_end_date=train_end_date,
                            horizon=int(horizon),
                            min_train_days=min_train_days,
                            validation_days=validation_days,
                            artifact_root=artifact_root,
                            model_spec=model_spec,
                        )
                        training_run_rows.append(training_run_row)
                        if not member_predictions.empty:
                            member_prediction_frames.append(member_predictions)
                        if not metric_summary.empty:
                            metric_frames.append(metric_summary)
                        if artifact_path is not None:
                            artifact_paths.append(str(artifact_path))

                upsert_model_training_runs(connection, pd.DataFrame(training_run_rows))
                if member_prediction_frames:
                    upsert_model_member_predictions(
                        connection,
                        pd.concat(member_prediction_frames, ignore_index=True),
                    )
                if metric_frames:
                    upsert_model_metric_summary(
                        connection,
                        pd.concat(metric_frames, ignore_index=True),
                    )

                summary_frame = pd.DataFrame(training_run_rows)
                summary_artifact = artifact_root / "training_summary.json"
                summary_artifact.write_text(
                    summary_frame.to_json(
                        orient="records", force_ascii=False, indent=2, date_format="iso"
                    ),
                    encoding="utf-8",
                )
                artifact_paths.append(str(summary_artifact))
                notes = (
                    f"{note_prefix} completed. "
                    f"dataset_rows={len(dataset)} "
                    f"training_runs={len(training_run_rows)} "
                    f"model_specs={len(model_specs)}"
                )
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="success",
                    output_artifacts=artifact_paths,
                    notes=notes,
                    model_version=MODEL_VERSION,
                )
                return AlphaTrainingResult(
                    run_id=run_context.run_id,
                    train_end_date=train_end_date,
                    row_count=len(dataset),
                    training_run_count=len(training_run_rows),
                    artifact_paths=artifact_paths,
                    notes=notes,
                    model_version=MODEL_VERSION,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Alpha model v1 training failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise


def _resolve_backfill_dates(
    connection,
    *,
    start_train_end_date: date,
    end_train_end_date: date,
) -> list[date]:
    rows = connection.execute(
        """
        SELECT DISTINCT as_of_date
        FROM fact_feature_snapshot
        WHERE as_of_date BETWEEN ? AND ?
        ORDER BY as_of_date
        """,
        [start_train_end_date, end_train_end_date],
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def backfill_alpha_oof_predictions(
    settings: Settings,
    *,
    start_train_end_date: date,
    end_train_end_date: date,
    horizons: list[int],
    min_train_days: int,
    validation_days: int,
    limit_models: int | None = None,
    symbols: list[str] | None = None,
    limit_symbols: int | None = None,
    market: str = "ALL",
) -> AlphaOOFBackfillResult:
    ensure_storage_layout(settings)
    with activate_run_context(
        "backfill_alpha_oof_predictions", as_of_date=end_train_end_date
    ) as run_context:
        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            record_run_start(
                connection,
                run_id=run_context.run_id,
                run_type=run_context.run_type,
                started_at=run_context.started_at,
                as_of_date=run_context.as_of_date,
                input_sources=["fact_feature_snapshot", "fact_forward_return_label"],
                notes=(
                    "Backfill time-aware validation predictions for alpha model v1. "
                    f"range={start_train_end_date.isoformat()}..{end_train_end_date.isoformat()}"
                ),
            )
            try:
                backfill_dates = _resolve_backfill_dates(
                    connection,
                    start_train_end_date=start_train_end_date,
                    end_train_end_date=end_train_end_date,
                )
            except Exception as exc:
                record_run_finish(
                    connection,
                    run_id=run_context.run_id,
                    finished_at=now_local(settings.app.timezone),
                    status="failed",
                    output_artifacts=[],
                    notes="Alpha OOF backfill failed.",
                    error_message=str(exc),
                    model_version=MODEL_VERSION,
                )
                raise

        if limit_models is not None and limit_models > 0:
            backfill_dates = backfill_dates[-int(limit_models) :]

        artifact_paths: list[str] = []
        for train_end in backfill_dates:
            result = train_alpha_model_v1(
                settings,
                train_end_date=train_end,
                horizons=horizons,
                min_train_days=min_train_days,
                validation_days=validation_days,
                symbols=symbols,
                limit_symbols=limit_symbols,
                market=market,
            )
            artifact_paths.extend(result.artifact_paths)

        with duckdb_connection(settings.paths.duckdb_path) as connection:
            bootstrap_core_tables(connection)
            notes = f"Alpha OOF backfill completed. train_end_dates={len(backfill_dates)}"
            record_run_finish(
                connection,
                run_id=run_context.run_id,
                finished_at=now_local(settings.app.timezone),
                status="success",
                output_artifacts=artifact_paths,
                notes=notes,
                model_version=MODEL_VERSION,
            )
        return AlphaOOFBackfillResult(
            run_id=run_context.run_id,
            start_train_end_date=start_train_end_date,
            end_train_end_date=end_train_end_date,
            run_count=len(backfill_dates),
            artifact_paths=artifact_paths,
            notes=notes,
        )
