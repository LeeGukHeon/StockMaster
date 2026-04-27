from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Iterable

import numpy as np
import pandas as pd

from app.ml.constants import get_alpha_model_spec, resolve_feature_columns_for_spec

D5_LTR_CONTRACT: dict[str, str] = {
    "objective_family": "ltr_shadow_rank",
    "score_semantics": "relative_rank_score_only",
    "query_group_key": "as_of_date+horizon+market",
    "relevance_label": "stable_d5_utility_relevance",
    "eval_metric": "ndcg@5",
}


@dataclass(frozen=True, slots=True)
class LtrFold:
    fold_id: str
    train_start_date: date
    train_end_date: date
    validation_start_date: date
    validation_end_date: date
    train_date_count: int
    validation_date_count: int
    purge_days: int
    embargo_days: int

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key in (
            "train_start_date",
            "train_end_date",
            "validation_start_date",
            "validation_end_date",
        ):
            payload[key] = payload[key].isoformat()
        return payload


def default_d5_ltr_feature_columns(frame: pd.DataFrame) -> list[str]:
    spec = get_alpha_model_spec("alpha_stable_buyable_d5_v1")
    return [
        column
        for column in resolve_feature_columns_for_spec(spec)
        if column in frame.columns and pd.to_numeric(frame[column], errors="coerce").notna().any()
    ]


def add_query_group_key(
    frame: pd.DataFrame,
    *,
    horizon: int,
    query_group_key: str = "as_of_date+horizon+market",
) -> pd.DataFrame:
    if query_group_key != "as_of_date+horizon+market":
        raise ValueError(f"Unsupported query_group_key: {query_group_key}")
    result = frame.copy()
    result["as_of_date"] = pd.to_datetime(result["as_of_date"]).dt.date
    result["horizon"] = int(horizon)
    result["market"] = result.get("market", "UNKNOWN").fillna("UNKNOWN").astype(str)
    result["query_group_key"] = (
        result["as_of_date"].map(lambda value: value.isoformat())
        + "|h"
        + str(int(horizon))
        + "|"
        + result["market"]
    )
    return result


def add_stable_d5_utility_relevance(
    frame: pd.DataFrame,
    *,
    target_column: str = "target_stable_practical_excess_h5",
    relevance_column: str = "stable_d5_utility_relevance",
) -> pd.DataFrame:
    """Add ordinal D5 LTR relevance from stable practical utility.

    Per as_of_date+horizon+market group, positive-utility rows are sorted descending.
    Relevance bins are frozen by the approved plan: top5=4, ranks 6-10=3,
    ranks 11-20=2, remaining positive=1, nonpositive/missing=0.
    """

    if target_column not in frame.columns:
        raise KeyError(f"Missing target column: {target_column}")
    result = frame.copy()
    if "query_group_key" not in result.columns:
        if "horizon" not in result.columns:
            raise KeyError("Missing horizon/query_group_key for relevance grouping")
        result = add_query_group_key(result, horizon=int(result["horizon"].iloc[0]))
    result[relevance_column] = 0
    target = pd.to_numeric(result[target_column], errors="coerce")
    for _, group in result.groupby("query_group_key", sort=False):
        positive = group.loc[target.reindex(group.index).gt(0.0).fillna(False)].copy()
        if positive.empty:
            continue
        ordered = positive.assign(_target=target.reindex(positive.index)).sort_values(
            ["_target", "symbol"],
            ascending=[False, True],
        )
        for rank, index in enumerate(ordered.index, start=1):
            if rank <= 5:
                relevance = 4
            elif rank <= 10:
                relevance = 3
            elif rank <= 20:
                relevance = 2
            else:
                relevance = 1
            result.loc[index, relevance_column] = relevance
    result[relevance_column] = result[relevance_column].astype(int)
    return result


def prepare_ltr_frame(
    dataset: pd.DataFrame,
    *,
    horizon: int,
    feature_columns: Iterable[str] | None = None,
    query_group_key: str = "as_of_date+horizon+market",
    target_column: str = "target_stable_practical_excess_h5",
) -> tuple[pd.DataFrame, list[str]]:
    frame = add_query_group_key(dataset, horizon=int(horizon), query_group_key=query_group_key)
    frame = add_stable_d5_utility_relevance(frame, target_column=target_column)
    selected_features = list(feature_columns or default_d5_ltr_feature_columns(frame))
    if not selected_features:
        raise ValueError("No usable numeric feature columns for LTR")
    keep = [
        "as_of_date",
        "symbol",
        "company_name",
        "market",
        "horizon",
        "query_group_key",
        target_column,
        "target_h5" if int(horizon) == 5 and "target_h5" in frame.columns else target_column,
        "stable_d5_utility_relevance",
        *selected_features,
    ]
    keep = list(dict.fromkeys(column for column in keep if column in frame.columns))
    prepared = frame[keep].copy().sort_values(["as_of_date", "market", "symbol"])
    return prepared.reset_index(drop=True), selected_features


def group_sizes(frame: pd.DataFrame, *, group_column: str = "query_group_key") -> list[int]:
    if frame.empty:
        return []
    return [int(size) for size in frame.groupby(group_column, sort=False).size().tolist()]


def build_temporal_folds(
    dates: Iterable[date],
    *,
    fold_count: int = 3,
    purge_days: int = 5,
    embargo_days: int = 5,
    min_train_dates: int = 20,
) -> list[LtrFold]:
    ordered_dates = sorted(dict.fromkeys(pd.Timestamp(value).date() for value in dates))
    if len(ordered_dates) <= min_train_dates + 1:
        return []
    candidate_validation_dates = ordered_dates[min_train_dates:]
    fold_count = max(1, min(int(fold_count), len(candidate_validation_dates)))
    blocks = np.array_split(np.array(candidate_validation_dates, dtype=object), fold_count)
    folds: list[LtrFold] = []
    for fold_index, block in enumerate(blocks, start=1):
        validation_dates = [pd.Timestamp(value).date() for value in block.tolist()]
        if not validation_dates:
            continue
        validation_start = min(validation_dates)
        validation_end = max(validation_dates)
        train_cutoff = validation_start - timedelta(days=int(purge_days))
        train_dates = [value for value in ordered_dates if value < train_cutoff]
        if len(train_dates) < min_train_dates:
            continue
        folds.append(
            LtrFold(
                fold_id=f"fold_{fold_index:02d}",
                train_start_date=min(train_dates),
                train_end_date=max(train_dates),
                validation_start_date=validation_start,
                validation_end_date=validation_end,
                train_date_count=len(train_dates),
                validation_date_count=len(validation_dates),
                purge_days=int(purge_days),
                embargo_days=int(embargo_days),
            )
        )
    return folds


def clean_feature_matrix(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    matrix = frame[feature_columns].apply(pd.to_numeric, errors="coerce")
    medians = matrix.median(numeric_only=True).replace([np.inf, -np.inf], np.nan)
    return matrix.replace([np.inf, -np.inf], np.nan).fillna(medians).fillna(0.0)


def mean_ndcg_at_k(group: pd.DataFrame, *, score_column: str, k: int = 5) -> float | None:
    if group.empty:
        return None
    values: list[float] = []
    for _, query in group.groupby("query_group_key", sort=False):
        labels = pd.to_numeric(query["stable_d5_utility_relevance"], errors="coerce").fillna(0.0)
        if labels.sum() <= 0.0:
            continue
        scores = pd.to_numeric(query[score_column], errors="coerce").fillna(float("-inf"))
        order = np.argsort(-scores.to_numpy(dtype=float))[: int(k)]
        ideal = np.argsort(-labels.to_numpy(dtype=float))[: int(k)]
        gains = (np.power(2.0, labels.to_numpy(dtype=float)) - 1.0)
        discounts = 1.0 / np.log2(np.arange(2, len(order) + 2))
        dcg = float(np.sum(gains[order] * discounts))
        idcg = float(np.sum(gains[ideal] * discounts))
        if idcg > 0.0:
            values.append(dcg / idcg)
    if not values:
        return None
    return float(np.mean(values))


def topn_by_rank_score(
    predictions: pd.DataFrame,
    *,
    top_ns: Iterable[int] = (3, 5),
    score_column: str = "rank_score",
    horizon: int = 5,
) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    target_column = f"target_h{int(horizon)}"
    rows: list[dict[str, object]] = []
    for (as_of_date, market), group in predictions.groupby(["as_of_date", "market"], sort=True):
        ordered = group.sort_values([score_column, "symbol"], ascending=[False, True])
        for top_n in top_ns:
            top = ordered.head(int(top_n))
            stable_utility = pd.to_numeric(
                top["target_stable_practical_excess_h5"],
                errors="coerce",
            )
            realized = (
                pd.to_numeric(top[target_column], errors="coerce")
                if target_column in top.columns
                else stable_utility
            )
            rows.append(
                {
                    "as_of_date": pd.Timestamp(as_of_date).date().isoformat(),
                    "market": str(market),
                    "top_n": int(top_n),
                    "n_names": int(len(top)),
                    "avg_stable_utility": float(stable_utility.mean()),
                    "avg_excess_return": float(realized.mean()),
                    "hit_stable_utility": float(stable_utility.gt(0.0).mean()),
                    "symbols": ",".join(top["symbol"].astype(str).tolist()),
                }
            )
    return pd.DataFrame(rows)


def summarize_topn(topn_frame: pd.DataFrame) -> pd.DataFrame:
    if topn_frame.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for top_n, group in topn_frame.groupby("top_n", sort=True):
        stable = pd.to_numeric(group["avg_stable_utility"], errors="coerce")
        excess = pd.to_numeric(group["avg_excess_return"], errors="coerce")
        rows.append(
            {
                "top_n": int(top_n),
                "dates": int(group["as_of_date"].nunique()),
                "avg_stable_utility": float(stable.mean()),
                "median_stable_utility": float(stable.median()),
                "p10_stable_utility": float(stable.quantile(0.10)),
                "hit_stable_utility": float(stable.gt(0.0).mean()),
                "avg_excess_return": float(excess.mean()),
            }
        )
    return pd.DataFrame(rows)
