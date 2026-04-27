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

D5_LTR_CANDIDATE_POOLS: tuple[str, ...] = (
    "full",
    "stable_buyable_v1",
    "stable_buyable_strict",
)


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


def _numeric_feature_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _date_market_rank(
    frame: pd.DataFrame,
    column: str,
    *,
    ascending: bool = True,
) -> pd.Series:
    values = _numeric_feature_series(frame, column)
    return values.groupby([frame["as_of_date"], frame["market"]]).rank(
        method="average",
        pct=True,
        ascending=ascending,
    )


def stable_buyable_candidate_pool_mask(
    frame: pd.DataFrame,
    *,
    candidate_pool: str = "full",
) -> pd.Series:
    """Return the point-in-time D5 LTR candidate-pool mask.

    ``full`` preserves the original smoke-test universe. ``stable_buyable_v1`` removes
    hard buyability blockers that the stable D5 utility target already penalizes:
    thin liquidity, severe recent drawdown/loss, stale or low-confidence data. The
    stricter variant also removes high-volatility and late-crowded names for diagnostic
    experiments, but neither mode changes active runtime selection.
    """

    if candidate_pool not in D5_LTR_CANDIDATE_POOLS:
        raise ValueError(f"Unsupported candidate_pool: {candidate_pool}")
    if candidate_pool == "full":
        return pd.Series(True, index=frame.index, dtype=bool)
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)

    liquidity_rank = _numeric_feature_series(frame, "liquidity_rank_pct")
    adv_rank = _date_market_rank(frame, "adv_20", ascending=True)
    vol_rank = _date_market_rank(frame, "realized_vol_20d", ascending=True)
    day_range_rank = _date_market_rank(frame, "hl_range_1d", ascending=True)
    drawdown_rank = _date_market_rank(frame, "drawdown_20d", ascending=True)
    max_loss_rank = _date_market_rank(frame, "max_loss_20d", ascending=True)
    crowding_rank = _date_market_rank(frame, "dist_from_20d_high", ascending=True)
    turnover_burst_rank = _date_market_rank(frame, "volume_ratio_1d_vs_20d", ascending=True)
    missing_count = _numeric_feature_series(frame, "missing_key_feature_count").fillna(99.0)
    data_confidence = _numeric_feature_series(frame, "data_confidence_score").fillna(0.0)
    stale_price = _numeric_feature_series(frame, "stale_price_flag").fillna(1.0)

    thin_liquidity = liquidity_rank.le(0.12).fillna(True) | adv_rank.le(0.12).fillna(True)
    large_drawdown = drawdown_rank.le(0.12).fillna(True) | max_loss_rank.le(0.12).fillna(True)
    data_missing = missing_count.ge(2.0) | data_confidence.lt(65.0) | stale_price.gt(0.0)
    hard_blocker = thin_liquidity | large_drawdown | data_missing
    if candidate_pool == "stable_buyable_v1":
        return ~hard_blocker

    high_volatility = vol_rank.ge(0.88).fillna(True) | day_range_rank.ge(0.90).fillna(True)
    late_crowding = crowding_rank.ge(0.90).fillna(False) & turnover_burst_rank.ge(
        0.85
    ).fillna(False)
    return ~(hard_blocker | high_volatility | late_crowding)


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
    relevance_column: str = "stable_d5_utility_relevance",
    candidate_pool: str = "full",
) -> tuple[pd.DataFrame, list[str]]:
    frame = add_query_group_key(dataset, horizon=int(horizon), query_group_key=query_group_key)
    pool_mask = stable_buyable_candidate_pool_mask(frame, candidate_pool=candidate_pool)
    frame = frame.loc[pool_mask].copy()
    frame = add_stable_d5_utility_relevance(
        frame,
        target_column=target_column,
        relevance_column=relevance_column,
    )
    selected_features = list(feature_columns or default_d5_ltr_feature_columns(frame))
    if not selected_features:
        raise ValueError("No usable numeric feature columns for LTR")
    stable_target_column = f"target_stable_practical_excess_h{int(horizon)}"
    keep = [
        "as_of_date",
        "symbol",
        "company_name",
        "market",
        "horizon",
        "query_group_key",
        stable_target_column,
        target_column,
        "target_h5" if int(horizon) == 5 and "target_h5" in frame.columns else target_column,
        relevance_column,
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


def mean_ndcg_at_k(
    group: pd.DataFrame,
    *,
    score_column: str,
    relevance_column: str = "stable_d5_utility_relevance",
    k: int = 5,
) -> float | None:
    if group.empty:
        return None
    values: list[float] = []
    for _, query in group.groupby("query_group_key", sort=False):
        labels = pd.to_numeric(query[relevance_column], errors="coerce").fillna(0.0)
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
    portfolio_group_key: str = "as_of_date+market",
    portfolio_score_mode: str = "raw",
    stable_utility_column: str | None = None,
    relevance_target_column: str | None = None,
) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    if portfolio_group_key == "as_of_date":
        group_columns = ["as_of_date"]
    elif portfolio_group_key == "as_of_date+market":
        group_columns = ["as_of_date", "market"]
    else:
        raise ValueError(f"Unsupported portfolio_group_key: {portfolio_group_key}")
    if portfolio_score_mode not in {"raw", "query_rank_pct"}:
        raise ValueError(f"Unsupported portfolio_score_mode: {portfolio_score_mode}")
    if portfolio_group_key == "as_of_date" and portfolio_score_mode == "raw":
        raise ValueError(
            "Raw LTR rank scores are query-relative; use portfolio_score_mode=query_rank_pct "
            "for cross-market daily baskets."
        )
    scored = predictions.copy()
    portfolio_score_column = score_column
    if portfolio_score_mode == "query_rank_pct":
        portfolio_score_column = "__portfolio_score"
        scored[portfolio_score_column] = pd.to_numeric(
            scored[score_column],
            errors="coerce",
        ).groupby(scored["query_group_key"]).rank(method="average", pct=True)
    target_column = f"target_h{int(horizon)}"
    stable_utility_column = stable_utility_column or (
        f"target_stable_practical_excess_h{int(horizon)}"
    )
    if stable_utility_column not in predictions.columns:
        stable_utility_column = relevance_target_column or target_column
    relevance_target_column = relevance_target_column or stable_utility_column
    rows: list[dict[str, object]] = []
    for group_key, group in scored.groupby(group_columns, sort=True):
        if portfolio_group_key == "as_of_date":
            as_of_date = group_key[0] if isinstance(group_key, tuple) else group_key
            market = "ALL"
        else:
            as_of_date, market = group_key
        ordered = group.sort_values([portfolio_score_column, "symbol"], ascending=[False, True])
        for top_n in top_ns:
            top = ordered.head(int(top_n))
            stable_utility = pd.to_numeric(
                top[stable_utility_column],
                errors="coerce",
            )
            relevance_target = pd.to_numeric(
                top[relevance_target_column],
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
                    "avg_relevance_target": float(relevance_target.mean()),
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
