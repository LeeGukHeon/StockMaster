from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from app.features.constants import FEATURE_GROUP_BY_NAME, FEATURE_NAMES


def _winsorize(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty or len(valid) < 10:
        return numeric
    lower = valid.quantile(0.01)
    upper = valid.quantile(0.99)
    return numeric.clip(lower=lower, upper=upper)


def compute_group_rank_pct(
    frame: pd.DataFrame,
    *,
    column: str,
    group_column: str = "market",
) -> pd.Series:
    def rank_group(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        return numeric.rank(method="average", pct=True)

    return frame.groupby(group_column, dropna=False)[column].transform(rank_group)


def compute_group_zscore(
    frame: pd.DataFrame,
    *,
    column: str,
    group_column: str = "market",
) -> pd.Series:
    def zscore_group(series: pd.Series) -> pd.Series:
        numeric = _winsorize(series)
        std = numeric.std(ddof=0)
        if pd.isna(std) or std == 0:
            return pd.Series(
                [0.0 if pd.notna(value) else pd.NA for value in numeric],
                index=series.index,
            )
        mean = numeric.mean()
        return (numeric - mean) / std

    return frame.groupby(group_column, dropna=False)[column].transform(zscore_group)


def build_feature_snapshot_frame(
    feature_matrix: pd.DataFrame,
    *,
    as_of_date,
    run_id: str,
    source_version: str,
    feature_names: Iterable[str] = FEATURE_NAMES,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    created_at = pd.Timestamp.utcnow()
    for feature_name in feature_names:
        feature_values = pd.to_numeric(feature_matrix[feature_name], errors="coerce")
        rank_pct = compute_group_rank_pct(
            feature_matrix.assign(**{feature_name: feature_values}),
            column=feature_name,
        )
        zscore = compute_group_zscore(
            feature_matrix.assign(**{feature_name: feature_values}),
            column=feature_name,
        )
        for symbol, value, rank_value, zscore_value in zip(
            feature_matrix["symbol"].tolist(),
            feature_values.tolist(),
            rank_pct.tolist(),
            zscore.tolist(),
            strict=False,
        ):
            records.append(
                {
                    "run_id": run_id,
                    "as_of_date": as_of_date,
                    "symbol": symbol,
                    "feature_name": feature_name,
                    "feature_value": value,
                    "feature_group": FEATURE_GROUP_BY_NAME[feature_name],
                    "source_version": source_version,
                    "feature_rank_pct": rank_value,
                    "feature_zscore": zscore_value,
                    "is_imputed": False,
                    "notes_json": None,
                    "created_at": created_at,
                }
            )
    return pd.DataFrame.from_records(records)
