from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd


GENERIC_OUTLOOK_LABELS = {"미분류", "기술성장기업", "지주/스팩"}


def sector_outlook_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of_date: date,
    ranking_version: str,
    prediction_version: str,
    horizon: int = 5,
    candidate_limit: int = 40,
    limit: int = 3,
) -> pd.DataFrame:
    ranked = connection.execute(
        """
        SELECT
            ranking.as_of_date AS selection_date,
            ranking.symbol,
            symbol.company_name,
            symbol.sector,
            symbol.industry,
            ranking.final_selection_value,
            prediction.expected_excess_return
        FROM fact_ranking AS ranking
        JOIN dim_symbol AS symbol
          ON ranking.symbol = symbol.symbol
        LEFT JOIN fact_prediction AS prediction
          ON ranking.as_of_date = prediction.as_of_date
         AND ranking.symbol = prediction.symbol
         AND ranking.horizon = prediction.horizon
         AND prediction.prediction_version = ?
         AND prediction.ranking_version = ranking.ranking_version
        WHERE ranking.as_of_date = ?
          AND ranking.horizon = ?
          AND ranking.ranking_version = ?
        ORDER BY ranking.final_selection_value DESC, ranking.symbol
        LIMIT ?
        """,
        [prediction_version, as_of_date, horizon, ranking_version, int(candidate_limit)],
    ).fetchdf()
    if ranked.empty:
        return pd.DataFrame()

    ranked = ranked.reset_index(drop=True)
    ranked["overall_rank"] = ranked.index + 1
    ranked["rank_weight"] = 1.0 / ranked["overall_rank"].astype(float)
    ranked["sector"] = ranked["sector"].replace("", pd.NA)
    ranked["industry"] = ranked["industry"].replace("", pd.NA)
    ranked["outlook_label"] = ranked["industry"].combine_first(ranked["sector"])
    ranked["broad_sector"] = ranked["sector"].combine_first(ranked["industry"])
    ranked = ranked.loc[ranked["outlook_label"].notna()].copy()
    ranked = ranked.loc[~ranked["outlook_label"].isin(GENERIC_OUTLOOK_LABELS)].copy()
    if ranked.empty:
        return pd.DataFrame()

    grouped = (
        ranked.groupby(["selection_date", "outlook_label", "broad_sector"], dropna=False)
        .agg(
            symbol_count=("symbol", "count"),
            top10_count=("overall_rank", lambda series: int((series <= 10).sum())),
            avg_expected_excess_return=("expected_excess_return", "mean"),
            avg_final_selection_value=("final_selection_value", "mean"),
            rank_weight_sum=("rank_weight", "sum"),
        )
        .reset_index()
    )
    sample_map = (
        ranked.sort_values(["outlook_label", "overall_rank", "symbol"])
        .groupby("outlook_label")["company_name"]
        .apply(lambda series: ", ".join(series.head(3).astype(str)))
        .to_dict()
    )
    grouped["sample_symbols"] = grouped["outlook_label"].map(sample_map)
    grouped["outlook_score"] = (
        grouped["rank_weight_sum"] * 100.0
        + grouped["top10_count"] * 5.0
        + grouped["avg_expected_excess_return"].fillna(0.0) * 1000.0
    )
    grouped = grouped.sort_values(
        ["outlook_score", "avg_expected_excess_return", "symbol_count", "outlook_label"],
        ascending=[False, False, False, True],
    )
    return grouped.head(int(limit)).reset_index(drop=True)
