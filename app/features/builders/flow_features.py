from __future__ import annotations

from datetime import date

import pandas as pd


def _safe_ratio(numerator, denominator):
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return pd.NA
    return float(numerator) / float(denominator)


def build_flow_feature_frame(
    flow_history: pd.DataFrame,
    *,
    ohlcv_history: pd.DataFrame,
    as_of_date: date,
) -> pd.DataFrame:
    if flow_history.empty:
        return pd.DataFrame(columns=["symbol"])

    flow = flow_history.copy()
    flow["symbol"] = flow["symbol"].astype(str).str.zfill(6)
    flow["trading_date"] = pd.to_datetime(flow["trading_date"]).dt.date
    for column in (
        "foreign_net_value",
        "institution_net_value",
        "individual_net_value",
    ):
        flow[column] = pd.to_numeric(flow[column], errors="coerce")

    price = ohlcv_history.copy()
    if price.empty:
        return pd.DataFrame(columns=["symbol"])
    price["symbol"] = price["symbol"].astype(str).str.zfill(6)
    price["trading_date"] = pd.to_datetime(price["trading_date"]).dt.date
    price["turnover_effective"] = pd.to_numeric(
        price["turnover_value"],
        errors="coerce",
    ).fillna(
        pd.to_numeric(price["close"], errors="coerce")
        * pd.to_numeric(price["volume"], errors="coerce")
    )

    rows: list[dict[str, object]] = []
    for symbol, group in flow.groupby("symbol", sort=False):
        group = group.loc[group["trading_date"] <= as_of_date].sort_values("trading_date")
        price_group = price.loc[
            (price["symbol"] == symbol) & (price["trading_date"] <= as_of_date)
        ].sort_values("trading_date")
        latest_flow = group.loc[group["trading_date"] == as_of_date]
        if latest_flow.empty:
            rows.append(
                {
                    "symbol": symbol,
                    "foreign_net_value_ratio_1d": pd.NA,
                    "foreign_net_value_ratio_5d": pd.NA,
                    "institution_net_value_ratio_5d": pd.NA,
                    "individual_net_value_ratio_5d": pd.NA,
                    "smart_money_flow_ratio_5d": pd.NA,
                    "smart_money_flow_ratio_20d": pd.NA,
                    "flow_alignment_score": pd.NA,
                    "flow_coverage_flag": 0.0,
                }
            )
            continue

        latest_row = latest_flow.tail(1).iloc[0]
        trailing_5 = group.tail(5)
        trailing_20 = group.tail(20)
        price_1d = price_group.loc[price_group["trading_date"] == as_of_date, "turnover_effective"]
        price_5d = price_group.tail(5)["turnover_effective"]
        price_20d = price_group.tail(20)["turnover_effective"]
        turnover_1d = float(price_1d.iloc[-1]) if not price_1d.empty else pd.NA
        turnover_5d = price_5d.sum(min_count=1) if not price_5d.empty else pd.NA
        turnover_20d = price_20d.sum(min_count=1) if not price_20d.empty else pd.NA

        foreign_5d = trailing_5["foreign_net_value"].sum(min_count=1)
        institution_5d = trailing_5["institution_net_value"].sum(min_count=1)
        individual_5d = trailing_5["individual_net_value"].sum(min_count=1)
        smart_money_5d = foreign_5d + institution_5d - individual_5d

        foreign_20d = trailing_20["foreign_net_value"].sum(min_count=1)
        institution_20d = trailing_20["institution_net_value"].sum(min_count=1)
        individual_20d = trailing_20["individual_net_value"].sum(min_count=1)
        smart_money_20d = foreign_20d + institution_20d - individual_20d

        alignment_checks = [
            float(foreign_5d > 0) if pd.notna(foreign_5d) else pd.NA,
            float(institution_5d > 0) if pd.notna(institution_5d) else pd.NA,
            float(individual_5d < 0) if pd.notna(individual_5d) else pd.NA,
        ]
        alignment_frame = pd.Series(alignment_checks, dtype="float64")
        flow_alignment_score = (
            float(alignment_frame.mean()) if alignment_frame.notna().any() else pd.NA
        )

        rows.append(
            {
                "symbol": symbol,
                "foreign_net_value_ratio_1d": _safe_ratio(
                    latest_row["foreign_net_value"],
                    turnover_1d,
                ),
                "foreign_net_value_ratio_5d": _safe_ratio(foreign_5d, turnover_5d),
                "institution_net_value_ratio_5d": _safe_ratio(institution_5d, turnover_5d),
                "individual_net_value_ratio_5d": _safe_ratio(individual_5d, turnover_5d),
                "smart_money_flow_ratio_5d": _safe_ratio(smart_money_5d, turnover_5d),
                "smart_money_flow_ratio_20d": _safe_ratio(smart_money_20d, turnover_20d),
                "flow_alignment_score": flow_alignment_score,
                "flow_coverage_flag": 1.0,
            }
        )

    return pd.DataFrame(rows)
