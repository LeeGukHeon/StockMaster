from __future__ import annotations

from datetime import date

import pandas as pd


def build_fundamentals_feature_frame(
    latest_fundamentals: pd.DataFrame,
    *,
    as_of_date: date,
) -> pd.DataFrame:
    if latest_fundamentals.empty:
        return pd.DataFrame(columns=["symbol"])

    frame = latest_fundamentals.copy()
    frame["net_margin_latest"] = frame["net_income"] / frame["revenue"].replace(0, pd.NA)
    frame["net_income_positive_flag"] = frame["net_income"].gt(0).astype(float)
    frame["operating_income_positive_flag"] = frame["operating_income"].gt(0).astype(float)
    disclosed_at = pd.to_datetime(
        frame["disclosed_at"],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    disclosed_local = disclosed_at.dt.tz_convert("Asia/Seoul").dt.tz_localize(None).dt.normalize()
    frame["days_since_latest_report"] = (
        pd.Timestamp(as_of_date).normalize() - disclosed_local
    ).dt.days
    frame["fundamental_coverage_flag"] = 1.0
    frame["low_debt_preference_proxy"] = 1.0 / (1.0 + frame["debt_ratio"].clip(lower=0))
    frame["profitability_support_proxy"] = frame["operating_margin"]

    return frame[
        [
            "symbol",
            "revenue",
            "operating_income",
            "net_income",
            "roe",
            "debt_ratio",
            "operating_margin",
            "net_margin_latest",
            "net_income_positive_flag",
            "operating_income_positive_flag",
            "days_since_latest_report",
            "fundamental_coverage_flag",
            "low_debt_preference_proxy",
            "profitability_support_proxy",
        ]
    ].rename(
        columns={
            "revenue": "revenue_latest",
            "operating_income": "operating_income_latest",
            "net_income": "net_income_latest",
            "roe": "roe_latest",
            "debt_ratio": "debt_ratio_latest",
            "operating_margin": "operating_margin_latest",
        }
    )
