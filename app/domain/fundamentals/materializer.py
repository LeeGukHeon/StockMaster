from __future__ import annotations

from datetime import date

import pandas as pd


def candidate_disclosures(disclosures: pd.DataFrame, *, as_of_date: date) -> pd.DataFrame:
    if disclosures.empty:
        return disclosures.copy()
    filtered = disclosures.loc[disclosures["rcept_dt"] <= as_of_date].copy()
    if filtered.empty:
        return filtered
    return filtered.sort_values(["rcept_dt", "rcept_no"], ascending=[False, False]).reset_index(
        drop=True
    )


def statement_basis_order() -> tuple[str, ...]:
    return ("CFS", "OFS")
