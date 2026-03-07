from __future__ import annotations

import pandas as pd


def assign_grades(frame: pd.DataFrame) -> pd.Series:
    grades = []
    for row in frame.itertuples(index=False):
        rank_pct = float(row.final_selection_rank_pct)
        critical_risk = bool(row.critical_risk_flag)
        eligible = bool(row.eligible_flag)
        if not eligible:
            grades.append("C")
        elif critical_risk:
            grades.append("B" if rank_pct >= 0.65 else "C")
        elif rank_pct >= 0.95:
            grades.append("A")
        elif rank_pct >= 0.85:
            grades.append("A-")
        elif rank_pct >= 0.65:
            grades.append("B")
        else:
            grades.append("C")
    return pd.Series(grades, index=frame.index)
