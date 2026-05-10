from __future__ import annotations

import pandas as pd

from app.reference.industry_grouping import industry_group


def test_industry_group_prefers_industry_code_and_label() -> None:
    group = industry_group(
        {
            "industry_code": "0013",
            "industry": "전기전자/반도체",
            "sector_code": "0027",
            "sector": "제조/산업재",
        }
    )

    assert group.group_type == "industry"
    assert group.key == "0013"
    assert group.label == "전기전자/반도체"


def test_industry_group_falls_back_to_sector_only_when_industry_missing() -> None:
    group = industry_group({"sector_code": "0027", "sector": "제조/산업재"})

    assert group.group_type == "sector"
    assert group.key == "0027"
    assert group.label == "제조/산업재"


def test_industry_group_treats_empty_values_as_missing() -> None:
    group = industry_group(
        pd.Series(
            {
                "industry_code": "",
                "industry": "-",
                "sector_code": None,
                "sector": "nan",
            }
        )
    )

    assert group.group_type == "unknown"
    assert group.key == "미분류"
