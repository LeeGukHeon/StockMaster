from __future__ import annotations

from datetime import date

import pandas as pd

from app.common.paths import project_root
from app.domain.fundamentals.account_normalizer import materialize_fundamentals_row


def test_materialize_fundamentals_row_computes_core_metrics():
    frame = pd.DataFrame(
        [
            {
                "sj_div": "BS",
                "account_nm": "자본총계",
                "thstrm_amount": "1000",
                "ord": "1",
                "currency": "KRW",
            },
            {
                "sj_div": "BS",
                "account_nm": "부채총계",
                "thstrm_amount": "500",
                "ord": "2",
                "currency": "KRW",
            },
            {
                "sj_div": "IS",
                "account_nm": "매출액",
                "thstrm_amount": "400",
                "ord": "3",
                "currency": "KRW",
            },
            {
                "sj_div": "IS",
                "account_nm": "영업이익",
                "thstrm_amount": "80",
                "ord": "4",
                "currency": "KRW",
            },
            {
                "sj_div": "IS",
                "account_nm": "당기순이익",
                "thstrm_amount": "50",
                "ord": "5",
                "currency": "KRW",
            },
        ]
    )
    disclosure = {
        "fiscal_year": 2025,
        "reprt_code": "11014",
        "rcept_no": "20251114000001",
        "rcept_dt": date(2025, 11, 14),
        "report_name_clean": "분기보고서 (2025.09)",
        "report_type_name": "분기보고서",
    }

    row = materialize_fundamentals_row(
        frame=frame,
        disclosure=disclosure,
        as_of_date=date(2026, 3, 6),
        symbol="005930",
        project_root=project_root(),
        statement_basis="CFS",
    )

    assert row is not None
    assert row["revenue"] == 400.0
    assert row["operating_income"] == 80.0
    assert row["net_income"] == 50.0
    assert row["roe"] == 5.0
    assert row["debt_ratio"] == 50.0
    assert row["operating_margin"] == 20.0
    assert row["statement_basis"] == "CFS"
