from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from app.reports.discord_eod import (
    _build_payload_content,
    _format_alpha_promotion_line,
    _load_official_target_rows,
)
from app.storage.duckdb import bootstrap_core_tables


def test_format_alpha_promotion_line_uses_korean_labels() -> None:
    row = pd.Series(
        {
            "horizon": 1,
            "decision_label": "Active kept",
            "active_model_label": "recursive",
            "comparison_model_label": "rolling 120d",
            "sample_count": 7,
            "p_value": 0.571,
            "decision_reason_label": "incumbent remained in the superior set",
        }
    )

    line = _format_alpha_promotion_line(row)

    assert "기존 모델 유지" in line
    assert "현재 모델이 우수 후보군에 남음" in line
    assert "하루 보유 기준 모델 점검 (D+1)" in line


def test_build_payload_content_labels_candidate_horizon_explicitly() -> None:
    content = _build_payload_content(
        as_of_date=date(2026, 3, 20),
        sector_horizon=1,
        candidate_horizon=1,
        market_pulse={},
        alpha_promotion=pd.DataFrame(),
        sector_outlook=pd.DataFrame(),
        single_buy_candidates=pd.DataFrame(),
        market_news=pd.DataFrame(),
    )

    assert "**다음 거래일 강세 예상 업종 | 하루 보유 기준 (D+1)**" in content
    assert "**다음 거래일 상위 후보 5종목 | 하루 보유 기준 (D+1)**" in content
    assert "모델 점검은 하루 보유 기준(D+1)과 5거래일 보유 기준(D+5)을 함께 보여줍니다." in content
    assert "공식 추천안" not in content


def test_load_official_target_rows_excludes_cash_and_zero_weight() -> None:
    connection = duckdb.connect(":memory:")
    bootstrap_core_tables(connection)
    as_of_date = date(2026, 3, 20)
    common_values = {
        "run_id": "seed-run",
        "as_of_date": as_of_date,
        "execution_mode": "OPEN_ALL",
        "portfolio_policy_id": "policy",
        "portfolio_policy_version": "v1",
        "created_at": "2026-03-20T18:40:00+09:00",
    }

    rows = [
        {
            **common_values,
            "symbol": "357580",
            "company_name": "아모센스",
            "market": "KOSDAQ",
            "target_rank": 1,
            "target_weight": 0.18,
            "included_flag": True,
        },
        {
            **common_values,
            "symbol": "476830",
            "company_name": "알지노믹스",
            "market": "KOSDAQ",
            "target_rank": 2,
            "target_weight": 0.0,
            "included_flag": True,
        },
        {
            **common_values,
            "symbol": "__CASH__",
            "company_name": "현금",
            "market": "CASH",
            "target_rank": 9999,
            "target_weight": 0.82,
            "included_flag": True,
        },
    ]

    for row in rows:
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        connection.execute(
            f"INSERT INTO fact_portfolio_target_book ({columns}) VALUES ({placeholders})",
            list(row.values()),
        )

    try:
        frame = _load_official_target_rows(connection, as_of_date=as_of_date, limit=10)
        assert frame["symbol"].tolist() == ["357580"]
    finally:
        connection.close()
