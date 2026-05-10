from __future__ import annotations

import json

import pandas as pd

from app.discord_bot.valuation_analysis import render_stock_valuation


def test_render_stock_valuation_displays_core_table_and_internal_disclosure(monkeypatch) -> None:
    payload = {
        "valuation_label": "판단 보류",
        "confidence_pass": False,
        "hard_gate_reasons": ["share_denominator_float_based"],
        "metrics": {
            "per": {"value": 10.0, "source_type": "internal_calculated_from_disclosure"},
            "pbr": {"value": 1.2, "source_type": "internal_calculated_from_disclosure"},
            "eps": {"value": 5000.0, "source_type": "internal_calculated_from_disclosure"},
            "bps": {"value": 42000.0, "source_type": "internal_calculated_from_disclosure"},
            "roe": {"value": 8.0, "source_type": "internal_calculated_from_disclosure"},
            "operating_margin": {
                "value": 12.0,
                "source_type": "internal_calculated_from_disclosure",
            },
            "net_margin": {"value": 9.0, "source_type": "internal_calculated_from_disclosure"},
            "debt_ratio": {"value": 50.0, "source_type": "internal_calculated_from_disclosure"},
        },
        "peer": {
            "group_type": "sector",
            "group_value": "Tech",
            "peer_count": 8,
            "median_per": 12.0,
            "median_pbr": 1.5,
        },
        "sector": "Tech",
        "industry": "Semi",
        "financial_quality": {"net_income": 100.0},
    }
    rows = pd.DataFrame(
        [
            {
                "title": "005930 삼성전자",
                "summary": "판단 보류",
                "as_of_date": "2026-03-06",
                "payload_json": json.dumps(payload, ensure_ascii=False),
            }
        ]
    )
    monkeypatch.setattr(
        "app.discord_bot.valuation_analysis.fetch_discord_bot_snapshot_rows",
        lambda *args, **kwargs: rows,
    )

    rendered = render_stock_valuation(object(), query="삼성전자")

    assert "005930 삼성전자 가치평가" in rendered
    assert "최종 판단: 판단 보류" in rendered
    assert "PER 10.00배" in rendered
    assert "PBR 1.20배" in rendered
    assert "주식수 기준" in rendered
    assert "공시 원자료에는 해당 지표가 비어 있어 StockMaster" in rendered
    assert "매수" not in rendered


def test_render_stock_valuation_returns_candidates_for_ambiguous_query(monkeypatch) -> None:
    rows = pd.DataFrame(
        [
            {"title": "005930 삼성전자", "payload_json": "{}"},
            {"title": "005935 삼성전자우", "payload_json": "{}"},
        ]
    )
    monkeypatch.setattr(
        "app.discord_bot.valuation_analysis.fetch_discord_bot_snapshot_rows",
        lambda *args, **kwargs: rows,
    )

    rendered = render_stock_valuation(object(), query="삼성")

    assert "가치평가 후보" in rendered
    assert "6자리 코드" in rendered
    assert "005930 삼성전자" in rendered
