from __future__ import annotations

import json

import pandas as pd

from app.discord_bot.live_analysis import render_live_stock_analysis
from app.discord_bot.live_recalc import LiveRecalcResult, build_live_analysis_payload


class _FakeKISProvider:
    def __init__(self, _settings) -> None:
        pass

    def fetch_current_quote(self, *, symbol: str, persist_probe_artifacts: bool = True):
        assert symbol == "005930"
        assert persist_probe_artifacts is False
        return {
            "output": {
                "stck_prpr": "71000",
                "prdy_vrss": "1200",
                "prdy_ctrt": "1.72",
                "stck_hgpr": "71500",
                "stck_lwpr": "70100",
                "acml_vol": "1234567",
            }
        }

    def close(self) -> None:
        return None


class _FakeNaverNewsProvider:
    def __init__(self, _settings) -> None:
        pass

    def search_news(self, *, query: str, limit: int = 3, start: int = 1, sort: str = "date"):
        assert query == "삼성전자"
        return {
            "items": [
                {"title_plain": "삼성전자 실적 개선"},
                {"title_plain": "삼성전자 AI 반도체 기대"},
            ]
        }

    def close(self) -> None:
        return None


def test_render_live_stock_analysis_formats_quote_and_news(monkeypatch) -> None:
    snapshot_rows = pd.DataFrame(
        [
            {
                "symbol": "005930",
                "company_name": "삼성전자",
                "market": "KOSPI",
                "title": "005930 삼성전자",
                "summary": "D1 A · D5 B",
                "payload_json": json.dumps(
                    {
                        "d1_grade": "A",
                        "d5_grade": "B",
                        "d5_expected_excess_return": 0.0123,
                        "ret_5d": 0.0345,
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    )

    monkeypatch.setattr(
        "app.discord_bot.live_analysis.fetch_discord_bot_snapshot_rows",
        lambda *args, **kwargs: snapshot_rows,
    )
    monkeypatch.setattr("app.discord_bot.live_analysis.KISProvider", _FakeKISProvider)
    monkeypatch.setattr("app.discord_bot.live_analysis.NaverNewsProvider", _FakeNaverNewsProvider)
    monkeypatch.setattr(
        "app.discord_bot.live_analysis.compute_live_stock_recommendation",
        lambda *args, **kwargs: LiveRecalcResult(
            pd.DataFrame(
                [
                    {
                        "live_d1_selection_v2_grade": "A",
                        "live_d5_selection_v2_grade": "S",
                        "live_d5_expected_excess_return": 0.021,
                        "live_d5_target_price": 72500,
                        "live_d5_stop_price": 68800,
                    }
                ]
            ),
            mode="live",
        ),
    )

    rendered = render_live_stock_analysis(object(), query="삼성전자")

    assert "현재가 71,000원" in rendered
    assert "D1 A · D5 S" in rendered
    assert "활성 head D1 -" in rendered
    assert "D5 예상 초과수익률 +2.10%" in rendered
    assert "최근 5일 수익률 +3.45%" in rendered
    assert "왜 지금 보나:" in rendered
    assert "신호 분해" in rendered
    assert "시세 기준 KIS 실시간 시세 기준" in rendered
    assert "뉴스 기준 Naver 최신 뉴스 2건 반영" in rendered
    assert "실시간 목표가 72,500원" in rendered
    assert "- 삼성전자 실적 개선" in rendered


def test_render_live_stock_analysis_returns_candidate_list_for_ambiguous_query(monkeypatch) -> None:
    snapshot_rows = pd.DataFrame(
        [
            {"title": "005930 삼성전자", "subtitle": "종목 요약"},
            {"title": "005935 삼성전자우", "subtitle": "종목 요약"},
        ]
    )
    monkeypatch.setattr(
        "app.discord_bot.live_analysis.fetch_discord_bot_snapshot_rows",
        lambda *args, **kwargs: snapshot_rows,
    )

    rendered = render_live_stock_analysis(object(), query="삼성")

    assert "**종목 후보**" in rendered
    assert "005930 삼성전자" in rendered
    assert "005935 삼성전자우" in rendered


def test_build_live_analysis_payload_marks_snapshot_reuse_for_busy_mode() -> None:
    payload = build_live_analysis_payload(
        {
            "d1_grade": "B",
            "d5_grade": "A",
            "d1_model_spec_id": "alpha_snapshot_d1",
            "d5_model_spec_id": "alpha_snapshot_d5",
            "ret_5d": 0.015,
        },
        LiveRecalcResult(pd.DataFrame(), mode="busy", note="배치 점유"),
        quote_timestamp_or_basis="snapshot quote",
        news_basis="snapshot news",
    )

    assert payload["snapshot_reused_flag"] is True
    assert payload["degradation_mode"] == "busy"
    assert payload["source_precedence"] == ["snapshot", "quote", "news"]
    assert payload["d1_head_spec_id"] == "alpha_snapshot_d1"
    assert payload["d5_head_spec_id"] == "alpha_snapshot_d5"
