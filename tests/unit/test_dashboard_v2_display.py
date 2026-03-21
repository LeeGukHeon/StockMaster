from __future__ import annotations

import pandas as pd

from app.ui.dashboard_v2 import (
    DASHBOARD_DEFAULT_PICK_HORIZON,
    display_text,
    display_token_list,
    display_value,
    filter_dashboard_leaderboard,
    filter_dashboard_target_book,
)
from app.ui.helpers import format_ui_value, translate_ui_token


def test_format_ui_value_translates_policy_terms_naturally() -> None:
    assert format_ui_value("template_id", "COHORT_GUARD_STRICT") == "동일 시작 묶음 방어 강화형"
    assert format_ui_value("scope_type", "HORIZON_REGIME_CLUSTER") == "기간·장세 조합별"
    assert format_ui_value("window_type", "cohort") == "동일 시작 묶음"
    assert format_ui_value("bin_type", "expected_return_bin") == "예상수익 구간별"


def test_translate_ui_token_handles_aliases_and_risk_labels() -> None:
    assert translate_ui_token("hort_term_momentum_strong") == "단기 탄력 강함"
    assert translate_ui_token("model_disagreement_high") == "모델 판단이 엇갈림"
    assert (
        translate_ui_token("expected_return_binmodel_disagreement_high")
        == "예상수익 구간별 / 모델 판단이 엇갈림"
    )


def test_dashboard_display_helpers_use_human_friendly_labels() -> None:
    assert display_text("GAP_GUARD_STRICT") == "갭 추격 억제형"
    assert display_value("decision_label", "Active kept") == "기존 모델 유지"
    assert (
        display_token_list('["hort_term_momentum_strong","breakout_near_20d_high","turnover_surge"]')
        == "단기 탄력 강함, 20일 고점 돌파 직전, 거래대금 급증"
    )


def test_filter_dashboard_leaderboard_uses_default_horizon() -> None:
    frame = pd.DataFrame(
        [
            {"symbol": "A", "horizon": 1, "market": "KOSPI"},
            {"symbol": "B", "horizon": 5, "market": "KOSPI"},
            {"symbol": "C", "horizon": 5, "market": "KOSDAQ"},
        ]
    )
    filtered = filter_dashboard_leaderboard(frame, horizon=DASHBOARD_DEFAULT_PICK_HORIZON)
    assert filtered["symbol"].tolist() == ["A"]


def test_filter_dashboard_target_book_excludes_cash_zero_weight_and_duplicates() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "357580",
                "market": "KOSDAQ",
                "included_flag": True,
                "execution_mode": "OPEN_ALL",
                "target_weight": 0.18,
            },
            {
                "symbol": "357580",
                "market": "KOSDAQ",
                "included_flag": True,
                "execution_mode": "TIMING_ASSISTED",
                "target_weight": 0.18,
            },
            {
                "symbol": "476830",
                "market": "KOSDAQ",
                "included_flag": True,
                "execution_mode": "OPEN_ALL",
                "target_weight": 0.0,
            },
            {
                "symbol": "__CASH__",
                "market": "CASH",
                "included_flag": True,
                "execution_mode": "OPEN_ALL",
                "target_weight": 0.82,
            },
        ]
    )
    filtered = filter_dashboard_target_book(frame)
    assert filtered["symbol"].tolist() == ["357580"]
    assert filtered["execution_mode"].tolist() == ["OPEN_ALL"]
