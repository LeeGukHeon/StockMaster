from __future__ import annotations

from datetime import date

import pandas as pd

from app.features.builders.quality_features import build_data_quality_feature_frame
from app.features.constants import CORE_FEATURES_FOR_MISSINGNESS
from app.ranking.reason_tags import build_eligibility_notes, build_risk_flags


def _complete_price_and_fundamental_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": "005930",
        "ret_3d": 0.02,
        "ret_5d": 0.03,
        "realized_vol_20d": 0.12,
        "adv_20": 1_000_000_000.0,
        "roe_latest": 0.15,
        "debt_ratio_latest": 45.0,
        "fundamental_coverage_flag": 1.0,
        "news_coverage_flag": 0.0,
    }
    row.update(overrides)
    return row


def test_news_is_not_core_data_quality_missingness():
    assert "news_count_3d" not in CORE_FEATURES_FOR_MISSINGNESS

    frame = pd.DataFrame([_complete_price_and_fundamental_row(news_count_3d=pd.NA)])

    quality = build_data_quality_feature_frame(frame, as_of_date=date(2026, 4, 24)).iloc[0]

    assert quality["has_news_flag"] == 0.0
    assert quality["missing_key_feature_count"] == 0
    assert quality["has_daily_ohlcv_flag"] == 1.0
    assert quality["stale_price_flag"] == 0.0
    assert quality["data_confidence_score"] == 100.0


def test_price_core_features_backfill_ohlcv_coverage_when_metadata_is_absent():
    frame = pd.DataFrame([_complete_price_and_fundamental_row(close=pd.NA)])

    quality = build_data_quality_feature_frame(frame, as_of_date=date(2026, 4, 24)).iloc[0]

    assert quality["has_daily_ohlcv_flag"] == 1.0
    assert quality["stale_price_flag"] == 0.0
    assert quality["data_confidence_score"] == 100.0


def test_known_stale_price_metadata_still_reduces_confidence():
    frame = pd.DataFrame(
        [
            _complete_price_and_fundamental_row(
                close=70_000.0,
                latest_price_date=date(2026, 4, 23),
            )
        ]
    )

    quality = build_data_quality_feature_frame(frame, as_of_date=date(2026, 4, 24)).iloc[0]

    assert quality["has_daily_ohlcv_flag"] == 1.0
    assert quality["stale_price_flag"] == 1.0
    assert quality["missing_key_feature_count"] == 0
    assert quality["data_confidence_score"] == 85.0


def test_legacy_news_and_price_metadata_gap_does_not_trigger_missingness_risk():
    row = pd.Series(
        _complete_price_and_fundamental_row(
            as_of_date=date(2026, 3, 3),
            has_daily_ohlcv_flag=0.0,
            has_fundamentals_flag=1.0,
            stale_price_flag=1.0,
            missing_key_feature_count=1.0,
            data_confidence_score=37.857143,
            news_count_3d=pd.NA,
        )
    )

    risk_flags = build_risk_flags(row)

    assert "data_missingness_high" not in risk_flags
    assert "missing_price" not in build_eligibility_notes(row, risk_flags=risk_flags)
    assert "stale_price" not in build_eligibility_notes(row, risk_flags=risk_flags)


def test_missing_price_core_still_triggers_missingness_risk():
    row = pd.Series(
        _complete_price_and_fundamental_row(
            ret_3d=pd.NA,
            ret_5d=pd.NA,
            realized_vol_20d=pd.NA,
            adv_20=pd.NA,
            has_daily_ohlcv_flag=0.0,
            stale_price_flag=1.0,
            data_confidence_score=37.857143,
        )
    )

    assert "data_missingness_high" in build_risk_flags(row)
