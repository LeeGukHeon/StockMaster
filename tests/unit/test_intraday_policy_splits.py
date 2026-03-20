from __future__ import annotations

from datetime import date, timedelta

from app.intraday.policy import _walkforward_splits


def _session_dates(count: int) -> list[date]:
    start = date(2026, 1, 2)
    return [start + timedelta(days=index) for index in range(count)]


def test_anchored_walkforward_splits_advance_validation_and_test_windows() -> None:
    session_dates = _session_dates(70)

    splits = _walkforward_splits(
        session_dates,
        mode="ANCHORED_WALKFORWARD",
        train_sessions=40,
        validation_sessions=10,
        test_sessions=10,
        step_sessions=5,
    )

    assert len(splits) == 3
    assert splits[0]["train_dates"] == session_dates[:40]
    assert splits[0]["validation_dates"] == session_dates[40:50]
    assert splits[0]["test_dates"] == session_dates[50:60]
    assert splits[1]["train_dates"] == session_dates[:45]
    assert splits[1]["validation_dates"] == session_dates[45:55]
    assert splits[1]["test_dates"] == session_dates[55:65]
    assert splits[2]["train_dates"] == session_dates[:50]
    assert splits[2]["validation_dates"] == session_dates[50:60]
    assert splits[2]["test_dates"] == session_dates[60:70]
