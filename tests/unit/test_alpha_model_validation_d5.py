from __future__ import annotations

from datetime import date

from app.ml.validation import _append_d5_primary_checks


def test_append_d5_primary_checks_uses_top5_and_bucket_segments_only(monkeypatch) -> None:
    seen_segments: list[str] = []

    def _fake_load_shadow_comparison_row(
        connection,
        *,
        as_of_date,
        window_type,
        horizon,
        focus_model_spec_id,
        comparator_model_spec_id,
        segment_value,
    ):
        seen_segments.append(str(segment_value))
        return {
            "summary_date": as_of_date,
            "focus_count_evaluated": 3,
            "focus_matured_selection_date_count": 3,
            "focus_mean_realized_excess_return": 0.031,
            "comparator_count_evaluated": 3,
            "comparator_matured_selection_date_count": 3,
            "comparator_mean_realized_excess_return": 0.020,
        }

    monkeypatch.setattr(
        "app.ml.validation._load_shadow_comparison_row",
        _fake_load_shadow_comparison_row,
    )

    checks: list[dict[str, object]] = []
    _append_d5_primary_checks(object(), as_of_date=date(2026, 3, 6), checks=checks)

    assert seen_segments.count("top5") == 3
    assert set(seen_segments) == {
        "top5",
        "bucket_continuation",
        "bucket_reversal_recovery",
        "bucket_crowded_risk",
    }
    assert len(checks) == 7
    assert {str(check["status"]) for check in checks} == {"pass"}
