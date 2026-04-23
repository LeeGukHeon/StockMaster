from __future__ import annotations

from app.ml.constants import D5_PRIMARY_FOCUS_MODEL_SPEC_ID
from app.scheduler.jobs import _candidate_model_specs_for_daily_pipeline


def test_daily_pipeline_includes_d5_primary_spec_only_when_active_swing_enabled() -> None:
    default_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=False)
    ]
    active_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=True)
    ]

    assert D5_PRIMARY_FOCUS_MODEL_SPEC_ID not in default_ids
    assert active_ids.count(D5_PRIMARY_FOCUS_MODEL_SPEC_ID) == 1
