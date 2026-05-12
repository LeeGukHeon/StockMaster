from __future__ import annotations

from app.ml.constants import D5_DAILY_H5_CANDIDATE_MODEL_SPEC_ID, D5_PRACTICAL_V3_MODEL_SPEC_ID
from app.ml.shadow import _load_candidate_specs
from app.scheduler.jobs import _candidate_model_specs_for_daily_pipeline


def test_daily_pipeline_includes_d5_buyable_spec_only_when_active_swing_enabled() -> None:
    default_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=False)
    ]
    active_ids = [
        spec.model_spec_id
        for spec in _candidate_model_specs_for_daily_pipeline(active_d5_swing=True)
    ]

    assert default_ids == ["alpha_lead_d1_v1"]
    assert D5_DAILY_H5_CANDIDATE_MODEL_SPEC_ID not in default_ids
    assert active_ids.count(D5_DAILY_H5_CANDIDATE_MODEL_SPEC_ID) == 1
    assert D5_DAILY_H5_CANDIDATE_MODEL_SPEC_ID == D5_PRACTICAL_V3_MODEL_SPEC_ID


def test_shadow_candidate_specs_include_daily_h5_v4_candidate() -> None:
    spec_ids = {str(row["model_spec_id"]) for row in _load_candidate_specs(None)}

    assert D5_DAILY_H5_CANDIDATE_MODEL_SPEC_ID in spec_ids
