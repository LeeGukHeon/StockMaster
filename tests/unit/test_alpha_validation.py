from __future__ import annotations

import inspect

from app.ml import validation
from app.ml.validation import _concentration_check_status


def test_concentration_check_status_pass_warn_fail() -> None:
    assert _concentration_check_status(0.014, pass_threshold=0.015, fail_threshold=0.0183) == "pass"
    assert _concentration_check_status(0.016, pass_threshold=0.015, fail_threshold=0.0183) == "warn"
    assert _concentration_check_status(0.0183, pass_threshold=0.015, fail_threshold=0.0183) == "fail"


def test_concentration_check_status_absolute_cap_only() -> None:
    assert _concentration_check_status(2, pass_threshold=2, fail_threshold=None) == "pass"
    assert _concentration_check_status(3, pass_threshold=2, fail_threshold=None) == "fail"


def test_validate_alpha_model_v1_exposes_explicit_focus_model_spec_parameter() -> None:
    signature = inspect.signature(validation.validate_alpha_model_v1)

    assert "focus_model_spec_id" in signature.parameters
    assert signature.parameters["focus_model_spec_id"].default is None
