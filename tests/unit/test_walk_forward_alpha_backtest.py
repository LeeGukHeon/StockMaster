from pathlib import Path

import pytest

from scripts.walk_forward_alpha_backtest import (
    BacktestJob,
    _job_output_paths,
    _needs_model_spec_output_token,
    _safe_file_token,
)


def _job(model_spec_id: str, horizon: int = 5) -> BacktestJob:
    return BacktestJob(
        horizon=horizon,
        model_spec_id=model_spec_id,
        active_alpha_model_id=None,
        source_training_run_id=None,
    )


def test_single_horizon_single_model_keeps_legacy_output_names() -> None:
    job = _job("alpha_practical_d5_v1")

    assert not _needs_model_spec_output_token([job])
    paths = _job_output_paths(Path("out"), job, include_model_spec=False)

    assert paths["predictions_outcomes"] == Path("out/h5_walk_forward_predictions_outcomes.csv")
    assert paths["metrics"] == Path("out/h5_walk_forward_metrics.csv")


def test_duplicate_horizon_models_are_disambiguated_by_model_spec() -> None:
    jobs = [_job("alpha_practical_d5_v1"), _job("alpha_practical_d5_v2")]

    assert _needs_model_spec_output_token(jobs)
    path_sets = [
        _job_output_paths(Path("out"), job, include_model_spec=True)
        for job in jobs
    ]

    prediction_paths = {paths["predictions_outcomes"] for paths in path_sets}
    metric_paths = {paths["metrics"] for paths in path_sets}
    assert prediction_paths == {
        Path("out/h5_alpha_practical_d5_v1_walk_forward_predictions_outcomes.csv"),
        Path("out/h5_alpha_practical_d5_v2_walk_forward_predictions_outcomes.csv"),
    }
    assert metric_paths == {
        Path("out/h5_alpha_practical_d5_v1_walk_forward_metrics.csv"),
        Path("out/h5_alpha_practical_d5_v2_walk_forward_metrics.csv"),
    }


def test_different_horizons_do_not_need_model_spec_disambiguation() -> None:
    assert not _needs_model_spec_output_token([
        _job("alpha_lead_d1_v1", horizon=1),
        _job("alpha_practical_d5_v1", horizon=5),
    ])


def test_safe_file_token_sanitizes_values_and_rejects_empty_tokens() -> None:
    assert _safe_file_token(" alpha/practical d5 ") == "alpha-practical-d5"
    with pytest.raises(ValueError):
        _safe_file_token(" /// ")
