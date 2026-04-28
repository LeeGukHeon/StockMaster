from pathlib import Path

import pytest

from scripts.walk_forward_alpha_backtest import (
    BacktestJob,
    _job_output_paths,
    _needs_model_spec_output_token,
    _safe_file_token,
    _safe_remove_tree,
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


def test_safe_remove_tree_allows_any_descendant_of_scratch_root(tmp_path: Path) -> None:
    scratch_root = tmp_path / "tmp" / "d5_wf_short_name"
    date_scratch = scratch_root / "as_of_date=2026-03-03" / "horizon=5"
    date_scratch.mkdir(parents=True)
    (date_scratch / "artifact.txt").write_text("temporary")

    _safe_remove_tree(date_scratch, scratch_root=scratch_root)

    assert not date_scratch.exists()
    assert scratch_root.exists()


def test_safe_remove_tree_rejects_scratch_root_and_outside_paths(tmp_path: Path) -> None:
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(RuntimeError, match="scratch root itself"):
        _safe_remove_tree(scratch_root, scratch_root=scratch_root)
    with pytest.raises(RuntimeError, match="unexpected artifact path"):
        _safe_remove_tree(outside, scratch_root=scratch_root)
