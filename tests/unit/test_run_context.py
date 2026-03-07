from __future__ import annotations

from app.common.run_context import (
    activate_run_context,
    current_run_id,
    current_run_type,
    make_run_id,
)


def test_make_run_id_is_prefixed():
    run_id = make_run_id("bootstrap")
    assert run_id.startswith("bootstrap-")


def test_activate_run_context_sets_and_resets_context():
    assert current_run_id() is None
    assert current_run_type() is None

    with activate_run_context("daily_pipeline") as context:
        assert current_run_id() == context.run_id
        assert current_run_type() == "daily_pipeline"

    assert current_run_id() is None
    assert current_run_type() is None
