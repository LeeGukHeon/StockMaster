from __future__ import annotations

from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from app.ml.constants import get_alpha_model_spec
from app.ml.shadow_report import render_alpha_shadow_comparison_report
from app.storage.duckdb import duckdb_connection
from tests.integration.test_alpha_shadow_pipeline import _prepare_shadow_settings
from app.ml.training import train_alpha_model_v1, train_alpha_candidate_models
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_selection_outcomes,
    materialize_alpha_shadow_evaluation_summary,
    upsert_alpha_shadow_evaluation_summary,
)


def test_render_alpha_shadow_comparison_report_creates_artifacts(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        materialize_alpha_shadow_candidates(
            settings,
            as_of_date=train_end_date,
            horizons=[1, 5],
            limit_symbols=4,
        )

    materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )
    materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[2],
    )

    result = render_alpha_shadow_comparison_report(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )

    assert result.row_count > 0
    assert len(result.artifact_paths) == 3

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        ledger_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_alpha_shadow_evaluation_summary
            WHERE summary_date = ?
            """,
            [date(2026, 3, 6)],
        ).fetchone()[0]

    assert int(ledger_count or 0) > 0
    preview = [path for path in result.artifact_paths if path.endswith('.md')][0]
    with open(preview, encoding='utf-8') as fh:
        content = fh.read()
    assert 'Alpha Shadow Comparison Report' in content
    assert 'alpha_rolling_120_v1' in content


def test_render_alpha_shadow_comparison_report_includes_d5_focus_sections(tmp_path):
    settings = _prepare_shadow_settings(tmp_path)

    swing_v1_spec = get_alpha_model_spec("alpha_swing_d5_v1")
    swing_v2_spec = get_alpha_model_spec("alpha_swing_d5_v2")
    legacy_h1_spec = get_alpha_model_spec("alpha_topbucket_h1_rolling_120_v1")

    for train_end_date in [date(2026, 3, 4), date(2026, 3, 5), date(2026, 3, 6)]:
        train_alpha_model_v1(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
        )
        train_alpha_candidate_models(
            settings,
            train_end_date=train_end_date,
            horizons=[1, 5],
            min_train_days=5,
            validation_days=2,
            limit_symbols=4,
            model_specs=(swing_v1_spec, swing_v2_spec, legacy_h1_spec),
        )
        materialize_alpha_shadow_candidates(
            settings,
            as_of_date=train_end_date,
            horizons=[1, 5],
            limit_symbols=4,
        )

    materialize_alpha_shadow_selection_outcomes(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )
    materialize_alpha_shadow_evaluation_summary(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[2],
    )

    result = render_alpha_shadow_comparison_report(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
    )

    preview = [path for path in result.artifact_paths if path.endswith(".md")][0]
    content = Path(preview).read_text(encoding="utf-8")

    assert "alpha_swing_d5_v2" in content
    assert "D+5 robustness buckets vs alpha_swing_d5_v1" in content
    assert "Continuation" in content
    assert "D+1 auxiliary interpretation" in content
