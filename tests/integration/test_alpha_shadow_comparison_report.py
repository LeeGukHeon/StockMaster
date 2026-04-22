from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from app.evaluation.alpha_shadow import (
    materialize_alpha_shadow_evaluation_summary,
    materialize_alpha_shadow_selection_gap_scorecard,
    materialize_alpha_shadow_selection_outcomes,
)
from app.ml.constants import MODEL_SPEC_ID, get_alpha_model_spec
from app.ml.shadow import materialize_alpha_shadow_candidates
from app.ml.shadow_report import _build_d5_primary_markdown, render_alpha_shadow_comparison_report
from app.ml.training import train_alpha_candidate_models, train_alpha_model_v1
from app.storage.duckdb import duckdb_connection
from tests.integration.test_alpha_shadow_pipeline import _prepare_shadow_settings


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
    materialize_alpha_shadow_selection_gap_scorecard(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[5],
        model_spec_ids=["alpha_swing_d5_v2", "alpha_swing_d5_v1", MODEL_SPEC_ID],
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
    materialize_alpha_shadow_selection_gap_scorecard(
        settings,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        horizons=[5],
        model_spec_ids=["alpha_swing_d5_v2", "alpha_swing_d5_v1", MODEL_SPEC_ID],
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
    assert "Lag-first proof" in content
    assert "top5_overlap" in content
    assert "pred_only_top5_mean_realized_excess_return" in content
    assert "sel_only_top5_mean_realized_excess_return" in content
    assert "D+5 raw-vs-selected drag" in content
    assert "drag_vs_raw_top5" in content
    assert "D+5 robustness buckets vs alpha_swing_d5_v1" in content
    assert "Continuation" in content
    assert "D+1 auxiliary interpretation" in content


def test_build_d5_primary_markdown_uses_summary_rows_for_d1_auxiliary_section() -> None:
    pairwise_ledger = pd.DataFrame(
        [
            {
                "horizon": 5,
                "window_type": "cohort",
                "segment_value": "top5",
                "comparator_model_spec_id": "alpha_swing_d5_v1",
                "mean_realized_excess_return_focus": 0.021,
                "mean_realized_excess_return_comparator": 0.014,
                "return_gap_vs_comparator": 0.007,
                "matured_selection_date_count_focus": 3,
            }
        ]
    )
    summary = pd.DataFrame(
        [
            {
                "horizon": 1,
                "window_type": "cohort",
                "segment_value": "top5",
                "model_spec_id": MODEL_SPEC_ID,
                "mean_realized_excess_return": 0.011,
                "rank_ic": 0.052,
                "matured_selection_date_count": 3,
            },
            {
                "horizon": 1,
                "window_type": "rolling_20",
                "segment_value": "top5",
                "model_spec_id": "alpha_topbucket_h1_rolling_120_v1",
                "mean_realized_excess_return": 0.015,
                "rank_ic": 0.061,
                "matured_selection_date_count": 2,
            },
        ]
    )
    drag_summary = pd.DataFrame(
        [
            {
                "window_name": "cohort",
                "model_spec_id": "alpha_swing_d5_v2",
                "matured_selection_date_count": 3,
                "raw_top5_mean_realized_excess_return": 0.018,
                "selected_top5_mean_realized_excess_return": 0.013,
                "top5_overlap": 0.6,
                "pred_only_top5_mean_realized_excess_return": 0.024,
                "sel_only_top5_mean_realized_excess_return": 0.007,
                "drag_vs_raw_top5": -0.005,
            }
        ]
    )

    markdown = _build_d5_primary_markdown(
        pairwise_ledger,
        summary=summary,
        drag_summary=drag_summary,
        start_selection_date=date(2026, 3, 4),
        end_selection_date=date(2026, 3, 6),
        promotion_summary=pd.DataFrame(),
    )

    assert "Lag-first proof" in markdown
    assert "top5_overlap" in markdown
    assert "D+1 auxiliary interpretation" in markdown
    assert f"cohort | {MODEL_SPEC_ID}" in markdown
    assert "rolling_20 | alpha_topbucket_h1_rolling_120_v1" in markdown
    assert "D+1 auxiliary comparator rows not available yet." not in markdown
    assert "report_candidates` remains a compatibility/reporting surface only" in markdown
