from __future__ import annotations

from datetime import date

from app.release.reporting import render_release_candidate_checklist
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import build_test_settings


def test_render_release_candidate_checklist_includes_alpha_serving_and_gap_sections(tmp_path):
    settings = build_test_settings(tmp_path)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        connection.execute(
            """
            INSERT INTO fact_alpha_active_model (
                active_alpha_model_id,
                horizon,
                model_spec_id,
                training_run_id,
                model_version,
                source_type,
                promotion_type,
                promotion_report_json,
                effective_from_date,
                effective_to_date,
                active_flag,
                rollback_of_active_alpha_model_id,
                note,
                created_at,
                updated_at
            ) VALUES (
                'active-d1', 1, 'alpha_lead_d1_v1', 'train-d1', 'alpha_model_v1',
                'test', 'AUTO_PROMOTION', NULL, ?, NULL, TRUE, NULL, 'seed', now(), now()
            )
            """,
            [date(2026, 3, 10)],
        )
        connection.execute(
            """
            INSERT INTO fact_alpha_shadow_selection_gap_scorecard (
                summary_date, window_name, window_start, window_end, horizon, model_spec_id,
                segment_name, matured_selection_date_count, required_selection_date_count,
                insufficient_history_flag, raw_top5_source, hit_rate_formula,
                raw_top5_mean_realized_excess_return, selected_top5_mean_realized_excess_return,
                report_candidates_mean_realized_excess_return, raw_top5_hit_rate,
                selected_top5_hit_rate, report_candidates_hit_rate, top5_overlap,
                pred_only_top5_mean_realized_excess_return,
                sel_only_top5_mean_realized_excess_return,
                drag_vs_raw_top5, evaluation_run_id, created_at
            ) VALUES (
                ?, 'rolling_20', ?, ?, 1, 'alpha_lead_d1_v1', 'top5',
                20, 20, FALSE, 'prediction desc', 'realized_excess_return > 0',
                0.020, 0.018, 0.017, 0.55, 0.53, 0.52, 0.60, 0.021, 0.015, -0.002, 'seed-gap', now()
            )
            """,
            [date(2026, 3, 10), date(2026, 2, 10), date(2026, 3, 10)],
        )
        result = render_release_candidate_checklist(
            settings,
            connection=connection,
            as_of_date=date(2026, 3, 10),
            dry_run=True,
        )

    preview = next(path for path in result.artifact_paths if path.endswith(".md"))
    content = open(preview, encoding="utf-8").read()

    assert "## Alpha serving baseline" in content
    assert "active serving spec 하루 선행 포착 v1" in content
    assert "기본 비교 모델=확장형 누적 학습" in content
    assert "## Selection gap gate" in content
    assert "drag_vs_raw=-0.20%" in content
