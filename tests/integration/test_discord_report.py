from __future__ import annotations

import json
from datetime import date

from app.features.feature_store import build_feature_store
from app.labels.forward_returns import build_forward_labels
from app.regime.snapshot import build_market_regime_snapshot
from app.reports.discord_eod import (
    _chunk_content,
    publish_discord_eod_report,
    render_discord_eod_report,
)
from app.selection.calibration import calibrate_proxy_prediction_bands
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
)


def _seed_alpha_promotion_surface(settings) -> None:
    detail_json = json.dumps(
        {
            "chosen_model_spec_id": "alpha_rolling_120_v1",
            "decision_reason": "single_challenger_survived",
            "superior_set": ["alpha_rolling_120_v1"],
            "mean_losses": {
                "alpha_recursive_expanding_v1": {
                    "loss_top10": -0.004,
                    "loss_top20": -0.003,
                    "loss_point": 0.00042,
                    "loss_rank": -0.11,
                },
                "alpha_rolling_120_v1": {
                    "loss_top10": -0.019,
                    "loss_top20": -0.016,
                    "loss_point": 0.00019,
                    "loss_rank": -0.23,
                },
            },
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        bootstrap_core_tables(connection)
        for horizon in (1, 5):
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
                )
                VALUES (
                    ?, ?, 'alpha_rolling_120_v1', ?, 'alpha_model_v1',
                    'alpha_auto_promotion', 'AUTO_PROMOTION', ?, ?,
                    NULL, TRUE, NULL, ?, now(), now()
                )
                """,
                [
                    f"seed-active-h{int(horizon)}",
                    int(horizon),
                    f"seed-train-h{int(horizon)}",
                    detail_json,
                    date(2026, 3, 5),
                    "seed promotion",
                ],
            )
            connection.execute(
                """
                INSERT INTO fact_alpha_promotion_test (
                    promotion_date,
                    horizon,
                    incumbent_model_spec_id,
                    challenger_model_spec_id,
                    loss_name,
                    window_start,
                    window_end,
                    sample_count,
                    mcs_member_flag,
                    incumbent_mcs_member_flag,
                    p_value,
                    decision,
                    detail_json,
                    created_at
                )
                VALUES (
                    ?, ?, 'alpha_recursive_expanding_v1', 'alpha_rolling_120_v1',
                    'loss_top10', ?, ?, 20, TRUE, FALSE, 0.041, 'PROMOTE_CHALLENGER', ?, now()
                )
                """,
                [
                    date(2026, 3, 5),
                    int(horizon),
                    date(2026, 2, 6),
                    date(2026, 3, 4),
                    detail_json,
                ],
            )


def test_discord_report_render_and_publish_dry_run(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)

    for as_of_date in [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4), date(2026, 3, 5)]:
        build_feature_store(settings, as_of_date=as_of_date, limit_symbols=4)
        build_market_regime_snapshot(settings, as_of_date=as_of_date)
        materialize_selection_engine_v1(settings, as_of_date=as_of_date, horizons=[1, 5])

    build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 5),
        horizons=[1],
        limit_symbols=4,
    )
    calibrate_proxy_prediction_bands(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 5),
        horizons=[1],
    )
    _seed_alpha_promotion_surface(settings)

    render_result = render_discord_eod_report(
        settings,
        as_of_date=date(2026, 3, 5),
        dry_run=True,
    )
    publish_result = publish_discord_eod_report(
        settings,
        as_of_date=date(2026, 3, 5),
        dry_run=True,
    )

    assert any(path.endswith(".md") for path in render_result.artifact_paths)
    all_messages = "\n".join(
        str(message["content"]) for message in render_result.payload["messages"]
    )
    assert "StockMaster 장마감 요약" in all_messages
    assert "**알파 모델 교체 점검**" in all_messages
    assert "비교 후보=" in all_messages
    assert render_result.payload["message_count"] >= 1
    assert len(render_result.payload["messages"]) == render_result.payload["message_count"]
    assert publish_result.published is False


def test_discord_report_publish_respects_disabled_flag(tmp_path):
    settings = build_test_settings(tmp_path)
    settings.discord.enabled = False
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)

    for as_of_date in [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4), date(2026, 3, 5)]:
        build_feature_store(settings, as_of_date=as_of_date, limit_symbols=4)
        build_market_regime_snapshot(settings, as_of_date=as_of_date)
        materialize_selection_engine_v1(settings, as_of_date=as_of_date, horizons=[1, 5])

    publish_result = publish_discord_eod_report(
        settings,
        as_of_date=date(2026, 3, 5),
        dry_run=False,
    )

    assert publish_result.published is False
    assert "DISCORD_REPORT_ENABLED=false" in publish_result.notes


def test_chunk_content_splits_long_messages():
    long_line = " ".join(["chunk"] * 700)
    chunks = _chunk_content(long_line, limit=200)

    assert len(chunks) > 1
    assert all(len(chunk) <= 200 for chunk in chunks)
