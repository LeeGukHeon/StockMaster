from __future__ import annotations

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
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
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
    assert "StockMaster EOD Report" in render_result.payload["content"]
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
