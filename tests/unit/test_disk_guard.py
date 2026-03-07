from __future__ import annotations

from app.common.disk import DiskWatermark, build_watermark_message, classify_usage_ratio


def test_classify_usage_ratio_respects_thresholds():
    assert (
        classify_usage_ratio(0.65, warning_ratio=0.70, prune_ratio=0.80, limit_ratio=0.90)
        == DiskWatermark.NORMAL
    )
    assert (
        classify_usage_ratio(0.75, warning_ratio=0.70, prune_ratio=0.80, limit_ratio=0.90)
        == DiskWatermark.WARNING
    )
    assert (
        classify_usage_ratio(0.85, warning_ratio=0.70, prune_ratio=0.80, limit_ratio=0.90)
        == DiskWatermark.PRUNE
    )
    assert (
        classify_usage_ratio(0.95, warning_ratio=0.70, prune_ratio=0.80, limit_ratio=0.90)
        == DiskWatermark.LIMIT
    )


def test_build_watermark_message_mentions_usage_percentage():
    message = build_watermark_message(DiskWatermark.WARNING, 0.75)
    assert "75.0%" in message
