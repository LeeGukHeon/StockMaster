from __future__ import annotations

import pandas as pd

from app.ops.report import _build_ops_discord_summary


def test_build_ops_discord_summary_is_short_and_korean_friendly() -> None:
    health = pd.DataFrame(
        [
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "failed_run_count_24h",
                "metric_value_double": 2.0,
                "metric_value_text": None,
            },
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "open_alert_count",
                "metric_value_double": 1.0,
                "metric_value_text": None,
            },
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "active_lock_count",
                "metric_value_double": 1.0,
                "metric_value_text": None,
            },
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "stale_lock_count",
                "metric_value_double": 0.0,
                "metric_value_text": None,
            },
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "disk_usage_ratio",
                "metric_value_double": 0.64,
                "metric_value_text": None,
            },
            {
                "health_scope": "overall",
                "component_name": "platform",
                "status": "DEGRADED_SUCCESS",
                "metric_name": "disk_watermark",
                "metric_value_double": None,
                "metric_value_text": "NORMAL",
            },
            {
                "health_scope": "pipeline",
                "component_name": "daily_report",
                "status": "SUCCESS",
                "metric_name": "latest_successful_output",
                "metric_value_double": None,
                "metric_value_text": "2026-03-11",
            },
            {
                "health_scope": "pipeline",
                "component_name": "evaluation_summary",
                "status": "SUCCESS",
                "metric_name": "latest_successful_output",
                "metric_value_double": None,
                "metric_value_text": "2026-03-10",
            },
        ]
    )
    recovery = pd.DataFrame(
        [
            {"status": "OPEN"},
            {"status": "SKIPPED"},
        ]
    )

    summary = _build_ops_discord_summary(
        as_of_date=pd.Timestamp("2026-03-12").date(),
        health=health,
        recovery=recovery,
    )

    assert "운영 요약" in summary
    assert "전체 상태: 주의" in summary
    assert "최근 24시간 실패 작업: 2건" in summary
    assert "열린 경고: 1건 / 복구 대기: 1건" in summary
    assert "디스크: 64.0% (NORMAL)" in summary
