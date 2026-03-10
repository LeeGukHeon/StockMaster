from __future__ import annotations

import pandas as pd

from app.ui.helpers import home_banner_freshness_levels


def test_home_banner_freshness_downgrades_research_only_critical_rows() -> None:
    freshness = pd.DataFrame(
        [
            {
                "page_name": "리서치 랩",
                "dataset_name": "policy_experiment",
                "warning_level": "CRITICAL",
            },
            {
                "page_name": "사후 평가",
                "dataset_name": "calibration",
                "warning_level": "CRITICAL",
            },
            {
                "page_name": "오늘",
                "dataset_name": "selection_v2",
                "warning_level": "CRITICAL",
            },
            {
                "page_name": "운영",
                "dataset_name": "health_snapshot",
                "warning_level": "WARNING",
            },
        ]
    )

    critical, warning = home_banner_freshness_levels(freshness)

    assert set(critical["dataset_name"]) == {"selection_v2"}
    assert set(warning["dataset_name"]) == {"policy_experiment", "calibration", "health_snapshot"}
