from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from app.evaluation.calibration_diagnostics import materialize_calibration_diagnostics
from app.evaluation.summary import materialize_prediction_evaluation
from app.reports.postmortem import (
    _build_report_content,
    _format_percent_text,
    _load_top_outcomes,
    publish_discord_postmortem_report,
    render_postmortem_report,
)
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
    seed_ticket005_selection_history,
)


def test_postmortem_report_render_and_publish_dry_run(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    seed_ticket005_selection_history(settings)

    materialize_prediction_evaluation(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        rolling_windows=[20, 60],
        limit_symbols=4,
    )
    materialize_calibration_diagnostics(
        settings,
        start_selection_date=date(2026, 3, 2),
        end_selection_date=date(2026, 3, 6),
        horizons=[1, 5],
        bin_count=4,
        limit_symbols=4,
    )

    render_result = render_postmortem_report(
        settings,
        evaluation_date=date(2026, 3, 13),
        horizons=[1, 5],
        dry_run=True,
    )
    publish_result = publish_discord_postmortem_report(
        settings,
        evaluation_date=date(2026, 3, 13),
        horizons=[1, 5],
        dry_run=True,
    )

    assert any(path.endswith(".md") for path in render_result.artifact_paths)
    assert "StockMaster" in render_result.payload["content"]
    assert publish_result.published is False


def test_postmortem_percent_text_uses_threshold_for_near_zero_values():
    assert _format_percent_text(2.833816789587776e-17, decimals=2, signed=True) == "+<0.01%"
    assert _format_percent_text(-2.833816789587776e-17, decimals=2, signed=True) == "-<0.01%"
    assert _format_percent_text(4.658876868337108e-05, decimals=2, signed=True) == "+<0.01%"
    assert _format_percent_text(None, decimals=1) == "n/a"


def test_postmortem_build_content_skips_zero_sample_rolling_rows():
    content = _build_report_content(
        evaluation_date=date(2026, 3, 12),
        summary=pd.DataFrame(),
        comparison=pd.DataFrame(),
        rolling_summary=pd.DataFrame(
            [
                {
                    "window_type": "rolling_20d",
                    "horizon": 5,
                    "ranking_version": "selection_engine_v1",
                    "count_evaluated": 0,
                    "mean_realized_excess_return": float("nan"),
                    "hit_rate": float("nan"),
                }
            ]
        ),
        calibration_summary=pd.DataFrame(),
        top_by_horizon={5: pd.DataFrame()},
    )

    assert "nan%" not in content
    assert "최근 구간 요약이 아직 없습니다." in content


def test_load_top_outcomes_excludes_zero_volume_entry_or_exit_rows():
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE fact_selection_outcome (
            selection_date DATE,
            evaluation_date DATE,
            symbol VARCHAR,
            horizon INTEGER,
            ranking_version VARCHAR,
            outcome_status VARCHAR,
            realized_excess_return DOUBLE,
            expected_excess_return_at_selection DOUBLE,
            band_status VARCHAR,
            top_reason_tags_json VARCHAR,
            source_label_version VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE dim_symbol (
            symbol VARCHAR,
            company_name VARCHAR,
            market VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE fact_forward_return_label (
            as_of_date DATE,
            symbol VARCHAR,
            horizon INTEGER,
            label_version VARCHAR,
            entry_date DATE,
            exit_date DATE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE fact_daily_ohlcv (
            trading_date DATE,
            symbol VARCHAR,
            volume BIGINT
        )
        """
    )
    con.execute(
        """
        INSERT INTO dim_symbol VALUES
            ('111111', '이상종목', 'KOSDAQ'),
            ('222222', '정상종목', 'KOSDAQ')
        """
    )
    con.execute(
        """
        INSERT INTO fact_selection_outcome VALUES
            (DATE '2026-03-17', DATE '2026-03-24', '111111', 5, 'selection_engine_v1', 'matured', 9.0, 0.01, 'above_upper', '[]', 'forward_label_v1'),
            (DATE '2026-03-17', DATE '2026-03-24', '222222', 5, 'selection_engine_v1', 'matured', 0.20, 0.01, 'above_upper', '[]', 'forward_label_v1')
        """
    )
    con.execute(
        """
        INSERT INTO fact_forward_return_label VALUES
            (DATE '2026-03-17', '111111', 5, 'forward_label_v1', DATE '2026-03-18', DATE '2026-03-24'),
            (DATE '2026-03-17', '222222', 5, 'forward_label_v1', DATE '2026-03-18', DATE '2026-03-24')
        """
    )
    con.execute(
        """
        INSERT INTO fact_daily_ohlcv VALUES
            (DATE '2026-03-18', '111111', 0),
            (DATE '2026-03-24', '111111', 1000),
            (DATE '2026-03-18', '222222', 5000),
            (DATE '2026-03-24', '222222', 7000)
        """
    )

    frame = _load_top_outcomes(
        con,
        evaluation_date=date(2026, 3, 24),
        horizon=5,
        limit=5,
    )

    assert frame["symbol"].tolist() == ["222222"]
