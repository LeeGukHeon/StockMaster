from __future__ import annotations

from datetime import date

import pytest

from app.labels.forward_returns import LABEL_VERSION, build_forward_labels
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_build_forward_labels_uses_same_market_baseline_beyond_selected_subset(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    result = build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 4),
        horizons=[1, 5],
        symbols=["005930"],
    )

    assert result.label_version == LABEL_VERSION
    assert result.available_row_count >= 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        baseline_expected = connection.execute(
            """
            SELECT AVG(exit_price.close / entry_price.open - 1.0)
            FROM fact_daily_ohlcv AS entry_price
            JOIN fact_daily_ohlcv AS exit_price
              ON entry_price.symbol = exit_price.symbol
            JOIN dim_symbol AS symbol
              ON entry_price.symbol = symbol.symbol
            WHERE symbol.market = 'KOSPI'
              AND entry_price.trading_date = ?
              AND exit_price.trading_date = ?
            """,
            [date(2026, 3, 3), date(2026, 3, 3)],
        ).fetchone()[0]
        h1_label_row = connection.execute(
            """
            SELECT baseline_forward_return, gross_forward_return, label_available_flag
            FROM fact_forward_return_label
            WHERE as_of_date = ?
              AND symbol = '005930'
              AND horizon = 1
            """,
            [date(2026, 3, 2)],
        ).fetchone()
        label_row = connection.execute(
            """
            SELECT
                baseline_forward_return,
                gross_forward_return,
                label_available_flag,
                max_forward_return,
                min_forward_return,
                take_profit_3_hit,
                take_profit_5_hit,
                stop_loss_3_hit,
                path_return_tp5_sl3_conservative,
                path_excess_return_tp5_sl3_conservative
            FROM fact_forward_return_label
            WHERE as_of_date = ?
              AND symbol = '005930'
              AND horizon = 5
            """,
            [date(2026, 3, 2)],
        ).fetchone()

        assert h1_label_row[2] is True
        assert h1_label_row[0] == pytest.approx(baseline_expected)
        assert h1_label_row[1] != pytest.approx(h1_label_row[0])
        assert label_row[2] is True
        assert label_row[0] is not None
        assert label_row[1] != pytest.approx(label_row[0])
        assert label_row[3] >= 0.05
        assert label_row[4] > -0.03
        assert label_row[5] is True
        assert label_row[6] is True
        assert label_row[7] is False
        assert label_row[8] == pytest.approx(0.05)
        assert label_row[9] == pytest.approx(0.05 - label_row[0])
