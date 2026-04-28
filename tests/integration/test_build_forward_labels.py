from __future__ import annotations

from datetime import date

import pytest

from app.labels.forward_returns import LABEL_VERSION, build_forward_labels
from app.ml.dataset import _load_dataset_frame
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


def test_path_overlay_only_writes_lightweight_label_table(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 2),
        horizons=[5],
        symbols=["005930"],
    )
    result = build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 2),
        horizons=[5],
        symbols=["005930"],
        force=True,
        path_overlay_only=True,
        chunk_trading_days=1,
    )

    assert result.available_row_count == 1
    assert result.artifact_paths == []
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        overlay_row = connection.execute(
            """
            SELECT
                path_return_tp5_sl3_conservative,
                path_excess_return_tp5_sl3_conservative,
                label_available_flag
            FROM fact_forward_return_path_label
            WHERE as_of_date = ?
              AND symbol = '005930'
              AND horizon = 5
            """,
            [date(2026, 3, 2)],
        ).fetchone()
        base_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_forward_return_label
            WHERE as_of_date = ?
              AND symbol = '005930'
              AND horizon = 5
            """,
            [date(2026, 3, 2)],
        ).fetchone()[0]

    assert overlay_row[0] == pytest.approx(0.05)
    assert overlay_row[1] is not None
    assert overlay_row[2] is True
    assert base_count == 1


def test_training_dataset_prefers_path_overlay_targets(tmp_path):
    settings = build_test_settings(tmp_path)

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol,
                company_name,
                market,
                is_common_stock,
                source,
                updated_at
            )
            VALUES ('005930', 'SamsungElec', 'KOSPI', TRUE, 'test', now())
            """
        )
        connection.execute(
            """
            INSERT INTO fact_forward_return_label (
                run_id,
                as_of_date,
                symbol,
                horizon,
                market,
                entry_date,
                exit_date,
                excess_forward_return,
                path_excess_return_tp5_sl3_conservative,
                label_available_flag,
                created_at
            )
            VALUES (
                'base-run',
                DATE '2026-03-02',
                '005930',
                5,
                'KOSPI',
                DATE '2026-03-03',
                DATE '2026-03-09',
                -0.10,
                -0.10,
                TRUE,
                now()
            )
            """
        )
        connection.execute(
            """
            INSERT INTO fact_forward_return_path_label (
                run_id,
                as_of_date,
                symbol,
                horizon,
                path_excess_return_tp5_sl3_conservative,
                label_available_flag,
                created_at
            )
            VALUES (
                'overlay-run',
                DATE '2026-03-02',
                '005930',
                5,
                0.04,
                TRUE,
                now()
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO fact_feature_snapshot (
                run_id,
                as_of_date,
                symbol,
                feature_name,
                feature_value,
                feature_group,
                source_version,
                is_imputed,
                created_at
            )
            VALUES ('feature-run', DATE '2026-03-02', '005930', ?, ?, 'test', 'test', FALSE, now())
            """,
            [
                ("liquidity_rank_pct", 0.50),
                ("adv_20", 100.0),
                ("realized_vol_20d", 0.10),
                ("drawdown_20d", 0.0),
                ("max_loss_20d", 0.0),
                ("missing_key_feature_count", 0.0),
                ("data_confidence_score", 100.0),
                ("stale_price_flag", 0.0),
            ],
        )

        dataset = _load_dataset_frame(
            connection,
            train_end_date=date(2026, 3, 9),
            horizons=[5],
            symbols=["005930"],
            limit_symbols=None,
            market="ALL",
        )

    assert dataset["target_h5"].iloc[0] == pytest.approx(-0.10)
    assert dataset["target_practical_excess_v2_h5"].iloc[0] > 0.0
