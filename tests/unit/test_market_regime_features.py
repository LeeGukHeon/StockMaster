from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from app.ml.dataset import augment_market_regime_features
from app.storage.duckdb import bootstrap_core_tables


def test_augment_market_regime_features_prefers_market_scope_and_falls_back_to_kr_all():
    connection = duckdb.connect(database=":memory:")
    bootstrap_core_tables(connection)
    connection.execute(
        """
        INSERT INTO fact_market_regime_snapshot (
            run_id,
            as_of_date,
            market_scope,
            breadth_up_ratio,
            breadth_down_ratio,
            median_symbol_return_1d,
            median_symbol_return_5d,
            market_realized_vol_20d,
            turnover_burst_z,
            new_high_ratio_20d,
            new_low_ratio_20d,
            regime_state,
            regime_score,
            notes_json,
            created_at
        ) VALUES
            (
                'run', DATE '2026-04-24', 'KR_ALL', 0.40, 0.30, 0.001, 0.005,
                0.020, 0.10, 0.04, 0.03, 'neutral', 52.0, '{}', now()
            ),
            (
                'run', DATE '2026-04-24', 'KOSPI', 0.70, 0.10, 0.003, 0.010,
                0.015, 0.30, 0.08, 0.01, 'risk_on', 76.0, '{}', now()
            )
        """
    )
    frame = pd.DataFrame(
        {
            "as_of_date": [date(2026, 4, 24), date(2026, 4, 24), date(2026, 4, 24)],
            "symbol": ["000001", "000002", "000003"],
            "market": ["KOSPI", "KOSDAQ", "UNKNOWN"],
        }
    )

    augmented = augment_market_regime_features(connection, frame)

    assert augmented.loc[0, "market_regime_score"] == 76.0
    assert augmented.loc[0, "market_breadth_up_ratio"] == 0.70
    assert augmented.loc[0, "market_regime_risk_on_flag"] == 1.0
    assert augmented.loc[0, "market_regime_neutral_flag"] == 0.0
    assert augmented.loc[1, "market_regime_score"] == 52.0
    assert augmented.loc[1, "market_regime_neutral_flag"] == 1.0
    assert augmented.loc[2, "market_regime_score"] == 52.0
    assert augmented["market_regime_coverage_flag"].tolist() == [1.0, 1.0, 1.0]
