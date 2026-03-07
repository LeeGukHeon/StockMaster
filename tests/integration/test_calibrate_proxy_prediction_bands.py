from __future__ import annotations

from datetime import date

from app.features.feature_store import build_feature_store
from app.labels.forward_returns import build_forward_labels
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.calibration import PREDICTION_VERSION, calibrate_proxy_prediction_bands
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
)


def test_calibrate_proxy_prediction_bands_writes_predictions(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)

    for as_of_date in [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4), date(2026, 3, 5)]:
        build_feature_store(settings, as_of_date=as_of_date, limit_symbols=4)
        build_market_regime_snapshot(settings, as_of_date=as_of_date)
        materialize_selection_engine_v1(settings, as_of_date=as_of_date, horizons=[1])

    build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 5),
        horizons=[1],
        limit_symbols=4,
    )

    result = calibrate_proxy_prediction_bands(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 5),
        horizons=[1],
    )

    assert result.prediction_version == PREDICTION_VERSION
    assert result.row_count > 0

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM fact_prediction
            WHERE prediction_version = ?
            """,
            [PREDICTION_VERSION],
        ).fetchone()[0]
        assert row_count == result.row_count
