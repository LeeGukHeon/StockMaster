from __future__ import annotations

from datetime import date

from app.features.feature_store import build_feature_store
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.engine_v1 import SELECTION_ENGINE_VERSION, materialize_selection_engine_v1
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import (
    build_test_settings,
    seed_ticket003_data,
    seed_ticket004_flow_data,
)


def test_materialize_selection_engine_v1_populates_fact_table(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    seed_ticket004_flow_data(settings)
    build_feature_store(settings, as_of_date=date(2026, 3, 6), limit_symbols=4)
    build_market_regime_snapshot(settings, as_of_date=date(2026, 3, 6))

    result = materialize_selection_engine_v1(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    assert result.ranking_version == SELECTION_ENGINE_VERSION
    assert result.row_count == 8

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT ranking_version, explanatory_score_json, grade
            FROM fact_ranking
            WHERE as_of_date = ?
              AND ranking_version = ?
            ORDER BY final_selection_value DESC, symbol
            LIMIT 1
            """,
            [date(2026, 3, 6), SELECTION_ENGINE_VERSION],
        ).fetchone()
        assert row[0] == SELECTION_ENGINE_VERSION
        assert '"flow_score"' in row[1]
        assert '"disagreement_score": null' in row[1]
        assert row[2] in {"A", "A-", "B", "C"}
