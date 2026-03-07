from __future__ import annotations

from datetime import date

from app.features.feature_store import build_feature_store
from app.ranking.explanatory_score import RANKING_VERSION, materialize_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_materialize_explanatory_ranking_populates_fact_table(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)
    build_feature_store(settings, as_of_date=date(2026, 3, 6), limit_symbols=4)
    build_market_regime_snapshot(settings, as_of_date=date(2026, 3, 6))

    result = materialize_explanatory_ranking(
        settings,
        as_of_date=date(2026, 3, 6),
        horizons=[1, 5],
        limit_symbols=4,
    )

    assert result.ranking_version == RANKING_VERSION
    assert result.row_count == 8

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT ranking_version, explanatory_score_json, grade
            FROM fact_ranking
            WHERE as_of_date = ?
            ORDER BY final_selection_value DESC, symbol
            LIMIT 1
            """,
            [date(2026, 3, 6)],
        ).fetchone()
        assert row[0] == RANKING_VERSION
        assert '"flow_score_status": "reserved"' in row[1]
        assert row[2] in {"A", "A-", "B", "C"}
