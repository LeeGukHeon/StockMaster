from __future__ import annotations

from datetime import date

from app.features.feature_store import build_feature_store
from app.labels.forward_returns import build_forward_labels
from app.ranking.explanatory_score import materialize_explanatory_ranking
from app.ranking.validation import validate_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.storage.duckdb import duckdb_connection
from tests._ticket003_support import build_test_settings, seed_ticket003_data


def test_validate_explanatory_ranking_writes_summary_and_artifacts(tmp_path):
    settings = build_test_settings(tmp_path)
    seed_ticket003_data(settings)

    for as_of_date in [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)]:
        build_feature_store(settings, as_of_date=as_of_date, limit_symbols=4)
        build_market_regime_snapshot(settings, as_of_date=as_of_date)
        materialize_explanatory_ranking(settings, as_of_date=as_of_date, horizons=[1, 5])

    build_forward_labels(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 4),
        horizons=[1, 5],
        limit_symbols=4,
    )

    result = validate_explanatory_ranking(
        settings,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 4),
        horizons=[1, 5],
    )

    assert result.row_count > 0
    assert len(result.artifact_paths) == 2

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM ops_ranking_validation_summary"
        ).fetchone()[0]
        assert row_count == result.row_count
