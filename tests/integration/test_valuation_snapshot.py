from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from app.common.paths import project_root
from app.pipelines.valuation_snapshot import materialize_valuation_snapshot
from app.providers.dart.financials import DartStockTotalStatusSnapshot
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


class FakeValuationDartProvider:
    def fetch_stock_total_status(self, *, corp_code: str, bsns_year: int, reprt_code: str):
        return DartStockTotalStatusSnapshot(
            frame=pd.DataFrame(
                [
                    {
                        "se": "보통주",
                        "istc_totqy": "100",
                        "tesstk_co": "0",
                        "distb_stock_co": "100",
                        "stlm_dt": "2025-12-31",
                    }
                ]
            ),
            payload={"status": "000"},
        )


def _settings(tmp_path):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "valuation.duckdb"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"APP_DATA_DIR={data_dir.as_posix()}",
                f"APP_DUCKDB_PATH={duckdb_path.as_posix()}",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_settings(project_root=project_root(), env_file=env_file)
    bootstrap_storage(settings)
    return settings


def _seed_valuation_inputs(settings) -> None:
    as_of = date(2026, 3, 6)
    ingested = datetime(2026, 3, 6, tzinfo=timezone.utc)
    disclosed = datetime(2026, 2, 28, tzinfo=timezone.utc)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        for idx in range(9):
            symbol = f"10000{idx}"
            connection.execute(
                """
                INSERT INTO dim_symbol (
                    symbol, company_name, market, sector, industry, is_common_stock,
                    is_etf, is_etn, is_spac, is_reit, is_delisted, is_trading_halt,
                    is_management_issue, dart_corp_code, source, updated_at
                ) VALUES (?, ?, 'KOSPI', 'Tech', 'Semi', TRUE, FALSE, FALSE, FALSE,
                    FALSE, FALSE, FALSE, FALSE, ?, 'test', ?)
                """,
                [symbol, f"테스트{idx}", f"CORP{idx:05d}", ingested],
            )
            connection.execute(
                """
                INSERT INTO fact_fundamentals_snapshot (
                    as_of_date, symbol, fiscal_year, report_code, revenue, operating_income,
                    net_income, equity, liabilities, roe, debt_ratio, operating_margin,
                    source_doc_id, source, disclosed_at, statement_basis, report_name, currency,
                    accounting_standard, source_notes_json, ingested_at
                ) VALUES (?, ?, 2025, '11011', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'dart_fnlttSinglAcntAll', ?, 'CFS', '사업보고서', 'KRW', NULL, '{}', ?)
                """,
                [
                    as_of,
                    symbol,
                    1000.0 + idx,
                    100.0 + idx,
                    100.0 + idx,
                    500.0 + idx,
                    250.0 + idx,
                    (100.0 + idx) / (500.0 + idx) * 100.0,
                    (250.0 + idx) / (500.0 + idx) * 100.0,
                    (100.0 + idx) / (1000.0 + idx) * 100.0,
                    f"DOC{idx}",
                    disclosed,
                    ingested,
                ],
            )
            connection.execute(
                """
                INSERT INTO fact_daily_ohlcv (
                    trading_date, symbol, open, high, low, close, volume, turnover_value,
                    market_cap, source, source_notes_json, ingested_at
                ) VALUES (?, ?, 100, 110, 90, ?, 1000, 100000, NULL, 'test_price', '{}', ?)
                """,
                [as_of, symbol, 50.0 + idx, ingested],
            )


def test_materialize_valuation_snapshot_populates_additive_tables_idempotently(tmp_path) -> None:
    settings = _settings(tmp_path)
    _seed_valuation_inputs(settings)

    result = materialize_valuation_snapshot(
        settings,
        as_of_date=date(2026, 3, 6),
        dart_provider=FakeValuationDartProvider(),
    )

    assert result.ingredient_row_count == 9
    assert result.metric_row_count == 81
    assert result.baseline_row_count == 9
    assert result.label_row_count == 9

    rerun = materialize_valuation_snapshot(
        settings,
        as_of_date=date(2026, 3, 6),
        force=True,
        dart_provider=FakeValuationDartProvider(),
    )
    assert rerun.label_row_count == 9

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        counts = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM fact_valuation_source_ingredient),
                (SELECT COUNT(*) FROM fact_valuation_metric_snapshot),
                (SELECT COUNT(*) FROM fact_sector_valuation_baseline),
                (SELECT COUNT(*) FROM fact_valuation_label_snapshot)
            """
        ).fetchone()
        assert counts == (9, 81, 9, 9)
        sample = connection.execute(
            """
            SELECT ingredient.denominator_basis, metric.metric_value, label.valuation_label
            FROM fact_valuation_source_ingredient AS ingredient
            JOIN fact_valuation_metric_snapshot AS metric
              ON metric.as_of_date = ingredient.as_of_date
             AND metric.symbol = ingredient.symbol
             AND metric.metric_name = 'net_margin'
            JOIN fact_valuation_label_snapshot AS label
              ON label.as_of_date = ingredient.as_of_date
             AND label.symbol = ingredient.symbol
            WHERE ingredient.symbol = '100000'
            """
        ).fetchone()
        assert sample[0] == "issued_shares"
        assert sample[1] == 10.0
        assert sample[2] in {"저평가", "적정", "고평가", "판단 보류"}
