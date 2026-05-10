from __future__ import annotations

import json
from datetime import date, datetime, timezone

from app.common.paths import project_root
from app.discord_bot import read_store
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


def _settings(tmp_path):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "discord-valuation.duckdb"
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


def test_discord_valuation_read_store_builds_stock_valuation_rows(tmp_path) -> None:
    settings = _settings(tmp_path)
    created = datetime(2026, 3, 6, tzinfo=timezone.utc)
    as_of = date(2026, 3, 6)
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol, company_name, market, sector, industry, is_common_stock, source, updated_at
            ) VALUES ('005930', '삼성전자', 'KOSPI', 'Tech', 'Semi', TRUE, 'test', ?)
            """,
            [created],
        )
        connection.execute(
            """
            INSERT INTO fact_valuation_source_ingredient (
                as_of_date, symbol, fiscal_year, report_code, statement_basis, source_doc_id,
                disclosed_at, basis_date, basis_close, revenue, operating_income, net_income,
                equity, liabilities, shares_outstanding, denominator_basis, selected_share_class,
                istc_totqy, tesstk_co, distb_stock_co, share_stlm_dt, source_lineage_json,
                reason_codes_json, ingested_at
            ) VALUES (?, '005930', 2025, '11011', 'CFS', 'DOC', ?, ?, 70000, 1000,
                120, 90, 500, 250, 100, 'issued_shares', '보통주', 100, 0, 100,
                '2025-12-31', '{}', '[]', ?)
            """,
            [as_of, created, as_of, created],
        )
        for name, value in {"per": 10.0, "pbr": 1.4, "eps": 7000.0, "bps": 50000.0}.items():
            connection.execute(
                """
                INSERT INTO fact_valuation_metric_snapshot (
                    as_of_date, symbol, metric_name, metric_value, metric_unit, source_type,
                    formula_version, reason, formula_inputs_json, created_at
                ) VALUES (?, '005930', ?, ?, 'multiple', 'internal_calculated_from_disclosure',
                    'valuation_metrics_v1', NULL, '{}', ?)
                """,
                [as_of, name, value, created],
            )
        connection.execute(
            """
            INSERT INTO fact_sector_valuation_baseline (
                as_of_date, symbol, group_type, group_value, peer_count, valid_pbr_count,
                valid_per_count, pbr_coverage, per_coverage, median_pbr, median_per,
                pbr_percentile, per_percentile, filter_note, reason_codes_json, created_at
            ) VALUES (?, '005930', 'sector', 'Tech', 8, 8, 8, 1.0, 1.0, 1.5, 12,
                30, 35, 'thin_sample_no_winsorization', '[]', ?)
            """,
            [as_of, created],
        )
        connection.execute(
            """
            INSERT INTO fact_valuation_label_snapshot (
                as_of_date, symbol, valuation_label, confidence_pass, hard_gate_reasons_json,
                annotations_json, source_notes_json, created_at
            ) VALUES (
                ?, '005930', '저평가', TRUE, '[]',
                '["internal_calculated_from_disclosure"]', '{}', ?
            )
            """,
            [as_of, created],
        )

        frame = read_store._valuation_snapshot_frame(connection, as_of_date=as_of)
        rows = read_store._build_stock_valuation_rows(
            frame,
            built_at="2026-03-06T12:00:00+09:00",
            as_of_date="2026-03-06",
            source_run_id="test",
        )

    assert len(rows) == 1
    assert rows[0]["snapshot_type"] == "stock_valuation"
    assert rows[0]["symbol"] == "005930"
    assert "저평가" in rows[0]["summary"]
    payload = json.loads(rows[0]["payload_json"])
    assert payload["metrics"]["per"]["value"] == 10.0
    assert payload["peer"]["peer_count"] == 8
    assert payload["evaluation_basis"] == "PER/PBR 동종업계 percentile 비교"
