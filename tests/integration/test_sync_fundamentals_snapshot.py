from __future__ import annotations

from datetime import date

import pandas as pd

from app.common.paths import project_root
from app.pipelines.fundamentals_snapshot import sync_fundamentals_snapshot
from app.providers.dart.financials import DartDisclosureSnapshot, DartFinancialStatementSnapshot
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


class FakeDartProvider:
    def fetch_regular_disclosures(
        self,
        *,
        corp_code: str,
        start_date,
        end_date,
        page_count: int = 100,
    ):
        frame = pd.DataFrame(
            [
                {
                    "rcept_no": "20251114000001",
                    "rcept_dt": date(2025, 11, 14),
                    "reprt_code": "11014",
                    "fiscal_year": 2025,
                    "report_name_clean": "분기보고서 (2025.09)",
                    "report_type_name": "분기보고서",
                }
            ]
        )
        return DartDisclosureSnapshot(frame=frame, payload={})

    def fetch_financial_statement(
        self,
        *,
        corp_code: str,
        bsns_year: int,
        reprt_code: str,
        fs_div: str,
    ):
        frame = pd.DataFrame(
            [
                {
                    "sj_div": "BS",
                    "account_nm": "자본총계",
                    "thstrm_amount": "1000",
                    "ord": "1",
                    "currency": "KRW",
                },
                {
                    "sj_div": "BS",
                    "account_nm": "부채총계",
                    "thstrm_amount": "500",
                    "ord": "2",
                    "currency": "KRW",
                },
                {
                    "sj_div": "IS",
                    "account_nm": "매출액",
                    "thstrm_amount": "400",
                    "ord": "3",
                    "currency": "KRW",
                },
                {
                    "sj_div": "IS",
                    "account_nm": "영업이익",
                    "thstrm_amount": "80",
                    "ord": "4",
                    "currency": "KRW",
                },
                {
                    "sj_div": "IS",
                    "account_nm": "당기순이익",
                    "thstrm_amount": "50",
                    "ord": "5",
                    "currency": "KRW",
                },
            ]
        )
        return DartFinancialStatementSnapshot(frame=frame, payload={"status": "000"})


def test_sync_fundamentals_snapshot_populates_fact_table(tmp_path):
    data_dir = tmp_path / "data"
    duckdb_path = data_dir / "marts" / "integration.duckdb"
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

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.execute(
            """
            INSERT INTO dim_symbol (
                symbol,
                company_name,
                market,
                is_common_stock,
                dart_corp_code,
                source,
                updated_at
            )
            VALUES ('005930', '삼성전자', 'KOSPI', TRUE, '00126380', 'test', now())
            """
        )

    result = sync_fundamentals_snapshot(
        settings,
        as_of_date=date(2026, 3, 6),
        limit_symbols=1,
        dart_provider=FakeDartProvider(),
    )

    assert result.row_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row = connection.execute(
            """
            SELECT report_code, revenue, operating_income
            FROM fact_fundamentals_snapshot
            WHERE symbol = '005930'
            """
        ).fetchone()
        assert row == ("11014", 400.0, 80.0)
