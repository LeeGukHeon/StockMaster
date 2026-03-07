from __future__ import annotations

import json
from datetime import date, datetime

from app.common.paths import project_root
from app.features.feature_store import build_feature_store
from app.ranking.explanatory_score import materialize_explanatory_ranking
from app.regime.snapshot import build_market_regime_snapshot
from app.selection.engine_v1 import materialize_selection_engine_v1
from app.settings import Settings, load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection

TRADING_DATES = [
    date(2026, 2, 26),
    date(2026, 2, 27),
    date(2026, 3, 2),
    date(2026, 3, 3),
    date(2026, 3, 4),
    date(2026, 3, 5),
    date(2026, 3, 6),
    date(2026, 3, 9),
    date(2026, 3, 10),
    date(2026, 3, 11),
    date(2026, 3, 12),
    date(2026, 3, 13),
]

SYMBOLS = [
    {
        "symbol": "005930",
        "company_name": "SamsungElec",
        "market": "KOSPI",
        "dart_corp_code": "00126380",
    },
    {
        "symbol": "000660",
        "company_name": "SKHynix",
        "market": "KOSPI",
        "dart_corp_code": "00164779",
    },
    {
        "symbol": "123456",
        "company_name": "KosdaqAlpha",
        "market": "KOSDAQ",
        "dart_corp_code": "00999991",
    },
    {
        "symbol": "123457",
        "company_name": "KosdaqBeta",
        "market": "KOSDAQ",
        "dart_corp_code": "00999992",
    },
]

_CLOSE_SERIES = {
    "005930": [94, 98, 100, 103, 107, 110, 116, 120, 122, 124, 127, 130],
    "000660": [78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89],
    "123456": [47, 48, 50, 51, 52, 53, 55, 57, 58, 59, 60, 61],
    "123457": [63, 62, 60, 58, 55, 54, 53, 52, 51, 50, 49, 48],
}
_VOLUME_SERIES = {
    "005930": [
        1_000_000,
        1_050_000,
        1_100_000,
        1_200_000,
        1_250_000,
        1_350_000,
        1_500_000,
        1_550_000,
        1_600_000,
        1_650_000,
        1_700_000,
        1_760_000,
    ],
    "000660": [
        750_000,
        760_000,
        780_000,
        790_000,
        810_000,
        820_000,
        830_000,
        840_000,
        850_000,
        860_000,
        870_000,
        880_000,
    ],
    "123456": [
        420_000,
        430_000,
        500_000,
        540_000,
        560_000,
        580_000,
        610_000,
        640_000,
        660_000,
        680_000,
        700_000,
        720_000,
    ],
    "123457": [
        360_000,
        355_000,
        340_000,
        335_000,
        320_000,
        315_000,
        310_000,
        300_000,
        295_000,
        290_000,
        285_000,
        280_000,
    ],
}
_MARKET_CAP_MULTIPLIER = {
    "005930": 1_000_000_000,
    "000660": 850_000_000,
    "123456": 180_000_000,
    "123457": 120_000_000,
}
_OPEN_FACTOR = {
    "005930": 0.99,
    "000660": 0.985,
    "123456": 0.995,
    "123457": 1.005,
}
_FLOW_VALUE_SERIES = {
    "005930": [
        2.0e10,
        2.2e10,
        2.4e10,
        2.8e10,
        3.1e10,
        3.3e10,
        3.6e10,
        3.8e10,
        4.0e10,
        4.2e10,
        4.4e10,
        4.6e10,
    ],
    "000660": [
        8.0e9,
        8.5e9,
        9.0e9,
        9.2e9,
        9.4e9,
        9.6e9,
        9.8e9,
        1.0e10,
        1.02e10,
        1.04e10,
        1.06e10,
        1.08e10,
    ],
    "123456": [3.0e9, 3.1e9, 3.4e9, 3.7e9, 3.8e9, 4.0e9, 4.2e9, 4.4e9, 4.5e9, 4.6e9, 4.7e9, 4.8e9],
    "123457": [
        -2.5e9,
        -2.7e9,
        -2.9e9,
        -3.0e9,
        -3.1e9,
        -3.2e9,
        -3.3e9,
        -3.4e9,
        -3.5e9,
        -3.6e9,
        -3.7e9,
        -3.8e9,
    ],
}


def build_test_settings(tmp_path) -> Settings:
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
    return settings


def seed_ticket003_data(settings: Settings) -> None:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        connection.executemany(
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
            VALUES (?, ?, ?, TRUE, ?, 'test', now())
            """,
            [
                (
                    symbol_row["symbol"],
                    symbol_row["company_name"],
                    symbol_row["market"],
                    symbol_row["dart_corp_code"],
                )
                for symbol_row in SYMBOLS
            ],
        )

        connection.executemany(
            """
            INSERT INTO dim_trading_calendar (
                trading_date,
                is_trading_day,
                market_session_type,
                weekday,
                is_weekend,
                is_public_holiday,
                source,
                source_confidence,
                is_override,
                updated_at
            )
            VALUES (?, TRUE, 'regular', ?, FALSE, FALSE, 'test', 'high', FALSE, now())
            """,
            [(trading_date, trading_date.weekday()) for trading_date in TRADING_DATES],
        )

        price_rows: list[tuple[object, ...]] = []
        for symbol_row in SYMBOLS:
            symbol = symbol_row["symbol"]
            for index, trading_date in enumerate(TRADING_DATES):
                close = float(_CLOSE_SERIES[symbol][index])
                open_price = round(close * _OPEN_FACTOR[symbol], 4)
                high = round(max(open_price, close) * 1.02, 4)
                low = round(min(open_price, close) * 0.98, 4)
                volume = int(_VOLUME_SERIES[symbol][index])
                turnover_value = float(close * volume)
                market_cap = float(close * _MARKET_CAP_MULTIPLIER[symbol])
                price_rows.append(
                    (
                        trading_date,
                        symbol,
                        open_price,
                        high,
                        low,
                        close,
                        volume,
                        turnover_value,
                        market_cap,
                    )
                )
        connection.executemany(
            """
            INSERT INTO fact_daily_ohlcv (
                trading_date,
                symbol,
                open,
                high,
                low,
                close,
                volume,
                turnover_value,
                market_cap,
                source,
                source_notes_json,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'test', NULL, now())
            """,
            price_rows,
        )

        fundamentals_rows = [
            (
                date(2026, 3, 2),
                "005930",
                2025,
                "11011",
                1000.0,
                180.0,
                140.0,
                0.18,
                0.40,
                0.18,
                "doc-005930",
                datetime.fromisoformat("2026-03-01T18:00:00+09:00"),
            ),
            (
                date(2026, 3, 2),
                "000660",
                2025,
                "11011",
                920.0,
                120.0,
                95.0,
                0.13,
                0.62,
                0.13,
                "doc-000660",
                datetime.fromisoformat("2026-03-01T18:05:00+09:00"),
            ),
            (
                date(2026, 3, 2),
                "123456",
                2025,
                "11011",
                510.0,
                76.0,
                55.0,
                0.16,
                0.48,
                0.15,
                "doc-123456",
                datetime.fromisoformat("2026-03-01T18:10:00+09:00"),
            ),
            (
                date(2026, 3, 2),
                "123457",
                2025,
                "11011",
                410.0,
                14.0,
                6.0,
                0.03,
                1.40,
                0.03,
                "doc-123457",
                datetime.fromisoformat("2026-03-01T18:15:00+09:00"),
            ),
        ]
        connection.executemany(
            """
            INSERT INTO fact_fundamentals_snapshot (
                as_of_date,
                symbol,
                fiscal_year,
                report_code,
                revenue,
                operating_income,
                net_income,
                roe,
                debt_ratio,
                operating_margin,
                source_doc_id,
                source,
                disclosed_at,
                statement_basis,
                report_name,
                currency,
                accounting_standard,
                source_notes_json,
                ingested_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'test', ?, 'CFS', 'annual', 'KRW',
                'K-IFRS', NULL, now()
            )
            """,
            fundamentals_rows,
        )

        news_rows = [
            (
                "news-005930-1",
                date(2026, 3, 6),
                "2026-03-06T09:00:00+09:00",
                ["005930"],
                "earnings",
                "SamsungElec demand improves",
                "news.example",
                "https://example.com/samsung-demand",
                ["earnings"],
                0.9,
                {"005930": "name_exact"},
                "focus",
            ),
            (
                "news-005930-2",
                date(2026, 3, 5),
                "2026-03-05T08:30:00+09:00",
                ["005930"],
                "semiconductor",
                "SamsungElec capacity expansion",
                "market.example",
                "https://example.com/samsung-capacity",
                ["capex"],
                0.6,
                {"005930": "query_context_exact"},
                "focus",
            ),
            (
                "news-000660-1",
                date(2026, 3, 5),
                "2026-03-05T07:40:00+09:00",
                ["000660"],
                "memory",
                "SKHynix shipment steady",
                "market.example",
                "https://example.com/sk-steady",
                ["supply"],
                0.3,
                {"000660": "name_exact"},
                "focus",
            ),
            (
                "news-123456-1",
                date(2026, 3, 4),
                "2026-03-04T13:10:00+09:00",
                ["123456"],
                "platform",
                "KosdaqAlpha user metrics rise",
                "tech.example",
                "https://example.com/alpha-users",
                ["users"],
                0.7,
                {"123456": "name_exact"},
                "focus",
            ),
            (
                "news-123457-1",
                date(2026, 3, 5),
                "2026-03-05T11:20:00+09:00",
                ["123457"],
                "rates",
                "KosdaqBeta pressured by rates",
                "macro.example",
                "https://example.com/beta-rates",
                ["rates"],
                -0.4,
                {"123457": "name_exact"},
                "focus",
            ),
        ]
        connection.executemany(
            """
            INSERT INTO fact_news_item (
                news_id,
                signal_date,
                published_at,
                symbol_candidates,
                query_keyword,
                title,
                publisher,
                link,
                snippet,
                tags_json,
                catalyst_score,
                sentiment_score,
                freshness_score,
                source,
                canonical_link,
                match_method_json,
                query_bucket,
                is_market_wide,
                source_notes_json,
                ingested_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, NULL, NULL, 'test', ?, ?, ?,
                FALSE, NULL, now()
            )
            """,
            [
                (
                    news_id,
                    signal_date,
                    published_at,
                    json.dumps(symbol_candidates),
                    query_keyword,
                    title,
                    publisher,
                    link,
                    json.dumps(tags),
                    catalyst_score,
                    link,
                    json.dumps(match_method),
                    query_bucket,
                )
                for (
                    news_id,
                    signal_date,
                    published_at,
                    symbol_candidates,
                    query_keyword,
                    title,
                    publisher,
                    link,
                    tags,
                    catalyst_score,
                    match_method,
                    query_bucket,
                ) in news_rows
            ],
        )


def seed_ticket004_flow_data(settings: Settings) -> None:
    with duckdb_connection(settings.paths.duckdb_path) as connection:
        flow_rows: list[tuple[object, ...]] = []
        for symbol_row in SYMBOLS:
            symbol = symbol_row["symbol"]
            market = symbol_row["market"]
            for index, trading_date in enumerate(TRADING_DATES):
                foreign_value = float(_FLOW_VALUE_SERIES[symbol][index])
                institution_value = round(foreign_value * 0.55, 4)
                individual_value = round(-(foreign_value + institution_value) * 0.85, 4)
                flow_rows.append(
                    (
                        "test-flow-run",
                        trading_date,
                        symbol,
                        market,
                        foreign_value / 1000.0,
                        institution_value / 1000.0,
                        individual_value / 1000.0,
                        foreign_value,
                        institution_value,
                        individual_value,
                    )
                )
        connection.executemany(
            """
            INSERT INTO fact_investor_flow (
                run_id,
                trading_date,
                symbol,
                market,
                foreign_net_volume,
                institution_net_volume,
                individual_net_volume,
                foreign_net_value,
                institution_net_value,
                individual_net_value,
                source,
                source_notes_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'test', NULL, now())
            """,
            flow_rows,
        )


def seed_ticket005_selection_history(
    settings: Settings,
    *,
    selection_dates: list[date] | None = None,
    limit_symbols: int = 4,
) -> list[date]:
    effective_dates = selection_dates or [
        date(2026, 3, 2),
        date(2026, 3, 3),
        date(2026, 3, 4),
        date(2026, 3, 5),
        date(2026, 3, 6),
    ]
    for as_of_date in effective_dates:
        build_feature_store(settings, as_of_date=as_of_date, limit_symbols=limit_symbols)
        build_market_regime_snapshot(settings, as_of_date=as_of_date)
        materialize_explanatory_ranking(
            settings,
            as_of_date=as_of_date,
            horizons=[1, 5],
            limit_symbols=limit_symbols,
        )
        materialize_selection_engine_v1(
            settings,
            as_of_date=as_of_date,
            horizons=[1, 5],
            limit_symbols=limit_symbols,
        )
    return effective_dates
