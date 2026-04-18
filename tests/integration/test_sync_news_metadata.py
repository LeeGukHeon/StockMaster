from __future__ import annotations

from datetime import date

from app.common.paths import project_root
from app.pipelines.news_metadata import sync_news_metadata
from app.settings import load_settings
from app.storage.bootstrap import bootstrap_storage
from app.storage.duckdb import duckdb_connection


class FakeNaverProvider:
    def search_news(self, *, query: str, limit: int = 10, start: int = 1, sort: str = "date"):
        return {
            "items": [
                {
                    "title_plain": "삼성전자 반도체 투자 확대",
                    "description_plain": "반도체 업황이 개선되고 있다.",
                    "originallink": "https://example.com/article-1",
                    "link": "https://search.naver.com/article-1",
                    "pubDate": "Fri, 06 Mar 2026 09:00:00 +0900",
                }
            ]
        }


def test_sync_news_metadata_populates_fact_table(tmp_path):
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
                source,
                updated_at
            )
            VALUES ('005930', '삼성전자', 'KOSPI', TRUE, 'test', now())
            """
        )

    result = sync_news_metadata(
        settings,
        signal_date=date(2026, 3, 6),
        mode="symbol_list",
        symbols=["005930"],
        naver_provider=FakeNaverProvider(),
    )

    assert result.deduped_row_count == 1

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM fact_news_item").fetchone()[0]
        assert row_count == 1


def test_sync_news_metadata_reuses_existing_rows_when_no_new_items_materialize(tmp_path):
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
                source,
                updated_at
            )
            VALUES ('005930', '삼성전자', 'KOSPI', TRUE, 'test', now())
            """
        )

    first = sync_news_metadata(
        settings,
        signal_date=date(2026, 3, 6),
        mode="symbol_list",
        symbols=["005930"],
        naver_provider=FakeNaverProvider(),
    )
    second = sync_news_metadata(
        settings,
        signal_date=date(2026, 3, 6),
        mode="symbol_list",
        symbols=["005930"],
        naver_provider=FakeNaverProvider(),
    )

    assert first.deduped_row_count == 1
    assert second.deduped_row_count == 0
    assert "existing_rows_reused=1" in second.notes

    with duckdb_connection(settings.paths.duckdb_path) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM fact_news_item").fetchone()[0]
        assert row_count == 1
