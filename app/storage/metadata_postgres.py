from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd
import psycopg
from psycopg import sql

from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.metadata_schema import METADATA_TABLES, postgres_metadata_ddl


def metadata_postgres_enabled(settings: Settings) -> bool:
    return bool(settings.metadata.enabled and settings.metadata.backend == "postgres")


@contextmanager
def postgres_metadata_connection(settings: Settings) -> Iterator[psycopg.Connection]:
    if not metadata_postgres_enabled(settings):
        raise RuntimeError("Postgres metadata store is not enabled in settings.")
    assert settings.metadata.db_url
    connection = psycopg.connect(settings.metadata.db_url)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def bootstrap_postgres_metadata_store(settings: Settings) -> None:
    if not metadata_postgres_enabled(settings):
        return
    schema = settings.metadata.db_schema
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            for ddl in postgres_metadata_ddl(schema):
                cursor.execute(ddl)


def _frame_from_duckdb(settings: Settings, table_name: str) -> pd.DataFrame:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(f"SELECT * FROM {table_name}").fetchdf()


def _normalize_value(value):
    if pd.isna(value):
        return None
    return value


def copy_duckdb_metadata_to_postgres(
    settings: Settings,
    *,
    tables: tuple[str, ...] = METADATA_TABLES,
    truncate_first: bool = False,
) -> dict[str, int]:
    if not metadata_postgres_enabled(settings):
        raise RuntimeError("Postgres metadata store is not enabled in settings.")
    bootstrap_postgres_metadata_store(settings)
    schema = settings.metadata.db_schema
    results: dict[str, int] = {}
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            for table_name in tables:
                frame = _frame_from_duckdb(settings, table_name)
                results[table_name] = int(len(frame))
                if truncate_first:
                    cursor.execute(
                        sql.SQL("TRUNCATE TABLE {}").format(
                            sql.Identifier(schema, table_name)
                        )
                    )
                if frame.empty:
                    continue
                columns = list(frame.columns)
                placeholders = sql.SQL(", ").join(sql.SQL("%s") for _ in columns)
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(schema, table_name),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    placeholders,
                )
                rows = [
                    tuple(_normalize_value(value) for value in row)
                    for row in frame.itertuples(index=False, name=None)
                ]
                cursor.executemany(query, rows)
    return results


def export_duckdb_metadata_snapshot(
    settings: Settings,
    *,
    output_dir: Path,
    tables: tuple[str, ...] = METADATA_TABLES,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths: list[Path] = []
    for table_name in tables:
        frame = _frame_from_duckdb(settings, table_name)
        path = output_dir / f"{table_name}.parquet"
        frame.to_parquet(path, index=False)
        artifact_paths.append(path)
    return artifact_paths
