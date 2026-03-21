from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Iterator

import pandas as pd

try:
    import psycopg
    from psycopg import sql
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local dev
    psycopg = None
    sql = None

from app.settings import Settings
from app.storage.duckdb import bootstrap_core_tables, duckdb_connection
from app.storage.metadata_schema import METADATA_TABLES, postgres_metadata_ddl

_BOOTSTRAPPED_POSTGRES_METADATA_KEYS: set[tuple[str, str]] = set()
_BOOTSTRAPPED_POSTGRES_METADATA_LOCK = Lock()


def metadata_postgres_enabled(settings: Settings) -> bool:
    return bool(settings.metadata.enabled and settings.metadata.backend == "postgres")


def _postgres_metadata_cache_key(settings: Settings) -> tuple[str, str]:
    return (settings.metadata.db_url or "", settings.metadata.db_schema)


@contextmanager
def postgres_metadata_connection(settings: Settings) -> Iterator[psycopg.Connection]:
    if not metadata_postgres_enabled(settings):
        raise RuntimeError("Postgres metadata store is not enabled in settings.")
    if psycopg is None or sql is None:
        raise RuntimeError("psycopg is required when postgres metadata store is enabled.")
    assert settings.metadata.db_url
    connection = psycopg.connect(settings.metadata.db_url)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET search_path TO {}").format(
                    sql.Identifier(settings.metadata.db_schema)
                )
            )
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
    with _BOOTSTRAPPED_POSTGRES_METADATA_LOCK:
        _BOOTSTRAPPED_POSTGRES_METADATA_KEYS.add(_postgres_metadata_cache_key(settings))


def ensure_postgres_metadata_store(settings: Settings, *, force: bool = False) -> None:
    if not metadata_postgres_enabled(settings):
        return
    cache_key = _postgres_metadata_cache_key(settings)
    with _BOOTSTRAPPED_POSTGRES_METADATA_LOCK:
        if not force and cache_key in _BOOTSTRAPPED_POSTGRES_METADATA_KEYS:
            return
    bootstrap_postgres_metadata_store(settings)


def _frame_from_duckdb(settings: Settings, table_name: str) -> pd.DataFrame:
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        return connection.execute(f"SELECT * FROM {table_name}").fetchdf()


def duckdb_metadata_row_counts(
    settings: Settings,
    *,
    tables: tuple[str, ...] = METADATA_TABLES,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    with duckdb_connection(settings.paths.duckdb_path, read_only=True) as connection:
        bootstrap_core_tables(connection)
        for table_name in tables:
            row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            counts[table_name] = int(row[0]) if row is not None and row[0] is not None else 0
    return counts


def postgres_metadata_row_counts(
    settings: Settings,
    *,
    tables: tuple[str, ...] = METADATA_TABLES,
) -> dict[str, int]:
    if not metadata_postgres_enabled(settings):
        raise RuntimeError("Postgres metadata store is not enabled in settings.")
    bootstrap_postgres_metadata_store(settings)
    counts: dict[str, int] = {}
    for table_name in tables:
        row = fetchone_postgres_sql(settings, f"SELECT COUNT(*) FROM {table_name}")
        counts[table_name] = int(row[0]) if row is not None and row[0] is not None else 0
    return counts


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


def _postgres_query(query: str) -> str:
    return query.replace("?", "%s")


def execute_postgres_sql(settings: Settings, query: str, params: list | tuple | None = None) -> None:
    if not metadata_postgres_enabled(settings):
        return
    ensure_postgres_metadata_store(settings)
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(_postgres_query(query), list(params or []))


def executemany_postgres_sql(
    settings: Settings,
    query: str,
    rows: list[list | tuple],
) -> None:
    if not metadata_postgres_enabled(settings) or not rows:
        return
    ensure_postgres_metadata_store(settings)
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(_postgres_query(query), rows)


def fetchone_postgres_sql(
    settings: Settings,
    query: str,
    params: list | tuple | None = None,
):
    if not metadata_postgres_enabled(settings):
        return None
    ensure_postgres_metadata_store(settings)
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(_postgres_query(query), list(params or []))
            return cursor.fetchone()


def fetchdf_postgres_sql(
    settings: Settings,
    query: str,
    params: list | tuple | None = None,
) -> pd.DataFrame:
    if not metadata_postgres_enabled(settings):
        return pd.DataFrame()
    ensure_postgres_metadata_store(settings)
    with postgres_metadata_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(_postgres_query(query), list(params or []))
            rows = cursor.fetchall()
            columns = [item.name for item in cursor.description] if cursor.description else []
    return pd.DataFrame(rows, columns=columns)


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
