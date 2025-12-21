from __future__ import annotations

from sqlalchemy import inspect

from metrics.sinks.sqlite import SQLiteMetricsSink


def _normalize_postgres_url(db_url: str) -> str:
    if "postgresql+asyncpg://" in db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


class PostgresMetricsSink(SQLiteMetricsSink):
    """Postgres sink for derived daily metrics (idempotent upserts by primary key)."""

    def __init__(self, db_url: str) -> None:
        super().__init__(_normalize_postgres_url(db_url))

    @staticmethod
    def _table_has_column(conn, table: str, column: str) -> bool:
        try:
            columns = inspect(conn).get_columns(table)
        except Exception:
            return False
        return column in {col.get("name") for col in columns}
