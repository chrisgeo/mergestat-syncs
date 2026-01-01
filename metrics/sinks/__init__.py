"""
Sink implementations for writing derived metrics.

Sinks persist derived metrics data to various backends:
- ClickHouse: append-only analytics store (primary)
- MongoDB: document store with idempotent upserts
- SQLite: file-based relational store
- PostgreSQL: production relational store

Usage:
    from metrics.sinks import create_sink, BaseMetricsSink

    sink = create_sink("clickhouse://localhost:8123/default")
    sink.ensure_schema()
    sink.write_repo_metrics(rows)
    sink.close()
"""

from metrics.sinks.base import BaseMetricsSink
from metrics.sinks.factory import SinkBackend, create_sink, detect_backend
from metrics.sinks.clickhouse import ClickHouseMetricsSink
from metrics.sinks.mongo import MongoMetricsSink
from metrics.sinks.sqlite import SQLiteMetricsSink
from metrics.sinks.postgres import PostgresMetricsSink

__all__ = [
    "BaseMetricsSink",
    "SinkBackend",
    "create_sink",
    "detect_backend",
    "ClickHouseMetricsSink",
    "MongoMetricsSink",
    "SQLiteMetricsSink",
    "PostgresMetricsSink",
]
