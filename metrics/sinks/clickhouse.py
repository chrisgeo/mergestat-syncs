from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import List, Optional, Sequence

import clickhouse_connect

from metrics.schemas import CommitMetricsRecord, RepoMetricsDailyRecord, UserMetricsDailyRecord


def _dt_to_clickhouse_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


class ClickHouseMetricsSink:
    """
    ClickHouse sink for derived daily metrics.

    This sink is append-only: re-computations insert new rows with a newer
    `computed_at`. Queries can select the latest version via `argMax`.
    """

    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("ClickHouse DSN is required")
        self.dsn = dsn
        self.client = clickhouse_connect.get_client(dsn=dsn)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

    def ensure_tables(self) -> None:
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS repo_metrics_daily (
                repo_id UUID,
                day Date,
                commits_count UInt32,
                total_loc_touched UInt32,
                avg_commit_size_loc Float64,
                large_commit_ratio Float64,
                prs_merged UInt32,
                median_pr_cycle_hours Float64,
                computed_at DateTime('UTC')
            ) ENGINE MergeTree
            PARTITION BY toYYYYMM(day)
            ORDER BY (repo_id, day)
            """,
            """
            CREATE TABLE IF NOT EXISTS user_metrics_daily (
                repo_id UUID,
                day Date,
                author_email String,
                commits_count UInt32,
                loc_added UInt32,
                loc_deleted UInt32,
                files_changed UInt32,
                large_commits_count UInt32,
                avg_commit_size_loc Float64,
                prs_authored UInt32,
                prs_merged UInt32,
                avg_pr_cycle_hours Float64,
                median_pr_cycle_hours Float64,
                review_response_count UInt32,
                avg_review_response_hours Float64,
                median_review_response_hours Float64,
                computed_at DateTime('UTC')
            ) ENGINE MergeTree
            PARTITION BY toYYYYMM(day)
            ORDER BY (repo_id, author_email, day)
            """,
            """
            CREATE TABLE IF NOT EXISTS commit_metrics (
                repo_id UUID,
                commit_hash String,
                day Date,
                author_email String,
                total_loc UInt32,
                files_changed UInt32,
                size_bucket LowCardinality(String),
                computed_at DateTime('UTC')
            ) ENGINE MergeTree
            PARTITION BY toYYYYMM(day)
            ORDER BY (repo_id, day, author_email, commit_hash)
            """,
        ]
        for stmt in stmts:
            self.client.command(stmt)

        # Forward-compatible columns (older tables may exist without these).
        alter_stmts = [
            "ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS review_response_count UInt32",
            "ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS avg_review_response_hours Float64",
            "ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS median_review_response_hours Float64",
        ]
        for stmt in alter_stmts:
            self.client.command(stmt)

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "repo_metrics_daily",
            [
                "repo_id",
                "day",
                "commits_count",
                "total_loc_touched",
                "avg_commit_size_loc",
                "large_commit_ratio",
                "prs_merged",
                "median_pr_cycle_hours",
                "computed_at",
            ],
            rows,
        )

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "user_metrics_daily",
            [
                "repo_id",
                "day",
                "author_email",
                "commits_count",
                "loc_added",
                "loc_deleted",
                "files_changed",
                "large_commits_count",
                "avg_commit_size_loc",
                "prs_authored",
                "prs_merged",
                "avg_pr_cycle_hours",
                "median_pr_cycle_hours",
                "review_response_count",
                "avg_review_response_hours",
                "median_review_response_hours",
                "computed_at",
            ],
            rows,
        )

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "commit_metrics",
            [
                "repo_id",
                "commit_hash",
                "day",
                "author_email",
                "total_loc",
                "files_changed",
                "size_bucket",
                "computed_at",
            ],
            rows,
        )

    def _insert_rows(self, table: str, columns: List[str], rows: Sequence[object]) -> None:
        matrix = []
        for row in rows:
            data = asdict(row)
            values = []
            for col in columns:
                value = data.get(col)
                if isinstance(value, datetime):
                    value = _dt_to_clickhouse_datetime(value)
                values.append(value)
            matrix.append(values)
        self.client.insert(table, matrix, column_names=columns)

    # Query helpers (useful for Grafana and validation)
    def latest_repo_metrics_query(
        self, *, repo_id: Optional[str] = None, start_day: Optional[date] = None, end_day: Optional[date] = None
    ) -> str:
        where = []
        if repo_id:
            where.append(f"repo_id = toUUID('{repo_id}')")
        if start_day:
            where.append(f"day >= toDate('{start_day.isoformat()}')")
        if end_day:
            where.append(f"day < toDate('{end_day.isoformat()}')")
        where_clause = ("WHERE " + " AND ".join(where)) if where else ""
        return f"""
        SELECT
          repo_id,
          day,
          argMax(commits_count, computed_at) AS commits_count,
          argMax(total_loc_touched, computed_at) AS total_loc_touched,
          argMax(avg_commit_size_loc, computed_at) AS avg_commit_size_loc,
          argMax(large_commit_ratio, computed_at) AS large_commit_ratio,
          argMax(prs_merged, computed_at) AS prs_merged,
          argMax(median_pr_cycle_hours, computed_at) AS median_pr_cycle_hours,
          max(computed_at) AS computed_at
        FROM repo_metrics_daily
        {where_clause}
        GROUP BY repo_id, day
        ORDER BY repo_id, day
        """

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
