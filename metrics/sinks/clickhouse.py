from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

import clickhouse_connect
import logging
from metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
)

logger = logging.getLogger(__name__)

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
        except Exception as e:
            logger.warning("Exception occurred when closing ClickHouse client: %s", e, exc_info=True)

    def _apply_sql_migrations(self) -> None:
        migrations_dir = Path(__file__).resolve().parents[2] / "migrations" / "clickhouse"
        if not migrations_dir.exists():
            return

        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8")
            # Very small splitter: migrations are expected to contain only DDL.
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                self.client.command(stmt)

    def ensure_tables(self) -> None:
        self._apply_sql_migrations()

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
                "pr_cycle_p75_hours",
                "pr_cycle_p90_hours",
                "prs_with_first_review",
                "pr_first_review_p50_hours",
                "pr_first_review_p90_hours",
                "pr_review_time_p50_hours",
                "pr_pickup_time_p50_hours",
                "large_pr_ratio",
                "pr_rework_ratio",
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
                "pr_cycle_p75_hours",
                "pr_cycle_p90_hours",
                "prs_with_first_review",
                "pr_first_review_p50_hours",
                "pr_first_review_p90_hours",
                "pr_review_time_p50_hours",
                "pr_pickup_time_p50_hours",
                "reviews_given",
                "changes_requested_given",
                "team_id",
                "team_name",
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

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "team_metrics_daily",
            [
                "day",
                "team_id",
                "team_name",
                "commits_count",
                "after_hours_commits_count",
                "weekend_commits_count",
                "after_hours_commit_ratio",
                "weekend_commit_ratio",
                "computed_at",
            ],
            rows,
        )

    def write_work_item_metrics(self, rows: Sequence[WorkItemMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_metrics_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "team_id",
                "team_name",
                "items_started",
                "items_completed",
                "items_started_unassigned",
                "items_completed_unassigned",
                "wip_count_end_of_day",
                "wip_unassigned_end_of_day",
                "cycle_time_p50_hours",
                "cycle_time_p90_hours",
                "lead_time_p50_hours",
                "lead_time_p90_hours",
                "wip_age_p50_hours",
                "wip_age_p90_hours",
                "bug_completed_ratio",
                "story_points_completed",
                "computed_at",
            ],
            rows,
        )

    def write_work_item_user_metrics(self, rows: Sequence[WorkItemUserMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_user_metrics_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "user_identity",
                "team_id",
                "team_name",
                "items_started",
                "items_completed",
                "wip_count_end_of_day",
                "cycle_time_p50_hours",
                "cycle_time_p90_hours",
                "computed_at",
            ],
            rows,
        )

    def write_work_item_cycle_times(self, rows: Sequence[WorkItemCycleTimeRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_cycle_times",
            [
                "work_item_id",
                "provider",
                "day",
                "work_scope_id",
                "team_id",
                "team_name",
                "assignee",
                "type",
                "status",
                "created_at",
                "started_at",
                "completed_at",
                "cycle_time_hours",
                "lead_time_hours",
                "computed_at",
            ],
            rows,
        )

    def write_work_item_state_durations(self, rows: Sequence[WorkItemStateDurationDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_state_durations_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "team_id",
                "team_name",
                "status",
                "duration_hours",
                "items_touched",
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
