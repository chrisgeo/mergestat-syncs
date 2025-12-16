from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from metrics.schemas import CommitMetricsRecord, RepoMetricsDailyRecord, UserMetricsDailyRecord


def _dt_to_sqlite(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat()
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat()


class SQLiteMetricsSink:
    """SQLite sink for derived daily metrics (idempotent upserts by primary key)."""

    def __init__(self, db_url: str) -> None:
        if not db_url:
            raise ValueError("SQLite DB URL is required")
        if "sqlite+aiosqlite://" in db_url:
            db_url = db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
        self.engine: Engine = create_engine(db_url, echo=False)

    def close(self) -> None:
        self.engine.dispose()

    def ensure_tables(self) -> None:
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS repo_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              total_loc_touched INTEGER NOT NULL,
              avg_commit_size_loc REAL NOT NULL,
              large_commit_ratio REAL NOT NULL,
              prs_merged INTEGER NOT NULL,
              median_pr_cycle_hours REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              author_email TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              loc_added INTEGER NOT NULL,
              loc_deleted INTEGER NOT NULL,
              files_changed INTEGER NOT NULL,
              large_commits_count INTEGER NOT NULL,
              avg_commit_size_loc REAL NOT NULL,
              prs_authored INTEGER NOT NULL,
              prs_merged INTEGER NOT NULL,
              avg_pr_cycle_hours REAL NOT NULL,
              median_pr_cycle_hours REAL NOT NULL,
              review_response_count INTEGER NOT NULL,
              avg_review_response_hours REAL NOT NULL,
              median_review_response_hours REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, author_email, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS commit_metrics (
              repo_id TEXT NOT NULL,
              commit_hash TEXT NOT NULL,
              day TEXT NOT NULL,
              author_email TEXT NOT NULL,
              total_loc INTEGER NOT NULL,
              files_changed INTEGER NOT NULL,
              size_bucket TEXT NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day, author_email, commit_hash)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_repo_metrics_daily_day ON repo_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_user_metrics_daily_day ON user_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_commit_metrics_day ON commit_metrics(day)",
        ]
        with self.engine.begin() as conn:
            for stmt in stmts:
                conn.execute(text(stmt))

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO repo_metrics_daily (
              repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
              large_commit_ratio, prs_merged, median_pr_cycle_hours, computed_at
            ) VALUES (
              :repo_id, :day, :commits_count, :total_loc_touched, :avg_commit_size_loc,
              :large_commit_ratio, :prs_merged, :median_pr_cycle_hours, :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              commits_count=excluded.commits_count,
              total_loc_touched=excluded.total_loc_touched,
              avg_commit_size_loc=excluded.avg_commit_size_loc,
              large_commit_ratio=excluded.large_commit_ratio,
              prs_merged=excluded.prs_merged,
              median_pr_cycle_hours=excluded.median_pr_cycle_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._repo_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO user_metrics_daily (
              repo_id, day, author_email, commits_count, loc_added, loc_deleted,
              files_changed, large_commits_count, avg_commit_size_loc,
              prs_authored, prs_merged, avg_pr_cycle_hours, median_pr_cycle_hours,
              review_response_count, avg_review_response_hours, median_review_response_hours,
              computed_at
            ) VALUES (
              :repo_id, :day, :author_email, :commits_count, :loc_added, :loc_deleted,
              :files_changed, :large_commits_count, :avg_commit_size_loc,
              :prs_authored, :prs_merged, :avg_pr_cycle_hours, :median_pr_cycle_hours,
              :review_response_count, :avg_review_response_hours, :median_review_response_hours,
              :computed_at
            )
            ON CONFLICT(repo_id, author_email, day) DO UPDATE SET
              commits_count=excluded.commits_count,
              loc_added=excluded.loc_added,
              loc_deleted=excluded.loc_deleted,
              files_changed=excluded.files_changed,
              large_commits_count=excluded.large_commits_count,
              avg_commit_size_loc=excluded.avg_commit_size_loc,
              prs_authored=excluded.prs_authored,
              prs_merged=excluded.prs_merged,
              avg_pr_cycle_hours=excluded.avg_pr_cycle_hours,
              median_pr_cycle_hours=excluded.median_pr_cycle_hours,
              review_response_count=excluded.review_response_count,
              avg_review_response_hours=excluded.avg_review_response_hours,
              median_review_response_hours=excluded.median_review_response_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._user_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO commit_metrics (
              repo_id, commit_hash, day, author_email, total_loc, files_changed, size_bucket, computed_at
            ) VALUES (
              :repo_id, :commit_hash, :day, :author_email, :total_loc, :files_changed, :size_bucket, :computed_at
            )
            ON CONFLICT(repo_id, day, author_email, commit_hash) DO UPDATE SET
              total_loc=excluded.total_loc,
              files_changed=excluded.files_changed,
              size_bucket=excluded.size_bucket,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._commit_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _repo_row(self, row: RepoMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "commits_count": int(data["commits_count"]),
            "total_loc_touched": int(data["total_loc_touched"]),
            "avg_commit_size_loc": float(data["avg_commit_size_loc"]),
            "large_commit_ratio": float(data["large_commit_ratio"]),
            "prs_merged": int(data["prs_merged"]),
            "median_pr_cycle_hours": float(data["median_pr_cycle_hours"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _user_row(self, row: UserMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "author_email": str(data["author_email"]),
            "commits_count": int(data["commits_count"]),
            "loc_added": int(data["loc_added"]),
            "loc_deleted": int(data["loc_deleted"]),
            "files_changed": int(data["files_changed"]),
            "large_commits_count": int(data["large_commits_count"]),
            "avg_commit_size_loc": float(data["avg_commit_size_loc"]),
            "prs_authored": int(data["prs_authored"]),
            "prs_merged": int(data["prs_merged"]),
            "avg_pr_cycle_hours": float(data["avg_pr_cycle_hours"]),
            "median_pr_cycle_hours": float(data["median_pr_cycle_hours"]),
            "review_response_count": int(data.get("review_response_count", 0)),
            "avg_review_response_hours": float(data.get("avg_review_response_hours", 0.0)),
            "median_review_response_hours": float(data.get("median_review_response_hours", 0.0)),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _commit_row(self, row: CommitMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "commit_hash": str(data["commit_hash"]),
            "day": data["day"].isoformat(),
            "author_email": str(data["author_email"]),
            "total_loc": int(data["total_loc"]),
            "files_changed": int(data["files_changed"]),
            "size_bucket": str(data["size_bucket"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }
