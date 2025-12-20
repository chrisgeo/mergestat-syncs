from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    FileMetricsRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
)


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
        self._wi_metrics_has_work_scope: bool = True
        self._wi_user_metrics_has_work_scope: bool = True
        self._wi_cycle_has_work_scope: bool = True
        self._wi_state_has_work_scope: bool = True

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
              pr_cycle_p75_hours REAL NOT NULL DEFAULT 0.0,
              pr_cycle_p90_hours REAL NOT NULL DEFAULT 0.0,
              prs_with_first_review INTEGER NOT NULL DEFAULT 0,
              pr_first_review_p50_hours REAL,
              pr_first_review_p90_hours REAL,
              pr_review_time_p50_hours REAL,
              pr_pickup_time_p50_hours REAL,
              large_pr_ratio REAL NOT NULL DEFAULT 0.0,
              pr_rework_ratio REAL NOT NULL DEFAULT 0.0,
              mttr_hours REAL,
              change_failure_rate REAL NOT NULL DEFAULT 0.0,
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
              pr_cycle_p75_hours REAL NOT NULL DEFAULT 0.0,
              pr_cycle_p90_hours REAL NOT NULL DEFAULT 0.0,
              prs_with_first_review INTEGER NOT NULL DEFAULT 0,
              pr_first_review_p50_hours REAL,
              pr_first_review_p90_hours REAL,
              pr_review_time_p50_hours REAL,
              pr_pickup_time_p50_hours REAL,
              reviews_given INTEGER NOT NULL DEFAULT 0,
              changes_requested_given INTEGER NOT NULL DEFAULT 0,
              reviews_received INTEGER NOT NULL DEFAULT 0,
              review_reciprocity REAL NOT NULL DEFAULT 0.0,
              team_id TEXT,
              team_name TEXT,
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
            """
            CREATE TABLE IF NOT EXISTS team_metrics_daily (
              day TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              after_hours_commits_count INTEGER NOT NULL,
              weekend_commits_count INTEGER NOT NULL,
              after_hours_commit_ratio REAL NOT NULL,
              weekend_commit_ratio REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (team_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS file_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              path TEXT NOT NULL,
              churn INTEGER NOT NULL,
              contributors INTEGER NOT NULL,
              commits_count INTEGER NOT NULL,
              hotspot_score REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day, path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_metrics_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              items_started INTEGER NOT NULL,
              items_completed INTEGER NOT NULL,
              items_started_unassigned INTEGER NOT NULL,
              items_completed_unassigned INTEGER NOT NULL,
              wip_count_end_of_day INTEGER NOT NULL,
              wip_unassigned_end_of_day INTEGER NOT NULL,
              cycle_time_p50_hours REAL,
              cycle_time_p90_hours REAL,
              lead_time_p50_hours REAL,
              lead_time_p90_hours REAL,
              wip_age_p50_hours REAL,
              wip_age_p90_hours REAL,
              bug_completed_ratio REAL NOT NULL,
              story_points_completed REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, day, team_id, work_scope_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_user_metrics_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              user_identity TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              items_started INTEGER NOT NULL,
              items_completed INTEGER NOT NULL,
              wip_count_end_of_day INTEGER NOT NULL,
              cycle_time_p50_hours REAL,
              cycle_time_p90_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_scope_id, user_identity, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_cycle_times (
              work_item_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              day TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT,
              team_name TEXT,
              assignee TEXT,
              type TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              cycle_time_hours REAL,
              lead_time_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_item_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_state_durations_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              status TEXT NOT NULL,
              duration_hours REAL NOT NULL,
              items_touched INTEGER NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_scope_id, team_id, status, day)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_repo_metrics_daily_day ON repo_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_user_metrics_daily_day ON user_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_commit_metrics_day ON commit_metrics(day)",
            "CREATE INDEX IF NOT EXISTS idx_team_metrics_daily_day ON team_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_work_item_metrics_daily_day ON work_item_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_file_metrics_daily_day ON file_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_work_item_state_durations_daily_day ON work_item_state_durations_daily(day)",
        ]
        with self.engine.begin() as conn:
            for stmt in stmts:
                conn.execute(text(stmt))
            # Best-effort upgrades for older SQLite schemas (no destructive migrations):
            # - Add work_scope_id columns
            # - Add UNIQUE indexes so ON CONFLICT(...) upserts work
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN work_scope_id TEXT"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "items_started_unassigned"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN items_started_unassigned INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "items_completed_unassigned"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN items_completed_unassigned INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "wip_unassigned_end_of_day"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN wip_unassigned_end_of_day INTEGER NOT NULL DEFAULT 0"
                    )
                )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uidx_work_item_metrics_daily_scope "
                    "ON work_item_metrics_daily(provider, day, team_id, work_scope_id)"
                )
            )

            if not self._table_has_column(
                conn, "work_item_user_metrics_daily", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_user_metrics_daily ADD COLUMN work_scope_id TEXT"
                    )
                )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uidx_work_item_user_metrics_daily_scope "
                    "ON work_item_user_metrics_daily(provider, work_scope_id, user_identity, day)"
                )
            )

            if not self._table_has_column(
                conn, "work_item_cycle_times", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_cycle_times ADD COLUMN work_scope_id TEXT"
                    )
                )

            self._wi_metrics_has_work_scope = self._table_has_column(
                conn, "work_item_metrics_daily", "work_scope_id"
            )
            self._wi_user_metrics_has_work_scope = self._table_has_column(
                conn, "work_item_user_metrics_daily", "work_scope_id"
            )
            self._wi_cycle_has_work_scope = self._table_has_column(
                conn, "work_item_cycle_times", "work_scope_id"
            )
            self._wi_state_has_work_scope = self._table_has_column(
                conn, "work_item_state_durations_daily", "work_scope_id"
            )

            # Upgrades for repo_metrics_daily
            for col, type_ in [
                ("pr_cycle_p75_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_cycle_p90_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("prs_with_first_review", "INTEGER NOT NULL DEFAULT 0"),
                ("pr_first_review_p50_hours", "REAL"),
                ("pr_first_review_p90_hours", "REAL"),
                ("pr_review_time_p50_hours", "REAL"),
                ("pr_pickup_time_p50_hours", "REAL"),
                ("large_pr_ratio", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_rework_ratio", "REAL NOT NULL DEFAULT 0.0"),
                ("mttr_hours", "REAL"),
                ("change_failure_rate", "REAL NOT NULL DEFAULT 0.0"),
            ]:
                if not self._table_has_column(conn, "repo_metrics_daily", col):
                    conn.execute(
                        text(f"ALTER TABLE repo_metrics_daily ADD COLUMN {col} {type_}")
                    )

            # Upgrades for user_metrics_daily
            for col, type_ in [
                ("pr_cycle_p75_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_cycle_p90_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("prs_with_first_review", "INTEGER NOT NULL DEFAULT 0"),
                ("pr_first_review_p50_hours", "REAL"),
                ("pr_first_review_p90_hours", "REAL"),
                ("pr_review_time_p50_hours", "REAL"),
                ("pr_pickup_time_p50_hours", "REAL"),
                ("reviews_given", "INTEGER NOT NULL DEFAULT 0"),
                ("changes_requested_given", "INTEGER NOT NULL DEFAULT 0"),
                ("reviews_received", "INTEGER NOT NULL DEFAULT 0"),
                ("review_reciprocity", "REAL NOT NULL DEFAULT 0.0"),
                ("team_id", "TEXT"),
                ("team_name", "TEXT"),
            ]:
                if not self._table_has_column(conn, "user_metrics_daily", col):
                    conn.execute(
                        text(f"ALTER TABLE user_metrics_daily ADD COLUMN {col} {type_}")
                    )

    @staticmethod
    def _table_has_column(conn, table: str, column: str) -> bool:
        try:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        except Exception:
            return False
        cols = {str(r[1]) for r in rows if len(r) >= 2}
        return column in cols

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO repo_metrics_daily (
              repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
              large_commit_ratio, prs_merged, median_pr_cycle_hours,
              pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review,
              pr_first_review_p50_hours, pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
              large_pr_ratio, pr_rework_ratio,
              mttr_hours, change_failure_rate,
              computed_at
            ) VALUES (
              :repo_id, :day, :commits_count, :total_loc_touched, :avg_commit_size_loc,
              :large_commit_ratio, :prs_merged, :median_pr_cycle_hours,
              :pr_cycle_p75_hours, :pr_cycle_p90_hours, :prs_with_first_review,
              :pr_first_review_p50_hours, :pr_first_review_p90_hours, :pr_review_time_p50_hours, :pr_pickup_time_p50_hours,
              :large_pr_ratio, :pr_rework_ratio,
              :mttr_hours, :change_failure_rate,
              :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              commits_count=excluded.commits_count,
              total_loc_touched=excluded.total_loc_touched,
              avg_commit_size_loc=excluded.avg_commit_size_loc,
              large_commit_ratio=excluded.large_commit_ratio,
              prs_merged=excluded.prs_merged,
              median_pr_cycle_hours=excluded.median_pr_cycle_hours,
              pr_cycle_p75_hours=excluded.pr_cycle_p75_hours,
              pr_cycle_p90_hours=excluded.pr_cycle_p90_hours,
              prs_with_first_review=excluded.prs_with_first_review,
              pr_first_review_p50_hours=excluded.pr_first_review_p50_hours,
              pr_first_review_p90_hours=excluded.pr_first_review_p90_hours,
              pr_review_time_p50_hours=excluded.pr_review_time_p50_hours,
              pr_pickup_time_p50_hours=excluded.pr_pickup_time_p50_hours,
              large_pr_ratio=excluded.large_pr_ratio,
              pr_rework_ratio=excluded.pr_rework_ratio,
              mttr_hours=excluded.mttr_hours,
              change_failure_rate=excluded.change_failure_rate,
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
              pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review,
              pr_first_review_p50_hours, pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
              reviews_given, changes_requested_given, reviews_received, review_reciprocity, team_id, team_name,
              computed_at
            ) VALUES (
              :repo_id, :day, :author_email, :commits_count, :loc_added, :loc_deleted,
              :files_changed, :large_commits_count, :avg_commit_size_loc,
              :prs_authored, :prs_merged, :avg_pr_cycle_hours, :median_pr_cycle_hours,
              :pr_cycle_p75_hours, :pr_cycle_p90_hours, :prs_with_first_review,
              :pr_first_review_p50_hours, :pr_first_review_p90_hours, :pr_review_time_p50_hours, :pr_pickup_time_p50_hours,
              :reviews_given, :changes_requested_given, :reviews_received, :review_reciprocity, :team_id, :team_name,
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
              pr_cycle_p75_hours=excluded.pr_cycle_p75_hours,
              pr_cycle_p90_hours=excluded.pr_cycle_p90_hours,
              prs_with_first_review=excluded.prs_with_first_review,
              pr_first_review_p50_hours=excluded.pr_first_review_p50_hours,
              pr_first_review_p90_hours=excluded.pr_first_review_p90_hours,
              pr_review_time_p50_hours=excluded.pr_review_time_p50_hours,
              pr_pickup_time_p50_hours=excluded.pr_pickup_time_p50_hours,
              reviews_given=excluded.reviews_given,
              changes_requested_given=excluded.changes_requested_given,
              reviews_received=excluded.reviews_received,
              review_reciprocity=excluded.review_reciprocity,
              team_id=excluded.team_id,
              team_name=excluded.team_name,
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

    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO file_metrics_daily (
              repo_id, day, path, churn, contributors, commits_count, hotspot_score, computed_at
            ) VALUES (
              :repo_id, :day, :path, :churn, :contributors, :commits_count, :hotspot_score, :computed_at
            )
            ON CONFLICT(repo_id, day, path) DO UPDATE SET
              churn=excluded.churn,
              contributors=excluded.contributors,
              commits_count=excluded.commits_count,
              hotspot_score=excluded.hotspot_score,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._file_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _file_row(self, row: FileMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "path": str(data["path"]),
            "churn": int(data["churn"]),
            "contributors": int(data["contributors"]),
            "commits_count": int(data["commits_count"]),
            "hotspot_score": float(data["hotspot_score"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

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
            "pr_cycle_p75_hours": float(data.get("pr_cycle_p75_hours", 0.0) or 0.0),
            "pr_cycle_p90_hours": float(data.get("pr_cycle_p90_hours", 0.0) or 0.0),
            "prs_with_first_review": int(data.get("prs_with_first_review", 0) or 0),
            "pr_first_review_p50_hours": data.get("pr_first_review_p50_hours"),
            "pr_first_review_p90_hours": data.get("pr_first_review_p90_hours"),
            "pr_review_time_p50_hours": data.get("pr_review_time_p50_hours"),
            "pr_pickup_time_p50_hours": data.get("pr_pickup_time_p50_hours"),
            "large_pr_ratio": float(data.get("large_pr_ratio", 0.0) or 0.0),
            "pr_rework_ratio": float(data.get("pr_rework_ratio", 0.0) or 0.0),
            "mttr_hours": data.get("mttr_hours"),
            "change_failure_rate": float(data.get("change_failure_rate", 0.0) or 0.0),
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
            "pr_cycle_p75_hours": float(data.get("pr_cycle_p75_hours", 0.0) or 0.0),
            "pr_cycle_p90_hours": float(data.get("pr_cycle_p90_hours", 0.0) or 0.0),
            "prs_with_first_review": int(data.get("prs_with_first_review", 0) or 0),
            "pr_first_review_p50_hours": data.get("pr_first_review_p50_hours"),
            "pr_first_review_p90_hours": data.get("pr_first_review_p90_hours"),
            "pr_review_time_p50_hours": data.get("pr_review_time_p50_hours"),
            "pr_pickup_time_p50_hours": data.get("pr_pickup_time_p50_hours"),
            "reviews_given": int(data.get("reviews_given", 0) or 0),
            "changes_requested_given": int(data.get("changes_requested_given", 0) or 0),
            "reviews_received": int(data.get("reviews_received", 0) or 0),
            "review_reciprocity": float(data.get("review_reciprocity", 0.0) or 0.0),
            "team_id": data.get("team_id"),
            "team_name": data.get("team_name"),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO team_metrics_daily (
              day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
              after_hours_commit_ratio, weekend_commit_ratio, computed_at
            ) VALUES (
              :day, :team_id, :team_name, :commits_count, :after_hours_commits_count, :weekend_commits_count,
              :after_hours_commit_ratio, :weekend_commit_ratio, :computed_at
            )
            ON CONFLICT(team_id, day) DO UPDATE SET
              team_name=excluded.team_name,
              commits_count=excluded.commits_count,
              after_hours_commits_count=excluded.after_hours_commits_count,
              weekend_commits_count=excluded.weekend_commits_count,
              after_hours_commit_ratio=excluded.after_hours_commit_ratio,
              weekend_commit_ratio=excluded.weekend_commit_ratio,
              computed_at=excluded.computed_at
            """
        )
        payload = [asdict(r) for r in rows]
        for doc in payload:
            doc["day"] = doc["day"].isoformat()
            doc["computed_at"] = _dt_to_sqlite(doc["computed_at"])
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        if self._wi_metrics_has_work_scope:
            stmt = text(
                """
                INSERT INTO work_item_metrics_daily (
                  day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
                  items_started_unassigned, items_completed_unassigned, wip_unassigned_end_of_day,
                  cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
                  wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed, computed_at
                ) VALUES (
                  :day, :provider, :work_scope_id, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
                  :items_started_unassigned, :items_completed_unassigned, :wip_unassigned_end_of_day,
                  :cycle_time_p50_hours, :cycle_time_p90_hours, :lead_time_p50_hours, :lead_time_p90_hours,
                  :wip_age_p50_hours, :wip_age_p90_hours, :bug_completed_ratio, :story_points_completed, :computed_at
                )
                ON CONFLICT(provider, day, team_id, work_scope_id) DO UPDATE SET
                  team_name=excluded.team_name,
                  items_started=excluded.items_started,
                  items_completed=excluded.items_completed,
                  items_started_unassigned=excluded.items_started_unassigned,
                  items_completed_unassigned=excluded.items_completed_unassigned,
                  wip_count_end_of_day=excluded.wip_count_end_of_day,
                  wip_unassigned_end_of_day=excluded.wip_unassigned_end_of_day,
                  cycle_time_p50_hours=excluded.cycle_time_p50_hours,
                  cycle_time_p90_hours=excluded.cycle_time_p90_hours,
                  lead_time_p50_hours=excluded.lead_time_p50_hours,
                  lead_time_p90_hours=excluded.lead_time_p90_hours,
                  wip_age_p50_hours=excluded.wip_age_p50_hours,
                  wip_age_p90_hours=excluded.wip_age_p90_hours,
                  bug_completed_ratio=excluded.bug_completed_ratio,
                  story_points_completed=excluded.story_points_completed,
                  computed_at=excluded.computed_at
                """
            )
        else:
            # Legacy schema used `repo_id` as the scope column.
            stmt = text(
                """
                INSERT INTO work_item_metrics_daily (
                  day, provider, repo_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
                  items_started_unassigned, items_completed_unassigned, wip_unassigned_end_of_day,
                  cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
                  wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed, computed_at
                ) VALUES (
                  :day, :provider, :repo_id, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
                  :items_started_unassigned, :items_completed_unassigned, :wip_unassigned_end_of_day,
                  :cycle_time_p50_hours, :cycle_time_p90_hours, :lead_time_p50_hours, :lead_time_p90_hours,
                  :wip_age_p50_hours, :wip_age_p90_hours, :bug_completed_ratio, :story_points_completed, :computed_at
                )
                ON CONFLICT(provider, day, team_id, repo_id) DO UPDATE SET
                  team_name=excluded.team_name,
                  items_started=excluded.items_started,
                  items_completed=excluded.items_completed,
                  items_started_unassigned=excluded.items_started_unassigned,
                  items_completed_unassigned=excluded.items_completed_unassigned,
                  wip_count_end_of_day=excluded.wip_count_end_of_day,
                  wip_unassigned_end_of_day=excluded.wip_unassigned_end_of_day,
                  cycle_time_p50_hours=excluded.cycle_time_p50_hours,
                  cycle_time_p90_hours=excluded.cycle_time_p90_hours,
                  lead_time_p50_hours=excluded.lead_time_p50_hours,
                  lead_time_p90_hours=excluded.lead_time_p90_hours,
                  wip_age_p50_hours=excluded.wip_age_p50_hours,
                  wip_age_p90_hours=excluded.wip_age_p90_hours,
                  bug_completed_ratio=excluded.bug_completed_ratio,
                  story_points_completed=excluded.story_points_completed,
                  computed_at=excluded.computed_at
                """
            )
        payload = []
        for row in rows:
            data = asdict(row)
            base = {
                **data,
                "day": data["day"].isoformat(),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            }
            if self._wi_metrics_has_work_scope:
                base["work_scope_id"] = str(data.get("work_scope_id") or "")
            else:
                base["repo_id"] = str(data.get("work_scope_id") or "")
            payload.append(base)
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_user_metrics_daily (
              day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
              cycle_time_p50_hours, cycle_time_p90_hours, computed_at
            ) VALUES (
              :day, :provider, :work_scope_id, :user_identity, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
              :cycle_time_p50_hours, :cycle_time_p90_hours, :computed_at
            )
            ON CONFLICT(provider, work_scope_id, user_identity, day) DO UPDATE SET
              team_id=excluded.team_id,
              team_name=excluded.team_name,
              items_started=excluded.items_started,
              items_completed=excluded.items_completed,
              wip_count_end_of_day=excluded.wip_count_end_of_day,
              cycle_time_p50_hours=excluded.cycle_time_p50_hours,
              cycle_time_p90_hours=excluded.cycle_time_p90_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_cycle_times (
              work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type, status,
              created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at
            ) VALUES (
              :work_item_id, :provider, :day, :work_scope_id, :team_id, :team_name, :assignee, :type, :status,
              :created_at, :started_at, :completed_at, :cycle_time_hours, :lead_time_hours, :computed_at
            )
            ON CONFLICT(provider, work_item_id) DO UPDATE SET
              day=excluded.day,
              work_scope_id=excluded.work_scope_id,
              team_id=excluded.team_id,
              team_name=excluded.team_name,
              assignee=excluded.assignee,
              type=excluded.type,
              status=excluded.status,
              created_at=excluded.created_at,
              started_at=excluded.started_at,
              completed_at=excluded.completed_at,
              cycle_time_hours=excluded.cycle_time_hours,
              lead_time_hours=excluded.lead_time_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "created_at": _dt_to_sqlite(data["created_at"]),
                "started_at": _dt_to_sqlite(data["started_at"])
                if data.get("started_at")
                else None,
                "completed_at": _dt_to_sqlite(data["completed_at"])
                if data.get("completed_at")
                else None,
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

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

    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_state_durations_daily (
              day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at
            ) VALUES (
              :day, :provider, :work_scope_id, :team_id, :team_name, :status, :duration_hours, :items_touched, :computed_at
            )
            ON CONFLICT(provider, work_scope_id, team_id, status, day) DO UPDATE SET
              team_name=excluded.team_name,
              duration_hours=excluded.duration_hours,
              items_touched=excluded.items_touched,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)
