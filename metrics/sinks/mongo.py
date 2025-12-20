from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import List, Optional, Sequence

from pymongo import MongoClient, ReplaceOne

from metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    FileMetricsRecord,
)
import logging


def _day_to_mongo_datetime(day: date) -> datetime:
    # BSON stores datetimes as UTC; naive values are treated as UTC by convention.
    return datetime(day.year, day.month, day.day)


def _dt_to_mongo_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


class MongoMetricsSink:
    """MongoDB sink for derived daily metrics (idempotent upserts by stable _id)."""

    def __init__(self, uri: str, db_name: Optional[str] = None) -> None:
        if not uri:
            raise ValueError("MongoDB URI is required")
        self.client = MongoClient(uri)
        if db_name:
            self.db = self.client[db_name]
        else:
            try:
                self.db = self.client.get_default_database() or self.client["mergestat"]
            except Exception:
                self.db = self.client["mergestat"]

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            logging.warning("Failed to close MongoDB client: %s", e)

    def ensure_indexes(self) -> None:
        self.db["repo_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([
            ("repo_id", 1),
            ("author_email", 1),
            ("day", 1),
        ])
        self.db["commit_metrics"].create_index([("repo_id", 1), ("day", 1)])
        self.db["commit_metrics"].create_index([
            ("repo_id", 1),
            ("author_email", 1),
            ("day", 1),
        ])
        self.db["team_metrics_daily"].create_index([("team_id", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("day", 1),
        ])
        self.db["work_item_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("day", 1),
        ])
        self.db["work_item_user_metrics_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("user_identity", 1),
            ("day", 1),
        ])
        self.db["work_item_cycle_times"].create_index([("provider", 1), ("day", 1)])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("day", 1),
        ])
        self.db["work_item_state_durations_daily"].create_index([
            ("provider", 1),
            ("work_scope_id", 1),
            ("team_id", 1),
            ("status", 1),
            ("day", 1),
        ])

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["repo_metrics_daily"].bulk_write(ops, ordered=False)

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.author_email}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["user_metrics_daily"].bulk_write(ops, ordered=False)

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.commit_hash}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["commit_metrics"].bulk_write(ops, ordered=False)

    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.repo_id}:{row.day.isoformat()}:{row.path}"
            doc["repo_id"] = str(row.repo_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["file_metrics_daily"].bulk_write(ops, ordered=False)

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = f"{row.day.isoformat()}:{row.team_id}"
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["team_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            team_key = row.team_id or ""
            scope_key = row.work_scope_id or ""
            doc["_id"] = f"{row.day.isoformat()}:{row.provider}:{scope_key}:{team_key}"
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            scope_key = row.work_scope_id or ""
            doc["_id"] = (
                f"{row.day.isoformat()}:{row.provider}:{scope_key}:{row.user_identity}"
            )
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_user_metrics_daily"].bulk_write(ops, ordered=False)

    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            doc["_id"] = str(row.work_item_id)
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["created_at"] = _dt_to_mongo_datetime(row.created_at)
            if row.started_at is not None:
                doc["started_at"] = _dt_to_mongo_datetime(row.started_at)
            if row.completed_at is not None:
                doc["completed_at"] = _dt_to_mongo_datetime(row.completed_at)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_cycle_times"].bulk_write(ops, ordered=False)

    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        if not rows:
            return
        ops: List[ReplaceOne] = []
        for row in rows:
            doc = asdict(row)
            scope_key = row.work_scope_id or ""
            team_key = row.team_id or ""
            doc["_id"] = (
                f"{row.day.isoformat()}:{row.provider}:{scope_key}:{team_key}:{row.status}"
            )
            doc["day"] = _day_to_mongo_datetime(row.day)
            doc["computed_at"] = _dt_to_mongo_datetime(row.computed_at)
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        self.db["work_item_state_durations_daily"].bulk_write(ops, ordered=False)
