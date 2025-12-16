from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import List, Optional, Sequence

from pymongo import MongoClient, ReplaceOne

from metrics.schemas import CommitMetricsRecord, RepoMetricsDailyRecord, UserMetricsDailyRecord


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
        except Exception:
            pass

    def ensure_indexes(self) -> None:
        self.db["repo_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([("repo_id", 1), ("day", 1)])
        self.db["user_metrics_daily"].create_index([("repo_id", 1), ("author_email", 1), ("day", 1)])
        self.db["commit_metrics"].create_index([("repo_id", 1), ("day", 1)])
        self.db["commit_metrics"].create_index([("repo_id", 1), ("author_email", 1), ("day", 1)])

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

