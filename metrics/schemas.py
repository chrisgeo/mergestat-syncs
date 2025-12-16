from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, TypedDict


class CommitStatRow(TypedDict):
    repo_id: uuid.UUID
    commit_hash: str
    author_email: Optional[str]
    author_name: Optional[str]
    committer_when: datetime
    file_path: Optional[str]
    additions: int
    deletions: int


class PullRequestRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    author_email: Optional[str]
    author_name: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]


@dataclass(frozen=True)
class CommitMetricsRecord:
    repo_id: uuid.UUID
    commit_hash: str
    day: date
    author_email: str
    total_loc: int
    files_changed: int
    size_bucket: str  # small|medium|large
    computed_at: datetime


@dataclass(frozen=True)
class UserMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    author_email: str
    commits_count: int
    loc_added: int
    loc_deleted: int
    files_changed: int
    large_commits_count: int
    avg_commit_size_loc: float
    prs_authored: int
    prs_merged: int
    avg_pr_cycle_hours: float
    median_pr_cycle_hours: float
    computed_at: datetime

    # Placeholder fields for future review-like signals (not yet available).
    review_response_count: int = 0
    avg_review_response_hours: float = 0.0
    median_review_response_hours: float = 0.0


@dataclass(frozen=True)
class RepoMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    commits_count: int
    total_loc_touched: int
    avg_commit_size_loc: float
    large_commit_ratio: float
    prs_merged: int
    median_pr_cycle_hours: float
    computed_at: datetime


@dataclass(frozen=True)
class DailyMetricsResult:
    day: date
    repo_metrics: List[RepoMetricsDailyRecord]
    user_metrics: List[UserMetricsDailyRecord]
    commit_metrics: List[CommitMetricsRecord]

