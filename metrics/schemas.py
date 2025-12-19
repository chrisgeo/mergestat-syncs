from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional, TypedDict
from typing_extensions import NotRequired


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
    # Optional PR facts when available from the synced store or derived joins.
    first_review_at: NotRequired[Optional[datetime]]
    first_comment_at: NotRequired[Optional[datetime]]
    reviews_count: NotRequired[int]
    changes_requested_count: NotRequired[int]
    comments_count: NotRequired[int]
    additions: NotRequired[int]
    deletions: NotRequired[int]
    changed_files: NotRequired[int]


class PullRequestReviewRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    reviewer: str
    submitted_at: datetime
    state: str  # APPROVED|CHANGES_REQUESTED|COMMENTED|DISMISSED|...


class PullRequestCommentRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    commenter: str
    created_at: datetime


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

    # PR cycle time distribution (merged PRs, by merged day).
    pr_cycle_p75_hours: float = 0.0
    pr_cycle_p90_hours: float = 0.0

    # Review / collaboration signals (best-effort, requires review/comment facts).
    prs_with_first_review: int = 0
    pr_first_review_p50_hours: Optional[float] = None
    pr_first_review_p90_hours: Optional[float] = None
    pr_review_time_p50_hours: Optional[float] = None
    pr_pickup_time_p50_hours: Optional[float] = None
    reviews_given: int = 0
    changes_requested_given: int = 0

    # Team dimension (optional).
    team_id: Optional[str] = None
    team_name: Optional[str] = None


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

    # PR cycle time distribution (merged PRs).
    pr_cycle_p75_hours: float = 0.0
    pr_cycle_p90_hours: float = 0.0

    # Review / collaboration signals.
    prs_with_first_review: int = 0
    pr_first_review_p50_hours: Optional[float] = None
    pr_first_review_p90_hours: Optional[float] = None
    pr_review_time_p50_hours: Optional[float] = None
    pr_pickup_time_p50_hours: Optional[float] = None

    # Quality signals.
    large_pr_ratio: float = 0.0
    pr_rework_ratio: float = 0.0


@dataclass(frozen=True)
class TeamMetricsDailyRecord:
    day: date
    team_id: str
    team_name: str
    commits_count: int
    after_hours_commits_count: int
    weekend_commits_count: int
    after_hours_commit_ratio: float
    weekend_commit_ratio: float
    computed_at: datetime


@dataclass(frozen=True)
class WorkItemCycleTimeRecord:
    work_item_id: str
    provider: str
    day: date  # completed day (UTC) when completed_at is present, else created day
    work_scope_id: str
    team_id: Optional[str]
    team_name: Optional[str]
    assignee: Optional[str]
    type: str
    status: str
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    cycle_time_hours: Optional[float]
    lead_time_hours: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class WorkItemMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    team_id: Optional[str]
    team_name: Optional[str]
    items_started: int
    items_completed: int
    items_started_unassigned: int
    items_completed_unassigned: int
    wip_count_end_of_day: int
    wip_unassigned_end_of_day: int
    cycle_time_p50_hours: Optional[float]
    cycle_time_p90_hours: Optional[float]
    lead_time_p50_hours: Optional[float]
    lead_time_p90_hours: Optional[float]
    wip_age_p50_hours: Optional[float]
    wip_age_p90_hours: Optional[float]
    bug_completed_ratio: float
    story_points_completed: float
    computed_at: datetime


@dataclass(frozen=True)
class WorkItemUserMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    user_identity: str
    team_id: Optional[str]
    team_name: Optional[str]
    items_started: int
    items_completed: int
    wip_count_end_of_day: int
    cycle_time_p50_hours: Optional[float]
    cycle_time_p90_hours: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class WorkItemStateDurationDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    team_id: str
    team_name: str
    status: str  # normalized status category
    duration_hours: float
    items_touched: int
    computed_at: datetime


@dataclass(frozen=True)
class DailyMetricsResult:
    day: date
    repo_metrics: List[RepoMetricsDailyRecord]
    user_metrics: List[UserMetricsDailyRecord]
    commit_metrics: List[CommitMetricsRecord]

    # Optional expanded outputs (may be empty depending on available inputs).
    team_metrics: List[TeamMetricsDailyRecord] = field(default_factory=list)
    work_item_metrics: List[WorkItemMetricsDailyRecord] = field(default_factory=list)
    work_item_user_metrics: List[WorkItemUserMetricsDailyRecord] = field(default_factory=list)
    work_item_cycle_times: List[WorkItemCycleTimeRecord] = field(default_factory=list)
    work_item_state_durations: List[WorkItemStateDurationDailyRecord] = field(default_factory=list)
