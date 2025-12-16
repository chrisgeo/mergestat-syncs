from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from metrics.schemas import (
    CommitMetricsRecord,
    CommitStatRow,
    DailyMetricsResult,
    PullRequestRow,
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
)


def commit_size_bucket(total_loc: int) -> str:
    """
    Bucket a commit by total lines of code touched (additions + deletions).

    - small:  total_loc <= 50
    - medium: 51..300
    - large:  total_loc > 300
    """
    if total_loc <= 50:
        return "small"
    if total_loc <= 300:
        return "medium"
    return "large"


def _normalize_identity(author_email: Optional[str], author_name: Optional[str]) -> str:
    """
    Prefer email when present; fall back to author_name; otherwise 'unknown'.

    The returned value is used as `author_email` in stored metrics for stability.
    """
    if author_email:
        normalized = author_email.strip()
        if normalized:
            return normalized
    if author_name:
        normalized = author_name.strip()
        if normalized:
            return normalized
    return "unknown"


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 1:
        return float(sorted_vals[mid])
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


@dataclass
class _CommitAgg:
    repo_id: uuid.UUID
    commit_hash: str
    author_identity: str
    committer_when: datetime
    additions: int = 0
    deletions: int = 0
    files: Set[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.files is None:
            self.files = set()

    @property
    def total_loc(self) -> int:
        return int(self.additions) + int(self.deletions)

    @property
    def files_changed(self) -> int:
        return len(self.files)


@dataclass
class _UserAgg:
    repo_id: uuid.UUID
    day: date
    author_identity: str
    commits_count: int = 0
    loc_added: int = 0
    loc_deleted: int = 0
    files: Set[str] = None  # type: ignore[assignment]
    large_commits_count: int = 0
    prs_authored: int = 0
    prs_merged: int = 0
    pr_cycle_times: List[float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.files is None:
            self.files = set()
        if self.pr_cycle_times is None:
            self.pr_cycle_times = []


def compute_daily_metrics(
    *,
    day: date,
    commit_stat_rows: List[CommitStatRow],
    pull_request_rows: List[PullRequestRow],
    computed_at: datetime,
    include_commit_metrics: bool = True,
) -> DailyMetricsResult:
    """
    Compute daily commit/user/repo metrics for a single UTC day.

    Inputs are simplified rows pulled from the synced relational store.
    This function is pure: it does no I/O and depends only on its arguments.

    Notes:
    - `files_changed` for users is computed as distinct file paths touched by the
      user on that day (union across commits).
    - PR cycle time metrics consider PRs with `merged_at` inside the day window.
    - When no PRs are merged, avg/median PR cycle times are 0.0.
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    # 1) Build per-commit aggregates from commit_stat_rows.
    commit_aggs: Dict[Tuple[uuid.UUID, str], _CommitAgg] = {}
    for row in commit_stat_rows:
        key = (row["repo_id"], row["commit_hash"])
        agg = commit_aggs.get(key)
        if agg is None:
            agg = _CommitAgg(
                repo_id=row["repo_id"],
                commit_hash=row["commit_hash"],
                author_identity=_normalize_identity(row.get("author_email"), row.get("author_name")),
                committer_when=_to_utc(row["committer_when"]),
            )
            commit_aggs[key] = agg

        additions = max(0, int(row.get("additions", 0) or 0))
        deletions = max(0, int(row.get("deletions", 0) or 0))
        agg.additions += additions
        agg.deletions += deletions

        file_path = row.get("file_path")
        if file_path:
            agg.files.add(str(file_path))

    # 2) Roll up commit aggregates to per-user.
    user_aggs: Dict[Tuple[uuid.UUID, str], _UserAgg] = {}
    for agg in commit_aggs.values():
        user_key = (agg.repo_id, agg.author_identity)
        ua = user_aggs.get(user_key)
        if ua is None:
            ua = _UserAgg(repo_id=agg.repo_id, day=day, author_identity=agg.author_identity)
            user_aggs[user_key] = ua

        ua.commits_count += 1
        ua.loc_added += agg.additions
        ua.loc_deleted += agg.deletions
        ua.files.update(agg.files)
        if agg.total_loc > 300:
            ua.large_commits_count += 1

    # 3) Process PR rows for the day window.
    repo_cycle_times: Dict[uuid.UUID, List[float]] = {}
    for pr in pull_request_rows:
        author_identity = _normalize_identity(pr.get("author_email"), pr.get("author_name"))
        user_key = (pr["repo_id"], author_identity)
        ua = user_aggs.get(user_key)
        if ua is None:
            ua = _UserAgg(repo_id=pr["repo_id"], day=day, author_identity=author_identity)
            user_aggs[user_key] = ua

        created_at = _to_utc(pr["created_at"])
        if start <= created_at < end:
            ua.prs_authored += 1

        merged_at = pr.get("merged_at")
        if merged_at is not None:
            merged_at_utc = _to_utc(merged_at)
            if start <= merged_at_utc < end:
                ua.prs_merged += 1
                cycle_hours = (merged_at_utc - created_at).total_seconds() / 3600.0
                ua.pr_cycle_times.append(float(cycle_hours))
                repo_cycle_times.setdefault(pr["repo_id"], []).append(float(cycle_hours))

    # 4) Finalize user metrics records.
    user_metrics: List[UserMetricsDailyRecord] = []
    for (repo_id, author_identity), ua in sorted(user_aggs.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
        commits_count = int(ua.commits_count)
        total_loc_touched = int(ua.loc_added) + int(ua.loc_deleted)
        avg_commit_size_loc = (total_loc_touched / commits_count) if commits_count else 0.0

        avg_pr_cycle = _mean(ua.pr_cycle_times)
        median_pr_cycle = _median(ua.pr_cycle_times)

        user_metrics.append(
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                author_email=author_identity,
                commits_count=commits_count,
                loc_added=int(ua.loc_added),
                loc_deleted=int(ua.loc_deleted),
                files_changed=len(ua.files),
                large_commits_count=int(ua.large_commits_count),
                avg_commit_size_loc=float(avg_commit_size_loc),
                prs_authored=int(ua.prs_authored),
                prs_merged=int(ua.prs_merged),
                avg_pr_cycle_hours=float(avg_pr_cycle),
                median_pr_cycle_hours=float(median_pr_cycle),
                computed_at=computed_at_utc,
            )
        )

    # 5) Roll up to per-repo metrics.
    repos: Set[uuid.UUID] = set()
    repos.update(repo_id for (repo_id, _author) in user_aggs.keys())
    repos.update(pr["repo_id"] for pr in pull_request_rows)
    repo_metrics: List[RepoMetricsDailyRecord] = []
    for repo_id in sorted(repos, key=lambda r: str(r)):
        repo_users = [u for u in user_metrics if u.repo_id == repo_id]
        commits_count = sum(u.commits_count for u in repo_users)
        total_loc_touched = sum(u.loc_added + u.loc_deleted for u in repo_users)
        large_commits_count = sum(u.large_commits_count for u in repo_users)
        prs_merged = sum(u.prs_merged for u in repo_users)

        avg_commit_size_loc = (total_loc_touched / commits_count) if commits_count else 0.0
        large_commit_ratio = (large_commits_count / commits_count) if commits_count else 0.0
        median_repo_cycle = _median(repo_cycle_times.get(repo_id, []))

        repo_metrics.append(
            RepoMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                commits_count=int(commits_count),
                total_loc_touched=int(total_loc_touched),
                avg_commit_size_loc=float(avg_commit_size_loc),
                large_commit_ratio=float(large_commit_ratio),
                prs_merged=int(prs_merged),
                median_pr_cycle_hours=float(median_repo_cycle),
                computed_at=computed_at_utc,
            )
        )

    # 6) Optional per-commit metrics.
    commit_metrics: List[CommitMetricsRecord] = []
    if include_commit_metrics:
        for agg in sorted(commit_aggs.values(), key=lambda a: (str(a.repo_id), a.commit_hash)):
            commit_metrics.append(
                CommitMetricsRecord(
                    repo_id=agg.repo_id,
                    commit_hash=agg.commit_hash,
                    day=day,
                    author_email=agg.author_identity,
                    total_loc=int(agg.total_loc),
                    files_changed=int(agg.files_changed),
                    size_bucket=commit_size_bucket(int(agg.total_loc)),
                    computed_at=computed_at_utc,
                )
            )

    return DailyMetricsResult(
        day=day,
        repo_metrics=repo_metrics,
        user_metrics=user_metrics,
        commit_metrics=commit_metrics,
    )
