from __future__ import annotations

import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from metrics.compute import compute_daily_metrics
from metrics.schemas import CommitStatRow, PullRequestRow
from metrics.sinks.clickhouse import ClickHouseMetricsSink
from metrics.sinks.mongo import MongoMetricsSink
from metrics.sinks.sqlite import SQLiteMetricsSink
from storage import detect_db_type


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _date_range(end_day: date, backfill_days: int) -> List[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def run_daily_metrics_job(
    *,
    db_url: Optional[str] = None,
    day: date,
    backfill_days: int,
    repo_id: Optional[uuid.UUID] = None,
    include_commit_metrics: bool = True,
) -> None:
    """
    Compute and persist daily metrics into a single backend (ClickHouse, MongoDB, or SQLite).

    This job expects the synced source tables/collections to already exist in the
    same backend pointed to by `db_url`:
    - ClickHouse: `git_commits`, `git_commit_stats`, `git_pull_requests`
    - MongoDB: `git_commits`, `git_commit_stats`, `git_pull_requests`
    - SQLite: `git_commits`, `git_commit_stats`, `git_pull_requests`

    Derived metrics are written back into the same backend:
    - ClickHouse tables: `repo_metrics_daily`, `user_metrics_daily`, `commit_metrics`
    - Mongo collections: `repo_metrics_daily`, `user_metrics_daily`, `commit_metrics`
    - SQLite tables: `repo_metrics_daily`, `user_metrics_daily`, `commit_metrics`
    """
    db_url = db_url or os.getenv("DB_CONN_STRING") or os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DB_CONN_STRING).")

    backend = detect_db_type(db_url)
    if backend not in {"clickhouse", "mongo", "sqlite"}:
        raise ValueError(
            f"Unsupported db backend for daily metrics: {backend}. "
            f"Use a ClickHouse, MongoDB, or SQLite connection URI."
        )

    days = _date_range(day, backfill_days)
    computed_at = datetime.now(timezone.utc)

    if backend == "clickhouse":
        sink = ClickHouseMetricsSink(db_url)
        try:
            sink.ensure_tables()
            for d in days:
                commit_rows, pr_rows = _load_clickhouse_rows(
                    sink.client, day=d, repo_id=repo_id
                )
                result = compute_daily_metrics(
                    day=d,
                    commit_stat_rows=commit_rows,
                    pull_request_rows=pr_rows,
                    computed_at=computed_at,
                    include_commit_metrics=include_commit_metrics,
                )
                sink.write_repo_metrics(result.repo_metrics)
                sink.write_user_metrics(result.user_metrics)
                sink.write_commit_metrics(result.commit_metrics)
        finally:
            sink.close()
        return

    if backend == "sqlite":
        sqlite_url = _normalize_sqlite_url(db_url)
        sink = SQLiteMetricsSink(sqlite_url)
        try:
            sink.ensure_tables()
            for d in days:
                commit_rows, pr_rows = _load_sqlite_rows(
                    sink.engine, day=d, repo_id=repo_id
                )
                result = compute_daily_metrics(
                    day=d,
                    commit_stat_rows=commit_rows,
                    pull_request_rows=pr_rows,
                    computed_at=computed_at,
                    include_commit_metrics=include_commit_metrics,
                )
                sink.write_repo_metrics(result.repo_metrics)
                sink.write_user_metrics(result.user_metrics)
                sink.write_commit_metrics(result.commit_metrics)
        finally:
            sink.close()
        return

    # MongoDB
    sink = MongoMetricsSink(db_url, db_name=os.getenv("MONGO_DB_NAME"))
    try:
        sink.ensure_indexes()
        for d in days:
            commit_rows, pr_rows = _load_mongo_rows(
                sink.db, day=d, repo_id=repo_id
            )
            result = compute_daily_metrics(
                day=d,
                commit_stat_rows=commit_rows,
                pull_request_rows=pr_rows,
                computed_at=computed_at,
                include_commit_metrics=include_commit_metrics,
            )
            sink.write_repo_metrics(result.repo_metrics)
            sink.write_user_metrics(result.user_metrics)
            sink.write_commit_metrics(result.commit_metrics)
    finally:
        sink.close()


def _normalize_sqlite_url(db_url: str) -> str:
    """
    Normalize SQLite URLs to a sync driver URL so callers can pass either:
    - sqlite:///...
    - sqlite+aiosqlite:///...
    """
    if "sqlite+aiosqlite://" in db_url:
        return db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
    return db_url


def _naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to naive UTC (BSON/ClickHouse friendly)."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_uuid(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _clickhouse_query_dicts(client: Any, query: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _load_clickhouse_rows(
    client: Any, *, day: date, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from ClickHouse.

    Uses ClickHouse SQL to:
    - join `git_commits` with `git_commit_stats` by (repo_id, commit_hash)
    - filter commits by `committer_when` inside the day window
    - fetch PRs created/merged within the day window
    """
    start, end = _utc_day_window(day)
    params: Dict[str, Any] = {"start": _naive_utc(start), "end": _naive_utc(end)}
    repo_filter = ""
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        repo_filter = " AND c.repo_id = {repo_id:UUID}"

    commit_query = f"""
    SELECT
      c.repo_id AS repo_id,
      c.hash AS commit_hash,
      c.author_email AS author_email,
      c.author_name AS author_name,
      c.committer_when AS committer_when,
      s.file_path AS file_path,
      s.additions AS additions,
      s.deletions AS deletions
    FROM git_commits AS c
    LEFT JOIN git_commit_stats AS s
      ON (s.repo_id = c.repo_id) AND (s.commit_hash = c.hash)
    WHERE c.committer_when >= {{start:DateTime}} AND c.committer_when < {{end:DateTime}}
    {repo_filter}
    """

    pr_query = f"""
    SELECT
      repo_id,
      number,
      author_email,
      author_name,
      created_at,
      merged_at
    FROM git_pull_requests
    WHERE
      (created_at >= {{start:DateTime}} AND created_at < {{end:DateTime}})
      OR (merged_at IS NOT NULL AND merged_at >= {{start:DateTime}} AND merged_at < {{end:DateTime}})
      {("AND repo_id = {repo_id:UUID}" if repo_id is not None else "")}
    """

    commit_dicts = _clickhouse_query_dicts(client, commit_query, params)
    pr_dicts = _clickhouse_query_dicts(client, pr_query, params)

    commit_rows: List[CommitStatRow] = []
    for row in commit_dicts:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        commit_hash = row.get("commit_hash")
        committer_when = row.get("committer_when")
        if repo_uuid is None or not commit_hash or not isinstance(committer_when, datetime):
            continue
        file_path = row.get("file_path") or None
        commit_rows.append(
            {
                "repo_id": repo_uuid,
                "commit_hash": str(commit_hash),
                "author_email": row.get("author_email"),
                "author_name": row.get("author_name"),
                "committer_when": committer_when,
                "file_path": str(file_path) if file_path else None,
                "additions": int(row.get("additions") or 0),
                "deletions": int(row.get("deletions") or 0),
            }
        )

    pr_rows: List[PullRequestRow] = []
    for row in pr_dicts:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        created_at = row.get("created_at")
        if repo_uuid is None or not isinstance(created_at, datetime):
            continue
        pr_rows.append(
            {
                "repo_id": repo_uuid,
                "number": int(row.get("number") or 0),
                "author_email": row.get("author_email"),
                "author_name": row.get("author_name"),
                "created_at": created_at,
                "merged_at": row.get("merged_at") if isinstance(row.get("merged_at"), datetime) else None,
            }
        )

    return commit_rows, pr_rows


def _chunked(values: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), chunk_size):
        yield list(values[i : i + chunk_size])


def _load_mongo_rows(
    db: Any, *, day: date, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from MongoDB.

    Mongo doesn't have a cheap join, so we:
    1) query commits for the day window
    2) add one synthetic "0-stats" row per commit (ensures commits with no stats are counted)
    3) query commit_stats for those commits and append real stat rows
    4) query PRs created/merged in the day window
    """
    start, end = _utc_day_window(day)
    start_naive = _naive_utc(start)
    end_naive = _naive_utc(end)

    commit_filter: Dict[str, Any] = {
        "committer_when": {"$gte": start_naive, "$lt": end_naive}
    }
    if repo_id is not None:
        commit_filter["repo_id"] = str(repo_id)

    commit_projection = {
        "repo_id": 1,
        "hash": 1,
        "author_email": 1,
        "author_name": 1,
        "committer_when": 1,
    }
    commits = list(db["git_commits"].find(commit_filter, commit_projection))

    commit_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
    commit_hashes_by_repo: Dict[str, List[str]] = {}
    commit_rows: List[CommitStatRow] = []

    for doc in commits:
        repo_id_raw = doc.get("repo_id")
        commit_hash = doc.get("hash")
        repo_uuid = _parse_uuid(repo_id_raw)
        if repo_uuid is None or not commit_hash:
            continue
        repo_id_str = str(repo_id_raw)
        commit_hash_str = str(commit_hash)
        meta = {
            "repo_uuid": repo_uuid,
            "author_email": doc.get("author_email"),
            "author_name": doc.get("author_name"),
            "committer_when": doc.get("committer_when") or start_naive,
        }
        commit_meta[(repo_id_str, commit_hash_str)] = meta
        commit_hashes_by_repo.setdefault(repo_id_str, []).append(commit_hash_str)

        # Synthetic row ensures commits with no stats are still counted.
        commit_rows.append(
            {
                "repo_id": repo_uuid,
                "commit_hash": commit_hash_str,
                "author_email": meta["author_email"],
                "author_name": meta["author_name"],
                "committer_when": meta["committer_when"],
                "file_path": None,
                "additions": 0,
                "deletions": 0,
            }
        )

    stat_projection = {
        "repo_id": 1,
        "commit_hash": 1,
        "file_path": 1,
        "additions": 1,
        "deletions": 1,
    }

    # Fetch stats per repo to keep $in lists reasonable.
    for repo_id_str, hashes in commit_hashes_by_repo.items():
        for chunk in _chunked(hashes, chunk_size=1000):
            stat_filter = {"repo_id": repo_id_str, "commit_hash": {"$in": chunk}}
            for stat in db["git_commit_stats"].find(stat_filter, stat_projection):
                commit_hash = stat.get("commit_hash")
                key = (repo_id_str, str(commit_hash))
                meta = commit_meta.get(key)
                if meta is None:
                    continue

                file_path = stat.get("file_path") or None
                commit_rows.append(
                    {
                        "repo_id": meta["repo_uuid"],
                        "commit_hash": key[1],
                        "author_email": meta["author_email"],
                        "author_name": meta["author_name"],
                        "committer_when": meta["committer_when"],
                        "file_path": str(file_path) if file_path else None,
                        "additions": int(stat.get("additions") or 0),
                        "deletions": int(stat.get("deletions") or 0),
                    }
                )

    pr_filter: Dict[str, Any] = {
        "$or": [
            {"created_at": {"$gte": start_naive, "$lt": end_naive}},
            {"merged_at": {"$gte": start_naive, "$lt": end_naive}},
        ]
    }
    if repo_id is not None:
        pr_filter["repo_id"] = str(repo_id)

    pr_projection = {
        "repo_id": 1,
        "number": 1,
        "author_email": 1,
        "author_name": 1,
        "created_at": 1,
        "merged_at": 1,
    }
    pr_docs = list(db["git_pull_requests"].find(pr_filter, pr_projection))

    pr_rows: List[PullRequestRow] = []
    for doc in pr_docs:
        repo_uuid = _parse_uuid(doc.get("repo_id"))
        created_at = doc.get("created_at")
        if repo_uuid is None or not isinstance(created_at, datetime):
            continue
        merged_at = doc.get("merged_at")
        pr_rows.append(
            {
                "repo_id": repo_uuid,
                "number": int(doc.get("number") or 0),
                "author_email": doc.get("author_email"),
                "author_name": doc.get("author_name"),
                "created_at": created_at,
                "merged_at": merged_at if isinstance(merged_at, datetime) else None,
            }
        )

    return commit_rows, pr_rows


def _load_sqlite_rows(
    engine: Any, *, day: date, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from SQLite via SQLAlchemy ORM models.

    Uses a left join between `git_commits` and `git_commit_stats` so commits with
    no stats are still counted (via a synthetic 0-stats row).
    """
    from sqlalchemy import and_, or_, select
    from sqlalchemy.orm import Session

    from models.git import GitCommit, GitCommitStat, GitPullRequest

    start, end = _utc_day_window(day)
    start_naive = _naive_utc(start)
    end_naive = _naive_utc(end)

    commit_stmt = (
        select(
            GitCommit.repo_id,
            GitCommit.hash.label("commit_hash"),
            GitCommit.author_email,
            GitCommit.author_name,
            GitCommit.committer_when,
            GitCommitStat.file_path,
            GitCommitStat.additions,
            GitCommitStat.deletions,
        )
        .select_from(GitCommit)
        .outerjoin(
            GitCommitStat,
            and_(
                GitCommitStat.repo_id == GitCommit.repo_id,
                GitCommitStat.commit_hash == GitCommit.hash,
            ),
        )
        .where(GitCommit.committer_when >= start_naive, GitCommit.committer_when < end_naive)
    )
    if repo_id is not None:
        commit_stmt = commit_stmt.where(GitCommit.repo_id == repo_id)

    pr_stmt = (
        select(
            GitPullRequest.repo_id,
            GitPullRequest.number,
            GitPullRequest.author_email,
            GitPullRequest.author_name,
            GitPullRequest.created_at,
            GitPullRequest.merged_at,
        )
        .select_from(GitPullRequest)
        .where(
            or_(
                and_(
                    GitPullRequest.created_at >= start_naive,
                    GitPullRequest.created_at < end_naive,
                ),
                and_(
                    GitPullRequest.merged_at.is_not(None),
                    GitPullRequest.merged_at >= start_naive,
                    GitPullRequest.merged_at < end_naive,
                ),
            )
        )
    )
    if repo_id is not None:
        pr_stmt = pr_stmt.where(GitPullRequest.repo_id == repo_id)

    commit_rows: List[CommitStatRow] = []
    pr_rows: List[PullRequestRow] = []

    with Session(engine) as session:
        for (
            repo_uuid,
            commit_hash,
            author_email,
            author_name,
            committer_when,
            file_path,
            additions,
            deletions,
        ) in session.execute(commit_stmt).all():
            commit_rows.append(
                {
                    "repo_id": repo_uuid,
                    "commit_hash": str(commit_hash),
                    "author_email": author_email,
                    "author_name": author_name,
                    "committer_when": committer_when,
                    "file_path": str(file_path) if file_path else None,
                    "additions": int(additions or 0),
                    "deletions": int(deletions or 0),
                }
            )

        for (
            repo_uuid,
            number,
            author_email,
            author_name,
            created_at,
            merged_at,
        ) in session.execute(pr_stmt).all():
            pr_rows.append(
                {
                    "repo_id": repo_uuid,
                    "number": int(number or 0),
                    "author_email": author_email,
                    "author_name": author_name,
                    "created_at": created_at,
                    "merged_at": merged_at,
                }
            )

    return commit_rows, pr_rows
