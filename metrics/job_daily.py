from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from metrics.compute import compute_daily_metrics
from metrics.compute_wellbeing import compute_team_wellbeing_metrics_daily
from metrics.compute_work_items import compute_work_item_metrics_daily
from metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from metrics.hotspots import compute_file_hotspots
from metrics.schemas import CommitStatRow, PullRequestRow, PullRequestReviewRow
from metrics.work_items import (
    DiscoveredRepo,
    fetch_github_project_v2_items,
    fetch_github_work_items,
    fetch_gitlab_work_items,
    fetch_jira_work_items,
    parse_github_projects_v2_env,
)
from metrics.sinks.clickhouse import ClickHouseMetricsSink
from metrics.sinks.mongo import MongoMetricsSink
from metrics.sinks.sqlite import SQLiteMetricsSink
from providers.identity import load_identity_resolver
from providers.status_mapping import load_status_mapping
from providers.teams import load_team_resolver
from storage import detect_db_type

logger = logging.getLogger(__name__)


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
    sink: str = "auto",  # auto|clickhouse|mongo|sqlite|both
    provider: str = "all",  # all|jira|github|gitlab|none
) -> None:
    """
    Compute and persist daily metrics into ClickHouse/MongoDB (and SQLite for dev).

    Source data:
    - git facts are read from the backend pointed to by `db_url`:
      - ClickHouse: `git_commits`, `git_commit_stats`, `git_pull_requests`
      - MongoDB: `git_commits`, `git_commit_stats`, `git_pull_requests`
      - SQLite: `git_commits`, `git_commit_stats`, `git_pull_requests`

    Derived metrics are written back into the same backend:
    - `repo_metrics_daily`, `user_metrics_daily`, `commit_metrics`
    - `team_metrics_daily`
    - `work_item_metrics_daily`, `work_item_user_metrics_daily`, `work_item_cycle_times`

    When `sink='both'`, metrics are written to the backend given by `db_url` and
    also to the other sink configured via env:
    - ClickHouse: `CLICKHOUSE_DSN` (or CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD)
    - MongoDB: `MONGO_URI` (+ optional `MONGO_DB_NAME`)
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

    sink = (sink or "auto").strip().lower()
    if sink == "auto":
        sink = backend

    if sink not in {"clickhouse", "mongo", "sqlite", "both"}:
        raise ValueError("sink must be one of: auto, clickhouse, mongo, sqlite, both")
    if sink != "both" and sink != backend:
        raise ValueError(
            f"sink='{sink}' requires db backend '{sink}', got '{backend}'. "
            "For cross-backend writes use sink='both'."
        )
    if sink == "both" and backend not in {"clickhouse", "mongo"}:
        raise ValueError(
            "sink='both' is only supported when source backend is clickhouse or mongo"
        )

    days = _date_range(day, backfill_days)
    computed_at = datetime.now(timezone.utc)

    status_mapping = load_status_mapping()
    identity = load_identity_resolver()
    team_resolver = load_team_resolver()

    logger.info(
        "Daily metrics job: backend=%s sink=%s day=%s backfill=%d repo_id=%s provider=%s",
        backend,
        sink,
        day.isoformat(),
        backfill_days,
        str(repo_id) if repo_id else "",
        provider,
    )

    # Optional work tracking fetch (provider APIs).
    provider = (provider or "all").strip().lower()
    provider_set = set()
    provider_strict = provider in {"jira", "github", "gitlab"}
    if provider in {"all", "*"}:
        provider_set = {"jira", "github", "gitlab"}
    elif provider in {"none", "off", "skip"}:
        provider_set = set()
    else:
        provider_set = {provider}
    unknown_providers = provider_set - {"jira", "github", "gitlab"}
    if unknown_providers:
        raise ValueError(f"Unknown provider(s): {sorted(unknown_providers)}")

    # Primary sink is always the same as `backend` unless sink='both'.
    primary_sink: Any
    secondary_sink: Optional[Any] = None

    if backend == "clickhouse":
        primary_sink = ClickHouseMetricsSink(db_url)
        if sink == "both":
            secondary_sink = MongoMetricsSink(
                _mongo_uri_from_env(), db_name=os.getenv("MONGO_DB_NAME")
            )
    elif backend == "mongo":
        primary_sink = MongoMetricsSink(db_url, db_name=os.getenv("MONGO_DB_NAME"))
        if sink == "both":
            secondary_sink = ClickHouseMetricsSink(_clickhouse_dsn_from_env())
    else:
        primary_sink = SQLiteMetricsSink(_normalize_sqlite_url(db_url))

    sinks: List[Any] = [primary_sink] + (
        [secondary_sink] if secondary_sink is not None else []
    )

    try:
        for s in sinks:
            if isinstance(s, ClickHouseMetricsSink):
                logger.info("Ensuring ClickHouse tables/migrations")
                s.ensure_tables()
            elif isinstance(s, MongoMetricsSink):
                logger.info("Ensuring Mongo indexes")
                s.ensure_indexes()
            elif isinstance(s, SQLiteMetricsSink):
                logger.info("Ensuring SQLite tables")
                s.ensure_tables()

        work_items: List[Any] = []
        work_item_transitions: List[Any] = []
        if provider_set:
            since_dt = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
            logger.info(
                "Fetching work items since %s (providers=%s)",
                since_dt.isoformat(),
                sorted(provider_set),
            )
            discovered_repos = _discover_repos(
                backend=backend,
                primary_sink=primary_sink,
                repo_id=repo_id,
            )
            logger.info(
                "Discovered %d repos from backend for work item ingestion",
                len(discovered_repos),
            )

            if "jira" in provider_set:
                try:
                    jira_items, jira_transitions = fetch_jira_work_items(
                        since=since_dt,
                        until=datetime.combine(
                            max(days), time.max, tzinfo=timezone.utc
                        ),
                        status_mapping=status_mapping,
                        identity=identity,
                    )
                    work_items.extend(jira_items)
                    work_item_transitions.extend(jira_transitions)
                except Exception as exc:
                    if provider_strict and provider == "jira":
                        raise
                    logger.warning("Skipping Jira work items fetch: %s", exc)

            if "github" in provider_set:
                try:
                    wi, transitions = fetch_github_work_items(
                        repos=discovered_repos,
                        since=since_dt,
                        status_mapping=status_mapping,
                        identity=identity,
                        include_issue_events=True,
                    )
                    work_items.extend(wi)
                    work_item_transitions.extend(transitions)
                except Exception as exc:
                    if provider_strict and provider == "github":
                        raise
                    logger.warning("Skipping GitHub work items fetch: %s", exc)

                try:
                    projects = parse_github_projects_v2_env()
                    if projects:
                        project_items, _project_transitions = (
                            fetch_github_project_v2_items(
                                projects=projects,
                                status_mapping=status_mapping,
                                identity=identity,
                            )
                        )
                        # Project item status can override plain issue status.
                        by_id = {w.work_item_id: w for w in work_items}
                        for w in project_items:
                            by_id[w.work_item_id] = w
                        work_items = list(by_id.values())
                except Exception as exc:
                    logger.warning("Skipping GitHub Projects v2 fetch: %s", exc)

            if "gitlab" in provider_set:
                try:
                    gitlab_items, gitlab_transitions = fetch_gitlab_work_items(
                        repos=discovered_repos,
                        since=since_dt,
                        status_mapping=status_mapping,
                        identity=identity,
                        include_label_events=True,
                    )
                    work_items.extend(gitlab_items)
                    work_item_transitions.extend(gitlab_transitions)
                except Exception as exc:
                    if provider_strict and provider == "gitlab":
                        raise
                    logger.warning("Skipping GitLab work items fetch: %s", exc)

            logger.info("Work items ready for compute: %d", len(work_items))
            logger.info(
                "Work item transitions ready for compute: %d",
                len(work_item_transitions),
            )

        business_tz = os.getenv("BUSINESS_TIMEZONE", "UTC")
        business_start = int(os.getenv("BUSINESS_HOURS_START", "9"))
        business_end = int(os.getenv("BUSINESS_HOURS_END", "17"))

        for d in days:
            logger.info("Computing metrics for day=%s", d.isoformat())
            start, end = _utc_day_window(d)
            if backend == "clickhouse":
                commit_rows, pr_rows = _load_clickhouse_rows(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
                review_rows = _load_clickhouse_reviews(
                    primary_sink.client,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
            elif backend == "sqlite":
                commit_rows, pr_rows = _load_sqlite_rows(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
                review_rows = _load_sqlite_reviews(
                    primary_sink.engine,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
            else:
                commit_rows, pr_rows = _load_mongo_rows(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
                review_rows = _load_mongo_reviews(
                    primary_sink.db,
                    start=start,
                    end=end,
                    repo_id=repo_id,  # type: ignore[attr-defined]
                )
            logger.info(
                "Loaded source facts: commits=%d pr_rows=%d",
                len(commit_rows),
                len(pr_rows),
            )

            # --- MTTR (proxied by Bug Cycle Time) ---
            mttr_by_repo: Dict[uuid.UUID, float] = {}
            bug_times: Dict[uuid.UUID, List[float]] = {}
            for item in work_items:
                if item.type == "bug" and item.completed_at and item.started_at:
                    start_dt = _to_utc(item.started_at)
                    comp_dt = _to_utc(item.completed_at)
                    if start_dt < end and comp_dt >= start:
                        # Find the repo_id. WorkItem from models/work_items.py has repo_id.
                        r_id = getattr(item, "repo_id", None)
                        if r_id:
                            hours = (comp_dt - start_dt).total_seconds() / 3600.0
                            bug_times.setdefault(r_id, []).append(hours)

            for r_id, times in bug_times.items():
                mttr_by_repo[r_id] = sum(times) / len(times)

            result = compute_daily_metrics(
                day=d,
                commit_stat_rows=commit_rows,
                pull_request_rows=pr_rows,
                pull_request_review_rows=review_rows,
                computed_at=computed_at,
                include_commit_metrics=include_commit_metrics,
                team_resolver=team_resolver,
                mttr_by_repo=mttr_by_repo,
            )

            # --- Hotspots (30-day window) ---
            window_days = 30
            h_start = datetime.combine(
                d - timedelta(days=window_days - 1), time.min, tzinfo=timezone.utc
            )
            if backend == "clickhouse":
                h_commit_rows, _ = _load_clickhouse_rows(
                    primary_sink.client, start=h_start, end=end, repo_id=repo_id
                )
            elif backend == "sqlite":
                h_commit_rows, _ = _load_sqlite_rows(
                    primary_sink.engine, start=h_start, end=end, repo_id=repo_id
                )
            else:
                h_commit_rows, _ = _load_mongo_rows(
                    primary_sink.db, start=h_start, end=end, repo_id=repo_id
                )

            # Discover repos for this day if not already done.
            # (In reality, we should iterate over each repo separately or group them).
            active_repos: Set[uuid.UUID] = {r["repo_id"] for r in commit_rows}
            all_file_metrics = []
            for r_id in sorted(active_repos, key=str):
                all_file_metrics.extend(
                    compute_file_hotspots(
                        repo_id=r_id,
                        day=d,
                        window_stats=h_commit_rows,
                        computed_at=computed_at,
                    )
                )

            team_metrics = compute_team_wellbeing_metrics_daily(
                day=d,
                commit_stat_rows=commit_rows,
                team_resolver=team_resolver,
                computed_at=computed_at,
                business_timezone=business_tz,
                business_hours_start=business_start,
                business_hours_end=business_end,
            )

            wi_metrics, wi_user_metrics, wi_cycle_times = (
                compute_work_item_metrics_daily(
                    day=d,
                    work_items=work_items,
                    computed_at=computed_at,
                    team_resolver=team_resolver,
                )
            )
            wi_state_durations = compute_work_item_state_durations_daily(
                day=d,
                work_items=work_items,
                transitions=work_item_transitions,
                computed_at=computed_at,
                team_resolver=team_resolver,
            )
            logger.info(
                "Computed derived metrics: repo=%d user=%d commit=%d team=%d wi=%d wi_user=%d wi_facts=%d",
                len(result.repo_metrics),
                len(result.user_metrics),
                len(result.commit_metrics),
                len(team_metrics),
                len(wi_metrics),
                len(wi_user_metrics),
                len(wi_cycle_times),
            )
            logger.info("Computed time-in-state rows: %d", len(wi_state_durations))

            for s in sinks:
                logger.debug("Writing derived metrics to sink=%s", type(s).__name__)
                s.write_repo_metrics(result.repo_metrics)
                s.write_user_metrics(result.user_metrics)
                s.write_commit_metrics(result.commit_metrics)
                s.write_file_metrics(all_file_metrics)
                s.write_team_metrics(team_metrics)
                s.write_work_item_metrics(wi_metrics)
                s.write_work_item_user_metrics(wi_user_metrics)
                s.write_work_item_cycle_times(wi_cycle_times)
                s.write_work_item_state_durations(wi_state_durations)
    finally:
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def _mongo_uri_from_env() -> str:
    uri = os.getenv("MONGO_URI") or ""
    if not uri:
        raise ValueError(
            "MONGO_URI is required for sink='both' when source is ClickHouse"
        )
    return uri


def _clickhouse_dsn_from_env() -> str:
    dsn = os.getenv("CLICKHOUSE_DSN") or ""
    if dsn:
        return dsn
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = os.getenv("CLICKHOUSE_PORT", "8123")
    db = os.getenv("CLICKHOUSE_DB", "default")
    user = os.getenv("CLICKHOUSE_USER", "")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")

    auth = ""
    if user and password:
        auth = f"{user}:{password}@"
    elif user:
        auth = f"{user}@"
    return f"clickhouse://{auth}{host}:{port}/{db}"


def _safe_json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return None


def _discover_repos(
    *, backend: str, primary_sink: Any, repo_id: Optional[uuid.UUID]
) -> List[DiscoveredRepo]:
    """
    Discover repos from the synced backend so work item ingestion can reuse them as sources.

    Returns DiscoveredRepo rows with `.source` set from repo.settings["source"] when present.
    """
    repos: List[DiscoveredRepo] = []

    if backend == "clickhouse":
        client = primary_sink.client  # type: ignore[attr-defined]
        params: Dict[str, Any] = {}
        where = ""
        if repo_id is not None:
            params["repo_id"] = str(repo_id)
            where = "WHERE id = {repo_id:UUID}"
        rows = _clickhouse_query_dicts(
            client,
            f"SELECT id, repo, settings, tags FROM repos {where}",
            params,
        )
        for row in rows:
            repo_uuid = _parse_uuid(row.get("id"))
            full_name = row.get("repo")
            if repo_uuid is None or not full_name:
                continue
            settings = _safe_json_loads(row.get("settings")) or {}
            tags = _safe_json_loads(row.get("tags")) or []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source and isinstance(tags, list):
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=repo_uuid,
                    full_name=str(full_name),
                    source=source or "unknown",
                    settings=settings if isinstance(settings, dict) else {},
                )
            )
        return repos

    if backend == "mongo":
        db = primary_sink.db  # type: ignore[attr-defined]
        query: Dict[str, Any] = {}
        if repo_id is not None:
            query["_id"] = str(repo_id)
        projection = {"_id": 1, "repo": 1, "settings": 1, "tags": 1}
        docs = list(db["repos"].find(query, projection))
        for doc in docs:
            repo_uuid = _parse_uuid(doc.get("_id") or doc.get("id"))
            full_name = doc.get("repo")
            if repo_uuid is None or not full_name:
                continue
            settings = (
                doc.get("settings") if isinstance(doc.get("settings"), dict) else {}
            )
            tags = doc.get("tags") if isinstance(doc.get("tags"), list) else []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source:
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=repo_uuid,
                    full_name=str(full_name),
                    source=source or "unknown",
                    settings=settings,
                )
            )
        return sorted(repos, key=lambda r: r.full_name)

    # sqlite
    from sqlalchemy.orm import Session

    from models.git import Repo

    engine = primary_sink.engine  # type: ignore[attr-defined]
    with Session(engine) as session:
        q = session.query(Repo)
        if repo_id is not None:
            q = q.filter(Repo.id == repo_id)
        for r in q.all():
            r_settings = getattr(r, "settings", None)
            settings = r_settings if isinstance(r_settings, dict) else {}
            r_tags = getattr(r, "tags", None)
            tags = r_tags if isinstance(r_tags, list) else []
            source = str((settings or {}).get("source") or "").strip().lower()
            if not source:
                for t in tags:
                    if str(t).strip().lower() in {"github", "gitlab"}:
                        source = str(t).strip().lower()
                        break
            repos.append(
                DiscoveredRepo(
                    repo_id=getattr(r, "id"),
                    full_name=str(getattr(r, "repo")),
                    source=source or "unknown",
                    settings=settings,
                )
            )
    return repos


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


def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC tzinfo."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_uuid(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _clickhouse_query_dicts(
    client: Any, query: str, parameters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=parameters)
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


def _load_clickhouse_rows(
    client: Any, *, start: datetime, end: datetime, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from ClickHouse.

    Uses ClickHouse SQL to:
    - join `git_commits` with `git_commit_stats` by (repo_id, commit_hash)
    - filter commits by `committer_when` inside the day window
    - fetch PRs created/merged within the day window
    """
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
        if (
            repo_uuid is None
            or not commit_hash
            or not isinstance(committer_when, datetime)
        ):
            continue
        file_path = row.get("file_path") or None
        commit_rows.append({
            "repo_id": repo_uuid,
            "commit_hash": str(commit_hash),
            "author_email": row.get("author_email"),
            "author_name": row.get("author_name"),
            "committer_when": committer_when,
            "file_path": str(file_path) if file_path else None,
            "additions": int(row.get("additions") or 0),
            "deletions": int(row.get("deletions") or 0),
        })

    pr_rows: List[PullRequestRow] = []
    for row in pr_dicts:
        repo_uuid = _parse_uuid(row.get("repo_id"))
        created_at = row.get("created_at")
        if repo_uuid is None or not isinstance(created_at, datetime):
            continue
        pr_rows.append({
            "repo_id": repo_uuid,
            "number": int(row.get("number") or 0),
            "author_email": row.get("author_email"),
            "author_name": row.get("author_name"),
            "created_at": created_at,
            "merged_at": row.get("merged_at")
            if isinstance(row.get("merged_at"), datetime)
            else None,
        })

    return commit_rows, pr_rows


def _chunked(values: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), chunk_size):
        yield list(values[i : i + chunk_size])


def _load_mongo_rows(
    db: Any, *, start: datetime, end: datetime, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from MongoDB.

    Mongo doesn't have a cheap join, so we:
    1) query commits for the day window
    2) add one synthetic "0-stats" row per commit (ensures commits with no stats are counted)
    3) query commit_stats for those commits and append real stat rows
    4) query PRs created/merged in the day window
    """
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
        commit_rows.append({
            "repo_id": repo_uuid,
            "commit_hash": commit_hash_str,
            "author_email": meta["author_email"],
            "author_name": meta["author_name"],
            "committer_when": meta["committer_when"],
            "file_path": None,
            "additions": 0,
            "deletions": 0,
        })

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
                commit_rows.append({
                    "repo_id": meta["repo_uuid"],
                    "commit_hash": key[1],
                    "author_email": meta["author_email"],
                    "author_name": meta["author_name"],
                    "committer_when": meta["committer_when"],
                    "file_path": str(file_path) if file_path else None,
                    "additions": int(stat.get("additions") or 0),
                    "deletions": int(stat.get("deletions") or 0),
                })

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
        pr_rows.append({
            "repo_id": repo_uuid,
            "number": int(doc.get("number") or 0),
            "author_email": doc.get("author_email"),
            "author_name": doc.get("author_name"),
            "created_at": created_at,
            "merged_at": merged_at if isinstance(merged_at, datetime) else None,
        })

    return commit_rows, pr_rows


def _load_sqlite_rows(
    engine: Any, *, start: datetime, end: datetime, repo_id: Optional[uuid.UUID]
) -> Tuple[List[CommitStatRow], List[PullRequestRow]]:
    """
    Load source rows for a single day from SQLite via SQLAlchemy ORM models.

    Uses a left join between `git_commits` and `git_commit_stats` so commits with
    no stats are still counted (via a synthetic 0-stats row).
    """
    from sqlalchemy import and_, or_, select
    from sqlalchemy.orm import Session

    from models.git import GitCommit, GitCommitStat, GitPullRequest

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
        .where(
            GitCommit.committer_when >= start_naive,
            GitCommit.committer_when < end_naive,
        )
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
            commit_rows.append({
                "repo_id": repo_uuid,
                "commit_hash": str(commit_hash),
                "author_email": author_email,
                "author_name": author_name,
                "committer_when": committer_when,
                "file_path": str(file_path) if file_path else None,
                "additions": int(additions or 0),
                "deletions": int(deletions or 0),
            })

        for (
            repo_uuid,
            number,
            author_email,
            author_name,
            created_at,
            merged_at,
        ) in session.execute(pr_stmt).all():
            pr_rows.append({
                "repo_id": repo_uuid,
                "number": int(number or 0),
                "author_email": author_email,
                "author_name": author_name,
                "created_at": created_at,
                "merged_at": merged_at,
            })

    return commit_rows, pr_rows


def _load_clickhouse_reviews(
    client, start: datetime, end: datetime, repo_id: Optional[uuid.UUID] = None
) -> List[PullRequestReviewRow]:
    # Simple query for clickhouse-connect
    sql = """
        SELECT repo_id, number, reviewer, submitted_at, state
        FROM git_pull_request_reviews
        WHERE submitted_at >= %(start)s AND submitted_at < %(end)s
    """
    params = {"start": start, "end": end}
    if repo_id:
        sql += " AND repo_id = %(repo_id)s"
        params["repo_id"] = str(repo_id)

    result = client.query(sql, parameters=params)
    rows: List[PullRequestReviewRow] = []
    for r in result.named_results():
        submitted_at = r["submitted_at"]
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))

        rows.append({
            "repo_id": uuid.UUID(str(r["repo_id"])),
            "number": int(r["number"]),
            "reviewer": str(r["reviewer"]),
            "submitted_at": submitted_at,
            "state": str(r["state"]),
        })
    return rows


def _load_mongo_reviews(
    db, start: datetime, end: datetime, repo_id: Optional[uuid.UUID] = None
) -> List[PullRequestReviewRow]:
    query = {"submitted_at": {"$gte": start, "$lt": end}}
    if repo_id:
        query["repo_id"] = str(repo_id)

    rows: List[PullRequestReviewRow] = []
    for doc in db["git_pull_request_reviews"].find(query):
        submitted_at = doc["submitted_at"]
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))

        rows.append({
            "repo_id": uuid.UUID(str(doc["repo_id"])),
            "number": int(doc["number"]),
            "reviewer": str(doc["reviewer"]),
            "submitted_at": submitted_at,
            "state": str(doc["state"]),
        })
    return rows


def _load_sqlite_reviews(
    engine, start: datetime, end: datetime, repo_id: Optional[uuid.UUID] = None
) -> List[PullRequestReviewRow]:
    from models.git import GitPullRequestReview
    from sqlalchemy import select

    with engine.connect() as conn:
        stmt = select(
            GitPullRequestReview.repo_id,
            GitPullRequestReview.number,
            GitPullRequestReview.reviewer,
            GitPullRequestReview.submitted_at,
            GitPullRequestReview.state,
        ).where(
            GitPullRequestReview.submitted_at >= start,
            GitPullRequestReview.submitted_at < end,
        )
        if repo_id:
            stmt = stmt.where(GitPullRequestReview.repo_id == repo_id)

        rows: List[PullRequestReviewRow] = []
        for r in conn.execute(stmt).all():
            submitted_at = r[3]
            if isinstance(submitted_at, str):
                submitted_at = datetime.fromisoformat(
                    submitted_at.replace("Z", "+00:00")
                )

            rows.append({
                "repo_id": r[0],
                "number": int(r[1] or 0),
                "reviewer": str(r[2]),
                "submitted_at": submitted_at,
                "state": str(r[4]),
            })
        return rows
