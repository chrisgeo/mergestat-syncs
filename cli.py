#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from processors.github import process_github_repo, process_github_repos_batch
from processors.gitlab import process_gitlab_project, process_gitlab_projects_batch
from processors.local import process_local_repo
from storage import create_store, detect_db_type
from utils import _parse_since

REPO_ROOT = Path(__file__).resolve().parent


def _load_dotenv(path: Path) -> int:
    """
    Load a .env file into process environment (without overriding existing vars).

    Keeps dependencies minimal (avoids python-dotenv).
    """
    if not path.exists():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in {"'", '"'}):
            value = value[1:-1]
        os.environ[key] = value
        loaded += 1
    return loaded


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def _since_from_date_backfill(day: date, backfill_days: int) -> datetime:
    backfill = max(1, int(backfill_days))
    start_day = day - timedelta(days=backfill - 1)
    return datetime(
        start_day.year,
        start_day.month,
        start_day.day,
        tzinfo=timezone.utc,
    )


def _resolve_db_type(db_url: str, db_type: Optional[str]) -> str:
    if db_type:
        resolved = db_type.lower()
    else:
        try:
            resolved = detect_db_type(db_url)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if resolved not in {"postgres", "mongo", "sqlite", "clickhouse"}:
        raise SystemExit(
            "DB_TYPE must be 'postgres', 'mongo', 'sqlite', or 'clickhouse'"
        )
    return resolved


async def _run_with_store(db_url: str, db_type: str, handler) -> None:
    store = create_store(db_url, db_type)
    async with store:
        await handler(store)


def _cmd_sync_local(ns: argparse.Namespace) -> int:
    db_type = _resolve_db_type(ns.db, ns.db_type)

    if ns.date is not None:
        since = _since_from_date_backfill(ns.date, ns.backfill)
    else:
        if int(ns.backfill) != 1:
            raise SystemExit("--backfill requires --date")
        since = _parse_since(ns.since)

    async def _handler(store):
        await process_local_repo(
            store=store,
            repo_path=ns.repo_path,
            since=since,
            fetch_blame=ns.fetch_blame,
        )

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _cmd_sync_github(ns: argparse.Namespace) -> int:
    token = ns.auth or os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitHub token (pass --auth or set GITHUB_TOKEN).")

    db_type = _resolve_db_type(ns.db, ns.db_type)

    if ns.date is not None:
        since = _since_from_date_backfill(ns.date, ns.backfill)
    else:
        if int(ns.backfill) != 1:
            raise SystemExit("--backfill requires --date")
        since = None

    async def _handler(store):
        if ns.search_pattern:
            org_name = ns.group
            user_name = ns.owner if not ns.group else None
            await process_github_repos_batch(
                store=store,
                token=token,
                org_name=org_name,
                user_name=user_name,
                pattern=ns.search_pattern,
                batch_size=ns.batch_size,
                max_concurrent=ns.max_concurrent,
                rate_limit_delay=ns.rate_limit_delay,
                max_commits_per_repo=ns.max_commits_per_repo,
                max_repos=ns.max_repos,
                use_async=ns.use_async,
                since=since,
            )
            return

        if not (ns.owner and ns.repo):
            raise SystemExit(
                "GitHub sync requires --owner and --repo (or --search-pattern for batch)."
            )
        await process_github_repo(
            store,
            ns.owner,
            ns.repo,
            token,
            fetch_blame=ns.fetch_blame,
            max_commits=ns.max_commits_per_repo or 100,
            since=since,
        )

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _cmd_sync_gitlab(ns: argparse.Namespace) -> int:
    token = ns.auth or os.getenv("GITLAB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")

    db_type = _resolve_db_type(ns.db, ns.db_type)

    if ns.date is not None:
        since = _since_from_date_backfill(ns.date, ns.backfill)
    else:
        if int(ns.backfill) != 1:
            raise SystemExit("--backfill requires --date")
        since = None

    async def _handler(store):
        if ns.search_pattern:
            await process_gitlab_projects_batch(
                store=store,
                token=token,
                gitlab_url=ns.gitlab_url,
                group_name=ns.group,
                pattern=ns.search_pattern,
                batch_size=ns.batch_size,
                max_concurrent=ns.max_concurrent,
                rate_limit_delay=ns.rate_limit_delay,
                max_commits_per_project=ns.max_commits_per_repo,
                max_projects=ns.max_repos,
                use_async=ns.use_async,
                since=since,
            )
            return

        if ns.project_id is None:
            raise SystemExit(
                "GitLab sync requires --project-id (or --search-pattern for batch)."
            )
        await process_gitlab_project(
            store,
            ns.project_id,
            token,
            ns.gitlab_url,
            fetch_blame=ns.fetch_blame,
            max_commits=ns.max_commits_per_repo or 100,
            since=since,
        )

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    # Import lazily to keep CLI startup fast and avoid optional deps at import time.
    from metrics.job_daily import run_daily_metrics_job

    run_daily_metrics_job(
        db_url=ns.db,
        day=ns.date,
        backfill_days=max(1, int(ns.backfill)),
        repo_id=ns.repo_id,
        repo_name=getattr(ns, "repo_name", None),
        include_commit_metrics=not ns.skip_commit_metrics,
        sink=ns.sink,
        provider=ns.provider,
    )
    return 0


def _cmd_grafana_up(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "grafana" / "docker-compose.yml"),
        "up",
        "-d",
    ]
    return subprocess.run(cmd, check=False).returncode


def _cmd_grafana_down(_ns: argparse.Namespace) -> int:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(REPO_ROOT / "grafana" / "docker-compose.yml"),
        "down",
    ]
    return subprocess.run(cmd, check=False).returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dev-health-ops",
        description="Sync git data and compute developer health metrics.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING). Defaults to env LOG_LEVEL or INFO.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- sync ----
    sync = sub.add_parser("sync", help="Sync git facts into a DB backend.")
    sync_sub = sync.add_subparsers(dest="source", required=True)

    local = sync_sub.add_parser("local", help="Sync from a local git repository.")
    local.add_argument("--db", required=True, help="Database connection string.")
    local.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    local.add_argument(
        "--repo-path", default=".", help="Path to the local git repository."
    )
    local_time = local.add_mutually_exclusive_group()
    local_time.add_argument("--since", help="Lower-bound ISO date/time (UTC).")
    local_time.add_argument(
        "--date",
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD (use with --backfill).",
    )
    local.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync N days ending at --date (inclusive). Requires --date.",
    )
    local.add_argument(
        "--fetch-blame", action="store_true", help="Fetch blame data (slow)."
    )
    local.add_argument(
        "--max-commits-per-repo", type=int, help="Limit commits analyzed."
    )
    local.set_defaults(func=_cmd_sync_local)

    gh = sync_sub.add_parser("github", help="Sync from GitHub (single repo or batch).")
    gh.add_argument("--db", required=True, help="Database connection string.")
    gh.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    gh.add_argument("--auth", help="GitHub token (defaults to GITHUB_TOKEN).")
    gh.add_argument("--owner", help="GitHub owner/org (single repo mode).")
    gh.add_argument("--repo", help="GitHub repo name (single repo mode).")
    gh.add_argument("--search-pattern", help="Batch mode pattern (e.g. 'org/*').")
    gh.add_argument("--group", help="Batch mode org/user name.")
    gh.add_argument("--batch-size", type=int, default=10)
    gh.add_argument("--max-concurrent", type=int, default=4)
    gh.add_argument("--rate-limit-delay", type=float, default=1.0)
    gh.add_argument("--max-repos", type=int)
    gh.add_argument("--use-async", action="store_true")
    gh.add_argument("--fetch-blame", action="store_true")
    gh.add_argument("--max-commits-per-repo", type=int)
    gh.add_argument(
        "--date",
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD (use with --backfill).",
    )
    gh.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync N days ending at --date (inclusive). Requires --date.",
    )
    gh.set_defaults(func=_cmd_sync_github)

    gl = sync_sub.add_parser(
        "gitlab", help="Sync from GitLab (single project or batch)."
    )
    gl.add_argument("--db", required=True, help="Database connection string.")
    gl.add_argument(
        "--db-type",
        choices=["postgres", "mongo", "sqlite", "clickhouse"],
        help="Optional DB backend override.",
    )
    gl.add_argument("--auth", help="GitLab token (defaults to GITLAB_TOKEN).")
    gl.add_argument(
        "--gitlab-url",
        default=os.getenv("GITLAB_URL", "https://gitlab.com"),
        help="GitLab instance URL.",
    )
    gl.add_argument(
        "--project-id", type=int, help="GitLab project ID (single project mode)."
    )
    gl.add_argument("--search-pattern", help="Batch mode pattern (e.g. 'group/*').")
    gl.add_argument("--group", help="Batch mode group name.")
    gl.add_argument("--batch-size", type=int, default=10)
    gl.add_argument("--max-concurrent", type=int, default=4)
    gl.add_argument("--rate-limit-delay", type=float, default=1.0)
    gl.add_argument("--max-repos", type=int)
    gl.add_argument("--use-async", action="store_true")
    gl.add_argument("--fetch-blame", action="store_true")
    gl.add_argument("--max-commits-per-repo", type=int)
    gl.add_argument(
        "--date",
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD (use with --backfill).",
    )
    gl.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Sync N days ending at --date (inclusive). Requires --date.",
    )
    gl.set_defaults(func=_cmd_sync_gitlab)

    # ---- metrics ----
    metrics = sub.add_parser("metrics", help="Compute and write derived metrics.")
    metrics_sub = metrics.add_subparsers(dest="metrics_command", required=True)

    daily = metrics_sub.add_parser(
        "daily", help="Compute daily metrics (optionally backfill)."
    )
    daily.add_argument(
        "--date",
        required=True,
        type=_parse_date,
        help="Target day (UTC) as YYYY-MM-DD.",
    )
    daily.add_argument(
        "--backfill",
        type=int,
        default=1,
        help="Compute N days ending at --date (inclusive).",
    )
    daily.add_argument(
        "--db",
        default=os.getenv("DB_CONN_STRING") or os.getenv("DATABASE_URL"),
        help="Source DB URI (and default sink).",
    )
    daily.add_argument(
        "--repo-id",
        type=lambda s: __import__("uuid").UUID(s),
        help="Optional repo_id UUID filter.",
    )
    daily.add_argument(
        "--repo-name",
        help="Optional repo name filter (e.g. 'owner/repo').",
    )
    daily.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "synthetic", "none"],
        default="all",
        help="Which work item providers to include.",
    )
    daily.add_argument(
        "--sink",
        choices=["auto", "clickhouse", "mongo", "sqlite", "both"],
        default="auto",
        help="Where to write derived metrics.",
    )
    daily.add_argument(
        "--skip-commit-metrics",
        action="store_true",
        help="Skip per-commit metrics output.",
    )
    daily.set_defaults(func=_cmd_metrics_daily)

    # ---- grafana ----
    graf = sub.add_parser(
        "grafana", help="Start/stop the Grafana + ClickHouse dev stack."
    )
    graf_sub = graf.add_subparsers(dest="grafana_command", required=True)
    graf_up = graf_sub.add_parser(
        "up", help="docker compose up -d for grafana/docker-compose.yml"
    )
    graf_up.set_defaults(func=_cmd_grafana_up)
    graf_down = graf_sub.add_parser(
        "down", help="docker compose down for grafana/docker-compose.yml"
    )
    graf_down.set_defaults(func=_cmd_grafana_down)

    # ---- fixtures ----
    fix = sub.add_parser("fixtures", help="Data simulation and fixtures.")
    fix_sub = fix.add_subparsers(dest="fixtures_command", required=True)
    fix_gen = fix_sub.add_parser("generate", help="Generate synthetic data.")
    fix_gen.add_argument(
        "--db",
        default=os.getenv("DB_CONN_STRING") or os.getenv("DATABASE_URL"),
        help="Target DB URI.",
    )
    fix_gen.add_argument(
        "--db-type", help="Explicit DB type (postgres, clickhouse, etc)."
    )
    fix_gen.add_argument("--repo-name", default="acme/demo-app", help="Repo name.")
    fix_gen.add_argument("--days", type=int, default=30, help="Number of days of data.")
    fix_gen.add_argument(
        "--commits-per-day", type=int, default=5, help="Avg commits per day."
    )
    fix_gen.add_argument("--pr-count", type=int, default=20, help="Total PRs.")
    fix_gen.add_argument(
        "--with-metrics", action="store_true", help="Also generate derived metrics."
    )
    fix_gen.set_defaults(func=_cmd_fixtures_generate)

    return parser


def _cmd_fixtures_generate(ns: argparse.Namespace) -> int:
    from fixtures.generator import SyntheticDataGenerator

    generator = SyntheticDataGenerator(repo_name=ns.repo_name)
    db_type = _resolve_db_type(ns.db, ns.db_type)

    async def _handler(store):
        # 1. Repo
        repo = generator.generate_repo()
        await store.insert_repo(repo)

        # 2. Files
        files = generator.generate_files()
        await store.insert_git_file_data(files)

        # 3. Commits & Stats
        commits = generator.generate_commits(
            days=ns.days, commits_per_day=ns.commits_per_day
        )
        await store.insert_git_commit_data(commits)
        stats = generator.generate_commit_stats(commits)
        await store.insert_git_commit_stats(stats)

        # 4. PRs & Reviews
        pr_data = generator.generate_prs(count=ns.pr_count)
        prs = [p["pr"] for p in pr_data]
        await store.insert_git_pull_requests(prs)

        all_reviews = []
        for p in pr_data:
            all_reviews.extend(p["reviews"])
        if all_reviews:
            await store.insert_git_pull_request_reviews(all_reviews)

        logging.info(f"Generated synthetic data for {ns.repo_name}")
        logging.info(f"- Repo ID: {repo.id}")
        logging.info(f"- Commits: {len(commits)}")
        logging.info(f"- PRs: {len(prs)}")
        logging.info(f"- Reviews: {len(all_reviews)}")

        if ns.with_metrics:
            from metrics.job_daily import (
                ClickHouseMetricsSink,
                SQLiteMetricsSink,
            )

            sink = None
            if db_type == "clickhouse":
                sink = ClickHouseMetricsSink(ns.db)
            elif db_type == "sqlite":
                from metrics.job_daily import _normalize_sqlite_url

                sink = SQLiteMetricsSink(_normalize_sqlite_url(ns.db))

            if sink:
                sink.ensure_tables()
                wi_metrics = generator.generate_work_item_metrics(days=ns.days)
                sink.write_work_item_metrics(wi_metrics)
                cycle_times = generator.generate_work_item_cycle_times(
                    count=ns.pr_count
                )
                sink.write_work_item_cycle_times(cycle_times)
                logging.info(f"Generated {len(wi_metrics)} work item metrics rows")

    asyncio.run(_run_with_store(ns.db, db_type, _handler))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        _load_dotenv(REPO_ROOT / ".env")

    parser = build_parser()
    ns = parser.parse_args(argv)

    level_name = str(getattr(ns, "log_level", "") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    func = getattr(ns, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return int(func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
