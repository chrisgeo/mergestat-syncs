#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

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


def _run_git_mergestat(args: List[str]) -> int:
    cmd = [sys.executable, str(REPO_ROOT / "git_mergestat.py")] + args
    return subprocess.run(cmd, check=False).returncode


def _cmd_sync_local(ns: argparse.Namespace) -> int:
    args: List[str] = [
        "--db",
        ns.db,
        "--connector",
        "local",
        "--repo-path",
        ns.repo_path,
    ]
    if ns.db_type:
        args += ["--db-type", ns.db_type]
    if ns.since:
        args += ["--since", ns.since]
    if ns.fetch_blame:
        args += ["--fetch-blame"]
    if ns.max_commits_per_repo:
        args += ["--max-commits-per-repo", str(ns.max_commits_per_repo)]
    return _run_git_mergestat(args)


def _cmd_sync_github(ns: argparse.Namespace) -> int:
    token = ns.auth or os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitHub token (pass --auth or set GITHUB_TOKEN).")

    args: List[str] = [
        "--db",
        ns.db,
        "--connector",
        "github",
        "--auth",
        token,
    ]
    if ns.db_type:
        args += ["--db-type", ns.db_type]
    if ns.fetch_blame:
        args += ["--fetch-blame"]
    if ns.max_commits_per_repo:
        args += ["--max-commits-per-repo", str(ns.max_commits_per_repo)]

    if ns.search_pattern:
        args += ["--search-pattern", ns.search_pattern]
        if ns.group:
            args += ["--group", ns.group]
        if ns.batch_size:
            args += ["--batch-size", str(ns.batch_size)]
        if ns.max_concurrent:
            args += ["--max-concurrent", str(ns.max_concurrent)]
        if ns.rate_limit_delay is not None:
            args += ["--rate-limit-delay", str(ns.rate_limit_delay)]
        if ns.max_repos:
            args += ["--max-repos", str(ns.max_repos)]
        if ns.use_async:
            args += ["--use-async"]
        return _run_git_mergestat(args)

    if not (ns.owner and ns.repo):
        raise SystemExit("GitHub sync requires --owner and --repo (or --search-pattern for batch).")
    args += ["--github-owner", ns.owner, "--github-repo", ns.repo]
    return _run_git_mergestat(args)


def _cmd_sync_gitlab(ns: argparse.Namespace) -> int:
    token = ns.auth or os.getenv("GITLAB_TOKEN") or ""
    if not token:
        raise SystemExit("Missing GitLab token (pass --auth or set GITLAB_TOKEN).")

    args: List[str] = [
        "--db",
        ns.db,
        "--connector",
        "gitlab",
        "--auth",
        token,
    ]
    if ns.db_type:
        args += ["--db-type", ns.db_type]
    if ns.gitlab_url:
        args += ["--gitlab-url", ns.gitlab_url]
    if ns.fetch_blame:
        args += ["--fetch-blame"]
    if ns.max_commits_per_repo:
        args += ["--max-commits-per-repo", str(ns.max_commits_per_repo)]

    if ns.search_pattern:
        args += ["--search-pattern", ns.search_pattern]
        if ns.group:
            args += ["--group", ns.group]
        if ns.batch_size:
            args += ["--batch-size", str(ns.batch_size)]
        if ns.max_concurrent:
            args += ["--max-concurrent", str(ns.max_concurrent)]
        if ns.rate_limit_delay is not None:
            args += ["--rate-limit-delay", str(ns.rate_limit_delay)]
        if ns.max_repos:
            args += ["--max-repos", str(ns.max_repos)]
        if ns.use_async:
            args += ["--use-async"]
        return _run_git_mergestat(args)

    if ns.project_id is None:
        raise SystemExit("GitLab sync requires --project-id (or --search-pattern for batch).")
    args += ["--gitlab-project-id", str(ns.project_id)]
    return _run_git_mergestat(args)


def _cmd_metrics_daily(ns: argparse.Namespace) -> int:
    # Import lazily to keep CLI startup fast and avoid optional deps at import time.
    from metrics.job_daily import run_daily_metrics_job

    run_daily_metrics_job(
        db_url=ns.db,
        day=ns.date,
        backfill_days=max(1, int(ns.backfill)),
        repo_id=ns.repo_id,
        include_commit_metrics=not ns.skip_commit_metrics,
        sink=ns.sink,
        provider=ns.provider,
    )
    return 0


def _cmd_grafana_up(_ns: argparse.Namespace) -> int:
    cmd = ["docker", "compose", "-f", str(REPO_ROOT / "grafana" / "docker-compose.yml"), "up", "-d"]
    return subprocess.run(cmd, check=False).returncode


def _cmd_grafana_down(_ns: argparse.Namespace) -> int:
    cmd = ["docker", "compose", "-f", str(REPO_ROOT / "grafana" / "docker-compose.yml"), "down"]
    return subprocess.run(cmd, check=False).returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mergestat-syncs", description="Sync git data and compute developer health metrics.")
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
    local.add_argument("--db-type", choices=["postgres", "mongo", "sqlite", "clickhouse"], help="Optional DB backend override.")
    local.add_argument("--repo-path", default=".", help="Path to the local git repository.")
    local.add_argument("--since", help="Lower-bound ISO date/time (UTC).")
    local.add_argument("--fetch-blame", action="store_true", help="Fetch blame data (slow).")
    local.add_argument("--max-commits-per-repo", type=int, help="Limit commits analyzed.")
    local.set_defaults(func=_cmd_sync_local)

    gh = sync_sub.add_parser("github", help="Sync from GitHub (single repo or batch).")
    gh.add_argument("--db", required=True, help="Database connection string.")
    gh.add_argument("--db-type", choices=["postgres", "mongo", "sqlite", "clickhouse"], help="Optional DB backend override.")
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
    gh.set_defaults(func=_cmd_sync_github)

    gl = sync_sub.add_parser("gitlab", help="Sync from GitLab (single project or batch).")
    gl.add_argument("--db", required=True, help="Database connection string.")
    gl.add_argument("--db-type", choices=["postgres", "mongo", "sqlite", "clickhouse"], help="Optional DB backend override.")
    gl.add_argument("--auth", help="GitLab token (defaults to GITLAB_TOKEN).")
    gl.add_argument("--gitlab-url", default=os.getenv("GITLAB_URL", "https://gitlab.com"), help="GitLab instance URL.")
    gl.add_argument("--project-id", type=int, help="GitLab project ID (single project mode).")
    gl.add_argument("--search-pattern", help="Batch mode pattern (e.g. 'group/*').")
    gl.add_argument("--group", help="Batch mode group name.")
    gl.add_argument("--batch-size", type=int, default=10)
    gl.add_argument("--max-concurrent", type=int, default=4)
    gl.add_argument("--rate-limit-delay", type=float, default=1.0)
    gl.add_argument("--max-repos", type=int)
    gl.add_argument("--use-async", action="store_true")
    gl.add_argument("--fetch-blame", action="store_true")
    gl.add_argument("--max-commits-per-repo", type=int)
    gl.set_defaults(func=_cmd_sync_gitlab)

    # ---- metrics ----
    metrics = sub.add_parser("metrics", help="Compute and write derived metrics.")
    metrics_sub = metrics.add_subparsers(dest="metrics_command", required=True)

    daily = metrics_sub.add_parser("daily", help="Compute daily metrics (optionally backfill).")
    daily.add_argument("--date", required=True, type=_parse_date, help="Target day (UTC) as YYYY-MM-DD.")
    daily.add_argument("--backfill", type=int, default=1, help="Compute N days ending at --date (inclusive).")
    daily.add_argument("--db", default=os.getenv("DB_CONN_STRING") or os.getenv("DATABASE_URL"), help="Source DB URI (and default sink).")
    daily.add_argument("--repo-id", type=lambda s: __import__("uuid").UUID(s), help="Optional repo_id UUID filter.")
    daily.add_argument("--provider", choices=["all", "jira", "github", "gitlab", "none"], default="all", help="Which work item providers to include.")
    daily.add_argument("--sink", choices=["auto", "clickhouse", "mongo", "sqlite", "both"], default="auto", help="Where to write derived metrics.")
    daily.add_argument("--skip-commit-metrics", action="store_true", help="Skip per-commit metrics output.")
    daily.set_defaults(func=_cmd_metrics_daily)

    # ---- grafana ----
    graf = sub.add_parser("grafana", help="Start/stop the Grafana + ClickHouse dev stack.")
    graf_sub = graf.add_subparsers(dest="grafana_command", required=True)
    graf_up = graf_sub.add_parser("up", help="docker compose up -d for grafana/docker-compose.yml")
    graf_up.set_defaults(func=_cmd_grafana_up)
    graf_down = graf_sub.add_parser("down", help="docker compose down for grafana/docker-compose.yml")
    graf_down.set_defaults(func=_cmd_grafana_down)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    if os.getenv("DISABLE_DOTENV", "").strip().lower() not in {"1", "true", "yes", "on"}:
        _load_dotenv(REPO_ROOT / ".env")

    parser = build_parser()
    ns = parser.parse_args(argv)

    level_name = str(getattr(ns, "log_level", "") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    func = getattr(ns, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return int(func(ns))


if __name__ == "__main__":
    raise SystemExit(main())
