#!/usr/bin/env python3
import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Tuple

from models.git import Repo
from storage import create_store, detect_db_type
from utils import (
    _parse_since,
    iter_commits_since,
    collect_changed_files,
    REPO_PATH,
)

# Import processors
from processors.local import (
    process_git_commits,
    process_git_commit_stats,
    process_files_and_blame,
)
from processors.github import process_github_repo, process_github_repos_batch
from processors.gitlab import process_gitlab_project, process_gitlab_projects_batch


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Global Config
DB_CONN_STRING = os.getenv("DB_CONN_STRING")
DB_TYPE = os.getenv("DB_TYPE", "postgres").lower()


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process git repository data from local repo or GitHub/GitLab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Simplified interface arguments
    parser.add_argument("--db", required=False, help="Database connection string.")
    parser.add_argument(
        "--connector",
        required=False,
        default="local",
        choices=["local", "github", "gitlab"],
        help="Connector type to use (local, github, or gitlab).",
    )
    parser.add_argument(
        "--auth",
        required=False,
        help="Authentication token (GitHub or GitLab token).",
    )
    parser.add_argument(
        "--repo-path", required=False, help="Path to the local git repository."
    )
    parser.add_argument(
        "--db-type",
        required=False,
        choices=["postgres", "mongo", "sqlite"],
        help="Database backend to use (auto-detected from --db if not specified).",
    )
    parser.add_argument(
        "--since",
        "--start-date",
        dest="since",
        required=False,
        help="Start date/time for filtering commits (e.g., 2024-01-01 or 2024-01-01T00:00:00).",
    )

    # GitHub connector options
    parser.add_argument(
        "--github-owner",
        required=False,
        help="GitHub repository owner/organization.",
    )
    parser.add_argument(
        "--github-repo",
        required=False,
        help="GitHub repository name.",
    )

    # GitLab connector options
    parser.add_argument(
        "--gitlab-url",
        required=False,
        default="https://gitlab.com",
        help="GitLab instance URL (default: https://gitlab.com).",
    )
    parser.add_argument(
        "--gitlab-project-id",
        required=False,
        type=int,
        help="GitLab project ID.",
    )

    # Batch processing options
    parser.add_argument(
        "-s",
        "--search-pattern",
        required=False,
        help="fnmatch-style pattern to filter repositories/projects (e.g., 'owner/repo*').",
    )
    parser.add_argument(
        "--batch-size",
        required=False,
        type=int,
        default=10,
        help="Number of repositories/projects to process in each batch (default: 10).",
    )
    parser.add_argument(
        "--group",
        required=False,
        help="Organization/group name to fetch repositories/projects from.",
    )
    parser.add_argument(
        "--max-concurrent",
        required=False,
        type=int,
        default=4,
        help="Maximum concurrent workers for batch processing (default: 4).",
    )
    parser.add_argument(
        "--rate-limit-delay",
        required=False,
        type=float,
        default=1.0,
        help="Delay in seconds between batches for rate limiting (default: 1.0).",
    )
    parser.add_argument(
        "--max-commits-per-repo",
        required=False,
        type=int,
        help="Maximum commits to analyze per repository/project in batch mode.",
    )
    parser.add_argument(
        "--max-repos",
        required=False,
        type=int,
        help="Maximum number of repositories/projects to process in batch mode.",
    )
    parser.add_argument(
        "--use-async",
        action="store_true",
        help="Use async processing for batch operations (better performance).",
    )
    parser.add_argument(
        "--fetch-blame",
        action="store_true",
        help="Fetch blame data (warning: slow and rate-limited)",
    )

    return parser.parse_args()


def _determine_mode(
    args, github_token: str, gitlab_token: str
) -> Tuple[bool, bool, bool, bool]:
    connector_type = args.connector

    if connector_type == "github":
        if not github_token:
            raise ValueError(
                "GitHub connector requires authentication. "
                "Use --auth or set GITHUB_TOKEN environment variable."
            )
        if args.search_pattern:
            return (False, True, False, False)
        elif args.github_owner and args.github_repo:
            return (True, False, False, False)
        else:
            raise ValueError(
                "GitHub connector requires --github-owner and --github-repo, "
                "or --search-pattern for batch processing."
            )
    elif connector_type == "gitlab":
        if not gitlab_token:
            raise ValueError(
                "GitLab connector requires authentication. "
                "Use --auth or set GITLAB_TOKEN environment variable."
            )
        if args.search_pattern:
            return (False, False, False, True)
        elif args.gitlab_project_id:
            return (False, False, True, False)
        else:
            raise ValueError(
                "GitLab connector requires --gitlab-project-id, "
                "or --search-pattern for batch processing."
            )
    elif connector_type == "local":
        return (False, False, False, False)
    else:
        # Legacy mode detection
        use_github = bool(github_token and args.github_owner and args.github_repo)
        use_github_batch = bool(github_token and args.search_pattern)
        use_gitlab = bool(gitlab_token and args.gitlab_project_id)
        use_gitlab_batch = bool(gitlab_token and args.search_pattern)

        if use_github_batch and use_gitlab_batch:
            raise ValueError(
                "Ambiguous batch mode: both GitHub and GitLab tokens are available "
                "and --search-pattern is set. Please specify --connector to disambiguate."
            )

        return (use_github, use_github_batch, use_gitlab, use_gitlab_batch)


async def main() -> None:
    args = parse_args()

    global DB_CONN_STRING, DB_TYPE, REPO_PATH
    if args.db:
        DB_CONN_STRING = args.db
    if args.repo_path:
        REPO_PATH = args.repo_path
    REPO_PATH = os.path.abspath(REPO_PATH)

    if args.db_type:
        DB_TYPE = args.db_type.lower()
    elif DB_CONN_STRING:
        try:
            DB_TYPE = detect_db_type(DB_CONN_STRING)
            logging.info(f"Auto-detected database type: {DB_TYPE}")
        except ValueError:
            pass

    if DB_TYPE not in {"postgres", "mongo", "sqlite"}:
        raise ValueError("DB_TYPE must be 'postgres', 'mongo', or 'sqlite'")
    if not DB_CONN_STRING:
        raise ValueError(
            "Database connection string is required (set DB_CONN_STRING or use --db)"
        )

    auth_token = args.auth
    github_token = auth_token or os.getenv("GITHUB_TOKEN")
    gitlab_token = auth_token or os.getenv("GITLAB_TOKEN")

    use_github, use_github_batch, use_gitlab, use_gitlab_batch = _determine_mode(
        args, github_token, gitlab_token
    )

    store = create_store(DB_CONN_STRING, DB_TYPE)
    async with store:
        try:
            if use_github_batch:
                await process_github_repos_batch(
                    store=store,
                    token=github_token,
                    org_name=args.github_owner or args.group,
                    user_name=args.github_owner if not args.group else None,
                    pattern=args.search_pattern,
                    batch_size=args.batch_size,
                    max_concurrent=args.max_concurrent,
                    rate_limit_delay=args.rate_limit_delay,
                    max_commits_per_repo=args.max_commits_per_repo,
                    max_repos=args.max_repos,
                    use_async=args.use_async,
                )

            elif use_gitlab_batch:
                await process_gitlab_projects_batch(
                    store=store,
                    token=gitlab_token,
                    gitlab_url=args.gitlab_url,
                    group_name=args.group,
                    pattern=args.search_pattern,
                    batch_size=args.batch_size,
                    max_concurrent=args.max_concurrent,
                    rate_limit_delay=args.rate_limit_delay,
                    max_commits_per_project=args.max_commits_per_repo,
                    max_projects=args.max_repos,
                    use_async=args.use_async,
                )

            elif use_github:
                await process_github_repo(
                    store,
                    args.github_owner,
                    args.github_repo,
                    github_token,
                    fetch_blame=args.fetch_blame,
                    max_commits=args.max_commits_per_repo or 100,
                )

            elif use_gitlab:
                await process_gitlab_project(
                    store,
                    args.gitlab_project_id,
                    gitlab_token,
                    args.gitlab_url,
                    fetch_blame=args.fetch_blame,
                    max_commits=args.max_commits_per_repo or 100,
                )

            else:
                # Local Mode
                repo_root = Path(REPO_PATH).resolve()
                logging.info(f"Processing local repository at {repo_root}")
                if not (repo_root / ".git").exists():
                    logging.error(f"No git repository found at {repo_root}")
                    return

                # Initialize Repo for Database
                repo_name = repo_root.name
                repo = Repo(repo_path=str(repo_root), repo=repo_name)
                await store.insert_repo(repo)
                logging.info(f"Repository stored: {repo.repo} ({repo.id})")

                # Iterate commits (using gitpython for iteration)
                from git import Repo as GitPythonRepo

                repo_obj = GitPythonRepo(str(repo_root))
                since_dt = _parse_since(args.since)

                # 1. Commits
                # Note: process_git_commits now expects iterable of commits
                # We must recreate the iterable or pass list
                commits_iter = list(iter_commits_since(repo_obj, since_dt))

                # 2. Files needing blame logic
                files_for_blame = set()

                if args.fetch_blame:
                    # Collect changed files if filtering by date, else all?
                    # Previously: iter_commits_since -> collect_changed_files -> files_for_blame
                    # And walk_repo -> all_files
                    files_for_blame = set(
                        collect_changed_files(repo_root, commits_iter)
                    )

                all_files_path = []
                for root, _, files in os.walk(str(repo_root)):
                    root_path = Path(root)
                    if ".git" in root_path.parts:
                        continue
                    for file in files:
                        file_path = (root_path / file).resolve()
                        all_files_path.append(file_path)

                files_for_blame_path = {Path(p).resolve() for p in files_for_blame}

                # Sequential Processing (SQLite doesn't handle concurrent writes well)
                # Process commits first
                await process_git_commits(repo, store, commits_iter, since_dt)

                # Then commit stats (need fresh iterator since commits_iter is exhausted)
                commits_for_stats = list(iter_commits_since(repo_obj, since_dt))
                await process_git_commit_stats(repo, store, commits_for_stats, since_dt)

                # Finally files and blame
                await process_files_and_blame(
                    repo,
                    all_files_path,
                    files_for_blame_path,
                    store,
                    str(repo_root),
                )

                logging.info("Local repository processing complete.")

        finally:
            pass  # Context manager handles close


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        exit(1)
