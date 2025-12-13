#!/usr/bin/env python3
import argparse
import asyncio
import logging
import mimetypes
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Union

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from storage import MongoStore, SQLAlchemyStore, create_store, detect_db_type

# Import connectors (optional - will be None if not available)
try:
    from connectors import (
        GitHubConnector,
        GitLabConnector,
        BatchResult,
        GitLabBatchResult,
    )
    from connectors.exceptions import ConnectorException

    CONNECTORS_AVAILABLE = True
except ImportError:
    CONNECTORS_AVAILABLE = False
    GitHubConnector = None
    GitLabConnector = None
    ConnectorException = Exception
    BatchResult = None
    GitLabBatchResult = None

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# === CONFIGURATION ===
# The line `REPO_PATH = os.getenv("REPO_PATH", ".")` retrieves the value of
# an environment variable named "REPO_PATH". If the environment variable is not
# set, it defaults to "." (current directory). This allows the script to be
# more flexible by allowing the user to specify the path to the repository as
# an environment variable, with a fallback to the current directory if the
# variable is not set.

REPO_PATH = os.getenv("REPO_PATH", ".")
DB_CONN_STRING = os.getenv("DB_CONN_STRING")
DB_TYPE = os.getenv("DB_TYPE", "postgres").lower()
DB_ECHO = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "100")
)  # Insert every n records (increased from 10 for better performance)
MAX_WORKERS = int(
    os.getenv("MAX_WORKERS", "4")
)  # Number of parallel workers for file processing

# Marker for aggregate statistics (not tied to a specific commit/file)
AGGREGATE_STATS_MARKER = "__aggregate__"

DataStore = Union[SQLAlchemyStore, MongoStore]


def _split_full_name(full_name: str) -> Tuple[str, str]:
    parts = (full_name or "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo/project full name: {full_name}")
    return parts[0], parts[1]


async def _backfill_github_missing_data(
    store: DataStore,
    connector: "GitHubConnector",
    db_repo: Repo,
    repo_full_name: str,
    default_branch: str,
    max_commits: Optional[int],
) -> None:
    owner, repo_name = _split_full_name(repo_full_name)

    if not (
        hasattr(store, "has_any_git_files")
        and hasattr(store, "has_any_git_blame")
        and hasattr(store, "has_any_git_commit_stats")
    ):
        return

    needs_files = not await store.has_any_git_files(db_repo.id)
    needs_commit_stats = not await store.has_any_git_commit_stats(db_repo.id)
    needs_blame = not await store.has_any_git_blame(db_repo.id)

    if not (needs_files or needs_commit_stats or needs_blame):
        return

    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")

    file_paths: List[str] = []
    if needs_files or needs_blame:
        try:
            branch = gh_repo.get_branch(default_branch)
            tree = gh_repo.get_git_tree(branch.commit.sha, recursive=True)
            for entry in getattr(tree, "tree", []) or []:
                if getattr(entry, "type", None) != "blob":
                    continue
                path = getattr(entry, "path", None)
                if not path:
                    continue
                file_paths.append(path)

            if needs_files and file_paths:
                batch: List[GitFile] = []
                for path in file_paths:
                    batch.append(
                        GitFile(
                            repo_id=db_repo.id,
                            path=path,
                            executable=False,
                            contents=None,
                        )
                    )
                    if len(batch) >= BATCH_SIZE:
                        await store.insert_git_file_data(batch)
                        batch.clear()
                if batch:
                    await store.insert_git_file_data(batch)
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub files for {repo_full_name}: {e}"
            )

    if needs_commit_stats:
        try:
            commits_iter = gh_repo.get_commits()
            commit_stats_batch: List[GitCommitStat] = []
            commit_count = 0
            for commit in commits_iter:
                if max_commits and commit_count >= max_commits:
                    break
                commit_count += 1
                try:
                    detailed = gh_repo.get_commit(commit.sha)
                    for file in getattr(detailed, "files", []) or []:
                        commit_stats_batch.append(
                            GitCommitStat(
                                repo_id=db_repo.id,
                                commit_hash=commit.sha,
                                file_path=getattr(
                                    file, "filename", AGGREGATE_STATS_MARKER
                                ),
                                additions=getattr(file, "additions", 0),
                                deletions=getattr(file, "deletions", 0),
                                old_file_mode="unknown",
                                new_file_mode="unknown",
                            )
                        )
                        if len(commit_stats_batch) >= BATCH_SIZE:
                            await store.insert_git_commit_stats(commit_stats_batch)
                            commit_stats_batch.clear()
                except Exception as e:
                    logging.debug(
                        f"Failed commit stat fetch for {repo_full_name}@{commit.sha}: {e}"
                    )

            if commit_stats_batch:
                await store.insert_git_commit_stats(commit_stats_batch)
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub commit stats for {repo_full_name}: {e}"
            )

    if needs_blame and file_paths:
        try:
            blame_batch: List[GitBlame] = []
            for path in file_paths:
                try:
                    blame = connector.get_file_blame(
                        owner=owner,
                        repo=repo_name,
                        path=path,
                        ref=default_branch,
                    )
                except Exception as e:
                    logging.debug(
                        f"Failed blame fetch for {repo_full_name}:{path}: {e}"
                    )
                    continue

                for rng in blame.ranges:
                    for line_no in range(rng.starting_line, rng.ending_line + 1):
                        blame_batch.append(
                            GitBlame(
                                repo_id=db_repo.id,
                                path=path,
                                line_no=line_no,
                                author_email=rng.author_email,
                                author_name=rng.author,
                                author_when=None,
                                commit_hash=rng.commit_sha,
                                line=None,
                            )
                        )
                        if len(blame_batch) >= BATCH_SIZE:
                            await store.insert_blame_data(blame_batch)
                            blame_batch.clear()

            if blame_batch:
                await store.insert_blame_data(blame_batch)
        except Exception as e:
            logging.warning(
                f"Failed to backfill GitHub blame for {repo_full_name}: {e}"
            )


async def _backfill_gitlab_missing_data(
    store: DataStore,
    connector: "GitLabConnector",
    db_repo: Repo,
    project_full_name: str,
    default_branch: str,
    max_commits: Optional[int],
) -> None:
    if not (
        hasattr(store, "has_any_git_files")
        and hasattr(store, "has_any_git_blame")
        and hasattr(store, "has_any_git_commit_stats")
    ):
        return

    needs_files = not await store.has_any_git_files(db_repo.id)
    needs_commit_stats = not await store.has_any_git_commit_stats(db_repo.id)
    needs_blame = not await store.has_any_git_blame(db_repo.id)

    if not (needs_files or needs_commit_stats or needs_blame):
        return

    try:
        project = connector.gitlab.projects.get(project_full_name)
    except Exception as e:
        logging.warning(f"Failed to load GitLab project {project_full_name}: {e}")
        return

    file_paths: List[str] = []
    if needs_files or needs_blame:
        try:
            items = project.repository_tree(
                ref=default_branch, recursive=True, get_all=True
            )
        except TypeError:
            # Older python-gitlab versions
            items = project.repository_tree(
                ref=default_branch, recursive=True, all=True
            )
        except Exception as e:
            logging.warning(f"Failed to list GitLab files for {project_full_name}: {e}")
            items = []

        for item in items or []:
            if item.get("type") != "blob":
                continue
            path = item.get("path")
            if not path:
                continue
            file_paths.append(path)

        if needs_files and file_paths:
            batch: List[GitFile] = []
            for path in file_paths:
                batch.append(
                    GitFile(
                        repo_id=db_repo.id,
                        path=path,
                        executable=False,
                        contents=None,
                    )
                )
                if len(batch) >= BATCH_SIZE:
                    await store.insert_git_file_data(batch)
                    batch.clear()
            if batch:
                await store.insert_git_file_data(batch)

    if needs_commit_stats:
        try:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, get_all=True
            )
        except TypeError:
            commits = project.commits.list(
                ref_name=default_branch, per_page=100, all=True
            )

        commit_stats_batch: List[GitCommitStat] = []
        commit_count = 0
        for commit in commits or []:
            if max_commits and commit_count >= max_commits:
                break
            commit_count += 1
            sha = getattr(commit, "id", None) or getattr(commit, "sha", None)
            if not sha:
                continue
            try:
                stats = connector.get_commit_stats_by_project(
                    sha=sha,
                    project_name=project_full_name,
                )
                commit_stats_batch.append(
                    GitCommitStat(
                        repo_id=db_repo.id,
                        commit_hash=sha,
                        file_path=AGGREGATE_STATS_MARKER,
                        additions=getattr(stats, "additions", 0),
                        deletions=getattr(stats, "deletions", 0),
                        old_file_mode="unknown",
                        new_file_mode="unknown",
                    )
                )
                if len(commit_stats_batch) >= BATCH_SIZE:
                    await store.insert_git_commit_stats(commit_stats_batch)
                    commit_stats_batch.clear()
            except Exception as e:
                logging.debug(
                    f"Failed commit stat fetch for {project_full_name}@{sha}: {e}"
                )

        if commit_stats_batch:
            await store.insert_git_commit_stats(commit_stats_batch)

    if needs_blame and file_paths:
        blame_batch: List[GitBlame] = []
        for path in file_paths:
            try:
                blame_items = connector.rest_client.get_file_blame(
                    project.id,
                    path,
                    default_branch,
                )
            except Exception as e:
                logging.debug(f"Failed blame fetch for {project_full_name}:{path}: {e}")
                continue

            line_no = 1
            for item in blame_items or []:
                commit = item.get("commit", {})
                for line in item.get("lines", []) or []:
                    blame_batch.append(
                        GitBlame(
                            repo_id=db_repo.id,
                            path=path,
                            line_no=line_no,
                            author_email=commit.get("author_email"),
                            author_name=commit.get("author_name"),
                            author_when=None,
                            commit_hash=commit.get("id"),
                            line=line.rstrip("\n") if isinstance(line, str) else None,
                        )
                    )
                    line_no += 1
                    if len(blame_batch) >= BATCH_SIZE:
                        await store.insert_blame_data(blame_batch)
                        blame_batch.clear()

        if blame_batch:
            await store.insert_blame_data(blame_batch)


async def insert_blame_data(store: DataStore, data_batch: List[GitBlame]) -> None:
    """
    Insert a batch of blame data into the configured storage backend.

    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param data_batch: List of GitBlame objects.
    """
    if not data_batch:
        return

    await store.insert_blame_data(data_batch)


async def insert_git_commit_data(
    store: DataStore, commit_data: List[GitCommit]
) -> None:
    """
    Insert a batch of git commit data into the database.

    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param commit_data: List of GitCommit objects.
    """
    if not commit_data:
        return

    await store.insert_git_commit_data(commit_data)


async def insert_git_commit_stats(
    store: DataStore, commit_stats: List[GitCommitStat]
) -> None:
    """
    Insert a batch of git commit stats into the database.

    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param commit_stats: List of GitCommitStat objects.
    """
    if not commit_stats:
        return

    await store.insert_git_commit_stats(commit_stats)


async def insert_git_file_data(store: DataStore, file_data: List[GitFile]) -> None:
    """
    Insert a batch of git file data into the database.

    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param file_data: List of GitFile objects.
    """
    if not file_data:
        return

    await store.insert_git_file_data(file_data)


async def insert_repo_data(store: DataStore, repo: Repo) -> None:
    """
    Insert the repository data into the database if it doesn't already exist.

    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param repo: Repo object to insert.
    """
    # Ensure required fields are populated
    if not repo.repo:
        repo.repo = REPO_PATH  # Use the repository path as the default value
    if not repo.settings:
        repo.settings = {}
    if not repo.tags:
        repo.tags = []

    await store.insert_repo(repo)


# Expanded SKIP_EXTENSIONS to include more binary and package-like file types
SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".pdf",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".ico",
    ".mp4",
    ".mp3",
    ".mov",
    ".avi",
    ".exe",
    ".dll",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".eot",
    ".rar",
    ".iso",
    ".dmg",
    ".pkg",
    ".deb",
    ".rpm",
    ".msi",
    ".class",
    ".jar",
    ".war",
    ".pyc",
    ".pyo",
    ".so",
    ".o",
    ".a",
    ".lib",
    ".bin",
    ".dat",
    ".swp",
    ".lock",
    ".bak",
    ".tmp",
}
NON_BINARY_OVERRIDES = {".ts"}  # TypeScript mime may be misdetected as video/mp2t


def is_skippable(path: str) -> bool:
    """
    Return True if the file is skippable (i.e. should not be processed for git blame).

    A file is considered skippable if it has a file extension that is known to be binary
    or if its mime type falls into one of the following categories:

    - image/
    - video/
    - audio/
    - application/pdf
    - font/

    Otherwise, the file is considered processable.

    :param path: The path to the file to be checked.
    :return: True if the file is skippable, False otherwise.
    """
    ext = Path(path).suffix.lower()
    if ext in SKIP_EXTENSIONS:
        return True
    if ext in NON_BINARY_OVERRIDES:
        return False
    mime, _ = mimetypes.guess_type(path)
    return mime and mime.startswith((
        "image/",
        "video/",
        "audio/",
        "application/pdf",
        "font/",
        "application/x-executable",
        "application/x-sharedlib",
        "application/x-object",
        "application/x-archive",
    ))


async def process_github_repos_batch(
    store: DataStore,
    repos: List[Tuple[str, str]],
    token: str,
    fetch_blame: bool = False,
    max_commits: int = 100,
) -> None:
    """
    Process a batch of GitHub repositories.

    :param store: Storage backend.
    :param repos: List of (owner, repo_name) tuples.
    :param token: GitHub token.
    :param fetch_blame: Whether to fetch blame data.
    :param max_commits: Maximum number of commits to fetch.
    """
    for owner, repo_name in repos:
        try:
            await process_github_repo(
                store,
                owner,
                repo_name,
                token,
                fetch_blame=fetch_blame,
                max_commits=max_commits,
            )
        except Exception as e:
            logging.error(f"Failed to process {owner}/{repo_name}: {e}")


async def process_git_files(
    repo: Repo, all_files: List[Path], store: DataStore
) -> None:
    """
    Process and insert GitFile data into the database.

    :param repo: Git repository object.
    :param all_files: List of file paths in the repository.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    logging.info(f"Processing {len(all_files)} git files...")
    file_batch: List[GitFile] = []
    failed_files: List[Tuple[Path, str]] = []  # Track failed files
    successfully_processed = 0

    for filepath in all_files:
        try:
            rel_path = os.path.relpath(filepath, REPO_PATH)

            # Check if file is executable
            is_executable = os.access(filepath, os.X_OK)

            # Read file contents (skip if too large or binary)
            contents = None
            try:
                if os.path.getsize(filepath) < 1_000_000:  # Skip files > 1MB
                    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                        contents = f.read()
            except Exception:
                pass  # Skip files that can't be read

            git_file = GitFile(
                repo_id=repo.id,
                path=rel_path,
                executable=is_executable,
                contents=contents,
            )
            file_batch.append(git_file)
            successfully_processed += 1

            if len(file_batch) >= BATCH_SIZE:
                await insert_git_file_data(store, file_batch)
                logging.info(f"Inserted {len(file_batch)} files")
                file_batch.clear()
        except Exception as e:
            error_msg = str(e)
            failed_files.append((filepath, error_msg))
            logging.error(f"Error processing file {filepath}: {error_msg}")

    # Insert remaining files
    if file_batch:
        await insert_git_file_data(store, file_batch)
        logging.info(f"Inserted final {len(file_batch)} files")

    # Report summary
    total_failed = len(failed_files)
    logging.info("Git file processing complete:")
    logging.info(f"  Successfully processed: {successfully_processed} files")
    logging.info(f"  Failed: {total_failed} files")

    if failed_files:
        logging.warning("Failed files:")
        for filepath, error in failed_files[:10]:  # Show first 10
            logging.warning(f"  - {filepath}: {error}")
        if total_failed > 10:
            logging.warning(f"  ... and {total_failed - 10} more")


async def process_git_commits(repo: Repo, store: DataStore) -> None:
    """
    Process and insert GitCommit data into the database.

    :param repo: Git repository object.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    logging.info("Processing git commits...")
    commit_batch: List[GitCommit] = []

    # Process commits using gitpython
    try:
        commit_count = 0
        for commit in repo.iter_commits():
            git_commit = GitCommit(
                repo_id=repo.id,
                hash=commit.hexsha,
                message=commit.message,
                author_name=commit.author.name,
                author_email=commit.author.email,
                author_when=commit.authored_datetime,
                committer_name=commit.committer.name,
                committer_email=commit.committer.email,
                committer_when=commit.committed_datetime,
                parents=len(commit.parents),
            )
            commit_batch.append(git_commit)
            commit_count += 1

            if len(commit_batch) >= BATCH_SIZE:
                await insert_git_commit_data(store, commit_batch)
                logging.info(
                    f"Inserted {len(commit_batch)} commits ({commit_count} total)"
                )
                commit_batch.clear()

        # Insert remaining commits
        if commit_batch:
            await insert_git_commit_data(store, commit_batch)
            logging.info(
                f"Inserted final {len(commit_batch)} commits ({commit_count} total)"
            )
    except Exception as e:
        logging.error(f"Error processing commits: {e}")


async def process_single_file_blame(
    filepath: Path, repo: Repo, repo_uuid: uuid.UUID, semaphore: asyncio.Semaphore
) -> List[GitBlame]:
    """
    Process a single file for blame data with concurrency control.

    :param filepath: Path to the file.
    :param repo: Git repository object (thread-safe if used correctly).
    :param repo_uuid: UUID of the repository.
    :param semaphore: Semaphore to control concurrent file processing.
    :return: List of GitBlame objects.
    """
    async with semaphore:
        # Run blame processing in executor to avoid blocking
        loop = asyncio.get_event_loop()

        def process_blame():
            # Use the existing repo instance - gitpython's blame is generally thread-safe
            # for read operations, or we can rely on the semaphore to limit concurrency
            # if needed. However, to be absolutely safe with gitpython which can have
            # issues with concurrency, we'll use the provided repo but we need to be
            # careful.
            #
            # Actually, creating a new Repo object is expensive (checking refs etc).
            # But sharing one across threads can be problematic.
            # A compromise is to use a lightweight approach or ensure the repo object
            # is thread-local if possible.
            #
            # Given the performance issue is "re-opening repo for every file", passing
            # the repo object is the right fix. We just need to ensure we don't have
            # race conditions. GitPython's `blame` command spawns a subprocess, so
            # it should be fine to call concurrently on the same Repo object.
            return GitBlame.process_file(
                repo.working_dir, filepath, repo_uuid, repo=repo
            )

        blame_objects = await loop.run_in_executor(None, process_blame)
        return blame_objects


async def process_git_blame(
    all_files: List[Path], store: DataStore, repo: Repo
) -> None:
    """
    Process and insert GitBlame data into the database with parallel processing.

    :param all_files: List of file paths in the repository.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param repo: Repo instance (only used for path reference, not shared across workers).
    """
    if not all_files:
        return

    logging.info(
        f"Processing git blame for {len(all_files)} files with {MAX_WORKERS} workers..."
    )

    # Create semaphore to limit concurrent file processing
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    blame_batch: List[GitBlame] = []
    failed_files: List[Tuple[Path, str]] = []  # Track failed files with error messages
    successfully_processed = 0

    # Process files in chunks to manage memory
    chunk_size = BATCH_SIZE
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i : i + chunk_size]

        # Process chunk in parallel
        # Pass the repo object directly to avoid re-opening it
        tasks = [
            process_single_file_blame(filepath, repo, repo.id, semaphore)
            for filepath in chunk
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results and filter out errors
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                filepath = chunk[idx]
                error_msg = str(result)
                failed_files.append((filepath, error_msg))
                logging.error(f"Error processing {filepath}: {error_msg}")
            elif result:
                blame_batch.extend(result)
                successfully_processed += 1

        # Insert batch if we have data
        if blame_batch:
            await insert_blame_data(store, blame_batch)
            logging.info(
                f"Inserted blame data for {len(blame_batch)} lines "
                f"({i + len(chunk)}/{len(all_files)} files)"
            )
            blame_batch.clear()

    # Report summary
    total_failed = len(failed_files)
    logging.info("Git blame processing complete:")
    logging.info(f"  Successfully processed: {successfully_processed} files")
    logging.info(f"  Failed: {total_failed} files")

    if failed_files:
        logging.warning("Failed files:")
        for filepath, error in failed_files[:10]:  # Show first 10
            logging.warning(f"  - {filepath}: {error}")
        if total_failed > 10:
            logging.warning(f"  ... and {total_failed - 10} more")


async def process_git_commit_stats(repo: Repo, store: DataStore) -> None:
    """
    Process and insert GitCommitStat data into the database.

    :param repo: Git repository object.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    logging.info("Processing git commit stats...")
    commit_stats_batch: List[GitCommitStat] = []

    try:
        commit_count = 0
        for commit in repo.iter_commits():
            # Get stats for each file changed in this commit
            if commit.parents:
                # Compare with first parent
                diffs = commit.parents[0].diff(commit, create_patch=False)
            else:
                # Initial commit - compare with empty tree
                diffs = commit.diff(None, create_patch=False)

            for diff in diffs:
                # Determine file path
                file_path = diff.b_path if diff.b_path else diff.a_path
                if not file_path:
                    continue

                # Calculate additions and deletions from diff stats
                additions = 0
                deletions = 0
                if hasattr(diff, "diff") and diff.diff:
                    # Count lines in diff
                    try:
                        diff_text = diff.diff.decode("utf-8", errors="ignore")
                        for line in diff_text.split("\n"):
                            if line.startswith("+") and not line.startswith("+++"):
                                additions += 1
                            elif line.startswith("-") and not line.startswith("---"):
                                deletions += 1
                    except Exception as e:
                        logging.warning(
                            f"Failed to decode diff or count lines for file "
                            f"'{file_path}' in commit "
                            f"'{getattr(commit, 'hexsha', None)}': {e}"
                        )

                # Determine file modes
                old_mode = "unknown"
                new_mode = "unknown"
                if diff.a_mode:
                    old_mode = oct(diff.a_mode)
                if diff.b_mode:
                    new_mode = oct(diff.b_mode)

                git_commit_stat = GitCommitStat(
                    repo_id=repo.id,
                    commit_hash=commit.hexsha,
                    file_path=file_path,
                    additions=additions,
                    deletions=deletions,
                    old_file_mode=old_mode,
                    new_file_mode=new_mode,
                )
                commit_stats_batch.append(git_commit_stat)

            commit_count += 1

            if len(commit_stats_batch) >= BATCH_SIZE:
                await insert_git_commit_stats(store, commit_stats_batch)
                logging.info(
                    f"Inserted {len(commit_stats_batch)} commit stats ({commit_count} commits)"
                )
                commit_stats_batch.clear()

        # Insert remaining stats
        if commit_stats_batch:
            await insert_git_commit_stats(store, commit_stats_batch)
            logging.info(
                f"Inserted final {len(commit_stats_batch)} commit stats ({commit_count} commits)"
            )
    except Exception as e:
        logging.error(f"Error processing commit stats: {e}")


async def process_github_repo(
    store: DataStore,
    owner: str,
    repo_name: str,
    token: str,
    fetch_blame: bool = False,
    max_commits: int = 100,
) -> None:
    """
    Process a GitHub repository using the GitHub connector.

    :param store: Storage backend.
    :param owner: Repository owner.
    :param repo_name: Repository name.
    :param token: GitHub token.
    :param fetch_blame: Whether to fetch blame data (expensive).
    :param max_commits: Maximum number of commits to fetch.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info(f"Processing GitHub repository: {owner}/{repo_name}")

    connector = GitHubConnector(token=token)
    try:
        # Get repository information directly using full name
        logging.info("Fetching repository information...")
        gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")

        # Convert to our Repository model
        from connectors.models import Repository

        repo_info = Repository(
            id=gh_repo.id,
            name=gh_repo.name,
            full_name=gh_repo.full_name,
            default_branch=gh_repo.default_branch,
            description=gh_repo.description,
            url=gh_repo.html_url,
            created_at=gh_repo.created_at,
            updated_at=gh_repo.updated_at,
            language=gh_repo.language,
            stars=gh_repo.stargazers_count,
            forks=gh_repo.forks_count,
        )
        logging.info(f"Found repository: {repo_info.full_name}")

        # Create Repo object for database
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=repo_info.full_name,
            settings={
                "source": "github",
                "repo_id": repo_info.id,
                "url": repo_info.url,
                "default_branch": repo_info.default_branch,
            },
            tags=["github", repo_info.language] if repo_info.language else ["github"],
        )

        logging.info(f"Storing repository: {db_repo.repo}")
        await store.insert_repo(db_repo)
        logging.info(f"Repository stored with ID: {db_repo.id}")

        # Get and store commits
        logging.info(f"Fetching up to {max_commits} commits from GitHub...")
        commits = list(gh_repo.get_commits()[:max_commits])

        commit_objects = []
        for commit in commits:
            git_commit = GitCommit(
                repo_id=db_repo.id,
                hash=commit.sha,
                message=commit.commit.message,
                author_name=(
                    commit.commit.author.name if commit.commit.author else "Unknown"
                ),
                author_email=commit.commit.author.email if commit.commit.author else "",
                author_when=(
                    commit.commit.author.date
                    if commit.commit.author
                    else datetime.now(timezone.utc)
                ),
                committer_name=(
                    commit.commit.committer.name
                    if commit.commit.committer
                    else "Unknown"
                ),
                committer_email=(
                    commit.commit.committer.email if commit.commit.committer else ""
                ),
                committer_when=(
                    commit.commit.committer.date
                    if commit.commit.committer
                    else datetime.now(timezone.utc)
                ),
                parents=len(commit.parents),
            )
            commit_objects.append(git_commit)

        await store.insert_git_commit_data(commit_objects)
        logging.info(f"Stored {len(commit_objects)} commits from GitHub")

        # Get and store commit stats
        logging.info("Fetching commit stats from GitHub...")
        stats_objects = []
        # Limit stats fetching to avoid rate limits, but respect max_commits if reasonable
        stats_limit = min(max_commits, 50)
        for commit in commits[:stats_limit]:
            try:
                for file in commit.files:
                    stat = GitCommitStat(
                        repo_id=db_repo.id,
                        commit_hash=commit.sha,
                        file_path=file.filename,
                        additions=file.additions,
                        deletions=file.deletions,
                        old_file_mode="unknown",
                        new_file_mode="unknown",
                    )
                    stats_objects.append(stat)
            except Exception as e:
                logging.warning(f"Failed to get stats for commit {commit.sha}: {e}")

        if stats_objects:
            await store.insert_git_commit_stats(stats_objects)
            logging.info(f"Stored {len(stats_objects)} commit stats from GitHub")

        # Fetch Pull Requests
        logging.info("Fetching pull requests from GitHub...")
        from models.git import GitPullRequest

        prs = connector.get_pull_requests(owner, repo_name, state="all", max_prs=100)
        pr_objects = []
        for pr in prs:
            git_pr = GitPullRequest(
                repo_id=db_repo.id,
                number=pr.number,
                title=pr.title,
                state=pr.state,
                author_name=pr.author.username if pr.author else "Unknown",
                author_email=pr.author.email if pr.author else None,
                created_at=pr.created_at,
                merged_at=pr.merged_at,
                closed_at=pr.closed_at,
                head_branch=pr.head_branch,
                base_branch=pr.base_branch,
            )
            pr_objects.append(git_pr)

        if pr_objects:
            await store.insert_git_pull_requests(pr_objects)
            logging.info(f"Stored {len(pr_objects)} pull requests from GitHub")

        # Fetch Blame Data (Optional)
        if fetch_blame:
            logging.info("Fetching blame data from GitHub (this may take a while)...")
            # Get all files from the default branch
            try:
                contents = gh_repo.get_contents("", ref=gh_repo.default_branch)
                files_to_process = []
                while contents:
                    file_content = contents.pop(0)
                    if file_content.type == "dir":
                        contents.extend(
                            gh_repo.get_contents(
                                file_content.path, ref=gh_repo.default_branch
                            )
                        )
                    else:
                        if not is_skippable(file_content.path):
                            files_to_process.append(file_content.path)

                # Limit files to avoid hitting API limits too hard
                files_to_process = files_to_process[:50]  # Safety limit
                logging.info(f"Processing blame for {len(files_to_process)} files...")

                blame_batch = []
                if blame_batch:
                    await store.insert_blame_data(blame_batch)
                    logging.info(f"Stored {len(blame_batch)} blame records from GitHub")

            except Exception as e:
                logging.error(f"Error fetching files for blame: {e}")

        logging.info(f"Successfully processed GitHub repository: {owner}/{repo_name}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitHub repository: {e}")
        raise
    finally:
        connector.close()


async def process_gitlab_projects_batch(
    store: DataStore,
    projects: List[int],
    token: str,
    gitlab_url: str,
    fetch_blame: bool = False,
    max_commits: int = 100,
) -> None:
    """
    Process a batch of GitLab projects.

    :param store: Storage backend.
    :param projects: List of project IDs.
    :param token: GitLab token.
    :param gitlab_url: GitLab instance URL.
    :param fetch_blame: Whether to fetch blame data.
    :param max_commits: Maximum number of commits to fetch.
    """
    for project_id in projects:
        try:
            await process_gitlab_project(
                store,
                project_id,
                token,
                gitlab_url,
                fetch_blame=fetch_blame,
                max_commits=max_commits,
            )
        except Exception as e:
            logging.error(f"Failed to process project {project_id}: {e}")
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )


async def process_gitlab_project(
    store: DataStore,
    project_id: int,
    token: str,
    gitlab_url: str,
    fetch_blame: bool = False,
    max_commits: int = 100,
) -> None:
    """
    Process a GitLab project using the GitLab connector.

    :param store: Storage backend.
    :param project_id: GitLab project ID.
    :param token: GitLab token.
    :param gitlab_url: GitLab instance URL.
    :param fetch_blame: Whether to fetch blame data (expensive).
    :param max_commits: Maximum number of commits to fetch.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info(f"Processing GitLab project: {project_id}")

    connector = GitLabConnector(url=gitlab_url, private_token=token)
    try:
        # Get project information
        gl_project = connector.gitlab.projects.get(project_id)
        logging.info(f"Found project: {gl_project.name}")

        # Create Repo object for database
        full_name = (
            gl_project.path_with_namespace
            if hasattr(gl_project, "path_with_namespace")
            else gl_project.name
        )

        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=full_name,
            settings={
                "source": "gitlab",
                "project_id": gl_project.id,
                "url": gl_project.web_url if hasattr(gl_project, "web_url") else None,
                "default_branch": (
                    gl_project.default_branch
                    if hasattr(gl_project, "default_branch")
                    else "main"
                ),
            },
            tags=["gitlab"],
        )

        logging.info(f"Storing project: {db_repo.repo}")
        await store.insert_repo(db_repo)
        logging.info(f"Project stored with ID: {db_repo.id}")

        # Get and store commits
        logging.info(f"Fetching up to {max_commits} commits from GitLab...")
        commits = gl_project.commits.list(per_page=100, get_all=False)
        # Handle pagination manually or rely on python-gitlab's lazy objects if we want more than 20
        # python-gitlab list() returns a list by default unless iterator=True
        # But we want to limit to max_commits.
        # If max_commits > 100, we might need to fetch more pages.
        # For simplicity, let's assume the user might want more than default page size.
        if max_commits > 100:
            commits = gl_project.commits.list(per_page=100, as_list=False)
            # We'll slice the iterator later or just break the loop

        commit_objects = []
        count = 0
        for commit in commits:
            if count >= max_commits:
                break

            git_commit = GitCommit(
                repo_id=db_repo.id,
                hash=commit.id,
                message=commit.message,
                author_name=(
                    commit.author_name if hasattr(commit, "author_name") else "Unknown"
                ),
                author_email=(
                    commit.author_email if hasattr(commit, "author_email") else ""
                ),
                author_when=(
                    datetime.fromisoformat(commit.authored_date.replace("Z", "+00:00"))
                    if hasattr(commit, "authored_date")
                    else datetime.now(timezone.utc)
                ),
                committer_name=(
                    commit.committer_name
                    if hasattr(commit, "committer_name")
                    else "Unknown"
                ),
                committer_email=(
                    commit.committer_email if hasattr(commit, "committer_email") else ""
                ),
                committer_when=(
                    datetime.fromisoformat(commit.committed_date.replace("Z", "+00:00"))
                    if hasattr(commit, "committed_date")
                    else datetime.now(timezone.utc)
                ),
                parents=len(commit.parent_ids) if hasattr(commit, "parent_ids") else 0,
            )
            commit_objects.append(git_commit)
            count += 1

        await store.insert_git_commit_data(commit_objects)
        logging.info(f"Stored {len(commit_objects)} commits from GitLab")

        # Get and store commit stats
        logging.info("Fetching commit stats from GitLab...")
        stats_objects = []
        # Limit stats fetching
        stats_limit = min(max_commits, 50)

        # We need to iterate over the commits we just fetched
        # Since 'commits' might be a generator or a list, let's use the commit_objects we created
        for git_commit in commit_objects[:stats_limit]:
            try:
                # We need to fetch the detailed commit to get stats
                detailed_commit = gl_project.commits.get(git_commit.hash)
                if hasattr(detailed_commit, "stats"):
                    # GitLab provides aggregate stats per commit
                    # We'll create a single entry per commit with aggregate data
                    stat = GitCommitStat(
                        repo_id=db_repo.id,
                        commit_hash=git_commit.hash,
                        file_path=AGGREGATE_STATS_MARKER,
                        additions=detailed_commit.stats.get("additions", 0),
                        deletions=detailed_commit.stats.get("deletions", 0),
                        old_file_mode="unknown",
                        new_file_mode="unknown",
                    )
                    stats_objects.append(stat)
            except Exception as e:
                logging.warning(
                    f"Failed to get stats for commit {git_commit.hash}: {e}"
                )

        if stats_objects:
            await store.insert_git_commit_stats(stats_objects)
            logging.info(f"Stored {len(stats_objects)} commit stats from GitLab")

        # Fetch Merge Requests (Pull Requests)
        logging.info("Fetching merge requests from GitLab...")
        from models.git import GitPullRequest

        mrs = connector.get_merge_requests(
            project_id=project_id, state="all", max_mrs=100
        )
        pr_objects = []
        for mr in mrs:
            git_pr = GitPullRequest(
                repo_id=db_repo.id,
                number=mr.number,
                title=mr.title,
                state=mr.state,
                author_name=mr.author.username if mr.author else "Unknown",
                author_email=mr.author.email if mr.author else None,
                created_at=mr.created_at,
                merged_at=mr.merged_at,
                closed_at=mr.closed_at,
                head_branch=mr.head_branch,
                base_branch=mr.base_branch,
            )
            pr_objects.append(git_pr)

        if pr_objects:
            await store.insert_git_pull_requests(pr_objects)
            logging.info(f"Stored {len(pr_objects)} merge requests from GitLab")

        # Fetch Blame Data (Optional)
        if fetch_blame:
            logging.info("Fetching blame data from GitLab (this may take a while)...")
            try:
                # Get files from repository tree
                items = gl_project.repository_tree(
                    ref=gl_project.default_branch, recursive=True, all=True
                )
                files_to_process = []
                for item in items:
                    if item["type"] == "blob" and not is_skippable(item["path"]):
                        files_to_process.append(item["path"])

                # Limit files
                files_to_process = files_to_process[:50]
                logging.info(f"Processing blame for {len(files_to_process)} files...")

                blame_batch = []
                for file_path in files_to_process:
                    try:
                        blame = connector.get_file_blame(
                            project_id=project_id,
                            file_path=file_path,
                            ref=gl_project.default_branch,
                        )
                        if blame and blame.ranges:
                            for r in blame.ranges:
                                blame_obj = GitBlame(
                                    repo_id=db_repo.id,
                                    path=file_path,
                                    line_no=r.starting_line,
                                    author_name=r.author,
                                    author_email=r.author_email,
                                    commit_hash=r.commit_sha,
                                    line="<remote>",
                                    author_when=datetime.now(timezone.utc),
                                )
                                blame_batch.append(blame_obj)
                    except Exception as e:
                        logging.warning(f"Failed to get blame for {file_path}: {e}")

                if blame_batch:
                    await store.insert_blame_data(blame_batch)
                    logging.info(f"Stored {len(blame_batch)} blame records from GitLab")

            except Exception as e:
                logging.error(f"Error fetching files for blame: {e}")

        logging.info(f"Successfully processed GitLab project: {project_id}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitLab project: {e}")
        raise
    finally:
        connector.close()


async def process_github_repos_batch(
    store: DataStore,
    token: str,
    org_name: str = None,
    user_name: str = None,
    pattern: str = None,
    batch_size: int = 10,
    max_concurrent: int = 4,
    rate_limit_delay: float = 1.0,
    max_commits_per_repo: int = None,
    max_repos: int = None,
    use_async: bool = False,
) -> None:
    """
    Process multiple GitHub repositories using batch processing with pattern matching.

    This function uses the GitHubConnector's batch processing methods to retrieve
    repositories matching a pattern and collect statistics for each. When use_async
    is True, it uses get_repos_with_stats_async for concurrent processing; otherwise,
    it uses get_repos_with_stats for synchronous processing.

    :param store: Storage backend.
    :param token: GitHub token.
    :param org_name: Optional organization name. When the --github-owner CLI argument
                     is provided, it is passed as org_name. The connector will treat
                     this as an organization and fetch its repositories.
    :param user_name: Optional user name. If you want to fetch a user's repos instead
                      of an organization's, provide this parameter directly.
    :param pattern: fnmatch-style pattern to filter repositories.
    :param batch_size: Number of repos to process in each batch.
    :param max_concurrent: Maximum concurrent workers for processing.
    :param rate_limit_delay: Delay in seconds between batches.
    :param max_commits_per_repo: Maximum commits to analyze per repository.
    :param max_repos: Maximum number of repositories to process.
    :param use_async: Use async processing (get_repos_with_stats_async) for better
                      performance, otherwise use sync processing (get_repos_with_stats).
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info("=== GitHub Batch Repository Processing ===")
    if pattern:
        logging.info(f"Pattern: {pattern}")
    if org_name:
        logging.info(f"Organization: {org_name}")
    if user_name:
        logging.info(f"User: {user_name}")
    logging.info(f"Batch size: {batch_size}, Max concurrent: {max_concurrent}")

    connector = GitHubConnector(token=token)
    loop = asyncio.get_running_loop()

    # Track results for summary and incremental storage
    all_results: List[BatchResult] = []
    stored_count = 0

    # When running async batch processing, we want to upsert as results complete.
    # We intentionally serialize DB writes through a single consumer to avoid
    # concurrent use of the same AsyncSession (SQLAlchemyStore).
    results_queue: Optional[asyncio.Queue] = None
    _queue_sentinel = object()

    async def store_result(result: BatchResult) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        repo_info = result.repository

        # Create Repo object for database
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=repo_info.full_name,
            settings={
                "source": "github",
                "repo_id": repo_info.id,
                "url": repo_info.url,
                "default_branch": repo_info.default_branch,
                "batch_processed": True,
            },
            tags=["github", repo_info.language] if repo_info.language else ["github"],
        )

        await store.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored repository ({stored_count}): {db_repo.repo}")

        # Store stats if available
        if result.stats:
            # Create commit stats entries from aggregated data
            stat = GitCommitStat(
                repo_id=db_repo.id,
                commit_hash=AGGREGATE_STATS_MARKER,
                file_path=AGGREGATE_STATS_MARKER,
                additions=result.stats.additions,
                deletions=result.stats.deletions,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
            await store.insert_git_commit_stats([stat])

    def on_repo_complete(result: BatchResult) -> None:
        """Callback for when each repository is processed - logs and queues for storage."""
        all_results.append(result)
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
            logging.info(f"  ✓ Processed: {result.repository.full_name}{stats_info}")
        else:
            logging.warning(
                f"  ✗ Failed: {result.repository.full_name}: {result.error}"
            )

        # Stream upserts during processing (callback may run on a worker thread
        # when sync connector methods are executed in an executor).
        if results_queue is not None:

            def _enqueue() -> None:
                assert results_queue is not None
                try:
                    results_queue.put_nowait(result)
                except asyncio.QueueFull:
                    asyncio.create_task(results_queue.put(result))

            loop.call_soon_threadsafe(_enqueue)

    try:
        # Choose async or sync processing
        if use_async:
            logging.info("Using async batch processing with incremental storage...")

            # Start a single consumer that writes results as they arrive.
            results_queue = asyncio.Queue(maxsize=max(1, max_concurrent * 2))

            async def _consume_results() -> None:
                assert results_queue is not None
                while True:
                    item = await results_queue.get()
                    try:
                        if item is _queue_sentinel:
                            return
                        await store_result(item)
                    finally:
                        results_queue.task_done()

            consumer_task = asyncio.create_task(_consume_results())

            await connector.get_repos_with_stats_async(
                org_name=org_name,
                user_name=user_name,
                pattern=pattern,
                batch_size=batch_size,
                max_concurrent=max_concurrent,
                rate_limit_delay=rate_limit_delay,
                max_commits_per_repo=max_commits_per_repo,
                max_repos=max_repos,
                on_repo_complete=on_repo_complete,
            )

            # Ensure all queued DB writes are flushed before continuing.
            assert results_queue is not None
            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task
        else:
            logging.info("Using sync batch processing with incremental storage...")

            # Run the sync connector method in an executor so the event loop
            # can keep writing to the database as callbacks fire.
            results_queue = asyncio.Queue(maxsize=max(1, max_concurrent * 2))

            async def _consume_results() -> None:
                assert results_queue is not None
                while True:
                    item = await results_queue.get()
                    try:
                        if item is _queue_sentinel:
                            return
                        await store_result(item)
                    finally:
                        results_queue.task_done()

            consumer_task = asyncio.create_task(_consume_results())

            await loop.run_in_executor(
                None,
                lambda: connector.get_repos_with_stats(
                    org_name=org_name,
                    user_name=user_name,
                    pattern=pattern,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    rate_limit_delay=rate_limit_delay,
                    max_commits_per_repo=max_commits_per_repo,
                    max_repos=max_repos,
                    on_repo_complete=on_repo_complete,
                ),
            )

            assert results_queue is not None
            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task

        # Summary
        successful = sum(1 for r in all_results if r.success)
        failed = sum(1 for r in all_results if not r.success)
        logging.info("=== Batch Processing Complete ===")
        logging.info(f"  Successful: {successful}")
        logging.info(f"  Failed: {failed}")
        logging.info(f"  Total: {len(all_results)}")
        logging.info(f"  Stored: {stored_count}")

        # Log rate limit status
        try:
            rate_limit = connector.get_rate_limit()
            logging.info(
                f"  Rate limit: {rate_limit['remaining']}/{rate_limit['limit']} remaining"
            )
        except Exception as e:
            logging.debug(f"Could not get rate limit: {e}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise
    finally:
        connector.close()


async def process_gitlab_projects_batch(
    store: DataStore,
    token: str,
    gitlab_url: str = "https://gitlab.com",
    group_name: str = None,
    pattern: str = None,
    batch_size: int = 10,
    max_concurrent: int = 4,
    rate_limit_delay: float = 1.0,
    max_commits_per_project: int = None,
    max_projects: int = None,
    use_async: bool = False,
) -> None:
    """
    Process multiple GitLab projects using batch processing with pattern matching.

    This function uses the GitLabConnector's batch processing methods to retrieve
    projects matching a pattern and collect statistics for each. When use_async
    is True, it uses get_projects_with_stats_async for concurrent processing;
    otherwise, it uses get_projects_with_stats for synchronous processing.

    :param store: Storage backend.
    :param token: GitLab private token.
    :param gitlab_url: GitLab instance URL.
    :param group_name: Optional group name/path. When the --gitlab-group CLI argument
                       is provided, it is passed here to fetch projects from that group.
    :param pattern: fnmatch-style pattern to filter projects.
    :param batch_size: Number of projects to process in each batch.
    :param max_concurrent: Maximum concurrent workers for processing.
    :param rate_limit_delay: Delay in seconds between batches.
    :param max_commits_per_project: Maximum commits to analyze per project.
    :param max_projects: Maximum number of projects to process.
    :param use_async: Use async processing (get_projects_with_stats_async) for better
                      performance, otherwise use sync processing (get_projects_with_stats).
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info("=== GitLab Batch Project Processing ===")
    if pattern:
        logging.info(f"Pattern: {pattern}")
    if group_name:
        logging.info(f"Group: {group_name}")
    logging.info(f"GitLab URL: {gitlab_url}")
    logging.info(f"Batch size: {batch_size}, Max concurrent: {max_concurrent}")

    connector = GitLabConnector(url=gitlab_url, private_token=token)
    loop = asyncio.get_running_loop()

    # Track results for summary and incremental storage
    all_results: List[GitLabBatchResult] = []
    stored_count = 0

    # Same streaming-write approach as GitHub batch processing.
    results_queue: Optional[asyncio.Queue] = None
    _queue_sentinel = object()

    async def store_result(result: GitLabBatchResult) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        project_info = result.project

        # Create Repo object for database
        db_repo = Repo(
            repo_path=None,  # Not a local repo
            repo=project_info.full_name,
            settings={
                "source": "gitlab",
                "project_id": project_info.id,
                "url": project_info.url,
                "default_branch": project_info.default_branch,
                "batch_processed": True,
            },
            tags=["gitlab"],
        )

        await store.insert_repo(db_repo)
        stored_count += 1
        logging.debug(f"Stored project ({stored_count}): {db_repo.repo}")

        # Store stats if available
        if result.stats:
            # Create commit stats entries from aggregated data
            stat = GitCommitStat(
                repo_id=db_repo.id,
                commit_hash=AGGREGATE_STATS_MARKER,
                file_path=AGGREGATE_STATS_MARKER,
                additions=result.stats.additions,
                deletions=result.stats.deletions,
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
            await store.insert_git_commit_stats([stat])

    def on_project_complete(result: GitLabBatchResult) -> None:
        """Callback for when each project is processed - logs and queues for storage."""
        all_results.append(result)
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
            logging.info(f"  ✓ Processed: {result.project.full_name}{stats_info}")
        else:
            logging.warning(f"  ✗ Failed: {result.project.full_name}: {result.error}")

        if results_queue is not None:

            def _enqueue() -> None:
                assert results_queue is not None
                try:
                    results_queue.put_nowait(result)
                except asyncio.QueueFull:
                    asyncio.create_task(results_queue.put(result))

            loop.call_soon_threadsafe(_enqueue)

    try:
        # Choose async or sync processing
        if use_async:
            logging.info("Using async batch processing with incremental storage...")

            results_queue = asyncio.Queue(maxsize=max(1, max_concurrent * 2))

            async def _consume_results() -> None:
                assert results_queue is not None
                while True:
                    item = await results_queue.get()
                    try:
                        if item is _queue_sentinel:
                            return
                        await store_result(item)
                    finally:
                        results_queue.task_done()

            consumer_task = asyncio.create_task(_consume_results())

            await connector.get_projects_with_stats_async(
                group_name=group_name,
                pattern=pattern,
                batch_size=batch_size,
                max_concurrent=max_concurrent,
                rate_limit_delay=rate_limit_delay,
                max_commits_per_project=max_commits_per_project,
                max_projects=max_projects,
                on_project_complete=on_project_complete,
            )

            assert results_queue is not None
            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task
        else:
            logging.info("Using sync batch processing with incremental storage...")

            results_queue = asyncio.Queue(maxsize=max(1, max_concurrent * 2))

            async def _consume_results() -> None:
                assert results_queue is not None
                while True:
                    item = await results_queue.get()
                    try:
                        if item is _queue_sentinel:
                            return
                        await store_result(item)
                    finally:
                        results_queue.task_done()

            consumer_task = asyncio.create_task(_consume_results())

            await loop.run_in_executor(
                None,
                lambda: connector.get_projects_with_stats(
                    group_name=group_name,
                    pattern=pattern,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    rate_limit_delay=rate_limit_delay,
                    max_commits_per_project=max_commits_per_project,
                    max_projects=max_projects,
                    on_project_complete=on_project_complete,
                ),
            )

            assert results_queue is not None
            await results_queue.join()
            await results_queue.put(_queue_sentinel)
            await consumer_task

        # Summary
        successful = sum(1 for r in all_results if r.success)
        failed = sum(1 for r in all_results if not r.success)
        logging.info("=== Batch Processing Complete ===")
        logging.info(f"  Successful: {successful}")
        logging.info(f"  Failed: {failed}")
        logging.info(f"  Total: {len(all_results)}")
        logging.info(f"  Stored: {stored_count}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        raise
    finally:
        connector.close()


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process git repository data from local repo or GitHub/GitLab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local repository processing
  python git_mergestat.py --db="postgresql://..." --connector=local --repo-path=/path/to/repo

  # GitHub repository processing
  python git_mergestat.py --db="postgresql://..." --connector=github --auth=$GITHUB_TOKEN \\
      --github-owner=myorg --github-repo=myrepo

  # GitLab repository processing
  python git_mergestat.py --db="mongodb://..." --connector=gitlab --auth=$GITLAB_TOKEN \\
      --gitlab-project-id=12345

  # Batch processing with search pattern
  python git_mergestat.py --db="postgresql://..." --connector=github --auth=$GITHUB_TOKEN \\
      --search-pattern="myorg/api-*" --group=myorg --batch-size=5

  # Auto-detect database type from connection string
  python git_mergestat.py --db="mongodb://localhost:27017/mydb" --connector=local
        """,
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
        "--start-date",
        required=False,
        help="Start date for filtering commits (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        required=False,
        help="End date for filtering commits (YYYY-MM-DD).",
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

    # Batch processing options (unified for both GitHub and GitLab)
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

    # Shared batch processing options (used by both GitHub and GitLab)
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
    """
    Determine the operating mode based on CLI arguments.

    :param args: Parsed command-line arguments.
    :param github_token: GitHub authentication token.
    :param gitlab_token: GitLab authentication token.
    :return: Tuple of (use_github, use_github_batch, use_gitlab, use_gitlab_batch).
    :raises ValueError: If connector type specified but required arguments missing.
    """
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
        # Legacy mode detection (no --connector specified)
        use_github = bool(github_token and args.github_owner and args.github_repo)
        use_github_batch = bool(github_token and args.search_pattern)
        use_gitlab = bool(gitlab_token and args.gitlab_project_id)
        use_gitlab_batch = bool(gitlab_token and args.search_pattern)

        # Check for ambiguity when both tokens are available and --search-pattern is set
        if use_github_batch and use_gitlab_batch:
            raise ValueError(
                "Ambiguous batch mode: both GitHub and GitLab tokens are available "
                "and --search-pattern is set. Please specify --connector to disambiguate."
            )

        return (use_github, use_github_batch, use_gitlab, use_gitlab_batch)


async def main() -> None:
    """
    Main entry point for the script.
    """
    args = parse_args()

    # Override environment variables with command-line arguments if provided
    global DB_CONN_STRING, REPO_PATH, DB_TYPE
    if args.db:
        DB_CONN_STRING = args.db
    if args.repo_path:
        REPO_PATH = args.repo_path

    # Auto-detect database type from connection string if not specified
    if args.db_type:
        DB_TYPE = args.db_type.lower()
    elif DB_CONN_STRING:
        try:
            DB_TYPE = detect_db_type(DB_CONN_STRING)
            logging.info(f"Auto-detected database type: {DB_TYPE}")
        except ValueError:
            pass  # Fall back to environment variable

    if DB_TYPE not in {"postgres", "mongo", "sqlite"}:
        raise ValueError("DB_TYPE must be 'postgres', 'mongo', or 'sqlite'")
    if not DB_CONN_STRING:
        raise ValueError(
            "Database connection string is required (set DB_CONN_STRING or use --db)"
        )

    # Determine authentication token (--auth or environment variables)
    auth_token = args.auth
    github_token = auth_token or os.getenv("GITHUB_TOKEN")
    gitlab_token = auth_token or os.getenv("GITLAB_TOKEN")

    # Determine operating mode
    use_github, use_github_batch, use_gitlab, use_gitlab_batch = _determine_mode(
        args, github_token, gitlab_token
    )

    modes_active = sum([use_github, use_github_batch, use_gitlab, use_gitlab_batch])
    if modes_active > 1:
        raise ValueError(
            "Cannot use multiple modes simultaneously. Choose one of: "
            "single GitHub repo (--github-owner + --github-repo), "
            "GitHub batch (--github-pattern), "
            "single GitLab project (--gitlab-project-id), "
            "or GitLab batch (--gitlab-pattern)."
        )

    # Initialize storage using factory function
    store: DataStore = create_store(
        DB_CONN_STRING,
        db_type=DB_TYPE,
        db_name=MONGO_DB_NAME,
        echo=DB_ECHO,
    )

    async with store as storage:
        if use_github_batch:
            # GitHub batch processing mode (PR 31)
            await process_github_repos_batch(
                store=storage,
                token=github_token,
                org_name=args.group,  # Can be used as org or user
                pattern=args.search_pattern,
                batch_size=args.batch_size,
                max_concurrent=args.max_concurrent,
                rate_limit_delay=args.rate_limit_delay,
                max_commits_per_repo=args.max_commits_per_repo,
                max_repos=args.max_repos,
                use_async=args.use_async,
            )
        elif use_gitlab_batch:
            # GitLab batch processing mode (PR 31)
            await process_gitlab_projects_batch(
                store=storage,
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
            # GitHub connector mode
            logging.info("=== GitHub Connector Mode ===")
            await process_github_repo(
                storage, args.github_owner, args.github_repo, github_token
            )
        elif use_gitlab:
            # GitLab connector mode
            logging.info("=== GitLab Connector Mode ===")
            await process_gitlab_project(
                storage, args.gitlab_project_id, gitlab_token, args.gitlab_url
            )
        else:
            # Local repository mode
            logging.info("=== Local Repository Mode ===")
            # TODO: Implement date filtering for commits
            # start_date = args.start_date
            # end_date = args.end_date

            repo: Repo = Repo(REPO_PATH)
            logging.info(f"Using repository UUID: {repo.id}")

            # Ensure the repository is inserted first
            await insert_repo_data(storage, repo)

            all_files: List[Path] = [
                Path(REPO_PATH) / f
                for f in repo.git.ls_files().splitlines()
                if not is_skippable(f)
            ]
            if not all_files:
                logging.info("No files to process.")
                return
            logging.info(f"Found {len(all_files)} files to process.")

            # Process GitFile data first
            await process_git_files(repo, all_files, storage)

            # Process other data asynchronously
            await asyncio.gather(
                process_git_commits(repo, storage),
                process_git_blame(all_files, storage, repo),
                process_git_commit_stats(repo, storage),
            )


if __name__ == "__main__":
    asyncio.run(main())
