#!/usr/bin/env python3
import argparse
import asyncio
import logging
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Union

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from storage import MongoStore, SQLAlchemyStore, create_store, detect_db_type

# Import connectors (optional - will be None if not available)
try:
    from connectors import (
        GitHubConnector, GitLabConnector, BatchResult, GitLabBatchResult
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
    return mime and mime.startswith(
        (
            "image/",
            "video/",
            "audio/",
            "application/pdf",
            "font/",
            "application/x-executable",
            "application/x-sharedlib",
            "application/x-object",
            "application/x-archive",
        )
    )


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
    filepath: Path, repo_path: str, semaphore: asyncio.Semaphore
) -> List[GitBlame]:
    """
    Process a single file for blame data with concurrency control.

    :param filepath: Path to the file.
    :param repo_path: Path to the git repository (used to create thread-safe Repo instance).
    :param semaphore: Semaphore to control concurrent file processing.
    :return: List of GitBlame objects.
    """
    async with semaphore:
        # Run blame processing in executor to avoid blocking
        # Note: Create a new Repo instance per worker for thread-safety
        # GitPython's Repo object is not thread-safe for concurrent operations
        loop = asyncio.get_event_loop()

        def process_with_new_repo():
            # Create a new Repo instance for this thread to avoid race conditions
            thread_repo = Repo(repo_path)
            return GitBlame.process_file(
                repo_path, filepath, thread_repo.id, thread_repo
            )

        blame_objects = await loop.run_in_executor(None, process_with_new_repo)
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
        chunk = all_files[i: i + chunk_size]

        # Process chunk in parallel
        # Note: Each worker creates its own Repo instance for thread-safety
        tasks = [
            process_single_file_blame(filepath, REPO_PATH, semaphore)
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
    store: DataStore, owner: str, repo_name: str, token: str
) -> None:
    """
    Process a GitHub repository using the GitHub connector.

    :param store: Storage backend.
    :param owner: Repository owner.
    :param repo_name: Repository name.
    :param token: GitHub token.
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
        logging.info("Fetching commits from GitHub...")
        commits = list(gh_repo.get_commits()[:100])  # Limit to 100 for now

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
        for commit in commits[:50]:  # Limit to 50 for stats
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

        logging.info(f"Successfully processed GitHub repository: {owner}/{repo_name}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitHub repository: {e}")
        raise
    finally:
        connector.close()


async def process_gitlab_project(
    store: DataStore, project_id: int, token: str, gitlab_url: str
) -> None:
    """
    Process a GitLab project using the GitLab connector.

    :param store: Storage backend.
    :param project_id: GitLab project ID.
    :param token: GitLab token.
    :param gitlab_url: GitLab instance URL.
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
        logging.info("Fetching commits from GitLab...")
        commits = gl_project.commits.list(per_page=100, get_all=False)

        commit_objects = []
        for commit in commits:
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

        await store.insert_git_commit_data(commit_objects)
        logging.info(f"Stored {len(commit_objects)} commits from GitLab")

        # Get and store commit stats
        logging.info("Fetching commit stats from GitLab...")
        stats_objects = []
        for commit in commits[:50]:  # Limit to 50 for stats
            try:
                detailed_commit = gl_project.commits.get(commit.id)
                if hasattr(detailed_commit, "stats"):
                    # GitLab provides aggregate stats per commit
                    # We'll create a single entry per commit with aggregate data
                    stat = GitCommitStat(
                        repo_id=db_repo.id,
                        commit_hash=commit.id,
                        file_path=AGGREGATE_STATS_MARKER,
                        additions=detailed_commit.stats.get("additions", 0),
                        deletions=detailed_commit.stats.get("deletions", 0),
                        old_file_mode="unknown",
                        new_file_mode="unknown",
                    )
                    stats_objects.append(stat)
            except Exception as e:
                logging.warning(f"Failed to get stats for commit {commit.id}: {e}")

        if stats_objects:
            await store.insert_git_commit_stats(stats_objects)
            logging.info(f"Stored {len(stats_objects)} commit stats from GitLab")

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

    def on_repo_complete(result: BatchResult) -> None:
        """Callback for when each repository is processed."""
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
            logging.info(f"  ✓ Processed: {result.repository.full_name}{stats_info}")
        else:
            logging.warning(f"  ✗ Failed: {result.repository.full_name}: {result.error}")

    try:
        # Choose async or sync processing
        if use_async:
            logging.info("Using async batch processing...")
            results = await connector.get_repos_with_stats_async(
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
        else:
            logging.info("Using sync batch processing...")
            results = connector.get_repos_with_stats(
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

        # Store results in database
        logging.info(f"Storing {len(results)} repository results...")
        for result in results:
            if not result.success:
                continue

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
            logging.debug(f"Stored repository: {db_repo.repo}")

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

        # Summary
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        logging.info("=== Batch Processing Complete ===")
        logging.info(f"  Successful: {successful}")
        logging.info(f"  Failed: {failed}")
        logging.info(f"  Total: {len(results)}")

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

    def on_project_complete(result: GitLabBatchResult) -> None:
        """Callback for when each project is processed."""
        if result.success:
            stats_info = ""
            if result.stats:
                stats_info = f" ({result.stats.total_commits} commits)"
            logging.info(f"  ✓ Processed: {result.project.full_name}{stats_info}")
        else:
            logging.warning(f"  ✗ Failed: {result.project.full_name}: {result.error}")

    try:
        # Choose async or sync processing
        if use_async:
            logging.info("Using async batch processing...")
            results = await connector.get_projects_with_stats_async(
                group_name=group_name,
                pattern=pattern,
                batch_size=batch_size,
                max_concurrent=max_concurrent,
                rate_limit_delay=rate_limit_delay,
                max_commits_per_project=max_commits_per_project,
                max_projects=max_projects,
                on_project_complete=on_project_complete,
            )
        else:
            logging.info("Using sync batch processing...")
            results = connector.get_projects_with_stats(
                group_name=group_name,
                pattern=pattern,
                batch_size=batch_size,
                max_concurrent=max_concurrent,
                rate_limit_delay=rate_limit_delay,
                max_commits_per_project=max_commits_per_project,
                max_projects=max_projects,
                on_project_complete=on_project_complete,
            )

        # Store results in database
        logging.info(f"Storing {len(results)} project results...")
        for result in results:
            if not result.success:
                continue

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
            logging.debug(f"Stored project: {db_repo.repo}")

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

        # Summary
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        logging.info("=== Batch Processing Complete ===")
        logging.info(f"  Successful: {successful}")
        logging.info(f"  Failed: {failed}")
        logging.info(f"  Total: {len(results)}")

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

  # GitHub repository processing (simplified)
  python git_mergestat.py --db="postgresql://..." --connector=github --auth=$GITHUB_TOKEN \\
      --github-owner=myorg --github-repo=myrepo

  # GitLab repository processing (simplified)
  python git_mergestat.py --db="mongodb://..." --connector=gitlab --auth=$GITLAB_TOKEN \\
      --gitlab-project-id=12345

  # Auto-detect database type from connection string
  python git_mergestat.py --db="mongodb://localhost:27017/mydb" --connector=local
        """,
    )

    # Simplified interface arguments
    parser.add_argument("--db", required=False, help="Database connection string.")
    parser.add_argument(
        "--connector",
        required=False,
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
        "--github-token",
        required=False,
        help="GitHub personal access token (or use --auth or GITHUB_TOKEN env var).",
    )
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
        "--gitlab-token",
        required=False,
        help="GitLab private token (or use --auth or GITLAB_TOKEN env var).",
    )
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

    # GitHub batch processing options (PR 31)
    parser.add_argument(
        "--github-pattern",
        required=False,
        help="fnmatch-style pattern to filter repositories (e.g., 'chrisgeo/m*').",
    )
    parser.add_argument(
        "--github-batch-size",
        required=False,
        type=int,
        default=10,
        help="Number of repositories to process in each batch (default: 10).",
    )

    # GitLab batch processing options (PR 31)
    parser.add_argument(
        "--gitlab-pattern",
        required=False,
        help="fnmatch-style pattern to filter GitLab projects (e.g., 'group/p*').",
    )
    parser.add_argument(
        "--gitlab-group",
        required=False,
        help="GitLab group name/path to fetch projects from.",
    )
    parser.add_argument(
        "--gitlab-batch-size",
        required=False,
        type=int,
        default=10,
        help="Number of GitLab projects to process in each batch (default: 10).",
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

    return parser.parse_args()


def _determine_mode(args, github_token: str, gitlab_token: str) -> tuple:
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
        if args.github_pattern:
            return (False, True, False, False)
        elif args.github_owner and args.github_repo:
            return (True, False, False, False)
        else:
            raise ValueError(
                "GitHub connector requires --github-owner and --github-repo, "
                "or --github-pattern for batch processing."
            )
    elif connector_type == "gitlab":
        if args.gitlab_pattern:
            return (False, False, False, True)
        elif args.gitlab_project_id:
            return (False, False, True, False)
        else:
            raise ValueError(
                "GitLab connector requires --gitlab-project-id, "
                "or --gitlab-pattern for batch processing."
            )
    elif connector_type == "local":
        return (False, False, False, False)
    else:
        # Legacy mode detection (no --connector specified)
        use_github = bool(github_token and args.github_owner and args.github_repo)
        use_github_batch = bool(github_token and args.github_pattern)
        use_gitlab = bool(gitlab_token and args.gitlab_project_id)
        use_gitlab_batch = bool(gitlab_token and args.gitlab_pattern)
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

    # Determine authentication token (--auth takes precedence, then specific tokens)
    auth_token = args.auth
    github_token = auth_token or args.github_token or os.getenv("GITHUB_TOKEN")
    gitlab_token = auth_token or args.gitlab_token or os.getenv("GITLAB_TOKEN")

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
                org_name=args.github_owner,  # Can be used as org or user
                pattern=args.github_pattern,
                batch_size=args.github_batch_size,
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
                group_name=args.gitlab_group,
                pattern=args.gitlab_pattern,
                batch_size=args.gitlab_batch_size,
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
