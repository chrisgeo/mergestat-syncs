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
from storage import MongoStore, SQLAlchemyStore

# Import connectors (optional - will be None if not available)
try:
    from connectors import GitHubConnector, GitLabConnector
    from connectors.exceptions import ConnectorException

    CONNECTORS_AVAILABLE = True
except ImportError:
    CONNECTORS_AVAILABLE = False
    GitHubConnector = None
    GitLabConnector = None
    ConnectorException = Exception

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
                        file_path="__aggregate__",  # Special marker for aggregate stats
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


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process git repository data from local repo or GitHub/GitLab."
    )
    parser.add_argument("--db", required=False, help="Database connection string.")
    parser.add_argument(
        "--repo-path", required=False, help="Path to the local git repository."
    )
    parser.add_argument(
        "--db-type",
        required=False,
        choices=["postgres", "mongo"],
        help="Database backend to use (postgres or mongo).",
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
        help="GitHub personal access token (or set GITHUB_TOKEN env var).",
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
        help="GitLab private token (or set GITLAB_TOKEN env var).",
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

    return parser.parse_args()


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
    if args.db_type:
        DB_TYPE = args.db_type.lower()

    if DB_TYPE not in {"postgres", "mongo"}:
        raise ValueError("DB_TYPE must be either 'postgres' or 'mongo'")
    if not DB_CONN_STRING:
        raise ValueError(
            "Database connection string is required (set DB_CONN_STRING or use --db)"
        )

    # Determine mode: local repo, GitHub, or GitLab
    github_token = args.github_token or os.getenv("GITHUB_TOKEN")
    gitlab_token = args.gitlab_token or os.getenv("GITLAB_TOKEN")

    use_github = github_token and args.github_owner and args.github_repo
    use_gitlab = gitlab_token and args.gitlab_project_id

    if use_github and use_gitlab:
        raise ValueError(
            "Cannot use both GitHub and GitLab modes simultaneously. Choose one."
        )

    # Initialize storage
    if DB_TYPE == "mongo":
        store: DataStore = MongoStore(DB_CONN_STRING, db_name=MONGO_DB_NAME)
    else:
        store = SQLAlchemyStore(DB_CONN_STRING, echo=DB_ECHO)

    async with store as storage:
        if use_github:
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
