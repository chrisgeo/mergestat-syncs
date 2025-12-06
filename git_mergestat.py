#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import logging
import mimetypes
import os
import uuid
from pathlib import Path
from typing import List, Tuple, Union

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from storage import MongoStore, SQLAlchemyStore

# Configure logging (add this near the top of file)
logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s"
)

# === CONFIGURATION ===# The line `REPO_PATH = os.getenv("REPO_PATH", ".")` is retrieving the value of
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
REPO_UUID = os.getenv("REPO_UUID")
MAX_WORKERS = int(
    os.getenv("MAX_WORKERS", "4")
)  # Number of parallel workers for file processing

DataStore = Union[SQLAlchemyStore, MongoStore]


def get_repo_uuid(repo_path: str) -> str:
    """
    Generate a deterministic UUID for a repository based on git data.

    Priority order:
    1. REPO_UUID environment variable (if set)
    2. Remote URL (if repository has a remote configured)
    3. Absolute path of the repository (fallback)

    :param repo_path: Path to the git repository.
    :return: UUID string for the repository.
    """
    # First check if REPO_UUID is explicitly set
    env_uuid = os.getenv("REPO_UUID")
    if env_uuid:
        return env_uuid

    try:
        # Try to get repository information from git
        from git import Repo as GitRepo

        git_repo = GitRepo(repo_path)

        # Try to get remote URL (most reliable identifier)
        if git_repo.remotes:
            remote_url = None
            # Try to get the origin remote first
            if "origin" in [r.name for r in git_repo.remotes]:
                origin = git_repo.remote("origin")
                remote_url = list(origin.urls)[0] if origin.urls else None
            else:
                # Use first available remote
                remote_url = (
                    list(git_repo.remotes[0].urls)[0]
                    if git_repo.remotes[0].urls
                    else None
                )

            if remote_url:
                # Create deterministic UUID from remote URL
                # Use SHA256 hash and convert to UUID format
                hash_obj = hashlib.sha256(remote_url.encode("utf-8"))
                # Take first 16 bytes of hash and create UUID
                uuid_bytes = hash_obj.digest()[:16]
                return str(uuid.UUID(bytes=uuid_bytes))

        # Fallback to absolute path if no remote
        abs_path = os.path.abspath(repo_path)
        hash_obj = hashlib.sha256(abs_path.encode("utf-8"))
        uuid_bytes = hash_obj.digest()[:16]
        return str(uuid.UUID(bytes=uuid_bytes))

    except Exception as e:
        # If anything fails, generate a random UUID
        logging.warning(
            f"Could not derive UUID from git repository data: {e}. Generating random UUID."
        )
        return str(uuid.uuid4())


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
    print(f"Processing {len(all_files)} git files...")
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
                repo_id=REPO_UUID,
                path=rel_path,
                executable=is_executable,
                contents=contents,
            )
            file_batch.append(git_file)
            successfully_processed += 1

            if len(file_batch) >= BATCH_SIZE:
                await insert_git_file_data(store, file_batch)
                print(f"Inserted {len(file_batch)} files")
                file_batch.clear()
        except Exception as e:
            error_msg = str(e)
            failed_files.append((filepath, error_msg))
            print(f"Error processing file {filepath}: {error_msg}")

    # Insert remaining files
    if file_batch:
        await insert_git_file_data(store, file_batch)
        print(f"Inserted final {len(file_batch)} files")

    # Report summary
    total_failed = len(failed_files)
    print(f"\nGit file processing complete:")
    print(f"  Successfully processed: {successfully_processed} files")
    print(f"  Failed: {total_failed} files")

    if failed_files:
        print(f"\nFailed files:")
        for filepath, error in failed_files[:10]:  # Show first 10
            print(f"  - {filepath}: {error}")
        if total_failed > 10:
            print(f"  ... and {total_failed - 10} more")


async def process_git_commits(repo: Repo, store: DataStore) -> None:
    """
    Process and insert GitCommit data into the database.

    :param repo: Git repository object.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    print("Processing git commits...")
    commit_batch: List[GitCommit] = []

    # Process commits using gitpython
    try:
        commit_count = 0
        for commit in repo.iter_commits():
            git_commit = GitCommit(
                repo_id=REPO_UUID,
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
                print(f"Inserted {len(commit_batch)} commits ({commit_count} total)")
                commit_batch.clear()

        # Insert remaining commits
        if commit_batch:
            await insert_git_commit_data(store, commit_batch)
            print(f"Inserted final {len(commit_batch)} commits ({commit_count} total)")
    except Exception as e:
        print(f"Error processing commits: {e}")


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
            return GitBlame.process_file(repo_path, filepath, REPO_UUID, thread_repo)

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

    print(
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
                print(f"Error processing {filepath}: {error_msg}")
            elif result:
                blame_batch.extend(result)
                successfully_processed += 1

        # Insert batch if we have data
        if blame_batch:
            await insert_blame_data(store, blame_batch)
            print(
                f"Inserted blame data for {len(blame_batch)} lines ({i + len(chunk)}/{len(all_files)} files)"
            )
            blame_batch.clear()

    # Report summary
    total_failed = len(failed_files)
    print(f"\nGit blame processing complete:")
    print(f"  Successfully processed: {successfully_processed} files")
    print(f"  Failed: {total_failed} files")

    if failed_files:
        print(f"\nFailed files:")
        for filepath, error in failed_files[:10]:  # Show first 10
            print(f"  - {filepath}: {error}")
        if total_failed > 10:
            print(f"  ... and {total_failed - 10} more")


async def process_git_commit_stats(repo: Repo, store: DataStore) -> None:
    """
    Process and insert GitCommitStat data into the database.

    :param repo: Git repository object.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    print("Processing git commit stats...")
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
                            f"Failed to decode diff or count lines for file '{file_path}' in commit '{getattr(commit, 'hexsha', None)}': {e}"
                        )

                # Determine file modes
                old_mode = "unknown"
                new_mode = "unknown"
                if diff.a_mode:
                    old_mode = oct(diff.a_mode)
                if diff.b_mode:
                    new_mode = oct(diff.b_mode)

                git_commit_stat = GitCommitStat(
                    repo_id=REPO_UUID,
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
                print(
                    f"Inserted {len(commit_stats_batch)} commit stats ({commit_count} commits)"
                )
                commit_stats_batch.clear()

        # Insert remaining stats
        if commit_stats_batch:
            await insert_git_commit_stats(store, commit_stats_batch)
            print(
                f"Inserted final {len(commit_stats_batch)} commit stats ({commit_count} commits)"
            )
    except Exception as e:
        print(f"Error processing commit stats: {e}")


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Process git repository data.")
    parser.add_argument("--db", required=False, help="Database connection string.")
    parser.add_argument(
        "--repo-path", required=False, help="Path to the git repository."
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
    return parser.parse_args()


async def main() -> None:
    """
    Main entry point for the script.
    """
    args = parse_args()

    # Override environment variables with command-line arguments if provided
    global DB_CONN_STRING, REPO_PATH, DB_TYPE, REPO_UUID
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

    # Derive REPO_UUID from git repository data (remote URL or path)
    REPO_UUID = get_repo_uuid(REPO_PATH)
    if os.getenv("REPO_UUID"):
        print(f"Using REPO_UUID from environment: {REPO_UUID}")
    else:
        print(f"Derived REPO_UUID from git repository: {REPO_UUID}")

    # TODO: Implement date filtering for commits
    # start_date = args.start_date
    # end_date = args.end_date

    repo: Repo = Repo(REPO_PATH)

    if DB_TYPE == "mongo":
        store: DataStore = MongoStore(DB_CONN_STRING, db_name=MONGO_DB_NAME)
    else:
        store = SQLAlchemyStore(DB_CONN_STRING, echo=DB_ECHO)

    async with store as storage:
        # Ensure the repository is inserted first
        await insert_repo_data(storage, repo)

        all_files: List[Path] = [
            Path(REPO_PATH) / f
            for f in repo.git.ls_files().splitlines()
            if not is_skippable(f)
        ]
        if not all_files:
            print("No files to process.")
            return
        print(f"Found {len(all_files)} files to process.")

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
