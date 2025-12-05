#!/usr/bin/env python3
import argparse
import asyncio
import functools
import mimetypes
import os
import uuid
from pathlib import Path
from typing import AsyncIterator, Callable, Iterable, List, Optional, Tuple, Union

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from storage import MongoStore, SQLAlchemyStore

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
REPO_UUID = os.getenv("REPO_UUID")
DEFAULT_WORKERS = max(4, (os.cpu_count() or 4))

DataStore = Union[SQLAlchemyStore, MongoStore]


def _parse_positive_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


FILE_CONTENT_MAX_BYTES = _parse_positive_int(
    os.getenv("FILE_CONTENT_MAX_BYTES"), 1_000_000
)
BLAME_MAX_FILE_SIZE = _parse_positive_int(os.getenv("BLAME_MAX_FILE_SIZE"), 5_000_000)
BATCH_SIZE = _parse_positive_int(os.getenv("BATCH_SIZE"), 50)
MAX_IO_WORKERS = _parse_positive_int(
    os.getenv("GIT_IO_WORKERS"), min(32, DEFAULT_WORKERS)
)


def resolve_repo_id(repo: Repo) -> uuid.UUID:
    """
    Determine the UUID to use for a repository and ensure the Repo object has it set.
    """
    if REPO_UUID:
        try:
            repo_id = uuid.UUID(REPO_UUID)
        except ValueError as exc:
            raise ValueError("REPO_UUID must be a valid UUID") from exc
    else:
        repo_id = getattr(repo, "id", None) or uuid.uuid4()
    repo.id = repo_id
    return repo_id


async def _bounded_to_thread_map(
    items: Iterable,
    func: Callable,
    *,
    concurrency: int = MAX_IO_WORKERS,
    buffer_factor: int = 4,
) -> AsyncIterator:
    """
    Run blocking work in a thread pool with bounded concurrency and yield results
    as soon as they are ready.
    """
    concurrency = max(1, concurrency)
    buffer_size = max(1, concurrency * buffer_factor)
    semaphore = asyncio.Semaphore(concurrency)
    tasks: List[asyncio.Task] = []

    async def runner(item):
        async with semaphore:
            return await asyncio.to_thread(func, item)

    for item in items:
        tasks.append(asyncio.create_task(runner(item)))
        if len(tasks) >= buffer_size:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            tasks = list(pending)
            for task in done:
                yield task.result()

    for task in asyncio.as_completed(tasks):
        yield await task


def _build_git_file(filepath: Path, repo_id: uuid.UUID) -> Optional[GitFile]:
    """
    Build a GitFile instance from a path, skipping unreadable files.
    """
    rel_path = os.path.relpath(filepath, REPO_PATH)
    try:
        stat = filepath.stat()
        executable = bool(stat.st_mode & 0o111)
        contents = None
        # Avoid loading very large files into memory
        if stat.st_size < FILE_CONTENT_MAX_BYTES:
            contents = filepath.read_text(errors="ignore")
    except (OSError, UnicodeDecodeError):
        return None

    return GitFile(
        repo_id=repo_id,
        path=rel_path,
        executable=executable,
        contents=contents,
    )


def _build_commit_and_stats(
    commit, repo_id: uuid.UUID
) -> Tuple[GitCommit, List[GitCommitStat]]:
    """
    Build GitCommit and related GitCommitStat rows from a commit object.
    """
    commit_obj = GitCommit(
        repo_id=repo_id,
        hash=commit.hexsha,
        message=commit.message,
        author_name=getattr(commit.author, "name", None),
        author_email=getattr(commit.author, "email", None),
        author_when=commit.authored_datetime,
        committer_name=getattr(commit.committer, "name", None),
        committer_email=getattr(commit.committer, "email", None),
        committer_when=commit.committed_datetime,
        parents=len(commit.parents),
    )

    stats_objects: List[GitCommitStat] = []
    for file_path, summary in commit.stats.files.items():
        stats_objects.append(
            GitCommitStat(
                repo_id=repo_id,
                commit_hash=commit.hexsha,
                file_path=file_path,
                additions=summary.get("insertions", 0),
                deletions=summary.get("deletions", 0),
                old_file_mode="unknown",
                new_file_mode="unknown",
            )
        )

    return commit_obj, stats_objects


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


@functools.lru_cache(maxsize=256)
def _cached_mime_for_extension(ext: str) -> Optional[str]:
    """
    Cache mime lookups per extension to avoid repeated mimetypes parsing.
    """
    dummy_name = f"file{ext}" if ext else "file"
    mime, _ = mimetypes.guess_type(dummy_name)
    return mime


@functools.lru_cache(maxsize=2048)
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
    mime = _cached_mime_for_extension(ext)
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
    repo: Repo, all_files: List[Path], store: DataStore, repo_id: uuid.UUID
) -> None:
    """
    Process and insert GitFile data into the database.

    :param repo: Git repository object.
    :param all_files: List of file paths in the repository.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param repo_id: UUID for the repository.
    """
    file_batch: List[GitFile] = []
    async for git_file in _bounded_to_thread_map(
        all_files, lambda path: _build_git_file(path, repo_id)
    ):
        if git_file is None:
            continue
        file_batch.append(git_file)

        if len(file_batch) >= BATCH_SIZE:
            await insert_git_file_data(store, file_batch)
            file_batch.clear()

    # Insert any remaining file data
    if file_batch:
        await insert_git_file_data(store, file_batch)


async def process_git_commits_and_stats(
    repo: Repo, store: DataStore, repo_id: uuid.UUID
) -> None:
    """
    Process commits and commit stats with bounded concurrency.
    """
    commit_batch: List[GitCommit] = []
    commit_stats_batch: List[GitCommitStat] = []

    commits = list(repo.iter_commits("HEAD"))
    if not commits:
        return

    async for commit_obj, stats_objects in _bounded_to_thread_map(
        commits,
        lambda commit: _build_commit_and_stats(commit, repo_id),
        concurrency=MAX_IO_WORKERS,
    ):
        commit_batch.append(commit_obj)
        commit_stats_batch.extend(stats_objects)

        if len(commit_batch) >= BATCH_SIZE:
            await insert_git_commit_data(store, commit_batch)
            commit_batch.clear()

        if len(commit_stats_batch) >= BATCH_SIZE:
            await insert_git_commit_stats(store, commit_stats_batch)
            commit_stats_batch.clear()

    if commit_batch:
        await insert_git_commit_data(store, commit_batch)
    if commit_stats_batch:
        await insert_git_commit_stats(store, commit_stats_batch)


async def process_git_blame(
    all_files: List[Path], store: DataStore, repo: Repo, repo_id: uuid.UUID
) -> None:
    """
    Process and insert GitBlame data into the database.

    :param all_files: List of file paths in the repository.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    :param repo: Repo instance to use for git operations.
    :param repo_id: UUID for the repository.
    """
    blame_batch: List[GitBlame] = []
    eligible_files: List[Path] = []

    for filepath in all_files:
        try:
            if BLAME_MAX_FILE_SIZE and filepath.stat().st_size > BLAME_MAX_FILE_SIZE:
                continue
        except OSError:
            continue
        eligible_files.append(filepath)

    build_blame = lambda f: GitBlame.process_file(REPO_PATH, f, repo_id, repo=repo)

    async for blame_objects in _bounded_to_thread_map(
        eligible_files, build_blame, concurrency=MAX_IO_WORKERS
    ):
        if not blame_objects:
            continue
        blame_batch.extend(blame_objects)

        if len(blame_batch) >= BATCH_SIZE:
            await insert_blame_data(store, blame_batch)
            blame_batch.clear()

    # Insert any remaining blame data
    if blame_batch:
        await insert_blame_data(store, blame_batch)


async def process_git_commit_stats(repo: Repo, store: DataStore) -> None:
    """
    Process and insert GitCommitStat data into the database.

    :param repo: Git repository object.
    :param store: Storage backend (SQLAlchemy or MongoDB).
    """
    commit_stats_batch: List[GitCommitStat] = []
    # Process commit stats (example logic, replace with actual implementation)
    commit_stats_objects = []  # Replace with logic to fetch GitCommitStat objects
    commit_stats_batch.extend(commit_stats_objects)

    if commit_stats_batch:
        await insert_git_commit_stats(store, commit_stats_batch)


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

    # TODO: Implement date filtering for commits
    # start_date = args.start_date
    # end_date = args.end_date

    repo: Repo = Repo(REPO_PATH)
    repo_id = resolve_repo_id(repo)

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
        await process_git_files(repo, all_files, storage, repo_id)

        # Process other data asynchronously
        await asyncio.gather(
            process_git_blame(all_files, storage, repo, repo_id),
            process_git_commits_and_stats(repo, storage, repo_id),
        )


if __name__ == "__main__":
    asyncio.run(main())
