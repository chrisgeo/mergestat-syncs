#!/usr/bin/env python3
import argparse
import asyncio
import mimetypes
import os
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from models import GitBlame, Repo, GitCommit, GitCommitStat, GitFile

# === CONFIGURATION ===# The line `REPO_PATH = os.getenv("REPO_PATH", ".")` is retrieving the value of
# an environment variable named "REPO_PATH". If the environment variable is not
# set, it defaults to "." (current directory). This allows the script to be
# more flexible by allowing the user to specify the path to the repository as
# an environment variable, with a fallback to the current directory if the
# variable is not set.

REPO_PATH = os.getenv("REPO_PATH", ".")
DB_CONN_STRING = os.getenv("DB_CONN_STRING")
BATCH_SIZE = 10  # Insert every n files
REPO_UUID = os.getenv("REPO_UUID")

# Create SQLAlchemy async engine and session factory
engine = create_async_engine(DB_CONN_STRING, echo=True)
AsyncSessionFactory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def insert_blame_data(session: AsyncSession, data_batch: List[GitBlame]) -> None:
    """
    Insert a batch of blame data into the database using SQLAlchemy ORM.

    :param session: SQLAlchemy AsyncSession
    :param data_batch: List of GitBlame objects.
    """
    if not data_batch:
        return

    session.add_all(data_batch)
    await session.commit()


async def insert_git_commit_data(
    session: AsyncSession, commit_data: List[GitCommit]
) -> None:
    """
    Insert a batch of git commit data into the database.

    :param session: SQLAlchemy AsyncSession
    :param commit_data: List of GitCommit objects.
    """
    if not commit_data:
        return

    session.add_all(commit_data)
    await session.commit()


async def insert_git_commit_stats(
    session: AsyncSession, commit_stats: List[GitCommitStat]
) -> None:
    """
    Insert a batch of git commit stats into the database.

    :param session: SQLAlchemy AsyncSession
    :param commit_stats: List of GitCommitStat objects.
    """
    if not commit_stats:
        return

    session.add_all(commit_stats)
    await session.commit()


async def insert_git_file_data(session: AsyncSession, file_data: List[GitFile]) -> None:
    """
    Insert a batch of git file data into the database.

    :param session: SQLAlchemy AsyncSession
    :param file_data: List of GitFile objects.
    """
    if not file_data:
        return

    session.add_all(file_data)
    await session.commit()


async def insert_repo_data(session: AsyncSession, repo: Repo) -> None:
    """
    Insert the repository data into the database if it doesn't already exist.

    :param session: SQLAlchemy AsyncSession
    :param repo: Repo object to insert.
    """
    existing_repo = await session.get(Repo, repo.id)
    if not existing_repo:
        # Ensure required fields are populated
        if not repo.repo:
            repo.repo = REPO_PATH  # Use the repository path as the default value
        if not repo.settings:
            repo.settings = {}
        if not repo.tags:
            repo.tags = []

        session.add(repo)
        await session.commit()


# Expanded SKIP_EXTENSIONS to include more binary and package-like file types
SKIP_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".pdf", ".ttf", ".otf", ".woff", ".woff2", ".ico",
    ".mp4", ".mp3", ".mov", ".avi", ".exe", ".dll", ".zip", ".tar", ".gz", ".7z", ".eot",
    ".rar", ".iso", ".dmg", ".pkg", ".deb", ".rpm", ".msi", ".class", ".jar", ".war", ".pyc",
    ".pyo", ".so", ".o", ".a", ".lib", ".bin", ".dat", ".swp", ".lock", ".bak", ".tmp"
}


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


async def process_git_files(
    repo: Repo, all_files: List[Path], session: AsyncSession
) -> None:
    """
    Process and insert GitFile data into the database.

    :param repo: Git repository object.
    :param all_files: List of file paths in the repository.
    :param session: SQLAlchemy AsyncSession.
    """
    file_batch: List[GitFile] = []
    for filepath in all_files:
        # Process file data (example logic, replace with actual implementation)
        file_objects = []  # Replace with logic to fetch GitFile objects
        file_batch.extend(file_objects)

        if len(file_batch) >= BATCH_SIZE:
            await insert_git_file_data(session, file_batch)
            file_batch.clear()

    # Insert any remaining file data
    if file_batch:
        await insert_git_file_data(session, file_batch)


async def process_git_commits(repo: Repo, session: AsyncSession) -> None:
    """
    Process and insert GitCommit data into the database.

    :param repo: Git repository object.
    :param session: SQLAlchemy AsyncSession.
    """
    commit_batch: List[GitCommit] = []
    # Process commit data (example logic, replace with actual implementation)
    commit_objects = []  # Replace with logic to fetch GitCommit objects
    commit_batch.extend(commit_objects)

    if commit_batch:
        await insert_git_commit_data(session, commit_batch)


async def process_git_blame(all_files: List[Path], session: AsyncSession) -> None:
    """
    Process and insert GitBlame data into the database.

    :param all_files: List of file paths in the repository.
    :param session: SQLAlchemy AsyncSession.
    """
    blame_batch: List[GitBlame] = []
    for filepath in all_files:
        blame_objects = GitBlame.process_file(REPO_PATH, filepath, REPO_UUID)
        blame_batch.extend(blame_objects)

        if len(blame_batch) >= BATCH_SIZE:
            await insert_blame_data(session, blame_batch)
            blame_batch.clear()

    # Insert any remaining blame data
    if blame_batch:
        await insert_blame_data(session, blame_batch)


async def process_git_commit_stats(repo: Repo, session: AsyncSession) -> None:
    """
    Process and insert GitCommitStat data into the database.

    :param repo: Git repository object.
    :param session: SQLAlchemy AsyncSession.
    """
    commit_stats_batch: List[GitCommitStat] = []
    # Process commit stats (example logic, replace with actual implementation)
    commit_stats_objects = []  # Replace with logic to fetch GitCommitStat objects
    commit_stats_batch.extend(commit_stats_objects)

    if commit_stats_batch:
        await insert_git_commit_stats(session, commit_stats_batch)


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Process git repository data.")
    parser.add_argument("--db", required=False, help="Database connection string.")
    parser.add_argument("--repo-path", required=False, help="Path to the git repository.")
    parser.add_argument("--start-date", required=False, help="Start date for filtering commits (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=False, help="End date for filtering commits (YYYY-MM-DD).")
    return parser.parse_args()


async def main() -> None:
    """
    Main entry point for the script.
    """
    args = parse_args()

    # Override environment variables with command-line arguments if provided
    global DB_CONN_STRING, REPO_PATH
    if args.db:
        DB_CONN_STRING = args.db
    if args.repo_path:
        REPO_PATH = args.repo_path

    start_date = args.start_date
    end_date = args.end_date

    repo: Repo = Repo(REPO_PATH)

    async with AsyncSessionFactory() as session:
        # Ensure the repository is inserted first
        await insert_repo_data(session, repo)

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
        await process_git_files(repo, all_files, session)

        # Process other data asynchronously
        await asyncio.gather(
            process_git_commits(repo, session),
            process_git_blame(all_files, session),
            process_git_commit_stats(repo, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
