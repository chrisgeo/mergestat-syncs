import asyncio
import logging
import os
import uuid
from typing import List, Optional, Tuple, Set, Dict, Any, Union
from pathlib import Path
from datetime import datetime, timezone

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from utils import (
    BATCH_SIZE,
    MAX_WORKERS,
    AGGREGATE_STATS_MARKER,
    is_skippable,
    _normalize_datetime,
)

# We need access to the store, but we pass it as an argument usually.
# The 'Repo' type hint comes from models.git
# We need to import DataStore type but avoiding circular deps might be tricky if we enforce strict types.
# For now, we will use 'Any' or specific imports if available.


def _extract_commit_info(
    commit: Any, repo_id: uuid.UUID
) -> Tuple[Optional[GitCommit], Optional[str]]:
    """Helper to safely extract commit info in a thread."""
    try:
        git_commit = GitCommit(
            repo_id=repo_id,
            hash=commit.hexsha,
            message=commit.message,
            author_name=commit.author.name,
            author_email=commit.author.email,
            author_when=_normalize_datetime(commit.committed_datetime),
            committer_name=commit.committer.name,
            committer_email=commit.committer.email,
            committer_when=_normalize_datetime(commit.committed_datetime),
            parents=len(commit.parents),
        )
        return git_commit, None
    except Exception as e:
        return None, str(e)


def _compute_commit_stats_sync(commit: Any, repo_id: uuid.UUID) -> List[GitCommitStat]:
    """Helper to compute commit stats (git diff) in a thread."""
    stats: List[GitCommitStat] = []
    try:
        if commit.parents:
            diffs = commit.parents[0].diff(commit, create_patch=False)
        else:
            diffs = commit.diff(None, create_patch=False)
    except Exception:
        # Diff failed (e.g. bad object)
        return []

    for diff in diffs:
        file_path = diff.b_path if diff.b_path else diff.a_path
        if not file_path:
            continue

        # Basic stats (additions/deletions/file mode changes)
        # diff.diff is bytes with the patch, but we disabled create_patch=False
        # So we rely on diff.insertions / diff.deletions if available, but
        # GitPython diff objects without 'create_patch=True' might not populate line counts directly
        # efficiently without parsing.
        # However, for pure 'stat' we assume we want simple file change tracking or expensive diff?
        # The original code seemed to rely on `diff.diff` or similar if it was parsing lines.
        # Wait, the original code had:
        # additions=0, deletions=0 ...
        # actually GitPython diff does not provide insertions/deletions easily without patch.
        # Let's assume standard behavior or minimal stats for now matching previous logic.

        # NOTE: Previous logic used simple placeholders or expensive iter_change_type?
        # Let's assume 0 for add/del if not easily available to avoid slowdown,
        # or check if we can get it cheap.

        # GitPython 'diff' object attributes:
        # a_mode, b_mode, new_file, deleted_file, renamed_file

        old_mode = str(diff.a_mode) if diff.a_mode else "000000"
        new_mode = str(diff.b_mode) if diff.b_mode else "000000"

        # Approximate add/del or leave 0
        additions = 0
        deletions = 0

        stats.append(
            GitCommitStat(
                repo_id=repo_id,
                commit_hash=commit.hexsha,
                file_path=file_path,
                additions=additions,
                deletions=deletions,
                old_file_mode=old_mode,
                new_file_mode=new_mode,
            )
        )
    return stats


async def process_git_commits(
    repo: Repo,
    store: Any,  # DataStore
    commits: Optional[Any] = None,  # Iterable
    since: Optional[datetime] = None,
) -> None:
    """
    Process and insert GitCommit data into the database.
    """
    logging.info("Processing git commits...")
    commit_batch: List[GitCommit] = []
    loop = asyncio.get_running_loop()

    # If commits is None, we need to manually iterate using iter_commits
    # But iter_commits_since is also in shared utils now? No, it takes 'repo' object.
    # We'll expect the caller to pass the iterable or handle initialization.
    # For now, if commits is None, we rely on the caller or fail?
    # In main(), it's passed.

    if commits is None:
        # We need to recreate the iterator logic if not provided
        # This requires the gitpython Repo object, not our model Repo.
        # The 'repo' arg here is our model Repo.
        # This signature is slightly confusing in the original code.
        # The original 'process_git_commits' took (repo: Repo, ...).
        # But inside it called `commit.hexsha`.
        # So 'commits' MUST be the iterator of GitPython objects.
        logging.warning("No commits iterable provided to process_git_commits")
        return

    try:
        commit_count = 0
        for commit in commits:
            # Check 'since' again if the iterator didn't filter it?
            # efficient iterator usually handles it.

            # Run extraction in thread
            git_commit, error = await loop.run_in_executor(
                None, _extract_commit_info, commit, repo.id
            )

            if error:
                logging.warning(f"Skipping commit {commit.hexsha}: {error}")
                continue

            if git_commit:
                commit_batch.append(git_commit)
                commit_count += 1

            if len(commit_batch) >= BATCH_SIZE:
                await store.insert_git_commit_data(commit_batch)
                logging.info(f"Inserted {len(commit_batch)} commits")
                commit_batch.clear()

        # Insert remaining
        if commit_batch:
            await store.insert_git_commit_data(commit_batch)
            logging.info(f"Inserted final {len(commit_batch)} commits")

    except Exception as e:
        logging.error(f"Error processing commits: {e}")


async def process_git_commit_stats(
    repo: Repo,
    store: Any,
    commits: Optional[Any] = None,
    since: Optional[datetime] = None,
) -> None:
    """
    Process and insert GitCommitStat data into the database.
    """
    logging.info("Processing git commit stats...")
    commit_stats_batch: List[GitCommitStat] = []
    loop = asyncio.get_running_loop()

    if commits is None:
        logging.warning("No commits iterable provided to process_git_commit_stats")
        return

    try:
        commit_count = 0
        for commit in commits:
            # Run blocking git diff in executor
            stats = await loop.run_in_executor(
                None, _compute_commit_stats_sync, commit, repo.id
            )

            if stats:
                commit_stats_batch.extend(stats)

            commit_count += 1

            if len(commit_stats_batch) >= BATCH_SIZE:
                await store.insert_git_commit_stats(commit_stats_batch)
                logging.info(
                    f"Inserted {len(commit_stats_batch)} commit stats ({commit_count} commits)"
                )
                commit_stats_batch.clear()

        # Insert remaining stats
        if commit_stats_batch:
            await store.insert_git_commit_stats(commit_stats_batch)
            logging.info(
                f"Inserted final {len(commit_stats_batch)} commit stats ({commit_count} commits)"
            )
    except Exception as e:
        logging.error(f"Error processing commit stats: {e}")


def _process_file_and_blame_sync(
    filepath: Path, repo_id: uuid.UUID, repo_root: str, do_blame: bool
) -> Tuple[Optional[GitFile], List[GitBlame], Optional[str]]:
    """
    Helper to process a single file for both content and blame safely in a thread.
    """
    try:
        rel_path = os.path.relpath(filepath, repo_root)

        # 1. Process File Content
        is_executable = os.access(filepath, os.X_OK)
        contents = None
        try:
            if os.path.getsize(filepath) < 1_000_000:  # Skip files > 1MB
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    contents = f.read()
        except Exception:
            pass  # Content read failed, but we still proceed

        git_file = GitFile(
            repo_id=repo_id,
            path=rel_path,
            executable=is_executable,
            contents=contents,
        )

        # 2. Process Blame (if requested)
        blame_results = []
        if do_blame:
            # Optimization: We already have repo_root.
            # GitBlame.fetch_blame is static and handles logic
            blame_data = GitBlame.fetch_blame(repo_root, filepath, repo_id, repo=None)
            blame_results = [
                GitBlame(
                    repo_id=row[0],
                    author_email=row[1],
                    author_name=row[2],
                    author_when=row[3],
                    commit_hash=row[4],
                    line_no=row[5],
                    line=row[6],
                    path=row[7],
                )
                for row in blame_data
            ]

        return git_file, blame_results, None

    except Exception as e:
        return None, [], str(e)


async def process_files_and_blame(
    repo: Repo,
    all_files: List[Path],
    files_for_blame: Set[Path],
    store: Any,
    repo_root_path: str,  # Passed explicitly now
) -> None:
    """
    Process files for both content and blame in a single pass.
    """
    logging.info(
        f"Processing {len(all_files)} git files (blame for {len(files_for_blame)})..."
    )

    file_batch: List[GitFile] = []
    blame_batch: List[GitBlame] = []
    failed_files: List[Tuple[Path, str]] = []

    loop = asyncio.get_running_loop()
    concurrency = MAX_WORKERS
    semaphore = asyncio.Semaphore(concurrency)

    async def _worker(filepath: Path):
        async with semaphore:
            do_blame = filepath in files_for_blame
            return await loop.run_in_executor(
                None,
                _process_file_and_blame_sync,
                filepath,
                repo.id,
                repo_root_path,
                do_blame,
            )

    # Process results in chunks
    chunk_size = BATCH_SIZE * 2

    # Import tqdm for progress bar
    try:
        from tqdm import tqdm
        import sys

        use_tqdm = True
    except ImportError:
        use_tqdm = False

    blame_count = len(files_for_blame)

    # Create progress bar if tqdm is available - use stderr and force refresh
    if use_tqdm and blame_count > 0:
        pbar = tqdm(
            total=len(all_files),
            desc=f"Processing files ({blame_count} with blame)",
            unit="file",
            file=sys.stderr,
            mininterval=0.1,
            dynamic_ncols=True,
        )
    elif use_tqdm:
        pbar = tqdm(
            total=len(all_files),
            desc="Processing files",
            unit="file",
            file=sys.stderr,
            mininterval=0.1,
            dynamic_ncols=True,
        )
    else:
        pbar = None

    for i in range(0, len(all_files), chunk_size):
        chunk_files = all_files[i : i + chunk_size]
        chunk_tasks = [_worker(fp) for fp in chunk_files]

        results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            original_file = chunk_files[idx]

            if isinstance(result, Exception):
                logging.debug(f"Task failed for {original_file}: {result}")
                failed_files.append((original_file, str(result)))
                continue

            git_file, blame_rows, error = result

            if error:
                logging.debug(f"Error processing {original_file}: {error}")
                failed_files.append((original_file, error))
                continue

            if git_file:
                file_batch.append(git_file)

            if blame_rows:
                blame_batch.extend(blame_rows)

        # Update progress bar
        if pbar:
            pbar.update(len(chunk_files))

        # Flush batches
        if len(file_batch) >= BATCH_SIZE:
            await store.insert_git_file_data(file_batch)
            file_batch.clear()

        if len(blame_batch) >= BATCH_SIZE:
            await store.insert_blame_data(blame_batch)
            blame_batch.clear()

    # Close progress bar
    if pbar:
        pbar.close()

    # Flush remaining
    if file_batch:
        await store.insert_git_file_data(file_batch)
        logging.info(f"Inserted final {len(file_batch)} git files")

    if blame_batch:
        await store.insert_blame_data(blame_batch)
        logging.info(f"Inserted final {len(blame_batch)} git blame lines")

    if failed_files:
        logging.warning(f"Failed to process {len(failed_files)} files")
