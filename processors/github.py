import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Tuple, List, Optional

from models.git import GitCommit, GitCommitStat, Repo, GitPullRequest, GitBlame, GitFile
from utils import AGGREGATE_STATS_MARKER, is_skippable, CONNECTORS_AVAILABLE, BATCH_SIZE

if CONNECTORS_AVAILABLE:
    from connectors import BatchResult, GitHubConnector, ConnectorException
    from connectors.models import Repository
    from connectors.utils import RateLimitConfig, RateLimitGate
    from github import RateLimitExceededException
else:
    BatchResult = None  # type: ignore
    GitHubConnector = None  # type: ignore
    ConnectorException = Exception
    Repository = None  # type: ignore
    RateLimitConfig = None  # type: ignore
    RateLimitGate = None  # type: ignore
    RateLimitExceededException = Exception


# --- GitHub Sync Helpers ---


def _fetch_github_repo_info_sync(connector, owner, repo_name):
    """Sync helper to fetch GitHub repository info."""
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")
    _ = gh_repo.id
    return gh_repo


def _fetch_github_commits_sync(gh_repo, max_commits, repo_id):
    """Sync helper to fetch and parse GitHub commits."""
    raw_commits = list(gh_repo.get_commits()[:max_commits])

    commit_objects = []
    for commit in raw_commits:
        git_commit = GitCommit(
            repo_id=repo_id,
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
                commit.commit.committer.name if commit.commit.committer else "Unknown"
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
    return raw_commits, commit_objects


def _fetch_github_commit_stats_sync(raw_commits, repo_id, max_stats):
    """Sync helper to fetch detailed commit stats (files)."""
    stats_objects = []
    for commit in raw_commits[:max_stats]:
        try:
            files = commit.files
            if files is None:
                continue

            for file in files:
                stat = GitCommitStat(
                    repo_id=repo_id,
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
    return stats_objects


def _fetch_github_prs_sync(connector, owner, repo_name, repo_id, max_prs):
    """Sync helper to fetch Pull Requests."""
    prs = connector.get_pull_requests(owner, repo_name, state="all", max_prs=max_prs)
    pr_objects = []
    for pr in prs:
        created_at = (
            pr.created_at or pr.merged_at or pr.closed_at or datetime.now(timezone.utc)
        )
        git_pr = GitPullRequest(
            repo_id=repo_id,
            number=pr.number,
            title=pr.title,
            state=pr.state,
            author_name=pr.author.username if pr.author else "Unknown",
            author_email=pr.author.email if pr.author else None,
            created_at=created_at,
            merged_at=pr.merged_at,
            closed_at=pr.closed_at,
            head_branch=pr.head_branch,
            base_branch=pr.base_branch,
        )
        pr_objects.append(git_pr)
    return pr_objects


def _sync_github_prs_to_store(
    connector,
    owner: str,
    repo_name: str,
    repo_id,
    store,
    loop: asyncio.AbstractEventLoop,
    batch_size: int,
    state: str = "all",
    gate: Optional[RateLimitGate] = None,
) -> int:
    """Fetch all PRs for a repo and insert them in batches.

    Runs in a worker thread; uses run_coroutine_threadsafe to write batches.
    """
    gh_repo = connector.github.get_repo(f"{owner}/{repo_name}")
    batch: List[GitPullRequest] = []
    total = 0

    if gate is None:
        gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

    pr_iter = iter(gh_repo.get_pulls(state=state))
    while True:
        try:
            gate.wait_sync()
            gh_pr = next(pr_iter)
            gate.reset()
        except StopIteration:
            break
        except RateLimitExceededException as e:
            retry_after = None
            if hasattr(connector, "_rate_limit_reset_delay_seconds"):
                try:
                    retry_after = connector._rate_limit_reset_delay_seconds()
                except Exception:
                    retry_after = None
            applied = gate.penalize(retry_after)
            logging.info(
                "GitHub rate limited fetching PRs; backoff %.1fs (%s)",
                applied,
                e,
            )
            continue
        except Exception as e:
            headers = getattr(e, "headers", None)
            retry_after = None
            if isinstance(headers, dict):
                # PyGithub header casing varies; treat keys case-insensitively.
                headers_ci = {str(k).lower(): v for k, v in headers.items()}
                ra = headers_ci.get("retry-after")
                if ra:
                    try:
                        retry_after = float(ra)
                    except ValueError:
                        retry_after = None
                if retry_after is None:
                    reset = headers_ci.get("x-ratelimit-reset")
                    if reset:
                        try:
                            import time

                            retry_after = max(0.0, float(reset) - time.time())
                        except ValueError:
                            retry_after = None

            if retry_after is not None:
                applied = gate.penalize(retry_after)
                logging.info(
                    "GitHub rate limited fetching PRs; backoff %.1fs (%s)",
                    applied,
                    e,
                )
                continue
            raise
        author_name = "Unknown"
        author_email = None
        if getattr(gh_pr, "user", None):
            author_name = getattr(gh_pr.user, "login", None) or author_name
            author_email = getattr(gh_pr.user, "email", None)

        created_at = (
            getattr(gh_pr, "created_at", None)
            or getattr(gh_pr, "merged_at", None)
            or getattr(gh_pr, "closed_at", None)
            or datetime.now(timezone.utc)
        )

        batch.append(
            GitPullRequest(
                repo_id=repo_id,
                number=int(getattr(gh_pr, "number", 0) or 0),
                title=getattr(gh_pr, "title", None),
                state=getattr(gh_pr, "state", None),
                author_name=author_name,
                author_email=author_email,
                created_at=created_at,
                merged_at=getattr(gh_pr, "merged_at", None),
                closed_at=getattr(gh_pr, "closed_at", None),
                head_branch=getattr(getattr(gh_pr, "head", None), "ref", None),
                base_branch=getattr(getattr(gh_pr, "base", None), "ref", None),
            )
        )
        total += 1

        if len(batch) >= batch_size:
            asyncio.run_coroutine_threadsafe(
                store.insert_git_pull_requests(batch),
                loop,
            ).result()
            batch.clear()

    if batch:
        asyncio.run_coroutine_threadsafe(
            store.insert_git_pull_requests(batch),
            loop,
        ).result()

    return total


def _fetch_github_blame_sync(gh_repo, repo_id, limit=50):
    """Sync helper to fetch (simulated) blame by listing files."""
    files_to_process = []
    try:
        contents = gh_repo.get_contents("", ref=gh_repo.default_branch)
        while contents:
            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(
                    gh_repo.get_contents(file_content.path, ref=gh_repo.default_branch)
                )
            else:
                if not is_skippable(file_content.path):
                    files_to_process.append(file_content.path)

            if len(files_to_process) >= limit:
                break
    except Exception as e:
        logging.error(f"Error listing files: {e}")
    return []


def _split_full_name(full_name: str) -> Tuple[str, str]:
    parts = (full_name or "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo/project full name: {full_name}")
    return parts[0], parts[1]


async def _backfill_github_missing_data(
    store: Any,
    connector: GitHubConnector,
    db_repo: Repo,
    repo_full_name: str,
    default_branch: str,
    max_commits: Optional[int],
) -> None:
    # Logic copied from git_mergestat.py
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


async def process_github_repo(
    store: Any,
    owner: str,
    repo_name: str,
    token: str,
    fetch_blame: bool = False,
    max_commits: int = 100,
) -> None:
    """
    Process a GitHub repository using the GitHub connector.
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info(f"Processing GitHub repository: {owner}/{repo_name}")
    loop = asyncio.get_running_loop()

    connector = GitHubConnector(token=token)
    try:
        # 1. Fetch Repo Info
        logging.info("Fetching repository information...")
        gh_repo = await loop.run_in_executor(
            None, _fetch_github_repo_info_sync, connector, owner, repo_name
        )

        # Create/Insert Repo
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

        db_repo = Repo(
            repo_path=None,
            repo=repo_info.full_name,
            settings={
                "source": "github",
                "repo_id": repo_info.id,
                "url": repo_info.url,
                "default_branch": repo_info.default_branch,
            },
            tags=["github", repo_info.language] if repo_info.language else ["github"],
        )

        await store.insert_repo(db_repo)
        logging.info(f"Repository stored: {db_repo.repo} ({db_repo.id})")

        # 2. Fetch Commits
        logging.info(f"Fetching up to {max_commits} commits from GitHub...")
        raw_commits, commit_objects = await loop.run_in_executor(
            None, _fetch_github_commits_sync, gh_repo, max_commits, db_repo.id
        )

        if commit_objects:
            await store.insert_git_commit_data(commit_objects)
            logging.info(f"Stored {len(commit_objects)} commits from GitHub")

        # 3. Fetch Stats
        logging.info("Fetching commit stats from GitHub...")
        stats_limit = min(max_commits, 50)
        stats_objects = await loop.run_in_executor(
            None, _fetch_github_commit_stats_sync, raw_commits, db_repo.id, stats_limit
        )

        if stats_objects:
            await store.insert_git_commit_stats(stats_objects)
            logging.info(f"Stored {len(stats_objects)} commit stats from GitHub")

        # 4. Fetch PRs
        logging.info("Fetching pull requests from GitHub...")
        pr_total = await loop.run_in_executor(
            None,
            _sync_github_prs_to_store,
            connector,
            owner,
            repo_name,
            db_repo.id,
            store,
            loop,
            BATCH_SIZE,
            "all",
        )
        logging.info(f"Stored {pr_total} pull requests from GitHub")

        # 5. Fetch Blame (Optional & Stubbed)
        if fetch_blame:
            logging.info("Fetching blame data (file list) from GitHub...")
            await loop.run_in_executor(
                None, _fetch_github_blame_sync, gh_repo, db_repo.id
            )

        logging.info(f"Successfully processed GitHub repository: {owner}/{repo_name}")

    except ConnectorException as e:
        logging.error(f"Connector error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing GitHub repository: {e}")
        raise
    finally:
        connector.close()


async def process_github_repos_batch(
    store: Any,
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
    """
    if not CONNECTORS_AVAILABLE:
        raise RuntimeError(
            "Connectors are not available. Please install required dependencies."
        )

    logging.info("=== GitHub Batch Repository Processing ===")
    connector = GitHubConnector(token=token)
    loop = asyncio.get_running_loop()

    pr_gate = RateLimitGate(
        RateLimitConfig(initial_backoff_seconds=max(1.0, rate_limit_delay))
    )
    pr_semaphore = asyncio.Semaphore(max(1, max_concurrent))

    # Track results for summary and incremental storage
    all_results: List[BatchResult] = []
    stored_count = 0

    results_queue: Optional[asyncio.Queue] = None
    _queue_sentinel = object()

    async def store_result(result: BatchResult) -> None:
        """Store a single result in the database (upsert)."""
        nonlocal stored_count
        if not result.success:
            return

        repo_info = result.repository
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

        # Fetch ALL PRs for batch-processed repos, storing in batches.
        try:
            owner, repo_name = _split_full_name(repo_info.full_name)
            async with pr_semaphore:
                await loop.run_in_executor(
                    None,
                    _sync_github_prs_to_store,
                    connector,
                    owner,
                    repo_name,
                    db_repo.id,
                    store,
                    loop,
                    BATCH_SIZE,
                    "all",
                    pr_gate,
                )
        except Exception as e:
            logging.warning(
                "Failed to fetch/store PRs for GitHub repo %s: %s",
                repo_info.full_name,
                e,
            )

        if result.stats:
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

        try:
            await _backfill_github_missing_data(
                store=store,
                connector=connector,
                db_repo=db_repo,
                repo_full_name=repo_info.full_name,
                default_branch=repo_info.default_branch,
                max_commits=max_commits_per_repo,
            )
        except Exception as e:
            logging.debug(f"Backfill failed for GitHub repo {repo_info.full_name}: {e}")

    def on_repo_complete(result: BatchResult) -> None:
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

        if results_queue is not None:

            def _enqueue() -> None:
                assert results_queue is not None
                try:
                    results_queue.put_nowait(result)
                except asyncio.QueueFull:
                    asyncio.create_task(results_queue.put(result))

            loop.call_soon_threadsafe(_enqueue)

    try:
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

        if use_async:
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
        else:
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
