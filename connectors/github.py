"""
GitHub connector using PyGithub and GraphQL.

This connector provides methods to retrieve organizations, repositories,
contributors, statistics, pull requests, and blame information from GitHub.
"""

import asyncio
import fnmatch
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, List, Optional

from github import Github, GithubException, RateLimitExceededException

from connectors.base import BatchResult, GitConnector
from connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from connectors.models import (
    Author,
    BlameRange,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    Repository,
    RepoStats,
)
from connectors.utils import GitHubGraphQLClient, retry_with_backoff

logger = logging.getLogger(__name__)


def match_repo_pattern(full_name: str, pattern: str) -> bool:
    """
    Match a repository full name against a pattern using fnmatch-style matching.

    :param full_name: Repository full name (e.g., 'chrisgeo/mergestat-syncs').
    :param pattern: Pattern to match (e.g., 'chrisgeo/m*', '*/sync*', 'chrisgeo/*').
    :return: True if the pattern matches, False otherwise.

    Examples:
        - 'chrisgeo/m*' matches 'chrisgeo/mergestat-syncs'
        - '*/sync*' matches 'anyorg/sync-tool'
        - 'org/repo' matches exactly 'org/repo'
    """
    return fnmatch.fnmatch(full_name.lower(), pattern.lower())


class GitHubConnector(GitConnector):
    """
    Production-grade GitHub connector using PyGithub and GraphQL.

    Provides methods to retrieve data from GitHub with automatic
    pagination, rate limiting, and error handling.
    """

    def __init__(
        self,
        token: str,
        base_url: Optional[str] = None,
        per_page: int = 100,
        max_workers: int = 4,
    ):
        """
        Initialize GitHub connector.

        :param token: GitHub personal access token.
        :param base_url: Optional base URL for GitHub Enterprise.
        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        """
        super().__init__(per_page=per_page, max_workers=max_workers)
        self.token = token

        # Initialize PyGithub client
        if base_url:
            self.github = Github(
                base_url=base_url, login_or_token=token, per_page=per_page
            )
        else:
            self.github = Github(login_or_token=token, per_page=per_page)

        # Initialize GraphQL client for blame operations
        self.graphql = GitHubGraphQLClient(token)

    def _handle_github_exception(self, e: Exception) -> None:
        """
        Handle GitHub API exceptions and convert to connector exceptions.

        :param e: Exception from GitHub API.
        :raises: Appropriate connector exception.
        """
        if isinstance(e, RateLimitExceededException):
            raise RateLimitException(f"GitHub rate limit exceeded: {e}")
        elif isinstance(e, GithubException):
            if e.status == 401:
                raise AuthenticationException(f"GitHub authentication failed: {e}")
            elif e.status == 404:
                raise APIException(f"GitHub resource not found: {e}")
            else:
                raise APIException(f"GitHub API error: {e}")
        else:
            raise APIException(f"Unexpected error: {e}")

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_organizations(
        self,
        max_orgs: Optional[int] = None,
    ) -> List[Organization]:
        """
        List organizations accessible to the authenticated user.

        :param max_orgs: Maximum number of organizations to retrieve.
        :return: List of Organization objects.
        """
        try:
            orgs = []
            user = self.github.get_user()

            for gh_org in user.get_orgs():
                if max_orgs and len(orgs) >= max_orgs:
                    break

                org = Organization(
                    id=gh_org.id,
                    name=gh_org.login,
                    description=gh_org.description,
                    url=gh_org.html_url,
                )
                orgs.append(org)
                logger.debug(f"Retrieved organization: {org.name}")

            logger.info(f"Retrieved {len(orgs)} organizations")
            return orgs

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_repositories(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        search: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """
        List repositories for an organization, user, or search query.

        :param org_name: Optional organization name. If provided, lists organization repos.
        :param user_name: Optional user name. If provided, lists that user's repos.
        :param search: Optional search query to filter repositories.
                      If provided with org_name/user_name, searches within that scope.
                      If provided alone, performs global search.
        :param pattern: Optional fnmatch-style pattern to filter repositories by full name
                       (e.g., 'chrisgeo/m*', '*/api-*'). Pattern matching is performed
                       client-side after fetching repositories. Case-insensitive.
        :param max_repos: Maximum number of repositories to retrieve. If None, retrieves all.
        :return: List of Repository objects.

        Examples:
            - pattern='chrisgeo/m*' matches 'chrisgeo/mergestat-syncs'
            - pattern='*/sync*' matches 'anyorg/sync-tool'
        """
        try:
            repos = []

            # Determine the appropriate API method and parameters
            if search:
                # Build search query with optional scope qualifiers
                query_parts = [search]
                if org_name:
                    query_parts.append(f"org:{org_name}")
                elif user_name:
                    query_parts.append(f"user:{user_name}")
                gh_repos = self.github.search_repositories(query=" ".join(query_parts))
            else:
                # Fetch repositories without search
                if org_name:
                    source = self.github.get_organization(org_name)
                elif user_name:
                    source = self.github.get_user(user_name)
                else:
                    source = self.github.get_user()
                gh_repos = source.get_repos()

            for gh_repo in gh_repos:
                if max_repos and len(repos) >= max_repos:
                    break

                # Apply pattern filter early to avoid unnecessary object creation
                if pattern and not match_repo_pattern(gh_repo.full_name, pattern):
                    continue

                repo = Repository(
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

                repos.append(repo)
                logger.debug(f"Retrieved repository: {repo.full_name}")

            pattern_msg = f" matching pattern '{pattern}'" if pattern else ""
            logger.info(f"Retrieved {len(repos)} repositories{pattern_msg}")
            return repos

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_contributors(
        self,
        owner: str,
        repo: str,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """
        Get contributors for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_contributors: Maximum number of contributors to retrieve.
        :return: List of Author objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            contributors = []

            for contributor in gh_repo.get_contributors():
                if max_contributors and len(contributors) >= max_contributors:
                    break

                author = Author(
                    id=contributor.id,
                    username=contributor.login,
                    name=contributor.name,
                    email=contributor.email,
                    url=contributor.html_url,
                )
                contributors.append(author)
                logger.debug(f"Retrieved contributor: {author.username}")

            logger.info(
                f"Retrieved {len(contributors)} contributors for {owner}/{repo}"
            )
            return contributors

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_commit_stats(
        self,
        owner: str,
        repo: str,
        sha: str,
    ) -> CommitStats:
        """
        Get statistics for a specific commit.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param sha: Commit SHA.
        :return: CommitStats object.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            commit = gh_repo.get_commit(sha)

            stats = commit.stats

            return CommitStats(
                additions=stats.additions,
                deletions=stats.deletions,
                commits=1,
            )

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_repo_stats(
        self,
        owner: str,
        repo: str,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """
        Get aggregated statistics for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_commits: Maximum number of commits to analyze.
        :return: RepoStats object.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")

            total_additions = 0
            total_deletions = 0
            commit_count = 0
            authors_dict = {}

            commits = gh_repo.get_commits()

            for commit in commits:
                if max_commits and commit_count >= max_commits:
                    break

                commit_count += 1

                # Get commit stats
                if commit.stats:
                    total_additions += commit.stats.additions
                    total_deletions += commit.stats.deletions

                # Track unique authors
                if commit.author:
                    author_id = commit.author.id
                    if author_id not in authors_dict:
                        authors_dict[author_id] = Author(
                            id=commit.author.id,
                            username=commit.author.login,
                            name=commit.author.name,
                            email=commit.author.email,
                            url=commit.author.html_url,
                        )

            # Calculate commits per week (rough estimate based on repo age)
            created_at = gh_repo.created_at
            age_days = (datetime.now(timezone.utc) - created_at).days
            weeks = max(age_days / 7, 1)
            commits_per_week = commit_count / weeks

            return RepoStats(
                total_commits=commit_count,
                additions=total_additions,
                deletions=total_deletions,
                commits_per_week=commits_per_week,
                authors=list(authors_dict.values()),
            )

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        max_prs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get pull requests for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param state: State filter ('open', 'closed', 'all').
        :param max_prs: Maximum number of pull requests to retrieve.
        :return: List of PullRequest objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            prs = []

            for gh_pr in gh_repo.get_pulls(state=state):
                if max_prs and len(prs) >= max_prs:
                    break

                author = None
                if gh_pr.user:
                    author = Author(
                        id=gh_pr.user.id,
                        username=gh_pr.user.login,
                        name=gh_pr.user.name,
                        email=gh_pr.user.email,
                        url=gh_pr.user.html_url,
                    )

                pr = PullRequest(
                    id=gh_pr.id,
                    number=gh_pr.number,
                    title=gh_pr.title,
                    state=gh_pr.state,
                    author=author,
                    created_at=gh_pr.created_at,
                    merged_at=gh_pr.merged_at,
                    closed_at=gh_pr.closed_at,
                    body=gh_pr.body,
                    url=gh_pr.html_url,
                    base_branch=gh_pr.base.ref,
                    head_branch=gh_pr.head.ref,
                )
                prs.append(pr)
                logger.debug(f"Retrieved PR #{pr.number}: {pr.title}")

            logger.info(f"Retrieved {len(prs)} pull requests for {owner}/{repo}")
            return prs

        except Exception as e:
            self._handle_github_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> FileBlame:
        """
        Get blame information for a file using GitHub GraphQL API.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param path: File path within the repository.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        try:
            result = self.graphql.get_blame(owner, repo, path, ref)

            ranges = []
            repo_data = result.get("repository", {})
            obj_data = repo_data.get("object", {})
            blame_data = obj_data.get("blame", {})
            ranges_data = blame_data.get("ranges", [])

            for range_item in ranges_data:
                commit = range_item.get("commit", {})
                author_info = commit.get("author", {})

                # Calculate age in seconds
                authored_date_str = commit.get("authoredDate")
                age_seconds = 0
                if authored_date_str:
                    try:
                        authored_date = datetime.fromisoformat(
                            authored_date_str.replace("Z", "+00:00")
                        )
                        age_seconds = int(
                            (datetime.now(timezone.utc) - authored_date).total_seconds()
                        )
                    except Exception as e:
                        logger.warning(f"Failed to parse date {authored_date_str}: {e}")

                blame_range = BlameRange(
                    starting_line=range_item.get("startingLine", 0),
                    ending_line=range_item.get("endingLine", 0),
                    commit_sha=commit.get("oid", ""),
                    author=author_info.get("name", "Unknown"),
                    author_email=author_info.get("email", ""),
                    age_seconds=age_seconds,
                )
                ranges.append(blame_range)

            logger.info(
                f"Retrieved blame for {owner}/{repo}:{path} with {len(ranges)} ranges"
            )
            return FileBlame(file_path=path, ranges=ranges)

        except Exception as e:
            self._handle_github_exception(e)

    def get_rate_limit(self) -> dict:
        """
        Get current rate limit status.

        :return: Dictionary with rate limit information.
        """
        try:
            rate_limit = self.github.get_rate_limit()
            core = rate_limit.core

            return {
                "limit": core.limit,
                "remaining": core.remaining,
                "reset": core.reset,
            }
        except Exception as e:
            self._handle_github_exception(e)

    def _get_repositories_for_processing(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """
        Get repositories for batch processing, optionally filtered by pattern.

        If neither org_name nor user_name is provided but pattern contains an owner
        (e.g., 'chrisgeo/*'), the owner is extracted from the pattern and used as
        the user_name for fetching repositories.

        :param org_name: Optional organization name.
        :param user_name: Optional user name.
        :param pattern: Optional fnmatch-style pattern.
        :param max_repos: Maximum number of repos to retrieve.
        :return: List of Repository objects.
        """
        # Extract owner from pattern if not explicitly provided
        effective_org = org_name
        effective_user = user_name

        if not org_name and not user_name and pattern:
            # Check if pattern has a specific owner prefix (e.g., 'chrisgeo/*')
            if "/" in pattern:
                parts = pattern.split("/", 1)
                owner_part = parts[0]
                # Only use as owner if it's not a wildcard
                if owner_part and "*" not in owner_part and "?" not in owner_part:
                    # Try as user first (works for both users and orgs via search)
                    effective_user = owner_part
                    logger.info(
                        f"Extracted owner '{owner_part}' from pattern '{pattern}'"
                    )

        return self.list_repositories(
            org_name=effective_org,
            user_name=effective_user,
            pattern=pattern,
            max_repos=max_repos,
        )

    def _process_single_repo_stats(
        self,
        repo: Repository,
        max_commits: Optional[int] = None,
    ) -> BatchResult:
        """
        Process a single repository and get its stats.

        :param repo: Repository object to process.
        :param max_commits: Maximum number of commits to analyze.
        :return: BatchResult containing repository and stats.
        """
        try:
            parts = repo.full_name.split("/")
            if len(parts) != 2:
                return BatchResult(
                    repository=repo,
                    error=f"Invalid repository name: {repo.full_name}",
                    success=False,
                )

            owner, repo_name = parts
            stats = self.get_repo_stats(owner, repo_name, max_commits=max_commits)

            return BatchResult(
                repository=repo,
                stats=stats,
                success=True,
            )

        except Exception as e:
            logger.warning(f"Failed to get stats for {repo.full_name}: {e}")
            return BatchResult(
                repository=repo,
                error=str(e),
                success=False,
            )

    def get_repos_with_stats(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_repo_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """
        Get repositories and their stats with batch processing and rate limiting.

        This method retrieves repositories from an organization or user,
        optionally filtering by pattern, and collects statistics for each
        repository with configurable batch processing and rate limiting.

        :param org_name: Optional organization name to fetch repos from.
        :param user_name: Optional user name to fetch repos from.
        :param pattern: Optional fnmatch-style pattern to filter repos (e.g., 'chrisgeo/m*').
        :param batch_size: Number of repos to process in each batch.
        :param max_concurrent: Maximum number of concurrent workers for processing.
        :param rate_limit_delay: Delay in seconds between batches for rate limiting.
        :param max_commits_per_repo: Maximum commits to analyze per repository.
        :param max_repos: Maximum number of repositories to process.
        :param on_repo_complete: Optional callback function called after each repo is processed.
        :return: List of BatchResult objects with repository and stats.

        Example:
            >>> results = connector.get_repos_with_stats(
            ...     org_name='myorg',
            ...     pattern='myorg/api-*',
            ...     batch_size=5,
            ...     max_concurrent=2,
            ...     rate_limit_delay=2.0,
            ... )
            >>> for result in results:
            ...     if result.success:
            ...         print(f"{result.repository.full_name}: {result.stats}")
        """
        # Step 1: Get repositories
        repos = self._get_repositories_for_processing(
            org_name=org_name,
            user_name=user_name,
            pattern=pattern,
            max_repos=max_repos,
        )

        logger.info(
            f"Processing {len(repos)} repositories with batch_size={batch_size}"
        )

        results = []

        # Step 2: Process in batches
        for batch_start in range(0, len(repos), batch_size):
            batch_end = min(batch_start + batch_size, len(repos))
            batch = repos[batch_start:batch_end]

            logger.info(
                f"Processing batch {batch_start // batch_size + 1}: "
                f"repos {batch_start + 1}-{batch_end} of {len(repos)}"
            )

            # Process batch concurrently using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = [
                    executor.submit(
                        self._process_single_repo_stats,
                        repo,
                        max_commits_per_repo,
                    )
                    for repo in batch
                ]

                for future in futures:
                    try:
                        result = future.result()
                        results.append(result)

                        # Call callback if provided
                        if on_repo_complete:
                            on_repo_complete(result)

                    except Exception as e:
                        logger.error(f"Unexpected error in batch processing: {e}")

            # Rate limiting delay between batches
            if batch_end < len(repos) and rate_limit_delay > 0:
                logger.debug(f"Rate limiting: waiting {rate_limit_delay}s")
                time.sleep(rate_limit_delay)

        logger.info(
            f"Completed processing {len(results)} repositories, "
            f"{sum(1 for r in results if r.success)} successful"
        )
        return results

    async def get_repos_with_stats_async(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_repo_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """
        Async version of get_repos_with_stats for better concurrent processing.

        This method retrieves repositories from an organization or user,
        optionally filtering by pattern, and collects statistics for each
        repository with configurable async batch processing and rate limiting.

        :param org_name: Optional organization name to fetch repos from.
        :param user_name: Optional user name to fetch repos from.
        :param pattern: Optional fnmatch-style pattern to filter repos (e.g., 'chrisgeo/m*').
        :param batch_size: Number of repos to process in each batch.
        :param max_concurrent: Maximum number of concurrent workers for processing.
        :param rate_limit_delay: Delay in seconds between batches for rate limiting.
        :param max_commits_per_repo: Maximum commits to analyze per repository.
        :param max_repos: Maximum number of repositories to process.
        :param on_repo_complete: Optional callback function called after each repo is processed.
        :return: List of BatchResult objects with repository and stats.

        Example:
            >>> import asyncio
            >>> async def main():
            ...     results = await connector.get_repos_with_stats_async(
            ...         org_name='myorg',
            ...         pattern='myorg/api-*',
            ...         batch_size=5,
            ...         max_concurrent=2,
            ...     )
            ...     for result in results:
            ...         if result.success:
            ...             print(f"{result.repository.full_name}: {result.stats}")
            >>> asyncio.run(main())
        """
        # Step 1: Get repositories (using sync method via run_in_executor)
        loop = asyncio.get_running_loop()

        repos = await loop.run_in_executor(
            None,
            lambda: self._get_repositories_for_processing(
                org_name=org_name,
                user_name=user_name,
                pattern=pattern,
                max_repos=max_repos,
            ),
        )

        logger.info(f"Processing {len(repos)} repositories asynchronously")

        results = []
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_repo_async(repo: Repository) -> BatchResult:
            """Process a single repository asynchronously."""
            async with semaphore:
                result = await loop.run_in_executor(
                    None,
                    lambda: self._process_single_repo_stats(repo, max_commits_per_repo),
                )

                if on_repo_complete:
                    on_repo_complete(result)

                return result

        # Step 2: Process in batches with rate limiting
        for batch_start in range(0, len(repos), batch_size):
            batch_end = min(batch_start + batch_size, len(repos))
            batch = repos[batch_start:batch_end]

            logger.info(
                f"Processing async batch {batch_start // batch_size + 1}: "
                f"repos {batch_start + 1}-{batch_end} of {len(repos)}"
            )

            # Process batch concurrently and consume results as they complete.
            tasks = [asyncio.create_task(process_repo_async(repo)) for repo in batch]
            for fut in asyncio.as_completed(tasks):
                try:
                    result = await fut
                except Exception as e:
                    logger.error(f"Unexpected error in async batch processing: {e}")
                    continue

                results.append(result)

            # Rate limiting delay between batches
            if batch_end < len(repos) and rate_limit_delay > 0:
                logger.debug(
                    f"Rate limiting: waiting {rate_limit_delay}s before next batch"
                )
                await asyncio.sleep(rate_limit_delay)

        logger.info(
            f"Completed async processing {len(results)} repositories, "
            f"{sum(1 for r in results if r.success)} successful"
        )
        return results

    def close(self) -> None:
        """Close the connector and cleanup resources."""
        if hasattr(self.github, "close"):
            self.github.close()
