"""
GitHub connector using PyGithub and GraphQL.

This connector provides methods to retrieve organizations, repositories,
contributors, statistics, pull requests, and blame information from GitHub.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from github import Github, GithubException, RateLimitExceededException

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
    RepoStats,
    Repository,
)
from connectors.utils import GitHubGraphQLClient, retry_with_backoff

logger = logging.getLogger(__name__)


class GitHubConnector:
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
        self.token = token
        self.per_page = per_page
        self.max_workers = max_workers

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
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """
        List repositories for an organization, user, or search query.

        :param org_name: Optional organization name. If provided, lists organization repos.
        :param user_name: Optional user name. If provided, lists that user's repos.
        :param search: Optional search query to filter repositories.
                      If provided with org_name/user_name, searches within that scope.
                      If provided alone, performs global search.
        :param max_repos: Maximum number of repositories to retrieve. If None, retrieves all.
        :return: List of Repository objects.
        """
        try:
            repos = []

            # Handle search-based repository listing
            if search and not org_name and not user_name:
                # Global search across all GitHub
                gh_repos = self.github.search_repositories(query=search)
            elif org_name:
                # Organization repositories
                source = self.github.get_organization(org_name)
                gh_repos = source.get_repos()
            elif user_name:
                # Specific user repositories
                source = self.github.get_user(user_name)
                gh_repos = source.get_repos()
            else:
                # Authenticated user's repositories
                user = self.github.get_user()
                gh_repos = user.get_repos()

            for gh_repo in gh_repos:
                if max_repos and len(repos) >= max_repos:
                    break

                # Apply search filter if specified and not using global search
                if search and (org_name or user_name or (not org_name and not user_name)):
                    # For scoped searches, filter by name/description containing search term
                    search_lower = search.lower()
                    name_match = (
                        gh_repo.name and search_lower in gh_repo.name.lower()
                    )
                    desc_match = (
                        gh_repo.description and
                        search_lower in gh_repo.description.lower()
                    )
                    full_name_match = (
                        gh_repo.full_name and
                        search_lower in gh_repo.full_name.lower()
                    )
                    if not (name_match or desc_match or full_name_match):
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

            logger.info(f"Retrieved {len(repos)} repositories")
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

    def close(self) -> None:
        """Close the connector and cleanup resources."""
        if hasattr(self.github, "close"):
            self.github.close()
