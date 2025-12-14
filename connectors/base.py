"""
Base connector class for Git repository connectors.

This module provides an abstract base class that defines the common interface
for all Git connectors (GitHub, GitLab, local, etc.).
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional

from connectors.models import (
    Author,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    Repository,
    RepoStats,
)

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Result of a batch repository processing operation."""

    repository: Repository
    stats: Optional[RepoStats] = None
    error: Optional[str] = None
    success: bool = True


class GitConnector(ABC):
    """
    Abstract base class for Git repository connectors.

    This class defines the common interface that all Git connectors
    (GitHub, GitLab, local) must implement.
    """

    def __init__(
        self,
        per_page: int = 100,
        max_workers: int = 4,
    ):
        """
        Initialize the base connector.

        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        """
        self.per_page = per_page
        self.max_workers = max_workers

    @abstractmethod
    def list_organizations(
        self,
        max_orgs: Optional[int] = None,
    ) -> List[Organization]:
        """
        List organizations/groups accessible to the authenticated user.

        :param max_orgs: Maximum number of organizations to retrieve.
        :return: List of Organization objects.
        """
        pass

    @abstractmethod
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

        :param org_name: Optional organization name.
        :param user_name: Optional user name.
        :param search: Optional search query.
        :param pattern: Optional fnmatch-style pattern to filter repositories.
        :param max_repos: Maximum number of repositories to retrieve.
        :return: List of Repository objects.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        max_prs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get pull requests/merge requests for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param state: State filter ('open', 'closed', 'all').
        :param max_prs: Maximum number of pull requests to retrieve.
        :return: List of PullRequest objects.
        """
        pass

    @abstractmethod
    def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> FileBlame:
        """
        Get blame information for a file.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param path: File path within the repository.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        pass

    @abstractmethod
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
        Get repositories and their stats with batch processing.

        Each connector must implement its own filtering and processing logic.

        :param org_name: Optional organization name.
        :param user_name: Optional user name.
        :param pattern: Optional fnmatch-style pattern to filter repos.
        :param batch_size: Number of repos to process in each batch.
        :param max_concurrent: Maximum concurrent workers for processing.
        :param rate_limit_delay: Delay in seconds between batches.
        :param max_commits_per_repo: Maximum commits to analyze per repository.
        :param max_repos: Maximum number of repositories to process.
        :param on_repo_complete: Callback function called after each repo.
        :return: List of BatchResult objects.
        """
        pass

    @abstractmethod
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
        Async version of get_repos_with_stats.

        Each connector must implement its own async filtering and processing logic.

        :param org_name: Optional organization name.
        :param user_name: Optional user name.
        :param pattern: Optional fnmatch-style pattern to filter repos.
        :param batch_size: Number of repos to process in each batch.
        :param max_concurrent: Maximum concurrent workers for processing.
        :param rate_limit_delay: Delay in seconds between batches.
        :param max_commits_per_repo: Maximum commits to analyze per repository.
        :param max_repos: Maximum number of repositories to process.
        :param on_repo_complete: Callback function called after each repo.
        :return: List of BatchResult objects.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connector and cleanup resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
