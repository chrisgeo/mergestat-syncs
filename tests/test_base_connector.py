"""
Tests for the base connector class.
"""

from unittest.mock import Mock, patch

import pytest

from connectors import GitConnector
from connectors.base import BatchResult
from connectors.models import Repository, RepoStats, Author


class TestBatchResult:
    """Tests for the BatchResult dataclass."""

    def test_successful_result(self):
        """Test creating a successful batch result."""
        repo = Repository(
            id=1,
            name="test-repo",
            full_name="owner/test-repo",
            default_branch="main",
        )
        stats = RepoStats(
            total_commits=10,
            additions=100,
            deletions=50,
            commits_per_week=2.0,
            authors=[],
        )
        result = BatchResult(repository=repo, stats=stats, success=True)

        assert result.success is True
        assert result.repository == repo
        assert result.stats == stats
        assert result.error is None

    def test_failed_result(self):
        """Test creating a failed batch result."""
        repo = Repository(
            id=1,
            name="test-repo",
            full_name="owner/test-repo",
            default_branch="main",
        )
        result = BatchResult(
            repository=repo,
            error="API error",
            success=False,
        )

        assert result.success is False
        assert result.repository == repo
        assert result.stats is None
        assert result.error == "API error"


class TestGitConnectorInterface:
    """Tests for the GitConnector abstract base class."""

    def test_cannot_instantiate_base_class(self):
        """Test that the base class cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            GitConnector()

    def test_concrete_class_must_implement_abstract_methods(self):
        """Test that concrete class must implement all abstract methods."""

        class IncompleteConnector(GitConnector):
            """A connector that doesn't implement all abstract methods."""
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteConnector()

    def test_concrete_class_with_all_methods(self):
        """Test that a concrete class can be instantiated when all methods are implemented."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                pass

        connector = ConcreteConnector()
        assert connector.per_page == 100
        assert connector.max_workers == 4

    def test_custom_per_page_and_max_workers(self):
        """Test that custom per_page and max_workers can be set."""

        class ConcreteConnector(GitConnector):
            """A complete connector implementation."""

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                pass

        connector = ConcreteConnector(per_page=50, max_workers=8)
        assert connector.per_page == 50
        assert connector.max_workers == 8


class TestGitConnectorDefaultBatchProcessing:
    """Tests for the default batch processing implementation."""

    def test_get_repos_with_stats_default_implementation(self):
        """Test the default get_repos_with_stats implementation."""

        class TestConnector(GitConnector):
            """Test connector implementation."""

            def __init__(self):
                super().__init__()
                self.repos = [
                    Repository(id=1, name="repo1", full_name="owner/repo1", default_branch="main"),
                    Repository(id=2, name="repo2", full_name="owner/repo2", default_branch="main"),
                ]

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return self.repos

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return RepoStats(total_commits=10, additions=100, deletions=50, commits_per_week=2.0, authors=[])

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                pass

        connector = TestConnector()
        results = connector.get_repos_with_stats()

        assert len(results) == 2
        assert all(r.success for r in results)
        assert all(r.stats.total_commits == 10 for r in results)

    def test_get_repos_with_stats_handles_errors(self):
        """Test that get_repos_with_stats handles errors gracefully."""

        class TestConnector(GitConnector):
            """Test connector that raises errors."""

            def __init__(self):
                super().__init__()
                self.repos = [
                    Repository(id=1, name="repo1", full_name="owner/repo1", default_branch="main"),
                ]

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return self.repos

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                raise Exception("API error")

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                pass

        connector = TestConnector()
        results = connector.get_repos_with_stats()

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].error == "API error"

    def test_get_repos_with_stats_callback(self):
        """Test that the callback is called for each repository."""

        class TestConnector(GitConnector):
            """Test connector implementation."""

            def __init__(self):
                super().__init__()
                self.repos = [
                    Repository(id=1, name="repo1", full_name="owner/repo1", default_branch="main"),
                    Repository(id=2, name="repo2", full_name="owner/repo2", default_branch="main"),
                ]

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return self.repos

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return RepoStats(total_commits=10, additions=100, deletions=50, commits_per_week=2.0, authors=[])

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                pass

        connector = TestConnector()
        callback_results = []

        def callback(result):
            callback_results.append(result)

        results = connector.get_repos_with_stats(on_repo_complete=callback)

        assert len(results) == 2
        assert len(callback_results) == 2


class TestGitConnectorContextManager:
    """Tests for the context manager protocol."""

    def test_context_manager(self):
        """Test that the connector can be used as a context manager."""

        class TestConnector(GitConnector):
            """Test connector implementation."""

            def __init__(self):
                super().__init__()
                self.closed = False

            def list_organizations(self, max_orgs=None):
                return []

            def list_repositories(self, org_name=None, user_name=None, search=None, pattern=None, max_repos=None):
                return []

            def get_contributors(self, owner, repo, max_contributors=None):
                return []

            def get_commit_stats(self, owner, repo, sha):
                return None

            def get_repo_stats(self, owner, repo, max_commits=None):
                return None

            def get_pull_requests(self, owner, repo, state="all", max_prs=None):
                return []

            def get_file_blame(self, owner, repo, path, ref="HEAD"):
                return None

            def close(self):
                self.closed = True

        with TestConnector() as connector:
            assert connector.closed is False

        assert connector.closed is True
