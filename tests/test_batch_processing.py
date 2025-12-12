"""
Tests for batch repository processing features.
"""

from unittest.mock import Mock, patch

import pytest

from connectors import GitHubConnector, BatchResult, match_repo_pattern
from connectors.github import BatchProcessingConfig


class TestPatternMatching:
    """Test repository pattern matching functionality."""

    def test_exact_match(self):
        """Test exact repository name matching."""
        assert match_repo_pattern("chrisgeo/mergestat-syncs", "chrisgeo/mergestat-syncs")

    def test_wildcard_suffix(self):
        """Test pattern with wildcard suffix."""
        assert match_repo_pattern("chrisgeo/mergestat-syncs", "chrisgeo/merge*")
        assert match_repo_pattern("chrisgeo/mergestat", "chrisgeo/merge*")
        assert not match_repo_pattern("chrisgeo/other-repo", "chrisgeo/merge*")

    def test_wildcard_prefix(self):
        """Test pattern with wildcard prefix."""
        assert match_repo_pattern("chrisgeo/api-service", "*-service")
        assert match_repo_pattern("org/web-service", "*-service")

    def test_wildcard_owner(self):
        """Test pattern with wildcard owner."""
        assert match_repo_pattern("chrisgeo/sync-tool", "*/sync-tool")
        assert match_repo_pattern("otherorg/sync-tool", "*/sync-tool")

    def test_wildcard_repo(self):
        """Test pattern with wildcard repo."""
        assert match_repo_pattern("chrisgeo/anything", "chrisgeo/*")
        assert match_repo_pattern("chrisgeo/another", "chrisgeo/*")

    def test_case_insensitive(self):
        """Test case insensitive matching."""
        assert match_repo_pattern("ChrisGeo/MergeStat-Syncs", "chrisgeo/mergestat*")
        assert match_repo_pattern("CHRISGEO/REPO", "chrisgeo/*")

    def test_question_mark_wildcard(self):
        """Test question mark wildcard matching single character."""
        assert match_repo_pattern("chrisgeo/api-v1", "chrisgeo/api-v?")
        assert match_repo_pattern("chrisgeo/api-v2", "chrisgeo/api-v?")
        assert not match_repo_pattern("chrisgeo/api-v10", "chrisgeo/api-v?")

    def test_double_wildcard(self):
        """Test double wildcard matching."""
        assert match_repo_pattern("org/sub-api-service", "*api*")
        assert match_repo_pattern("chrisgeo/my-api", "*api*")

    def test_no_match(self):
        """Test non-matching patterns."""
        assert not match_repo_pattern("chrisgeo/repo", "other/*")
        assert not match_repo_pattern("org/api", "org/web*")


class TestBatchResult:
    """Test BatchResult dataclass."""

    def test_successful_result(self):
        """Test creating a successful batch result."""
        repo = Mock()
        repo.full_name = "org/repo"
        stats = Mock()

        result = BatchResult(repository=repo, stats=stats, success=True)

        assert result.repository == repo
        assert result.stats == stats
        assert result.success is True
        assert result.error is None

    def test_failed_result(self):
        """Test creating a failed batch result."""
        repo = Mock()
        repo.full_name = "org/repo"

        result = BatchResult(
            repository=repo,
            error="API error",
            success=False,
        )

        assert result.repository == repo
        assert result.stats is None
        assert result.success is False
        assert result.error == "API error"


class TestBatchProcessingConfig:
    """Test BatchProcessingConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = BatchProcessingConfig()

        assert config.batch_size == 10
        assert config.max_concurrent == 4
        assert config.rate_limit_delay == 1.0
        assert config.max_retries == 3
        assert config.pattern is None
        assert config.max_commits_per_repo is None
        assert config.include_stats is True
        assert config.on_repo_complete is None

    def test_custom_values(self):
        """Test custom configuration values."""
        callback = Mock()
        config = BatchProcessingConfig(
            batch_size=5,
            max_concurrent=2,
            rate_limit_delay=2.0,
            max_retries=5,
            pattern="org/*",
            max_commits_per_repo=100,
            include_stats=False,
            on_repo_complete=callback,
        )

        assert config.batch_size == 5
        assert config.max_concurrent == 2
        assert config.rate_limit_delay == 2.0
        assert config.max_retries == 5
        assert config.pattern == "org/*"
        assert config.max_commits_per_repo == 100
        assert config.include_stats is False
        assert config.on_repo_complete == callback


class TestGitHubConnectorBatchProcessing:
    """Test GitHub connector batch processing features."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def _create_mock_repo(self, name: str, full_name: str):
        """Create a mock repository."""
        mock_repo = Mock()
        mock_repo.id = hash(full_name)
        mock_repo.name = name
        mock_repo.full_name = full_name
        mock_repo.default_branch = "main"
        mock_repo.description = f"Test repository {name}"
        mock_repo.html_url = f"https://github.com/{full_name}"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5
        return mock_repo

    def test_list_repositories_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test listing repositories with pattern matching."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("mergestat-syncs", "chrisgeo/mergestat-syncs"),
            self._create_mock_repo("mergestat-lite", "chrisgeo/mergestat-lite"),
            self._create_mock_repo("other-repo", "chrisgeo/other-repo"),
            self._create_mock_repo("api-service", "chrisgeo/api-service"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test pattern matching
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories_with_pattern(
            pattern="chrisgeo/merge*",
            user_name="chrisgeo",
        )

        # Should only return repos matching the pattern
        assert len(repos) == 2
        assert all("merge" in repo.full_name.lower() for repo in repos)

    def test_list_repositories_with_pattern_max_repos(
        self, mock_github_client, mock_graphql_client
    ):
        """Test list_repositories_with_pattern respects max_repos."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo(f"mergestat-{i}", f"chrisgeo/mergestat-{i}")
            for i in range(10)
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Test with max_repos limit
        connector = GitHubConnector(token="test_token")
        repos = connector.list_repositories_with_pattern(
            pattern="chrisgeo/merge*",
            user_name="chrisgeo",
            max_repos=3,
        )

        assert len(repos) == 3

    def test_get_repos_with_stats_sync(
        self, mock_github_client, mock_graphql_client
    ):
        """Test synchronous batch processing of repositories with stats."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "chrisgeo/repo1"),
            self._create_mock_repo("repo2", "chrisgeo/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo (used by get_repo_stats)
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test batch processing
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            user_name="chrisgeo",
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,  # Small delay for testing
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    def test_get_repos_with_stats_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test batch processing with pattern filtering."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("api-v1", "org/api-v1"),
            self._create_mock_repo("api-v2", "org/api-v2"),
            self._create_mock_repo("web-app", "org/web-app"),
        ]

        mock_org = Mock()
        mock_org.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_organization.return_value = mock_org

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test with pattern
        connector = GitHubConnector(token="test_token")
        results = connector.get_repos_with_stats(
            org_name="org",
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process repos matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)

    def test_get_repos_with_stats_callback(
        self, mock_github_client, mock_graphql_client
    ):
        """Test callback is called for each processed repository."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test callback
        callback = Mock()
        connector = GitHubConnector(token="test_token")
        _ = connector.get_repos_with_stats(
            user_name="user",
            on_repo_complete=callback,
            rate_limit_delay=0.1,
        )

        # Callback should be called for each repo
        assert callback.call_count == 2


class TestGitHubConnectorAsyncBatchProcessing:
    """Test GitHub connector async batch processing features."""

    @pytest.fixture
    def mock_github_client(self):
        """Create a mock GitHub client."""
        with patch("connectors.github.Github") as mock_github:
            yield mock_github

    @pytest.fixture
    def mock_graphql_client(self):
        """Create a mock GraphQL client."""
        with patch("connectors.github.GitHubGraphQLClient") as mock_graphql:
            yield mock_graphql

    def _create_mock_repo(self, name: str, full_name: str):
        """Create a mock repository."""
        mock_repo = Mock()
        mock_repo.id = hash(full_name)
        mock_repo.name = name
        mock_repo.full_name = full_name
        mock_repo.default_branch = "main"
        mock_repo.description = f"Test repository {name}"
        mock_repo.html_url = f"https://github.com/{full_name}"
        mock_repo.created_at = None
        mock_repo.updated_at = None
        mock_repo.language = "Python"
        mock_repo.stargazers_count = 10
        mock_repo.forks_count = 5
        return mock_repo

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async batch processing of repositories with stats."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "chrisgeo/repo1"),
            self._create_mock_repo("repo2", "chrisgeo/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test async batch processing
        connector = GitHubConnector(token="test_token")
        results = await connector.get_repos_with_stats_async(
            user_name="chrisgeo",
            batch_size=2,
            max_concurrent=2,
            rate_limit_delay=0.1,
        )

        assert len(results) == 2
        assert all(isinstance(r, BatchResult) for r in results)

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async_with_pattern(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async batch processing with pattern filtering."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("api-v1", "org/api-v1"),
            self._create_mock_repo("api-v2", "org/api-v2"),
            self._create_mock_repo("web-app", "org/web-app"),
        ]

        mock_org = Mock()
        mock_org.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_organization.return_value = mock_org

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test with pattern
        connector = GitHubConnector(token="test_token")
        results = await connector.get_repos_with_stats_async(
            org_name="org",
            pattern="org/api-*",
            batch_size=5,
            rate_limit_delay=0.1,
        )

        # Should only process repos matching pattern
        assert len(results) == 2
        assert all("api-" in r.repository.full_name for r in results)

    @pytest.mark.asyncio
    async def test_get_repos_with_stats_async_callback(
        self, mock_github_client, mock_graphql_client
    ):
        """Test async callback is called for each processed repository."""
        # Setup mock repos
        mock_repos = [
            self._create_mock_repo("repo1", "user/repo1"),
            self._create_mock_repo("repo2", "user/repo2"),
        ]

        mock_user = Mock()
        mock_user.get_repos.return_value = mock_repos

        mock_github_instance = mock_github_client.return_value
        mock_github_instance.get_user.return_value = mock_user

        # Setup mock for get_repo
        mock_gh_repo = Mock()
        mock_gh_repo.get_commits.return_value = []
        mock_gh_repo.created_at = None
        mock_github_instance.get_repo.return_value = mock_gh_repo

        # Test callback
        callback = Mock()
        connector = GitHubConnector(token="test_token")
        _ = await connector.get_repos_with_stats_async(
            user_name="user",
            on_repo_complete=callback,
            rate_limit_delay=0.1,
        )

        # Callback should be called for each repo
        assert callback.call_count == 2
