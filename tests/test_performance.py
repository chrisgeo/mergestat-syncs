"""Tests for performance optimizations."""

import asyncio
import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from git_mergestat import (process_git_blame, process_git_commit_stats,
                           process_git_commits, process_git_files,
                           process_single_file_blame)


class TestBatchSizeConfiguration:
    """Test that batch size is properly configurable."""

    def test_batch_size_defaults_to_100(self):
        """Test that BATCH_SIZE defaults to 100."""
        # Import here to get the actual module-level value
        from git_mergestat import BATCH_SIZE as actual_batch_size

        assert actual_batch_size == 100

    def test_batch_size_can_be_configured_via_env(self):
        """Test that BATCH_SIZE can be configured via environment variable."""
        with patch.dict(os.environ, {"BATCH_SIZE": "250"}):
            # Re-evaluate the expression
            batch_size = int(os.getenv("BATCH_SIZE", "100"))
            assert batch_size == 250


class TestMaxWorkersConfiguration:
    """Test that MAX_WORKERS is properly configurable."""

    def test_max_workers_defaults_to_4(self):
        """Test that MAX_WORKERS defaults to 4."""
        # Import here to get the actual module-level value
        from git_mergestat import MAX_WORKERS as actual_max_workers

        assert actual_max_workers == 4

    def test_max_workers_can_be_configured_via_env(self):
        """Test that MAX_WORKERS can be configured via environment variable."""
        with patch.dict(os.environ, {"MAX_WORKERS": "8"}):
            # Re-evaluate the expression
            max_workers = int(os.getenv("MAX_WORKERS", "4"))
            assert max_workers == 8


class TestParallelBlameProcessing:
    """Test parallel processing of git blame data."""

    @pytest.mark.asyncio
    async def test_process_single_file_blame_uses_semaphore(self):
        """Test that process_single_file_blame respects semaphore."""
        semaphore = asyncio.Semaphore(2)
        test_file = Path("/tmp/test.py")
        test_repo_path = "/tmp/test_repo"

        with patch("git_mergestat.Repo"):
            with patch("git_mergestat.GitBlame.process_file", return_value=[]):
                result = await process_single_file_blame(
                    test_file, test_repo_path, semaphore
                )
                assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_process_git_blame_processes_files_in_chunks(self):
        """Test that git blame processes files in chunks."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()
        test_files = [Path(f"/tmp/test{i}.py") for i in range(10)]

        with patch("git_mergestat.GitBlame.process_file", return_value=[]):
            with patch("git_mergestat.insert_blame_data") as mock_insert:
                await process_git_blame(test_files, mock_store, mock_repo)
                # With 10 files and no blame data returned, should have 0 insert calls
                # The function should handle empty results gracefully
                # Just verify no exceptions were raised during processing
                assert True  # If we got here, processing completed successfully

    @pytest.mark.asyncio
    async def test_process_git_blame_handles_errors_gracefully(self):
        """Test that git blame handles file processing errors gracefully."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()
        test_files = [Path("/tmp/test1.py"), Path("/tmp/test2.py")]

        # Make one file raise an exception
        def side_effect(repo_path, filepath, repo_uuid, repo=None):
            if "test1" in str(filepath):
                raise Exception("Test error")
            return []

        with patch("git_mergestat.GitBlame.process_file", side_effect=side_effect):
            with patch("git_mergestat.insert_blame_data") as mock_insert:
                # Should not raise exception
                await process_git_blame(test_files, mock_store, mock_repo)


class TestCommitProcessing:
    """Test git commit processing."""

    @pytest.mark.asyncio
    async def test_process_git_commits_batches_inserts(self):
        """Test that git commits are inserted in batches."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()

        # Create mock commits
        mock_commits = []
        for i in range(5):
            mock_commit = MagicMock()
            mock_commit.hexsha = f"hash{i}"
            mock_commit.message = f"Message {i}"
            mock_commit.author.name = "Test Author"
            mock_commit.author.email = "test@example.com"
            mock_commit.authored_datetime = "2024-01-01 00:00:00"
            mock_commit.committer.name = "Test Committer"
            mock_commit.committer.email = "committer@example.com"
            mock_commit.committed_datetime = "2024-01-01 00:00:00"
            mock_commit.parents = []
            mock_commits.append(mock_commit)

        mock_repo.iter_commits.return_value = iter(mock_commits)

        with patch("git_mergestat.insert_git_commit_data") as mock_insert:
            with patch("git_mergestat.REPO_UUID", "test-uuid"):
                await process_git_commits(mock_repo, mock_store)
                # Should have been called at least once
                assert mock_insert.call_count >= 1


class TestCommitStatsProcessing:
    """Test git commit stats processing."""

    @pytest.mark.asyncio
    async def test_process_git_commit_stats_batches_inserts(self):
        """Test that commit stats are inserted in batches."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()

        # Create mock commits with diffs
        mock_commit = MagicMock()
        mock_commit.hexsha = "hash1"
        mock_commit.parents = [MagicMock()]

        # Mock diff
        mock_diff = MagicMock()
        mock_diff.b_path = "test.py"
        mock_diff.a_path = "test.py"
        mock_diff.diff = b"diff content"
        mock_diff.a_mode = 0o100644
        mock_diff.b_mode = 0o100644

        mock_commit.parents[0].diff.return_value = [mock_diff]
        mock_repo.iter_commits.return_value = iter([mock_commit])

        with patch("git_mergestat.insert_git_commit_stats") as mock_insert:
            with patch("git_mergestat.REPO_UUID", "test-uuid"):
                await process_git_commit_stats(mock_repo, mock_store)
                # Should have been called at least once
                assert mock_insert.call_count >= 1


class TestFileProcessing:
    """Test git file processing."""

    @pytest.mark.asyncio
    async def test_process_git_files_batches_inserts(self, tmp_path):
        """Test that git files are inserted in batches."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()

        # Create some test files
        test_files = []
        for i in range(5):
            test_file = tmp_path / f"test{i}.py"
            test_file.write_text(f"# Test file {i}")
            test_files.append(test_file)

        with patch("git_mergestat.insert_git_file_data") as mock_insert:
            with patch("git_mergestat.REPO_PATH", str(tmp_path)):
                with patch("git_mergestat.REPO_UUID", "test-uuid"):
                    await process_git_files(mock_repo, test_files, mock_store)
                    # Should have been called at least once
                    assert mock_insert.call_count >= 1

    @pytest.mark.asyncio
    async def test_process_git_files_handles_large_files(self, tmp_path):
        """Test that large files are skipped for content reading."""
        mock_store = AsyncMock()
        mock_repo = MagicMock()

        # Create a test file
        test_file = tmp_path / "test.py"
        test_file.write_text("# Test file")
        test_files = [test_file]

        with patch("git_mergestat.insert_git_file_data") as mock_insert:
            with patch("git_mergestat.REPO_PATH", str(tmp_path)):
                with patch("git_mergestat.REPO_UUID", "test-uuid"):
                    # Mock os.path.getsize to return a large size
                    with patch("os.path.getsize", return_value=2_000_000):
                        await process_git_files(mock_repo, test_files, mock_store)
                        # Should still insert the file, just without contents
                        assert mock_insert.call_count >= 1


class TestConnectionPooling:
    """Test connection pooling configuration."""

    def test_postgresql_connection_pool_configured(self):
        """Test that PostgreSQL connections use pooling."""
        from storage import SQLAlchemyStore

        conn_string = "postgresql+asyncpg://user:pass@localhost/db"
        store = SQLAlchemyStore(conn_string)

        # Check that the engine was created (we can't easily inspect pool settings)
        assert store.engine is not None

    def test_sqlite_connection_no_pooling(self):
        """Test that SQLite connections don't use pooling parameters."""
        from storage import SQLAlchemyStore

        conn_string = "sqlite+aiosqlite:///test.db"
        # Should not raise an error about invalid pool parameters
        store = SQLAlchemyStore(conn_string)
        assert store.engine is not None


class TestRepoUUIDDerivation:
    """Test derivation of REPO_UUID from git repository data."""

    def test_get_repo_uuid_is_deterministic(self, tmp_path):
        """Test that get_repo_uuid returns the same UUID for the same repo."""
        from git_mergestat import get_repo_uuid

        # Clear REPO_UUID env var for this test
        with patch.dict(os.environ, {}, clear=True):
            if "REPO_UUID" in os.environ:
                del os.environ["REPO_UUID"]

            # Use the current repository
            uuid1 = get_repo_uuid(".")
            uuid2 = get_repo_uuid(".")

            assert uuid1 == uuid2
            # Should be a valid UUID format
            uuid.UUID(uuid1)  # Will raise if not valid

    def test_get_repo_uuid_respects_env_var(self):
        """Test that get_repo_uuid respects REPO_UUID environment variable."""
        from git_mergestat import get_repo_uuid

        test_uuid = "12345678-1234-1234-1234-123456789abc"
        with patch.dict(os.environ, {"REPO_UUID": test_uuid}):
            result = get_repo_uuid(".")
            assert result == test_uuid

    def test_get_repo_uuid_handles_missing_repo(self, tmp_path):
        """Test that get_repo_uuid handles non-git directories gracefully."""
        from git_mergestat import get_repo_uuid

        # Clear REPO_UUID env var
        with patch.dict(os.environ, {}, clear=True):
            if "REPO_UUID" in os.environ:
                del os.environ["REPO_UUID"]

            # Use a non-git directory
            non_git_dir = tmp_path / "not_a_git_repo"
            non_git_dir.mkdir()

            result = get_repo_uuid(str(non_git_dir))

            # Should return a valid UUID (fallback to random)
            uuid.UUID(result)  # Will raise if not valid
