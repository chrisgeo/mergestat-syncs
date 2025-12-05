"""Tests for models.git timezone-aware datetime functionality."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from models.git import Repo, GitRef, GitFile, GitCommit, GitCommitStat, GitBlame


class TestTimezoneAwareDatetimes:
    """Test that all datetime defaults are timezone-aware."""

    def test_repo_created_at_is_timezone_aware(self):
        """Test that Repo.created_at default is timezone-aware."""
        repo = Repo()
        # Get the default value by calling the lambda with a mock context
        default_func = repo.__table__.columns["created_at"].default.arg
        created_at = default_func(MagicMock())

        assert created_at.tzinfo is not None
        assert created_at.tzinfo == timezone.utc

    def test_git_ref_synced_at_is_timezone_aware(self):
        """Test that GitRef._mergestat_synced_at default is timezone-aware."""
        git_ref = GitRef()
        default_func = git_ref.__table__.columns["_mergestat_synced_at"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_file_synced_at_is_timezone_aware(self):
        """Test that GitFile._mergestat_synced_at default is timezone-aware."""
        git_file = GitFile()
        default_func = git_file.__table__.columns["_mergestat_synced_at"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_commit_synced_at_is_timezone_aware(self):
        """Test that GitCommit._mergestat_synced_at default is timezone-aware."""
        git_commit = GitCommit()
        default_func = git_commit.__table__.columns["_mergestat_synced_at"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_commit_stat_synced_at_is_timezone_aware(self):
        """Test that GitCommitStat._mergestat_synced_at default is timezone-aware."""
        git_commit_stat = GitCommitStat()
        default_func = git_commit_stat.__table__.columns[
            "_mergestat_synced_at"
        ].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc

    def test_git_blame_synced_at_is_timezone_aware(self):
        """Test that GitBlame._mergestat_synced_at default is timezone-aware."""
        git_blame = GitBlame()
        default_func = git_blame.__table__.columns["_mergestat_synced_at"].default.arg
        synced_at = default_func(MagicMock())

        assert synced_at.tzinfo is not None
        assert synced_at.tzinfo == timezone.utc


class TestDatetimeDefaultsReturnCurrentTime:
    """Test that datetime defaults return current time, not a fixed time."""

    def test_multiple_repo_creations_have_different_timestamps(self):
        """Test that creating multiple Repo instances generates different timestamps."""
        with patch("models.git.datetime") as mock_datetime:
            # Mock two different times
            time1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            time2 = datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
            mock_datetime.now.side_effect = [time1, time2]

            default_func = Repo.__table__.columns["created_at"].default.arg

            timestamp1 = default_func(MagicMock())
            timestamp2 = default_func(MagicMock())

            # Timestamps should be different because datetime.now was called twice
            assert timestamp1 != timestamp2
            assert mock_datetime.now.call_count == 2

    def test_datetime_now_called_with_timezone_utc(self):
        """Test that datetime.now is called with timezone.utc argument."""
        with patch("models.git.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc
            )

            default_func = Repo.__table__.columns["created_at"].default.arg
            default_func(MagicMock())

            # Verify datetime.now was called with timezone.utc
            mock_datetime.now.assert_called_once_with(timezone.utc)


class TestBackwardCompatibility:
    """Test that the changes maintain backward compatibility."""

    def test_all_models_have_datetime_columns_with_timezone(self):
        """Test that all datetime columns are configured with timezone=True."""
        models_and_columns = [
            (Repo, "created_at"),
            (GitRef, "_mergestat_synced_at"),
            (GitFile, "_mergestat_synced_at"),
            (GitCommit, "_mergestat_synced_at"),
            (GitCommitStat, "_mergestat_synced_at"),
            (GitBlame, "_mergestat_synced_at"),
        ]

        for model, column_name in models_and_columns:
            column = model.__table__.columns[column_name]
            # Check that the column type has timezone=True
            assert (
                column.type.timezone is True
            ), f"{model.__name__}.{column_name} should have timezone=True"

    def test_datetime_defaults_are_callable(self):
        """Test that all datetime defaults are callable (lambdas)."""
        models_and_columns = [
            (Repo, "created_at"),
            (GitRef, "_mergestat_synced_at"),
            (GitFile, "_mergestat_synced_at"),
            (GitCommit, "_mergestat_synced_at"),
            (GitCommitStat, "_mergestat_synced_at"),
            (GitBlame, "_mergestat_synced_at"),
        ]

        for model, column_name in models_and_columns:
            column = model.__table__.columns[column_name]
            assert (
                column.default is not None
            ), f"{model.__name__}.{column_name} should have a default"
            assert callable(
                column.default.arg
            ), f"{model.__name__}.{column_name} default should be callable"
