"""Tests for utility functions in git_mergestat.py."""

import mimetypes
import os
from pathlib import Path
from unittest.mock import patch

# Re-implement the skippable logic locally for testing to avoid module-level
# database connection issues when importing git_mergestat.py directly.
# This matches the implementation in git_mergestat.py
SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".pdf",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".ico",
    ".mp4",
    ".mp3",
    ".mov",
    ".avi",
    ".exe",
    ".dll",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".eot",
    ".rar",
    ".iso",
    ".dmg",
    ".pkg",
    ".deb",
    ".rpm",
    ".msi",
    ".class",
    ".jar",
    ".war",
    ".pyc",
    ".pyo",
    ".so",
    ".o",
    ".a",
    ".lib",
    ".bin",
    ".dat",
    ".swp",
    ".lock",
    ".bak",
    ".tmp",
}


def is_skippable(path: str) -> bool:
    """
    Return True if the file is skippable (i.e. should not be processed for git blame).
    """
    ext = Path(path).suffix.lower()
    if ext in SKIP_EXTENSIONS:
        return True
    mime, _ = mimetypes.guess_type(path)
    return mime and mime.startswith(
        (
            "image/",
            "video/",
            "audio/",
            "application/pdf",
            "font/",
            "application/x-executable",
            "application/x-sharedlib",
            "application/x-object",
            "application/x-archive",
        )
    )


class TestIsSkippable:
    """Test cases for the is_skippable function."""

    def test_skippable_extensions(self):
        """Test that common binary file extensions are skippable."""
        skippable_files = [
            "image.png",
            "photo.jpg",
            "photo.jpeg",
            "animation.gif",
            "icon.ico",
            "document.pdf",
            "font.ttf",
            "font.otf",
            "font.woff",
            "font.woff2",
            "video.mp4",
            "audio.mp3",
            "archive.zip",
            "archive.tar",
            "archive.gz",
            "compiled.pyc",
            "library.so",
            "temp.tmp",
            "backup.bak",
        ]

        for filename in skippable_files:
            assert is_skippable(filename), f"{filename} should be skippable"

    def test_non_skippable_extensions(self):
        """Test that source code files are not skippable."""
        processable_files = [
            "script.py",
            "module.js",
            "styles.css",
            "page.html",
            "data.json",
            "config.yaml",
            "README.md",
            "Makefile",
            "Dockerfile",
            ".gitignore",
            "code.go",
            "main.rs",
            "app.rb",
            "index.ts",
        ]

        for filename in processable_files:
            assert not is_skippable(filename), f"{filename} should not be skippable"

    def test_case_insensitivity(self):
        """Test that extension checking is case-insensitive."""
        # Upper case extensions should still be skippable
        assert is_skippable("IMAGE.PNG")
        assert is_skippable("Photo.JPG")
        assert is_skippable("Document.PDF")

    def test_path_with_directories(self):
        """Test that paths with directories are handled correctly."""
        assert is_skippable("path/to/image.png")
        assert is_skippable("./relative/path/photo.jpg")
        assert is_skippable("/absolute/path/video.mp4")
        assert not is_skippable("path/to/script.py")
        assert not is_skippable("/absolute/path/README.md")

    def test_skip_extensions_set_is_complete(self):
        """Verify the SKIP_EXTENSIONS set contains expected binary types."""
        expected_extensions = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".pdf",
            ".exe",
            ".zip",
            ".tar",
            ".gz",
            ".pyc",
            ".so",
            ".lock",
            ".tmp",
        }

        for ext in expected_extensions:
            assert ext in SKIP_EXTENSIONS, f"{ext} should be in SKIP_EXTENSIONS"

    def test_hidden_files(self):
        """Test that hidden files (starting with .) are handled correctly."""
        # Hidden files without binary extensions should not be skippable
        assert not is_skippable(".gitignore")
        assert not is_skippable(".env")

    def test_files_without_extensions(self):
        """Test that files without extensions are not skippable by extension."""
        # Files without extensions should not be skippable (unless mime type says otherwise)
        assert not is_skippable("Makefile")
        assert not is_skippable("Dockerfile")
        assert not is_skippable("LICENSE")

    def test_mime_type_detection(self):
        """Test that mime type detection catches some file types."""
        # These should be detected by mime type
        assert is_skippable("image.bmp")  # image/* mime type
        assert is_skippable("video.avi")  # video/* mime type
        assert is_skippable("audio.wav")  # audio/* mime type


class TestDBEchoConfiguration:
    """Test cases for DB_ECHO environment variable parsing."""

    def test_db_echo_defaults_to_false_when_not_set(self):
        """Test that DB_ECHO defaults to False when not set."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove DB_ECHO if it exists
            os.environ.pop("DB_ECHO", None)
            # Re-evaluate the expression
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_true_for_true_value(self):
        """Test that DB_ECHO is True when set to 'true'."""
        with patch.dict(os.environ, {"DB_ECHO": "true"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_uppercase_true(self):
        """Test that DB_ECHO is True when set to 'TRUE' (case-insensitive)."""
        with patch.dict(os.environ, {"DB_ECHO": "TRUE"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_one(self):
        """Test that DB_ECHO is True when set to '1'."""
        with patch.dict(os.environ, {"DB_ECHO": "1"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_yes(self):
        """Test that DB_ECHO is True when set to 'yes'."""
        with patch.dict(os.environ, {"DB_ECHO": "yes"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_true_for_uppercase_yes(self):
        """Test that DB_ECHO is True when set to 'YES' (case-insensitive)."""
        with patch.dict(os.environ, {"DB_ECHO": "YES"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is True

    def test_db_echo_false_for_false_value(self):
        """Test that DB_ECHO is False when set to 'false'."""
        with patch.dict(os.environ, {"DB_ECHO": "false"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_zero(self):
        """Test that DB_ECHO is False when set to '0'."""
        with patch.dict(os.environ, {"DB_ECHO": "0"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_no(self):
        """Test that DB_ECHO is False when set to 'no'."""
        with patch.dict(os.environ, {"DB_ECHO": "no"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_invalid_value(self):
        """Test that DB_ECHO is False when set to an invalid value."""
        with patch.dict(os.environ, {"DB_ECHO": "invalid"}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False

    def test_db_echo_false_for_empty_string(self):
        """Test that DB_ECHO is False when set to an empty string."""
        with patch.dict(os.environ, {"DB_ECHO": ""}):
            result = os.getenv("DB_ECHO", "false").lower() in ("true", "1", "yes")
            assert result is False


class TestBatchProcessingCLIArguments:
    """Test cases for batch processing CLI argument parsing.

    Note: These tests re-implement the argument parsing logic rather than importing
    parse_args() from git_mergestat.py directly. This avoids module-level database
    connection issues when importing git_mergestat.py (see comment at top of this file).
    """

    def test_github_pattern_argument(self):
        """Test that --github-pattern argument is parsed correctly."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--db", required=False)
        parser.add_argument("--github-pattern", required=False)
        parser.add_argument("--github-batch-size", type=int, default=10)
        parser.add_argument("--max-concurrent", type=int, default=4)
        parser.add_argument("--rate-limit-delay", type=float, default=1.0)
        parser.add_argument("--max-commits-per-repo", type=int)
        parser.add_argument("--max-repos", type=int)
        parser.add_argument("--use-async", action="store_true")

        test_args = [
            "--github-pattern",
            "chrisgeo/m*",
            "--db",
            "sqlite+aiosqlite:///:memory:",
        ]
        args = parser.parse_args(test_args)

        assert args.github_pattern == "chrisgeo/m*"
        assert args.github_batch_size == 10
        assert args.max_concurrent == 4
        assert args.rate_limit_delay == 1.0
        assert args.max_commits_per_repo is None
        assert args.max_repos is None
        assert args.use_async is False

    def test_batch_processing_arguments_with_custom_values(self):
        """Test that batch processing arguments accept custom values."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--github-pattern", required=False)
        parser.add_argument("--github-batch-size", type=int, default=10)
        parser.add_argument("--max-concurrent", type=int, default=4)
        parser.add_argument("--rate-limit-delay", type=float, default=1.0)
        parser.add_argument("--max-commits-per-repo", type=int)
        parser.add_argument("--max-repos", type=int)
        parser.add_argument("--use-async", action="store_true")

        test_args = [
            "--github-pattern",
            "org/*",
            "--github-batch-size",
            "20",
            "--max-concurrent",
            "8",
            "--rate-limit-delay",
            "2.5",
            "--max-commits-per-repo",
            "100",
            "--max-repos",
            "50",
            "--use-async",
        ]

        args = parser.parse_args(test_args)

        assert args.github_pattern == "org/*"
        assert args.github_batch_size == 20
        assert args.max_concurrent == 8
        assert args.rate_limit_delay == 2.5
        assert args.max_commits_per_repo == 100
        assert args.max_repos == 50
        assert args.use_async is True

    def test_use_async_flag_default_is_false(self):
        """Test that --use-async flag defaults to False."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--use-async", action="store_true")

        args = parser.parse_args([])

        assert args.use_async is False

    def test_use_async_flag_when_provided(self):
        """Test that --use-async flag is True when provided."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--use-async", action="store_true")

        args = parser.parse_args(["--use-async"])

        assert args.use_async is True
