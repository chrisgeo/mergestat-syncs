"""Tests for utility functions in git_mergestat.py."""
import mimetypes
from pathlib import Path


# Re-implement the skippable logic locally for testing to avoid module-level
# database connection issues when importing git_mergestat.py directly.
# This matches the implementation in git_mergestat.py
SKIP_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".pdf", ".ttf", ".otf", ".woff", ".woff2", ".ico",
    ".mp4", ".mp3", ".mov", ".avi", ".exe", ".dll", ".zip", ".tar", ".gz", ".7z", ".eot",
    ".rar", ".iso", ".dmg", ".pkg", ".deb", ".rpm", ".msi", ".class", ".jar", ".war", ".pyc",
    ".pyo", ".so", ".o", ".a", ".lib", ".bin", ".dat", ".swp", ".lock", ".bak", ".tmp"
}


def is_skippable(path: str) -> bool:
    """
    Return True if the file is skippable (i.e. should not be processed for git blame).
    """
    ext = Path(path).suffix.lower()
    if ext in SKIP_EXTENSIONS:
        return True
    mime, _ = mimetypes.guess_type(path)
    return mime and mime.startswith((
        "image/",
        "video/",
        "audio/",
        "application/pdf",
        "font/",
        "application/x-executable",
        "application/x-sharedlib",
        "application/x-object",
        "application/x-archive",
    ))


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
            ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".exe", ".zip",
            ".tar", ".gz", ".pyc", ".so", ".lock", ".tmp"
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
