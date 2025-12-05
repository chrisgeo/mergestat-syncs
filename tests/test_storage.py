from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from models import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from storage import MongoStore, model_to_dict


def test_model_to_dict_serializes_uuid_and_fields(repo_uuid):
    blame = GitBlame(
        repo_id=repo_uuid,
        path="file.txt",
        line_no=1,
        author_email="author@example.com",
        author_name="Author",
        author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
        commit_hash="abc123",
        line="content",
    )

    doc = model_to_dict(blame)

    assert doc["repo_id"] == str(repo_uuid)
    assert doc["path"] == "file.txt"
    assert doc["line_no"] == 1
    assert doc["commit_hash"] == "abc123"


class TestMongoStore:
    """Test cases for MongoStore class."""

    @pytest.mark.asyncio
    async def test_init_with_valid_connection_string(self):
        """Test MongoStore initialization with valid connection string."""
        conn_string = "mongodb://localhost:27017"
        store = MongoStore(conn_string, db_name="test_db")
        
        assert store.db_name == "test_db"
        assert store.db is None  # db is None until __aenter__ is called
        store.client.close()

    def test_init_with_empty_connection_string(self):
        """Test MongoStore raises ValueError for empty connection string."""
        with pytest.raises(ValueError, match="MongoDB connection string is required"):
            MongoStore("")

    def test_init_with_none_connection_string(self):
        """Test MongoStore raises ValueError for None connection string."""
        with pytest.raises(ValueError, match="MongoDB connection string is required"):
            MongoStore(None)

    @pytest.mark.asyncio
    async def test_context_manager_with_explicit_db_name(self):
        """Test context manager sets db to specified database."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                assert store.db == mock_db
                mock_client.__getitem__.assert_called_once_with("test_db")
            
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_default_db(self):
        """Test context manager uses get_default_database when no db_name provided."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_default_db = MagicMock()
            mock_client.get_default_database.return_value = mock_default_db
            mock_client_class.return_value = mock_client
            
            async with MongoStore(conn_string) as store:
                assert store.db == mock_default_db
                mock_client.get_default_database.assert_called_once()
            
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_fallback_to_mergestat(self):
        """Test context manager falls back to 'mergestat' db when no default."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_mergestat_db = MagicMock()
            mock_client.get_default_database.return_value = None
            mock_client.__getitem__ = MagicMock(return_value=mock_mergestat_db)
            mock_client_class.return_value = mock_client
            
            async with MongoStore(conn_string) as store:
                assert store.db == mock_mergestat_db
                mock_client.__getitem__.assert_called_once_with("mergestat")
            
            mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_repo_upserts_document(self, repo_uuid):
        """Test insert_repo performs upsert operation."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.update_one = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            repo = Repo(id=repo_uuid, repo="https://github.com/test/repo")
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_repo(repo)
            
            # Verify update_one was called with correct parameters
            mock_collection.update_one.assert_called_once()
            call_args = mock_collection.update_one.call_args
            assert call_args[0][0] == {"_id": str(repo_uuid)}
            assert call_args[1]["upsert"] is True

    @pytest.mark.asyncio
    async def test_insert_git_file_data_empty_list(self):
        """Test insert_git_file_data handles empty list gracefully."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                # Should not raise an error
                await store.insert_git_file_data([])

    @pytest.mark.asyncio
    async def test_insert_git_file_data_batch_upsert(self, repo_uuid):
        """Test insert_git_file_data performs batch upsert."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            file1 = GitFile(
                repo_id=repo_uuid,
                path="file1.txt",
                executable=False,
                contents="content1"
            )
            file2 = GitFile(
                repo_id=repo_uuid,
                path="file2.txt",
                executable=True,
                contents="content2"
            )
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_git_file_data([file1, file2])
            
            # Verify bulk_write was called
            mock_collection.bulk_write.assert_called_once()
            call_args = mock_collection.bulk_write.call_args
            operations = call_args[0][0]
            assert len(operations) == 2
            assert call_args[1]["ordered"] is False

    @pytest.mark.asyncio
    async def test_insert_git_commit_data_batch_upsert(self, repo_uuid):
        """Test insert_git_commit_data performs batch upsert."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            commit = GitCommit(
                repo_id=repo_uuid,
                hash="abc123",
                message="Test commit",
                author_name="Author",
                author_email="author@example.com",
                author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                committer_name="Committer",
                committer_email="committer@example.com",
                committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                parents=1
            )
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_git_commit_data([commit])
            
            mock_collection.bulk_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_git_commit_stats_batch_upsert(self, repo_uuid):
        """Test insert_git_commit_stats performs batch upsert."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            stat = GitCommitStat(
                repo_id=repo_uuid,
                commit_hash="abc123",
                file_path="file.txt",
                additions=10,
                deletions=5,
                old_file_mode="100644",
                new_file_mode="100644"
            )
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_git_commit_stats([stat])
            
            mock_collection.bulk_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_blame_data_batch_upsert(self, repo_uuid):
        """Test insert_blame_data performs batch upsert."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            blame = GitBlame(
                repo_id=repo_uuid,
                path="file.txt",
                line_no=1,
                author_email="author@example.com",
                author_name="Author",
                author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
                commit_hash="abc123",
                line="content"
            )
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_blame_data([blame])
            
            mock_collection.bulk_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_many_with_empty_payload(self):
        """Test _upsert_many handles empty payload gracefully."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                # Empty payload should not call bulk_write
                await store._upsert_many("test_collection", [], lambda x: x)
            
            mock_collection.bulk_write.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_many_with_dict_items(self):
        """Test _upsert_many handles dict items correctly."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            mock_collection = MagicMock()
            mock_collection.bulk_write = AsyncMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            dict_items = [
                {"id": "1", "name": "item1"},
                {"id": "2", "name": "item2"}
            ]
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store._upsert_many("test_collection", dict_items, lambda x: x["id"])
            
            mock_collection.bulk_write.assert_called_once()
            operations = mock_collection.bulk_write.call_args[0][0]
            assert len(operations) == 2

    @pytest.mark.asyncio
    async def test_insert_operations_use_correct_collection_names(self, repo_uuid):
        """Test that insert operations use the correct collection names."""
        conn_string = "mongodb://localhost:27017"
        
        with patch("storage.AsyncIOMotorClient") as mock_client_class:
            mock_client = MagicMock()
            mock_db = MagicMock()
            
            # Track which collections were accessed
            accessed_collections = []
            
            def mock_getitem(self_arg, name):
                accessed_collections.append(name)
                mock_collection = MagicMock()
                mock_collection.bulk_write = AsyncMock()
                mock_collection.update_one = AsyncMock()
                return mock_collection
            
            mock_db.__getitem__ = mock_getitem
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_class.return_value = mock_client
            
            repo = Repo(id=repo_uuid, repo="https://github.com/test/repo")
            file_data = [GitFile(repo_id=repo_uuid, path="f.txt", executable=False)]
            commit_data = [GitCommit(
                repo_id=repo_uuid, hash="abc", message="msg",
                author_name="A", author_email="a@e.com",
                author_when=datetime.now(timezone.utc),
                committer_name="C", committer_email="c@e.com",
                committer_when=datetime.now(timezone.utc), parents=0
            )]
            commit_stats = [GitCommitStat(
                repo_id=repo_uuid, commit_hash="abc", file_path="f.txt",
                additions=1, deletions=0
            )]
            blame_data = [GitBlame(
                repo_id=repo_uuid, path="f.txt", line_no=1,
                author_email="a@e.com", author_name="A",
                author_when=datetime.now(timezone.utc),
                commit_hash="abc", line="line"
            )]
            
            async with MongoStore(conn_string, db_name="test_db") as store:
                await store.insert_repo(repo)
                await store.insert_git_file_data(file_data)
                await store.insert_git_commit_data(commit_data)
                await store.insert_git_commit_stats(commit_stats)
                await store.insert_blame_data(blame_data)
            
            assert "repos" in accessed_collections
            assert "git_files" in accessed_collections
            assert "git_commits" in accessed_collections
            assert "git_commit_stats" in accessed_collections
            assert "git_blame" in accessed_collections
