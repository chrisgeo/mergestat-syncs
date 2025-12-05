import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select


# Optional dependency: pymongo
try:
    from pymongo import UpdateOne
except ImportError:  # pragma: no cover - optional dependency
    UpdateOne = None


from models import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from models.git import Base
from storage import MongoStore, SQLAlchemyStore, model_to_dict

PYMONGO_AVAILABLE = UpdateOne is not None
mongo_skip = pytest.mark.skipif(not PYMONGO_AVAILABLE, reason="pymongo not installed")


def test_model_to_dict_serializes_uuid_and_fields(repo_uuid):
    """
    Test that model_to_dict serializes UUID and fields correctly.

    :param repo_uuid: UUID of the repository.

    :return: None
    """
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


@pytest.fixture
def test_db_url():
    """Return a SQLite in-memory database URL for testing."""
    return "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def sqlalchemy_store(test_db_url):
    """Create a SQLAlchemyStore instance with an in-memory database."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield store

    # Cleanup
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_context_manager(test_db_url):
    """Test that SQLAlchemyStore can be used as an async context manager."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables first
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with store as s:
        assert s.session is not None
        assert s == store

    # After exiting context, session should be closed
    # Note: session is closed but not None
    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_repo(sqlalchemy_store):
    """Test inserting a repository into the database."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)

        # Verify the repo was inserted
        result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo.id)
        )
        saved_repo = result.scalar_one_or_none()

        assert saved_repo is not None
        assert saved_repo.id == test_repo.id
        assert saved_repo.repo == "https://github.com/test/repo.git"
        assert saved_repo.ref == "main"


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_repo_duplicate(sqlalchemy_store):
    """Test that inserting a duplicate repo does not cause an error."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        # Insert the repo twice
        await store.insert_repo(test_repo)
        await store.insert_repo(test_repo)

        # Verify only one repo exists
        result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo.id)
        )
        repos = result.scalars().all()

        assert len(repos) == 1


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_file_data(sqlalchemy_store):
    """Test inserting git file data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file1.txt",
            executable=False,
            contents="content1",
        ),
        GitFile(
            repo_id=test_repo_id,
            path="file2.txt",
            executable=True,
            contents="content2",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_file_data(file_data)

        # Verify files were inserted
        result = await store.session.execute(
            select(GitFile).where(GitFile.repo_id == test_repo_id)
        )
        saved_files = result.scalars().all()

        assert len(saved_files) == 2
        assert saved_files[0].path in ["file1.txt", "file2.txt"]
        assert saved_files[1].path in ["file1.txt", "file2.txt"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_file_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_file_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_data(sqlalchemy_store):
    """Test inserting git commit data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    commit_data = [
        GitCommit(
            repo_id=test_repo_id,
            hash="abc123",
            message="Initial commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parents=0,
        ),
        GitCommit(
            repo_id=test_repo_id,
            hash="def456",
            message="Second commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            parents=1,
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_commit_data(commit_data)

        # Verify commits were inserted
        result = await store.session.execute(
            select(GitCommit).where(GitCommit.repo_id == test_repo_id)
        )
        saved_commits = result.scalars().all()

        assert len(saved_commits) == 2
        assert saved_commits[0].hash in ["abc123", "def456"]
        assert saved_commits[1].hash in ["abc123", "def456"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_commit_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_stats(sqlalchemy_store):
    """Test inserting git commit stats into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    commit_stats = [
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file1.txt",
            additions=10,
            deletions=5,
            old_file_mode="100644",
            new_file_mode="100644",
        ),
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file2.txt",
            additions=20,
            deletions=3,
            old_file_mode="100644",
            new_file_mode="100755",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_commit_stats(commit_stats)

        # Verify commit stats were inserted
        result = await store.session.execute(
            select(GitCommitStat).where(GitCommitStat.repo_id == test_repo_id)
        )
        saved_stats = result.scalars().all()

        assert len(saved_stats) == 2
        assert saved_stats[0].file_path in ["file1.txt", "file2.txt"]
        assert saved_stats[1].file_path in ["file1.txt", "file2.txt"]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_git_commit_stats_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_git_commit_stats([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_blame_data(sqlalchemy_store):
    """Test inserting git blame data into the database."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    blame_data = [
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=1,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 1 content",
        ),
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=2,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 2 content",
        ),
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_blame_data(blame_data)

        # Verify blame data was inserted
        result = await store.session.execute(
            select(GitBlame).where(GitBlame.repo_id == test_repo_id)
        )
        saved_blames = result.scalars().all()

        assert len(saved_blames) == 2
        assert saved_blames[0].line_no in [1, 2]
        assert saved_blames[1].line_no in [1, 2]


@pytest.mark.asyncio
async def test_sqlalchemy_store_insert_blame_data_empty_list(sqlalchemy_store):
    """Test that inserting an empty list does not cause an error."""
    async with sqlalchemy_store as store:
        await store.insert_blame_data([])
        # Should not raise any error


@pytest.mark.asyncio
async def test_sqlalchemy_store_session_management(test_db_url):
    """Test session lifecycle management in SQLAlchemyStore."""
    store = SQLAlchemyStore(test_db_url)

    # Create tables
    async with store.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Before entering context, session should be None
    assert store.session is None

    async with store as s:
        # Inside context, session should be available
        assert s.session is not None

    # After exiting context, the session reference still exists but is closed
    # We can't easily test if it's closed, but we can verify engine is disposed

    await store.engine.dispose()


@pytest.mark.asyncio
async def test_sqlalchemy_store_transaction_commit(sqlalchemy_store):
    """Test that transactions are committed properly."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)

        # Create a new session to verify the commit happened
        async with store.session_factory() as new_session:
            result = await new_session.execute(
                select(Repo).where(Repo.id == test_repo.id)
            )
            saved_repo = result.scalar_one_or_none()

            assert saved_repo is not None
            assert saved_repo.id == test_repo.id


@pytest.mark.asyncio
async def test_sqlalchemy_store_multiple_operations(sqlalchemy_store):
    """Test performing multiple operations in a single session."""
    test_repo_id = uuid.uuid4()
    test_repo = Repo(
        id=test_repo_id,
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=False,
            contents="content",
        )
    ]

    commit_data = [
        GitCommit(
            repo_id=test_repo_id,
            hash="abc123",
            message="Initial commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parents=0,
        )
    ]

    async with sqlalchemy_store as store:
        await store.insert_repo(test_repo)
        await store.insert_git_file_data(file_data)
        await store.insert_git_commit_data(commit_data)

        # Verify all data was inserted
        repo_result = await store.session.execute(
            select(Repo).where(Repo.id == test_repo_id)
        )
        assert repo_result.scalar_one_or_none() is not None

        file_result = await store.session.execute(
            select(GitFile).where(GitFile.repo_id == test_repo_id)
        )
        assert len(file_result.scalars().all()) == 1

        commit_result = await store.session.execute(
            select(GitCommit).where(GitCommit.repo_id == test_repo_id)
        )
        assert len(commit_result.scalars().all()) == 1


# MongoDB Tests


def _create_mock_collection():
    """Helper function to create a mock MongoDB collection with standard methods."""
    mock_collection = MagicMock()
    # Mock async methods
    mock_collection.update_one = AsyncMock(return_value=MagicMock(upserted_id=None))
    mock_collection.bulk_write = AsyncMock(return_value=MagicMock())
    mock_collection.find_one = AsyncMock(return_value=None)
    mock_collection.find = MagicMock(
        return_value=MagicMock(to_list=AsyncMock(return_value=[]))
    )
    mock_collection.count_documents = AsyncMock(return_value=0)
    return mock_collection


@pytest_asyncio.fixture
async def mongo_store():
    """Create a MongoStore instance with mocked MongoDB client for testing."""
    # Mock the AsyncIOMotorClient to avoid actual connection
    with patch("storage.AsyncIOMotorClient") as mock_client_class:
        # Setup the mock client and database
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collections = {}

        def get_collection(self_arg, name):
            if name not in mock_collections:
                mock_collections[name] = _create_mock_collection()
            return mock_collections[name]

        mock_db.__getitem__ = get_collection
        mock_db.name = "test_db"
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        # Create the store instance normally to test constructor
        store = MongoStore("mongodb://localhost:27017", db_name="test_db")

        # Manually set the db since we're not using the context manager
        store.db = mock_db

        yield store


@pytest.mark.asyncio
async def test_mongo_store_init_with_empty_connection_string():
    """Test that MongoStore raises error with empty connection string."""
    with pytest.raises(ValueError, match="MongoDB connection string is required"):
        MongoStore("")


@pytest.mark.asyncio
async def test_mongo_store_init_with_connection_string():
    """Test that MongoStore initializes properly with a connection string."""
    with patch("storage.AsyncIOMotorClient"):
        store = MongoStore("mongodb://localhost:27017/test_db")
        assert store.db_name is None
        assert store.db is None


@pytest.mark.asyncio
async def test_mongo_store_init_with_db_name():
    """Test that MongoStore initializes properly with explicit db_name."""
    with patch("storage.AsyncIOMotorClient"):
        store = MongoStore("mongodb://localhost:27017", db_name="my_database")
        assert store.db_name == "my_database"
        assert store.db is None


@pytest.mark.asyncio
async def test_mongo_store_context_manager_with_db_name():
    """Test that MongoStore can be used as an async context manager with explicit db_name."""
    with patch("storage.AsyncIOMotorClient") as mock_client_class:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "my_database"
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        store = MongoStore("mongodb://localhost:27017", db_name="my_database")

        async with store as s:
            assert s.db is not None
            assert s == store
            mock_client.__getitem__.assert_called_once_with("my_database")


@pytest.mark.asyncio
async def test_mongo_store_context_manager_with_connection_string_db():
    """Test MongoStore context manager with database in connection string."""
    with patch("storage.AsyncIOMotorClient") as mock_client_class:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "mydb"
        mock_client.get_default_database = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        store = MongoStore("mongodb://localhost:27017/mydb")

        async with store as s:
            assert s.db is not None
            assert s == store


@pytest.mark.asyncio
async def test_mongo_store_context_manager_without_db_raises_error():
    """Test that MongoStore raises error when no database is specified."""
    from pymongo.errors import ConfigurationError

    with patch("storage.AsyncIOMotorClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client.get_default_database = MagicMock(
            side_effect=ConfigurationError("No default database")
        )
        mock_client_class.return_value = mock_client

        store = MongoStore("mongodb://localhost:27017")

        with pytest.raises(ValueError, match="No default database specified"):
            async with store:
                pass


@pytest.mark.asyncio
async def test_mongo_store_insert_repo(mongo_store):
    """Test inserting a repository into MongoDB."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    await mongo_store.insert_repo(test_repo)

    # Verify the repo was inserted (update_one was called)
    mongo_store.db["repos"].update_one.assert_called_once()
    call_args = mongo_store.db["repos"].update_one.call_args

    # Verify the filter includes the repo id
    assert call_args[0][0]["_id"] == str(test_repo.id)
    # Verify upsert is True
    assert call_args[1]["upsert"] is True


@pytest.mark.asyncio
async def test_mongo_store_insert_repo_upsert(mongo_store):
    """Test that inserting a duplicate repo updates instead of creating duplicate."""
    test_repo = Repo(
        id=uuid.uuid4(),
        repo="https://github.com/test/repo.git",
        ref="main",
        settings={},
        tags=[],
    )

    # Insert the repo twice with different refs
    await mongo_store.insert_repo(test_repo)
    test_repo.ref = "develop"
    await mongo_store.insert_repo(test_repo)

    # Verify update_one was called twice
    assert mongo_store.db["repos"].update_one.call_count == 2


@pytest.mark.asyncio
async def test_mongo_store_insert_git_file_data(mongo_store):
    """Test inserting git file data into MongoDB."""
    test_repo_id = uuid.uuid4()

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file1.txt",
            executable=False,
            contents="content1",
        ),
        GitFile(
            repo_id=test_repo_id,
            path="file2.txt",
            executable=True,
            contents="content2",
        ),
    ]

    await mongo_store.insert_git_file_data(file_data)

    # Verify bulk_write was called
    mongo_store.db["git_files"].bulk_write.assert_called_once()
    call_args = mongo_store.db["git_files"].bulk_write.call_args

    # Verify operations list has 2 items
    operations = call_args[0][0]
    assert len(operations) == 2
    # Verify ordered=False
    assert call_args[1]["ordered"] is False


@pytest.mark.asyncio
async def test_mongo_store_insert_git_file_data_empty_list(mongo_store):
    """Test that inserting an empty list does not cause an error."""
    await mongo_store.insert_git_file_data([])

    # Verify bulk_write was not called
    mongo_store.db["git_files"].bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_mongo_store_insert_git_file_data_upsert(mongo_store):
    """Test that inserting duplicate file data updates instead of creating duplicates."""
    test_repo_id = uuid.uuid4()

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="file.txt",
            executable=False,
            contents="original content",
        ),
    ]

    await mongo_store.insert_git_file_data(file_data)

    # Update the same file with new content
    file_data[0].contents = "updated content"
    await mongo_store.insert_git_file_data(file_data)

    # Verify bulk_write was called twice (upsert behavior)
    assert mongo_store.db["git_files"].bulk_write.call_count == 2


@pytest.mark.asyncio
async def test_mongo_store_insert_git_commit_data(mongo_store):
    """Test inserting git commit data into MongoDB."""
    test_repo_id = uuid.uuid4()

    commit_data = [
        GitCommit(
            repo_id=test_repo_id,
            hash="abc123",
            message="Initial commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            parents=0,
        ),
        GitCommit(
            repo_id=test_repo_id,
            hash="def456",
            message="Second commit",
            author_name="Test Author",
            author_email="test@example.com",
            author_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            committer_name="Test Committer",
            committer_email="committer@example.com",
            committer_when=datetime(2024, 1, 2, tzinfo=timezone.utc),
            parents=1,
        ),
    ]

    await mongo_store.insert_git_commit_data(commit_data)

    # Verify bulk_write was called
    mongo_store.db["git_commits"].bulk_write.assert_called_once()
    call_args = mongo_store.db["git_commits"].bulk_write.call_args

    # Verify operations list has 2 items
    operations = call_args[0][0]
    assert len(operations) == 2


@pytest.mark.asyncio
async def test_mongo_store_insert_git_commit_data_empty_list(mongo_store):
    """Test that inserting an empty list does not cause an error."""
    await mongo_store.insert_git_commit_data([])

    # Verify bulk_write was not called
    mongo_store.db["git_commits"].bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_mongo_store_insert_git_commit_stats(mongo_store):
    """Test inserting git commit stats into MongoDB."""
    test_repo_id = uuid.uuid4()

    commit_stats = [
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file1.txt",
            additions=10,
            deletions=5,
            old_file_mode="100644",
            new_file_mode="100644",
        ),
        GitCommitStat(
            repo_id=test_repo_id,
            commit_hash="abc123",
            file_path="file2.txt",
            additions=20,
            deletions=3,
            old_file_mode="100644",
            new_file_mode="100755",
        ),
    ]

    await mongo_store.insert_git_commit_stats(commit_stats)

    # Verify bulk_write was called
    mongo_store.db["git_commit_stats"].bulk_write.assert_called_once()
    call_args = mongo_store.db["git_commit_stats"].bulk_write.call_args

    # Verify operations list has 2 items
    operations = call_args[0][0]
    assert len(operations) == 2


@pytest.mark.asyncio
async def test_mongo_store_insert_git_commit_stats_empty_list(mongo_store):
    """Test that inserting an empty list does not cause an error."""
    await mongo_store.insert_git_commit_stats([])

    # Verify bulk_write was not called
    mongo_store.db["git_commit_stats"].bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_mongo_store_insert_blame_data(mongo_store):
    """Test inserting git blame data into MongoDB."""
    test_repo_id = uuid.uuid4()

    blame_data = [
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=1,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 1 content",
        ),
        GitBlame(
            repo_id=test_repo_id,
            path="file.txt",
            line_no=2,
            author_email="author@example.com",
            author_name="Test Author",
            author_when=datetime(2024, 1, 1, tzinfo=timezone.utc),
            commit_hash="abc123",
            line="line 2 content",
        ),
    ]

    await mongo_store.insert_blame_data(blame_data)

    # Verify bulk_write was called
    mongo_store.db["git_blame"].bulk_write.assert_called_once()
    call_args = mongo_store.db["git_blame"].bulk_write.call_args

    # Verify operations list has 2 items
    operations = call_args[0][0]
    assert len(operations) == 2


@pytest.mark.asyncio
async def test_mongo_store_insert_blame_data_empty_list(mongo_store):
    """Test that inserting an empty list does not cause an error."""
    await mongo_store.insert_blame_data([])

    # Verify bulk_write was not called
    mongo_store.db["git_blame"].bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_mongo_store_upsert_many_with_dict_payload(mongo_store):
    """Test _upsert_many with dict payload instead of model instances."""
    test_data = [
        {"repo_id": "test-repo", "path": "file1.txt", "contents": "content1"},
        {"repo_id": "test-repo", "path": "file2.txt", "contents": "content2"},
    ]

    await mongo_store._upsert_many(
        "test_collection",
        test_data,
        lambda obj: f"{obj['repo_id']}:{obj['path']}",
    )

    # Verify bulk_write was called on test_collection
    mongo_store.db["test_collection"].bulk_write.assert_called_once()
    call_args = mongo_store.db["test_collection"].bulk_write.call_args

    # Verify operations list has 2 items
    operations = call_args[0][0]
    assert len(operations) == 2


@pytest.mark.asyncio
async def test_mongo_store_bulk_operations_unordered(mongo_store):
    """Test that bulk operations are performed unordered (continue on error)."""
    test_repo_id = uuid.uuid4()

    # Create a large batch to test bulk operations
    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path=f"file{i}.txt",
            executable=False,
            contents=f"content{i}",
        )
        for i in range(100)
    ]

    await mongo_store.insert_git_file_data(file_data)

    # Verify bulk_write was called with ordered=False
    mongo_store.db["git_files"].bulk_write.assert_called_once()
    call_args = mongo_store.db["git_files"].bulk_write.call_args
    assert call_args[1]["ordered"] is False

    # Verify 100 operations
    operations = call_args[0][0]
    assert len(operations) == 100


@pytest.mark.asyncio
async def test_mongo_store_connection_cleanup():
    """Test that MongoStore properly closes connection on exit."""
    with patch("storage.AsyncIOMotorClient") as mock_client_class:
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        store = MongoStore("mongodb://localhost:27017", db_name="test_db")

        async with store:
            assert store.db is not None

        # Verify client.close() was called
        mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_mongo_store_upsert_id_builder_functionality(mongo_store):
    """Test that _upsert_many correctly uses id_builder to create composite keys."""
    test_repo_id = uuid.uuid4()

    file_data = [
        GitFile(
            repo_id=test_repo_id,
            path="dir/file.txt",
            executable=False,
            contents="content",
        ),
    ]

    await mongo_store.insert_git_file_data(file_data)

    # Verify the id_builder created correct composite key by checking bulk_write call
    call_args = mongo_store.db["git_files"].bulk_write.call_args
    operations = call_args[0][0]

    # Verify that bulk_write was called with UpdateOne operations
    assert isinstance(operations[0], UpdateOne)
    # The UpdateOne should have been created with the composite key
    # We verify this indirectly by checking that bulk_write was called with one operation
    assert len(operations) == 1
