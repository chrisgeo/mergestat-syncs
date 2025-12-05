import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from models import GitBlame, GitCommit, GitCommitStat, GitFile, Repo
from models.git import Base
from storage import SQLAlchemyStore, model_to_dict


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
        session_ref = s.session
    
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
