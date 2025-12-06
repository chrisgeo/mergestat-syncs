import uuid
from collections.abc import Iterable
from typing import Any, Callable, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import ConfigurationError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, Repo


def _serialize_value(value: Any) -> Any:
    """Convert values so they are safe to store in MongoDB."""
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def model_to_dict(model: Any) -> Dict[str, Any]:
    """Convert a SQLAlchemy model instance to a plain dict."""
    mapper = inspect(model.__class__)
    data: Dict[str, Any] = {}
    for column in mapper.columns:
        data[column.key] = _serialize_value(getattr(model, column.key))
    return data


class SQLAlchemyStore:
    """Async storage implementation backed by SQLAlchemy."""

    def __init__(self, conn_string: str, echo: bool = False) -> None:
        # Configure connection pool for better performance (PostgreSQL/MySQL only)
        engine_kwargs = {"echo": echo}

        # Only add pooling parameters for databases that support them
        if "sqlite" not in conn_string.lower():
            engine_kwargs.update(
                {
                    "pool_size": 20,  # Increased from default 5
                    "max_overflow": 30,  # Increased from default 10
                    "pool_pre_ping": True,  # Verify connections before using
                    "pool_recycle": 3600,  # Recycle connections after 1 hour
                }
            )

        self.engine = create_async_engine(conn_string, **engine_kwargs)
        self.session_factory = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        self.session: Optional[AsyncSession] = None

    async def __aenter__(self) -> "SQLAlchemyStore":
        self.session = self.session_factory()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.session is not None:
            await self.session.close()

    async def insert_repo(self, repo: Repo) -> None:
        assert self.session is not None
        existing_repo = await self.session.get(Repo, repo.id)
        if not existing_repo:
            self.session.add(repo)
            await self.session.commit()

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        assert self.session is not None
        self.session.add_all(file_data)
        await self.session.commit()

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        assert self.session is not None
        self.session.add_all(commit_data)
        await self.session.commit()

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        assert self.session is not None
        self.session.add_all(commit_stats)
        await self.session.commit()

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        assert self.session is not None
        self.session.add_all(data_batch)
        await self.session.commit()


class MongoStore:
    """Async storage implementation backed by MongoDB (via Motor)."""

    def __init__(self, conn_string: str, db_name: Optional[str] = None) -> None:
        if not conn_string:
            raise ValueError("MongoDB connection string is required")
        self.client = AsyncIOMotorClient(conn_string)
        self.db_name = db_name
        self.db = None

    async def __aenter__(self) -> "MongoStore":
        if self.db_name:
            self.db = self.client[self.db_name]
        else:
            try:
                default_db = self.client.get_default_database()
                self.db = (
                    default_db if default_db is not None else self.client["mergestat"]
                )
            except ConfigurationError:
                raise ValueError(
                    "No default database specified. Please provide a database name "
                    "either via the MONGO_DB_NAME environment variable or include it "
                    "in your MongoDB connection string (e.g., 'mongodb://localhost:27017/mydb')"
                )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.client.close()

    async def insert_repo(self, repo: Repo) -> None:
        doc = model_to_dict(repo)
        doc["_id"] = doc["id"]
        await self.db["repos"].update_one(
            {"_id": doc["_id"]}, {"$set": doc}, upsert=True
        )

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        await self._upsert_many(
            "git_files",
            file_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'path')}",
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        await self._upsert_many(
            "git_commits",
            commit_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'hash')}",
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        await self._upsert_many(
            "git_commit_stats",
            commit_stats,
            lambda obj: (
                f"{getattr(obj, 'repo_id')}:"
                f"{getattr(obj, 'commit_hash')}:"
                f"{getattr(obj, 'file_path')}"
            ),
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        await self._upsert_many(
            "git_blame",
            data_batch,
            lambda obj: (
                f"{getattr(obj, 'repo_id')}:"
                f"{getattr(obj, 'path')}:"
                f"{getattr(obj, 'line_no')}"
            ),
        )

    async def _upsert_many(
        self,
        collection: str,
        payload: Iterable[Any],
        id_builder: Callable[[Any], str],
    ) -> None:
        docs = []
        for item in payload:
            doc = model_to_dict(item) if not isinstance(item, dict) else dict(item)
            doc["_id"] = id_builder(item)
            docs.append(doc)

        if not docs:
            return

        operations = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for doc in docs
        ]
        await self.db[collection].bulk_write(operations, ordered=False)
