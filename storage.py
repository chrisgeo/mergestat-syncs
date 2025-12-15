import asyncio
import json
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import clickhouse_connect
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import ConfigurationError
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker

from models.git import GitBlame, GitCommit, GitCommitStat, GitFile, GitPullRequest, Repo


def detect_db_type(conn_string: str) -> str:
    """
    Detect database type from connection string.

    :param conn_string: Database connection string.
    :return: Database type ('postgres', 'sqlite', 'mongo', or 'clickhouse').
    :raises ValueError: If database type cannot be determined.
    """
    if not conn_string:
        raise ValueError("Connection string is required")

    conn_lower = conn_string.lower()

    # ClickHouse connection strings
    if conn_lower.startswith("clickhouse://") or conn_lower.startswith(
        ("clickhouse+http://", "clickhouse+https://", "clickhouse+native://")
    ):
        return "clickhouse"

    # MongoDB connection strings
    if conn_lower.startswith("mongodb://") or conn_lower.startswith("mongodb+srv://"):
        return "mongo"

    # PostgreSQL connection strings
    if conn_lower.startswith("postgresql://") or conn_lower.startswith("postgres://"):
        return "postgres"
    if conn_lower.startswith("postgresql+asyncpg://"):
        return "postgres"

    # SQLite connection strings
    if conn_lower.startswith("sqlite://") or conn_lower.startswith(
        "sqlite+aiosqlite://"
    ):
        return "sqlite"

    # Extract scheme for better error reporting
    scheme = conn_string.split("://", 1)[0] if "://" in conn_string else "unknown"
    raise ValueError(
        f"Could not detect database type from connection string. "
        f"Supported: mongodb://, postgresql://, postgres://, sqlite://, "
        f"clickhouse://, or variations with async drivers. Got scheme: '{scheme}', "
        f"connection string (first 100 chars): {conn_string[:100]}..."
    )


def create_store(
    conn_string: str,
    db_type: Optional[str] = None,
    db_name: Optional[str] = None,
    echo: bool = False,
) -> Union["SQLAlchemyStore", "MongoStore", "ClickHouseStore"]:
    """
    Create a storage backend based on the connection string.

    This factory function automatically detects the database type from the
    connection string and returns the appropriate store implementation.

    :param conn_string: Database connection string.
    :param db_type: Optional explicit database type ('postgres', 'sqlite', 'mongo', 'clickhouse').
                   If not provided, it will be auto-detected from conn_string.
    :param db_name: Optional database name (for MongoDB).
    :param echo: Whether to echo SQL statements (for SQLAlchemy).
    :return: Appropriate store instance (SQLAlchemyStore, MongoStore, or ClickHouseStore).
    """
    if db_type is None:
        db_type = detect_db_type(conn_string)

    db_type = db_type.lower()

    if db_type == "mongo":
        return MongoStore(conn_string, db_name=db_name)
    elif db_type == "clickhouse":
        return ClickHouseStore(conn_string)
    elif db_type in ("postgres", "postgresql", "sqlite"):
        return SQLAlchemyStore(conn_string, echo=echo)
    else:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: postgres, sqlite, mongo, clickhouse"
        )


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

    def _insert_for_dialect(self, model: Any):
        dialect = self.engine.dialect.name
        if dialect == "sqlite":
            return sqlite_insert(model)
        if dialect in ("postgres", "postgresql"):
            return pg_insert(model)
        raise ValueError(f"Unsupported SQL dialect for upserts: {dialect}")

    async def _upsert_many(
        self,
        model: Any,
        rows: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: List[str],
    ) -> None:
        if not rows:
            return
        assert self.session is not None

        stmt = self._insert_for_dialect(model)
        stmt = stmt.on_conflict_do_update(
            index_elements=[getattr(model, col) for col in conflict_columns],
            set_={col: getattr(stmt.excluded, col) for col in update_columns},
        )
        await self.session.execute(stmt, rows)
        await self.session.commit()

    async def __aenter__(self) -> "SQLAlchemyStore":
        self.session = self.session_factory()

        # Create tables for SQLite automatically
        if "sqlite" in str(self.engine.url):
            from models.git import Base

            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

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

    async def has_any_git_files(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count()).select_from(GitFile).where(GitFile.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitCommitStat)
            .where(GitCommitStat.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def has_any_git_blame(self, repo_id) -> bool:
        assert self.session is not None
        result = await self.session.execute(
            select(func.count())
            .select_from(GitBlame)
            .where(GitBlame.repo_id == repo_id)
        )
        return (result.scalar() or 0) > 0

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in file_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "executable": item.get("executable"),
                    "contents": item.get("contents"),
                    "_mergestat_synced_at": item.get("_mergestat_synced_at")
                    or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "executable": getattr(item, "executable"),
                    "contents": getattr(item, "contents"),
                    "_mergestat_synced_at": getattr(item, "_mergestat_synced_at", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitFile,
            rows,
            conflict_columns=["repo_id", "path"],
            update_columns=["executable", "contents", "_mergestat_synced_at"],
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in commit_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "hash": item.get("hash"),
                    "message": item.get("message"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "author_when": item.get("author_when"),
                    "committer_name": item.get("committer_name"),
                    "committer_email": item.get("committer_email"),
                    "committer_when": item.get("committer_when"),
                    "parents": item.get("parents"),
                    "_mergestat_synced_at": item.get("_mergestat_synced_at")
                    or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "hash": getattr(item, "hash"),
                    "message": getattr(item, "message"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "author_when": getattr(item, "author_when"),
                    "committer_name": getattr(item, "committer_name"),
                    "committer_email": getattr(item, "committer_email"),
                    "committer_when": getattr(item, "committer_when"),
                    "parents": getattr(item, "parents"),
                    "_mergestat_synced_at": getattr(item, "_mergestat_synced_at", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommit,
            rows,
            conflict_columns=["repo_id", "hash"],
            update_columns=[
                "message",
                "author_name",
                "author_email",
                "author_when",
                "committer_name",
                "committer_email",
                "committer_when",
                "parents",
                "_mergestat_synced_at",
            ],
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in commit_stats:
            if isinstance(item, dict):
                old_mode = item.get("old_file_mode") or "unknown"
                new_mode = item.get("new_file_mode") or "unknown"
                row = {
                    "repo_id": item.get("repo_id"),
                    "commit_hash": item.get("commit_hash"),
                    "file_path": item.get("file_path"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "_mergestat_synced_at": item.get("_mergestat_synced_at")
                    or synced_at_default,
                }
            else:
                old_mode = getattr(item, "old_file_mode", None) or "unknown"
                new_mode = getattr(item, "new_file_mode", None) or "unknown"
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "file_path": getattr(item, "file_path"),
                    "additions": getattr(item, "additions"),
                    "deletions": getattr(item, "deletions"),
                    "old_file_mode": old_mode,
                    "new_file_mode": new_mode,
                    "_mergestat_synced_at": getattr(item, "_mergestat_synced_at", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitCommitStat,
            rows,
            conflict_columns=["repo_id", "commit_hash", "file_path"],
            update_columns=[
                "additions",
                "deletions",
                "old_file_mode",
                "new_file_mode",
                "_mergestat_synced_at",
            ],
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in data_batch:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "path": item.get("path"),
                    "line_no": item.get("line_no"),
                    "author_email": item.get("author_email"),
                    "author_name": item.get("author_name"),
                    "author_when": item.get("author_when"),
                    "commit_hash": item.get("commit_hash"),
                    "line": item.get("line"),
                    "_mergestat_synced_at": item.get("_mergestat_synced_at")
                    or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "path": getattr(item, "path"),
                    "line_no": getattr(item, "line_no"),
                    "author_email": getattr(item, "author_email"),
                    "author_name": getattr(item, "author_name"),
                    "author_when": getattr(item, "author_when"),
                    "commit_hash": getattr(item, "commit_hash"),
                    "line": getattr(item, "line"),
                    "_mergestat_synced_at": getattr(item, "_mergestat_synced_at", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitBlame,
            rows,
            conflict_columns=["repo_id", "path", "line_no"],
            update_columns=[
                "author_email",
                "author_name",
                "author_when",
                "commit_hash",
                "line",
                "_mergestat_synced_at",
            ],
        )

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        if not pr_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in pr_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "number": item.get("number"),
                    "title": item.get("title"),
                    "state": item.get("state"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "created_at": item.get("created_at"),
                    "merged_at": item.get("merged_at"),
                    "closed_at": item.get("closed_at"),
                    "head_branch": item.get("head_branch"),
                    "base_branch": item.get("base_branch"),
                    "_mergestat_synced_at": item.get("_mergestat_synced_at")
                    or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "number": getattr(item, "number"),
                    "title": getattr(item, "title"),
                    "state": getattr(item, "state"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "created_at": getattr(item, "created_at"),
                    "merged_at": getattr(item, "merged_at"),
                    "closed_at": getattr(item, "closed_at"),
                    "head_branch": getattr(item, "head_branch"),
                    "base_branch": getattr(item, "base_branch"),
                    "_mergestat_synced_at": getattr(item, "_mergestat_synced_at", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitPullRequest,
            rows,
            conflict_columns=["repo_id", "number"],
            update_columns=[
                "title",
                "state",
                "author_name",
                "author_email",
                "created_at",
                "merged_at",
                "closed_at",
                "head_branch",
                "base_branch",
                "_mergestat_synced_at",
            ],
        )


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

    async def has_any_git_files(self, repo_id) -> bool:
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_files"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_commit_stats"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

    async def has_any_git_blame(self, repo_id) -> bool:
        repo_id_val = _serialize_value(repo_id)
        count = await self.db["git_blame"].count_documents(
            {"repo_id": repo_id_val}, limit=1
        )
        return count > 0

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

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        await self._upsert_many(
            "git_pull_requests",
            pr_data,
            lambda obj: f"{getattr(obj, 'repo_id')}:{getattr(obj, 'number')}",
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


class ClickHouseStore:
    """Async storage implementation backed by ClickHouse (via clickhouse-connect)."""

    def __init__(self, conn_string: str) -> None:
        if not conn_string:
            raise ValueError("ClickHouse connection string is required")
        self.conn_string = conn_string
        self.client = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "ClickHouseStore":
        self.client = await asyncio.to_thread(
            clickhouse_connect.get_client, dsn=self.conn_string
        )
        await self._ensure_tables()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client is not None:
            await asyncio.to_thread(self.client.close)

    @staticmethod
    def _normalize_uuid(value: Any) -> uuid.UUID:
        if value is None:
            raise ValueError("UUID value is required")
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))

    @staticmethod
    def _normalize_datetime(value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, datetime):
            return value
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _json_or_none(value: Any) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value, default=str)

    async def _ensure_tables(self) -> None:
        assert self.client is not None
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS repos (
                id UUID,
                repo String,
                ref Nullable(String),
                created_at DateTime64(3, 'UTC'),
                settings Nullable(String),
                tags Nullable(String),
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (id)
            """,
            """
            CREATE TABLE IF NOT EXISTS git_files (
                repo_id UUID,
                path String,
                executable UInt8,
                contents Nullable(String),
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (repo_id, path)
            """,
            """
            CREATE TABLE IF NOT EXISTS git_commits (
                repo_id UUID,
                hash String,
                message Nullable(String),
                author_name Nullable(String),
                author_email Nullable(String),
                author_when DateTime64(3, 'UTC'),
                committer_name Nullable(String),
                committer_email Nullable(String),
                committer_when DateTime64(3, 'UTC'),
                parents UInt32,
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (repo_id, hash)
            """,
            """
            CREATE TABLE IF NOT EXISTS git_commit_stats (
                repo_id UUID,
                commit_hash String,
                file_path String,
                additions Int32,
                deletions Int32,
                old_file_mode String,
                new_file_mode String,
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (repo_id, commit_hash, file_path)
            """,
            """
            CREATE TABLE IF NOT EXISTS git_blame (
                repo_id UUID,
                path String,
                line_no UInt32,
                author_email Nullable(String),
                author_name Nullable(String),
                author_when Nullable(DateTime64(3, 'UTC')),
                commit_hash Nullable(String),
                line Nullable(String),
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (repo_id, path, line_no)
            """,
            """
            CREATE TABLE IF NOT EXISTS git_pull_requests (
                repo_id UUID,
                number UInt32,
                title Nullable(String),
                state Nullable(String),
                author_name Nullable(String),
                author_email Nullable(String),
                created_at DateTime64(3, 'UTC'),
                merged_at Nullable(DateTime64(3, 'UTC')),
                closed_at Nullable(DateTime64(3, 'UTC')),
                head_branch Nullable(String),
                base_branch Nullable(String),
                _mergestat_synced_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
            ORDER BY (repo_id, number)
            """,
        ]

        async with self._lock:
            for stmt in stmts:
                await asyncio.to_thread(self.client.command, stmt)

    async def _insert_rows(
        self, table: str, columns: List[str], rows: List[Dict[str, Any]]
    ) -> None:
        if not rows:
            return
        assert self.client is not None
        matrix = [[row.get(col) for col in columns] for row in rows]
        async with self._lock:
            await asyncio.to_thread(
                self.client.insert, table, matrix, column_names=columns
            )

    async def _has_any(self, table: str, repo_id: uuid.UUID) -> bool:
        assert self.client is not None
        query = f"SELECT 1 FROM {table} WHERE repo_id = {{repo_id:UUID}} LIMIT 1"
        async with self._lock:
            result = await asyncio.to_thread(
                self.client.query, query, parameters={"repo_id": str(repo_id)}
            )
        return bool(getattr(result, "result_rows", None))

    async def insert_repo(self, repo: Repo) -> None:
        assert self.client is not None
        repo_id = self._normalize_uuid(getattr(repo, "id"))
        async with self._lock:
            existing = await asyncio.to_thread(
                self.client.query,
                "SELECT 1 FROM repos WHERE id = {id:UUID} LIMIT 1",
                parameters={"id": str(repo_id)},
            )
        if getattr(existing, "result_rows", None):
            return

        synced_at = self._normalize_datetime(datetime.now(timezone.utc))
        created_at = (
            self._normalize_datetime(getattr(repo, "created_at", None)) or synced_at
        )

        row = {
            "id": repo_id,
            "repo": getattr(repo, "repo"),
            "ref": getattr(repo, "ref", None),
            "created_at": created_at,
            "settings": self._json_or_none(getattr(repo, "settings", None)),
            "tags": self._json_or_none(getattr(repo, "tags", None)),
            "_mergestat_synced_at": synced_at,
        }
        await self._insert_rows(
            "repos",
            [
                "id",
                "repo",
                "ref",
                "created_at",
                "settings",
                "tags",
                "_mergestat_synced_at",
            ],
            [row],
        )

    async def has_any_git_files(self, repo_id) -> bool:
        return await self._has_any("git_files", self._normalize_uuid(repo_id))

    async def has_any_git_commit_stats(self, repo_id) -> bool:
        return await self._has_any("git_commit_stats", self._normalize_uuid(repo_id))

    async def has_any_git_blame(self, repo_id) -> bool:
        return await self._has_any("git_blame", self._normalize_uuid(repo_id))

    async def insert_git_file_data(self, file_data: List[GitFile]) -> None:
        if not file_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in file_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "path": item.get("path"),
                        "executable": 1 if item.get("executable") else 0,
                        "contents": item.get("contents"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            item.get("_mergestat_synced_at") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "path": getattr(item, "path"),
                        "executable": 1 if getattr(item, "executable") else 0,
                        "contents": getattr(item, "contents"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            getattr(item, "_mergestat_synced_at", None)
                            or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_files",
            ["repo_id", "path", "executable", "contents", "_mergestat_synced_at"],
            rows,
        )

    async def insert_git_commit_data(self, commit_data: List[GitCommit]) -> None:
        if not commit_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in commit_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "hash": item.get("hash"),
                        "message": item.get("message"),
                        "author_name": item.get("author_name"),
                        "author_email": item.get("author_email"),
                        "author_when": self._normalize_datetime(
                            item.get("author_when")
                        ),
                        "committer_name": item.get("committer_name"),
                        "committer_email": item.get("committer_email"),
                        "committer_when": self._normalize_datetime(
                            item.get("committer_when")
                        ),
                        "parents": int(item.get("parents") or 0),
                        "_mergestat_synced_at": self._normalize_datetime(
                            item.get("_mergestat_synced_at") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "hash": getattr(item, "hash"),
                        "message": getattr(item, "message"),
                        "author_name": getattr(item, "author_name"),
                        "author_email": getattr(item, "author_email"),
                        "author_when": self._normalize_datetime(
                            getattr(item, "author_when")
                        ),
                        "committer_name": getattr(item, "committer_name"),
                        "committer_email": getattr(item, "committer_email"),
                        "committer_when": self._normalize_datetime(
                            getattr(item, "committer_when")
                        ),
                        "parents": int(getattr(item, "parents") or 0),
                        "_mergestat_synced_at": self._normalize_datetime(
                            getattr(item, "_mergestat_synced_at", None)
                            or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_commits",
            [
                "repo_id",
                "hash",
                "message",
                "author_name",
                "author_email",
                "author_when",
                "committer_name",
                "committer_email",
                "committer_when",
                "parents",
                "_mergestat_synced_at",
            ],
            rows,
        )

    async def insert_git_commit_stats(self, commit_stats: List[GitCommitStat]) -> None:
        if not commit_stats:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in commit_stats:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "commit_hash": item.get("commit_hash"),
                        "file_path": item.get("file_path"),
                        "additions": int(item.get("additions") or 0),
                        "deletions": int(item.get("deletions") or 0),
                        "old_file_mode": item.get("old_file_mode") or "unknown",
                        "new_file_mode": item.get("new_file_mode") or "unknown",
                        "_mergestat_synced_at": self._normalize_datetime(
                            item.get("_mergestat_synced_at") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "commit_hash": getattr(item, "commit_hash"),
                        "file_path": getattr(item, "file_path"),
                        "additions": int(getattr(item, "additions") or 0),
                        "deletions": int(getattr(item, "deletions") or 0),
                        "old_file_mode": getattr(item, "old_file_mode", None)
                        or "unknown",
                        "new_file_mode": getattr(item, "new_file_mode", None)
                        or "unknown",
                        "_mergestat_synced_at": self._normalize_datetime(
                            getattr(item, "_mergestat_synced_at", None)
                            or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_commit_stats",
            [
                "repo_id",
                "commit_hash",
                "file_path",
                "additions",
                "deletions",
                "old_file_mode",
                "new_file_mode",
                "_mergestat_synced_at",
            ],
            rows,
        )

    async def insert_blame_data(self, data_batch: List[GitBlame]) -> None:
        if not data_batch:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in data_batch:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "path": item.get("path"),
                        "line_no": int(item.get("line_no") or 0),
                        "author_email": item.get("author_email"),
                        "author_name": item.get("author_name"),
                        "author_when": self._normalize_datetime(
                            item.get("author_when")
                        ),
                        "commit_hash": item.get("commit_hash"),
                        "line": item.get("line"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            item.get("_mergestat_synced_at") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "path": getattr(item, "path"),
                        "line_no": int(getattr(item, "line_no") or 0),
                        "author_email": getattr(item, "author_email"),
                        "author_name": getattr(item, "author_name"),
                        "author_when": self._normalize_datetime(
                            getattr(item, "author_when")
                        ),
                        "commit_hash": getattr(item, "commit_hash"),
                        "line": getattr(item, "line"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            getattr(item, "_mergestat_synced_at", None)
                            or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_blame",
            [
                "repo_id",
                "path",
                "line_no",
                "author_email",
                "author_name",
                "author_when",
                "commit_hash",
                "line",
                "_mergestat_synced_at",
            ],
            rows,
        )

    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        if not pr_data:
            return
        synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
        rows: List[Dict[str, Any]] = []
        for item in pr_data:
            if isinstance(item, dict):
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(item.get("repo_id")),
                        "number": int(item.get("number") or 0),
                        "title": item.get("title"),
                        "state": item.get("state"),
                        "author_name": item.get("author_name"),
                        "author_email": item.get("author_email"),
                        "created_at": self._normalize_datetime(item.get("created_at")),
                        "merged_at": self._normalize_datetime(item.get("merged_at")),
                        "closed_at": self._normalize_datetime(item.get("closed_at")),
                        "head_branch": item.get("head_branch"),
                        "base_branch": item.get("base_branch"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            item.get("_mergestat_synced_at") or synced_at_default
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "repo_id": self._normalize_uuid(getattr(item, "repo_id")),
                        "number": int(getattr(item, "number") or 0),
                        "title": getattr(item, "title"),
                        "state": getattr(item, "state"),
                        "author_name": getattr(item, "author_name"),
                        "author_email": getattr(item, "author_email"),
                        "created_at": self._normalize_datetime(
                            getattr(item, "created_at")
                        ),
                        "merged_at": self._normalize_datetime(
                            getattr(item, "merged_at")
                        ),
                        "closed_at": self._normalize_datetime(
                            getattr(item, "closed_at")
                        ),
                        "head_branch": getattr(item, "head_branch"),
                        "base_branch": getattr(item, "base_branch"),
                        "_mergestat_synced_at": self._normalize_datetime(
                            getattr(item, "_mergestat_synced_at", None)
                            or synced_at_default
                        ),
                    }
                )

        await self._insert_rows(
            "git_pull_requests",
            [
                "repo_id",
                "number",
                "title",
                "state",
                "author_name",
                "author_email",
                "created_at",
                "merged_at",
                "closed_at",
                "head_branch",
                "base_branch",
                "_mergestat_synced_at",
            ],
            rows,
        )
