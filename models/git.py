import os
import uuid
from datetime import datetime
from importlib import import_module
from git import Repo as GitRepo
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Repo(Base, GitRepo):
    __tablename__ = "repos"

    def __init__(self, repo_path: str = None, **kwargs):
        """
        Initialize the Repo class with the given repository path.

        :param repo_path: Path to the git repository (optional).
        :param kwargs: Additional keyword arguments for SQLAlchemy ORM.
        """
        super().__init__(**kwargs)  # Initialize SQLAlchemy ORM
        if repo_path:
            GitRepo.__init__(self, repo_path)  # Initialize GitPython Repo

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="MergeStat identifier for the repo",
    )
    repo = Column(Text, nullable=False, comment="URL for the repo")
    ref = Column(Text, comment="ref for the repo")
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp of when the MergeStat repo entry was created",
    )
    settings = Column(
        JSON, nullable=False, default=dict, comment="JSON settings for the repo"
    )
    tags = Column(
        JSON, nullable=False, default=list, comment="array of tags for the repo"
    )
    # repo_import_id = Column(
    #     UUID(as_uuid=True),
    #     ForeignKey("mergestat.repo_imports.id", ondelete="CASCADE"),
    #     comment="foreign key for mergestat.repo_imports.id",
    # )
    # provider = Column(
    #     UUID(as_uuid=True),
    #     ForeignKey("mergestat.providers.id", ondelete="CASCADE"),
    #     nullable=False,
    # )

    # Relationships
    git_refs = relationship("GitRef", back_populates="repo")
    git_files = relationship("GitFile", back_populates="repo")
    git_commits = relationship("GitCommit", back_populates="repo")
    git_commit_stats = relationship("GitCommitStat", back_populates="repo")
    git_blames = relationship("GitBlame", back_populates="repo")


class GitRef(Base):
    __tablename__ = "git_refs"
    repo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    full_name = Column(Text, primary_key=True)
    hash = Column(Text, comment="hash of the commit for refs that are not of type tag")
    name = Column(Text, comment="name of the ref")
    remote = Column(Text, comment="remote of the ref")
    target = Column(Text, comment="target of the ref")
    type = Column(Text, comment="type of the ref")
    tag_commit_hash = Column(
        Text, comment="hash of the commit for refs that are of type tag"
    )
    _mergestat_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_refs")


class GitFile(Base):
    __tablename__ = "git_files"
    repo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    path = Column(Text, primary_key=True, comment="path of the file")
    executable = Column(
        Boolean,
        nullable=False,
        comment="boolean to determine if the file is an executable",
    )
    contents = Column(Text, comment="contents of the file")
    _mergestat_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_files")


class GitCommit(Base):
    __tablename__ = "git_commits"
    repo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    hash = Column(Text, primary_key=True, comment="hash of the commit")
    message = Column(Text, comment="message of the commit")
    author_name = Column(Text, comment="name of the author of the modification")
    author_email = Column(Text, comment="email of the author of the modification")
    author_when = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp of when the modification was authored",
    )
    committer_name = Column(
        Text, comment="name of the author who committed the modification"
    )
    committer_email = Column(
        Text, comment="email of the author who committed the modification"
    )
    committer_when = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp of when the commit was made",
    )
    parents = Column(
        Integer, nullable=False, comment="the number of parents of the commit"
    )
    _mergestat_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_commits")


class GitCommitStat(Base):
    __tablename__ = "git_commit_stats"
    repo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    commit_hash = Column(Text, primary_key=True, comment="hash of the commit")
    file_path = Column(
        Text, primary_key=True, comment="path of the file the modification was made in"
    )
    additions = Column(
        Integer,
        nullable=False,
        comment="the number of additions in this path of the commit",
    )
    deletions = Column(
        Integer,
        nullable=False,
        comment="the number of deletions in this path of the commit",
    )
    old_file_mode = Column(
        Text,
        nullable=False,
        default="unknown",
        comment="old file mode derived from git mode",
    )
    new_file_mode = Column(
        Text, default="unknown", comment="new file mode derived from git mode"
    )
    _mergestat_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_commit_stats")


class GitBlameMixin:
    """
    Mixin to provide functionality for fetching blame data using gitpython.
    """

    @staticmethod
    def fetch_blame(repo_path, filepath, repo_uuid, repo=None):
        """
        Fetch blame data for a given file using gitpython.

        :param repo_path: Path to the git repository.
        :param filepath: Path to the file to fetch blame data for.
        :param repo_uuid: UUID of the repository.
        :param repo: Optional existing Repo instance to reuse (improves performance).
        :return: List of blame data tuples.
        """
        blame_data = []
        if repo is None:
            repo_cls = import_module("models").Repo
            repo = repo_cls(repo_path)
        rel_path = os.path.relpath(filepath, repo_path)
        try:
            blame_info = repo.blame("HEAD", rel_path)
            line_no = 1
            for commit, lines in blame_info:
                for line in lines:
                    blame_data.append((
                        repo_uuid,
                        commit.author.email,
                        commit.author.name,
                        commit.committed_datetime,
                        commit.hexsha,
                        line_no,
                        line.rstrip("\n"),
                        rel_path,
                    ))
                    line_no += 1
        except Exception as e:
            print(f"Error processing {rel_path}: {e}")
        return blame_data


class GitBlame(Base, GitBlameMixin):
    __tablename__ = "git_blame"
    repo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    path = Column(
        Text, primary_key=True, comment="path of the file the modification was made in"
    )
    line_no = Column(
        Integer, primary_key=True, comment="line number of the modification"
    )
    author_email = Column(Text, comment="email of the author who modified the line")
    author_name = Column(Text, comment="name of the author who modified the line")
    author_when = Column(
        DateTime(timezone=True),
        comment="timestamp of when the modification was authored",
    )
    commit_hash = Column(
        Text, comment="hash of the commit the modification was made in"
    )
    line = Column(Text, comment="content of the line")
    _mergestat_synced_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_blames")

    @classmethod
    def process_file(cls, repo_path, filepath, repo_uuid, repo=None):
        """
        Process a file to fetch blame data and return it as a list of GitBlame objects.

        :param repo_path: Path to the git repository.
        :param filepath: Path to the file to process.
        :param repo_uuid: UUID of the repository.
        :param repo: Optional existing Repo instance to reuse (improves performance).
        :return: List of GitBlame objects.
        """
        blame_data = cls.fetch_blame(repo_path, filepath, repo_uuid, repo=repo)
        return [
            cls(
                repo_id=row[0],
                author_email=row[1],
                author_name=row[2],
                author_when=row[3],
                commit_hash=row[4],
                line_no=row[5],
                line=row[6],
                path=row[7],
            )
            for row in blame_data
        ]
