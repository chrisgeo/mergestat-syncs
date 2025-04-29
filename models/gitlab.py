import glab
from typing import List, Dict, Optional
from .git import Git, GitBlame, GitCommit, GitCommitStat, GitFile

# class Org:
#     def __init__(self, name: str):


class GitLabAPI:
    def __init__(self, token: str):
        self.client = glab.GitLab(token)

    def get_repositories(self) -> List[Dict]:
        # Call the GitLab API to fetch all repositories in the organization
        return self.client.get_all_repos()

    def get_merge_requests(self, repo_id: int) -> List[Dict]:
        # Call the GitLab API to fetch all merge requests for a specific repository
        return self.client.get_merge_requests(repo_id)

    def get_metadata(self, repo_id: int) -> Dict:
        # Call the GitLab API to fetch metadata for a specific repository
        return self.client.get_repo_metadata(repo_id)


# class Org:
#     self.name = name
#     self.groups = []
#     self.repositories = []
#     self.merge_requests = []
#     self.metadata = {}


class Repository:
    def __init__(
        self,
        id: int,
        name: str,
        description: Optional[str],
        visibility: str,
        created_at: str,
        last_activity_at: str,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.visibility = visibility
        self.created_at = created_at
        self.last_activity_at = last_activity_at


class MergeRequest:
    def __init__(
        self,
        id: int,
        title: str,
        state: str,
        author: str,
        created_at: str,
        updated_at: str,
    ):
        self.id = id
        self.title = title
        self.state = state
        self.author = author
        self.created_at = created_at
        self.updated_at = updated_at


class Metadata:
    def __init__(self, created_at: str, updated_at: str, permissions: Optional[Dict]):
        self.created_at = created_at
        self.updated_at = updated_at
        self.permissions = permissions
