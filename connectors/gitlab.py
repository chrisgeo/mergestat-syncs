"""
GitLab connector using python-gitlab and REST API.

This connector provides methods to retrieve groups, projects,
contributors, statistics, merge requests, and blame information from GitLab.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabError

from connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from connectors.models import (
    Author,
    BlameRange,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    RepoStats,
    Repository,
)
from connectors.utils import GitLabRESTClient, retry_with_backoff

logger = logging.getLogger(__name__)


class GitLabConnector:
    """
    Production-grade GitLab connector using python-gitlab and REST API.

    Provides methods to retrieve data from GitLab with automatic
    pagination, rate limiting, and error handling.
    """

    def __init__(
        self,
        url: str = "https://gitlab.com",
        private_token: Optional[str] = None,
        per_page: int = 100,
        max_workers: int = 4,
    ):
        """
        Initialize GitLab connector.

        :param url: GitLab instance URL.
        :param private_token: GitLab private token.
        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        """
        self.url = url
        self.private_token = private_token
        self.per_page = per_page
        self.max_workers = max_workers

        # Initialize python-gitlab client
        self.gitlab = gitlab.Gitlab(url=url, private_token=private_token)

        # Authenticate if token provided
        if private_token:
            try:
                self.gitlab.auth()
            except GitlabAuthenticationError as e:
                raise AuthenticationException(f"GitLab authentication failed: {e}")

        # Initialize REST client for operations not supported by python-gitlab
        api_url = f"{url}/api/v4"
        self.rest_client = GitLabRESTClient(
            base_url=api_url,
            private_token=private_token,
        )

    def _handle_gitlab_exception(self, e: Exception) -> None:
        """
        Handle GitLab API exceptions and convert to connector exceptions.

        :param e: Exception from GitLab API.
        :raises: Appropriate connector exception.
        """
        if isinstance(e, GitlabAuthenticationError):
            raise AuthenticationException(f"GitLab authentication failed: {e}")
        elif isinstance(e, GitlabError):
            if hasattr(e, "response_code"):
                if e.response_code == 429:
                    raise RateLimitException(f"GitLab rate limit exceeded: {e}")
                elif e.response_code == 404:
                    raise APIException(f"GitLab resource not found: {e}")
            raise APIException(f"GitLab API error: {e}")
        else:
            raise APIException(f"Unexpected error: {e}")

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_groups(
        self,
        max_groups: Optional[int] = None,
    ) -> List[Organization]:
        """
        List groups accessible to the authenticated user.

        :param max_groups: Maximum number of groups to retrieve.
        :return: List of Organization objects (representing GitLab groups).
        """
        try:
            groups = []

            gl_groups = self.gitlab.groups.list(
                per_page=self.per_page,
                get_all=False,
            )

            for gl_group in gl_groups:
                if max_groups and len(groups) >= max_groups:
                    break

                group = Organization(
                    id=gl_group.id,
                    name=gl_group.name,
                    description=(
                        gl_group.description
                        if hasattr(gl_group, "description")
                        else None
                    ),
                    url=gl_group.web_url if hasattr(gl_group, "web_url") else None,
                )
                groups.append(group)
                logger.debug(f"Retrieved group: {group.name}")

            logger.info(f"Retrieved {len(groups)} groups")
            return groups

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_projects(
        self,
        group_id: Optional[int] = None,
        group_name: Optional[str] = None,
        search: Optional[str] = None,
        max_projects: Optional[int] = None,
    ) -> List[Repository]:
        """
        List projects for a group or all accessible projects.

        :param group_id: Optional group ID. If provided, lists group projects.
        :param group_name: Optional group name/path. If provided, lists group projects.
                          Takes precedence over group_id if both are provided.
        :param search: Optional search query to filter projects by name.
        :param max_projects: Maximum number of projects to retrieve. If None, retrieves all.
        :return: List of Repository objects (representing GitLab projects).
        """
        try:
            projects = []

            # Build common list parameters
            list_params = {
                'per_page': self.per_page,
                'get_all': (max_projects is None)
            }
            if search:
                list_params['search'] = search

            # Determine source and fetch projects
            if group_name or group_id:
                # Get group by name or ID and list its projects
                group_identifier = group_name if group_name else group_id
                group = self.gitlab.groups.get(group_identifier)
                gl_projects = group.projects.list(**list_params)
            else:
                # List all accessible projects
                gl_projects = self.gitlab.projects.list(**list_params)

            for gl_project in gl_projects:
                if max_projects and len(projects) >= max_projects:
                    break

                # Get full project details
                if hasattr(gl_project, "path_with_namespace"):
                    full_name = gl_project.path_with_namespace
                elif hasattr(gl_project, "name"):
                    full_name = gl_project.name
                else:
                    full_name = str(gl_project.id)

                project = Repository(
                    id=gl_project.id,
                    name=(
                        gl_project.name
                        if hasattr(gl_project, "name")
                        else str(gl_project.id)
                    ),
                    full_name=full_name,
                    default_branch=(
                        gl_project.default_branch
                        if hasattr(gl_project, "default_branch")
                        else "main"
                    ),
                    description=(
                        gl_project.description
                        if hasattr(gl_project, "description")
                        else None
                    ),
                    url=gl_project.web_url if hasattr(gl_project, "web_url") else None,
                    created_at=(
                        datetime.fromisoformat(
                            gl_project.created_at.replace("Z", "+00:00")
                        )
                        if hasattr(gl_project, "created_at") and gl_project.created_at
                        else None
                    ),
                    updated_at=(
                        datetime.fromisoformat(
                            gl_project.last_activity_at.replace("Z", "+00:00")
                        )
                        if hasattr(gl_project, "last_activity_at")
                        and gl_project.last_activity_at
                        else None
                    ),
                    language=None,  # GitLab doesn't provide primary language in list
                    stars=(
                        gl_project.star_count
                        if hasattr(gl_project, "star_count")
                        else 0
                    ),
                    forks=(
                        gl_project.forks_count
                        if hasattr(gl_project, "forks_count")
                        else 0
                    ),
                )
                projects.append(project)
                logger.debug(f"Retrieved project: {project.full_name}")

            logger.info(f"Retrieved {len(projects)} projects")
            return projects

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_contributors(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """
        Get contributors for a project.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param max_contributors: Maximum number of contributors to retrieve.
        :return: List of Author objects.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)
            contributors = []

            gl_contributors = project.repository_contributors(
                per_page=self.per_page,
                get_all=False,
            )

            for contributor in gl_contributors:
                if max_contributors and len(contributors) >= max_contributors:
                    break

                author = Author(
                    id=0,  # GitLab contributors API doesn't provide user ID
                    username=contributor.get("name", "Unknown"),
                    email=contributor.get("email"),
                    name=contributor.get("name"),
                    url=None,
                )
                contributors.append(author)
                logger.debug(f"Retrieved contributor: {author.username}")

            logger.info(
                f"Retrieved {len(contributors)} contributors for project {project_identifier}"
            )
            return contributors

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_commit_stats(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        sha: str = None,
    ) -> CommitStats:
        """
        Get statistics for a specific commit.

        :param sha: Commit SHA.
        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :return: CommitStats object.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)
            commit = project.commits.get(sha)

            return CommitStats(
                additions=(
                    commit.stats.get("additions", 0) if hasattr(commit, "stats") else 0
                ),
                deletions=(
                    commit.stats.get("deletions", 0) if hasattr(commit, "stats") else 0
                ),
                commits=1,
            )

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_repo_stats(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """
        Get aggregated statistics for a project.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param max_commits: Maximum number of commits to analyze.
        :return: RepoStats object.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)

            total_additions = 0
            total_deletions = 0
            commit_count = 0
            authors_dict = {}

            commits = project.commits.list(per_page=self.per_page, get_all=False)

            for commit in commits:
                if max_commits and commit_count >= max_commits:
                    break

                commit_count += 1

                # Get detailed commit with stats
                detailed_commit = project.commits.get(commit.id)
                if hasattr(detailed_commit, "stats"):
                    total_additions += detailed_commit.stats.get("additions", 0)
                    total_deletions += detailed_commit.stats.get("deletions", 0)

                # Track unique authors
                author_name = (
                    commit.author_name if hasattr(commit, "author_name") else "Unknown"
                )
                author_email = (
                    commit.author_email if hasattr(commit, "author_email") else ""
                )

                author_key = f"{author_name}:{author_email}"
                if author_key not in authors_dict:
                    authors_dict[author_key] = Author(
                        id=0,  # GitLab doesn't provide author ID in commit API
                        username=author_name,
                        name=author_name,
                        email=author_email,
                        url=None,
                    )

            # Calculate commits per week
            created_at = None
            if hasattr(project, "created_at") and project.created_at:
                created_at = datetime.fromisoformat(
                    project.created_at.replace("Z", "+00:00")
                )

            if created_at:
                age_days = (datetime.now(timezone.utc) - created_at).days
                weeks = max(age_days / 7, 1)
                commits_per_week = commit_count / weeks
            else:
                commits_per_week = 0.0

            return RepoStats(
                total_commits=commit_count,
                additions=total_additions,
                deletions=total_deletions,
                commits_per_week=commits_per_week,
                authors=list(authors_dict.values()),
            )

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_merge_requests(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        state: str = "all",
        max_mrs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get merge requests for a project using REST API.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param state: State filter ('opened', 'closed', 'merged', 'all').
        :param max_mrs: Maximum number of merge requests to retrieve.
        :return: List of PullRequest objects (representing GitLab merge requests).
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        # If project_name is provided, we need to get the project_id for the REST API
        if project_name:
            try:
                project = self.gitlab.projects.get(project_name)
                actual_project_id = project.id
            except Exception as e:
                self._handle_gitlab_exception(e)
        else:
            actual_project_id = project_id

        try:
            merge_requests = []
            page = 1

            while True:
                if max_mrs and len(merge_requests) >= max_mrs:
                    break

                mrs = self.rest_client.get_merge_requests(
                    project_id=actual_project_id,
                    state=state,
                    page=page,
                    per_page=self.per_page,
                )

                if not mrs:
                    break

                for mr in mrs:
                    if max_mrs and len(merge_requests) >= max_mrs:
                        break

                    author = None
                    if mr.get("author"):
                        author_data = mr["author"]
                        author = Author(
                            id=author_data.get("id", 0),
                            username=author_data.get("username", "Unknown"),
                            name=author_data.get("name"),
                            email=None,  # Not provided in MR API
                            url=author_data.get("web_url"),
                        )

                    # Parse dates
                    created_at = None
                    if mr.get("created_at"):
                        try:
                            created_at = datetime.fromisoformat(
                                mr["created_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    merged_at = None
                    if mr.get("merged_at"):
                        try:
                            merged_at = datetime.fromisoformat(
                                mr["merged_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    closed_at = None
                    if mr.get("closed_at"):
                        try:
                            closed_at = datetime.fromisoformat(
                                mr["closed_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    pr = PullRequest(
                        id=mr.get("id", 0),
                        number=mr.get("iid", 0),
                        title=mr.get("title", ""),
                        state=mr.get("state", "unknown"),
                        author=author,
                        created_at=created_at,
                        merged_at=merged_at,
                        closed_at=closed_at,
                        body=mr.get("description"),
                        url=mr.get("web_url"),
                        base_branch=mr.get("target_branch"),
                        head_branch=mr.get("source_branch"),
                    )
                    merge_requests.append(pr)
                    logger.debug(f"Retrieved MR !{pr.number}: {pr.title}")

                page += 1

            logger.info(
                f"Retrieved {len(merge_requests)} merge requests for project {project_identifier}"
            )
            return merge_requests

        except Exception as e:
            self._handle_gitlab_exception(e)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_file_blame(
        self,
        file_path: str,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        ref: str = "main",
    ) -> FileBlame:
        """
        Get blame information for a file using GitLab REST API.

        :param file_path: File path within the repository.
        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        # If project_name is provided, we need to get the project_id for the REST API
        if project_name:
            try:
                project = self.gitlab.projects.get(project_name)
                actual_project_id = project.id
            except Exception as e:
                self._handle_gitlab_exception(e)
        else:
            actual_project_id = project_id

        try:
            blame_data = self.rest_client.get_file_blame(actual_project_id, file_path, ref)

            ranges = []
            current_line = 1

            for blame_item in blame_data:
                lines = blame_item.get("lines", [])
                commit = blame_item.get("commit", {})

                # Calculate age in seconds
                committed_date_str = commit.get("committed_date")
                age_seconds = 0
                if committed_date_str:
                    try:
                        committed_date = datetime.fromisoformat(
                            committed_date_str.replace("Z", "+00:00")
                        )
                        age_seconds = int(
                            (
                                datetime.now(timezone.utc) - committed_date
                            ).total_seconds()
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse date {committed_date_str}: {e}"
                        )

                num_lines = len(lines)
                if num_lines > 0:
                    blame_range = BlameRange(
                        starting_line=current_line,
                        ending_line=current_line + num_lines - 1,
                        commit_sha=commit.get("id", ""),
                        author=commit.get("author_name", "Unknown"),
                        author_email=commit.get("author_email", ""),
                        age_seconds=age_seconds,
                    )
                    ranges.append(blame_range)
                    current_line += num_lines

            logger.info(
                f"Retrieved blame for project {project_identifier}:{file_path} "
                f"with {len(ranges)} ranges"
            )
            return FileBlame(file_path=file_path, ranges=ranges)

        except Exception as e:
            self._handle_gitlab_exception(e)

    def close(self) -> None:
        """Close the connector and cleanup resources."""
        # python-gitlab doesn't need explicit cleanup
        pass
