# GitHub and GitLab Connectors

Production-grade connectors for retrieving data from GitHub and GitLab APIs with automatic pagination, rate limiting, and error handling.

## Features

- **GitHub Connector**: Uses PyGithub and GraphQL API
- **GitLab Connector**: Uses python-gitlab and REST API
- **Automatic Pagination**: Handles paginated responses seamlessly
- **Rate Limiting**: Exponential backoff for API rate limits
- **Retry Logic**: Automatic retries with configurable backoff
- **Type-Safe Models**: Dataclasses for all data structures
- **Error Handling**: Clear exception types for different errors

## Installation

The connectors are included with this project. Dependencies are listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

Key dependencies:
- `PyGithub` - GitHub API client
- `python-gitlab` - GitLab API client
- `requests` - HTTP library

## Quick Start

### GitHub Connector

```python
from connectors import GitHubConnector

# Initialize with GitHub token
connector = GitHubConnector(token="your_github_token")

# List organizations
orgs = connector.list_organizations(max_orgs=10)

# List repositories
repos = connector.list_repositories(max_repos=20)

# Get contributors
contributors = connector.get_contributors("owner", "repo")

# Get repository statistics
stats = connector.get_repo_stats("owner", "repo", max_commits=100)

# Get pull requests
prs = connector.get_pull_requests("owner", "repo", state="open")

# Get file blame using GraphQL
blame = connector.get_file_blame("owner", "repo", "path/to/file.py")

# Check rate limit
rate_limit = connector.get_rate_limit()

# Close connector
connector.close()
```

### GitLab Connector

```python
from connectors import GitLabConnector

# Initialize with GitLab token
connector = GitLabConnector(
    url="https://gitlab.com",  # Or your GitLab instance URL
    private_token="your_gitlab_token"
)

# List groups
groups = connector.list_groups(max_groups=10)

# List projects
projects = connector.list_projects(max_projects=20)

# Get contributors
contributors = connector.get_contributors(project_id=123)

# Get project statistics
stats = connector.get_repo_stats(project_id=123, max_commits=100)

# Get merge requests
mrs = connector.get_merge_requests(project_id=123, state="opened")

# Get file blame using REST API
blame = connector.get_file_blame(project_id=123, file_path="path/to/file.py")

# Close connector
connector.close()
```

## Data Models

All connectors use the same data models:

- **Organization**: GitHub organization or GitLab group
- **Repository**: GitHub repository or GitLab project
- **Author**: Contributor or author
- **CommitStats**: Statistics for a single commit
- **RepoStats**: Aggregated statistics for a repository
- **PullRequest**: GitHub PR or GitLab MR
- **BlameRange**: Range of lines with blame information
- **FileBlame**: Blame information for a file

## Configuration

### GitHub Connector

```python
connector = GitHubConnector(
    token="your_token",              # Required: GitHub personal access token
    base_url=None,                   # Optional: For GitHub Enterprise
    per_page=100,                    # Items per page (max 100)
    max_workers=4,                   # Concurrent workers
)
```

### GitLab Connector

```python
connector = GitLabConnector(
    url="https://gitlab.com",        # GitLab instance URL
    private_token="your_token",      # GitLab private token
    per_page=100,                    # Items per page (max 100)
    max_workers=4,                   # Concurrent workers
)
```

## Error Handling

The connectors use custom exceptions for clear error handling:

```python
from connectors.exceptions import (
    ConnectorException,        # Base exception
    RateLimitException,        # Rate limit exceeded
    AuthenticationException,   # Authentication failed
    NotFoundException,         # Resource not found
    APIException,             # Generic API error
)

try:
    repos = connector.list_repositories()
except RateLimitException as e:
    print(f"Rate limit exceeded: {e}")
except AuthenticationException as e:
    print(f"Authentication failed: {e}")
except APIException as e:
    print(f"API error: {e}")
```

## Pagination

Pagination is handled automatically by the connectors:

```python
# Get all repositories (paginated automatically)
repos = connector.list_repositories()

# Limit results
repos = connector.list_repositories(max_repos=50)
```

For custom pagination control, use the utility classes:

```python
from connectors.utils import PaginationHandler

handler = PaginationHandler(per_page=100, max_pages=5, max_items=400)
results = handler.paginate_all(fetch_function)
```

## Rate Limiting

Rate limiting is handled automatically with exponential backoff:

```python
from connectors.utils import retry_with_backoff

@retry_with_backoff(max_retries=5, initial_delay=1.0, max_delay=60.0)
def api_call():
    # Your API call here
    pass
```

## GraphQL (GitHub)

The GitHub connector uses GraphQL for operations not well-supported by PyGithub:

```python
from connectors.utils import GitHubGraphQLClient

client = GitHubGraphQLClient(token="your_token")
result = client.query("""
    query {
        viewer {
            login
        }
    }
""")
```

## REST API (GitLab)

The GitLab connector uses REST API for blame and merge requests:

```python
from connectors.utils import GitLabRESTClient

client = GitLabRESTClient(
    base_url="https://gitlab.com/api/v4",
    private_token="your_token"
)
blame = client.get_file_blame(project_id=123, file_path="file.py")
```

## Examples

See the `examples/` directory for complete usage examples:

- `examples/github_example.py` - GitHub connector examples
- `examples/gitlab_example.py` - GitLab connector examples

Run examples:

```bash
export GITHUB_TOKEN=your_token
python examples/github_example.py

export GITLAB_TOKEN=your_token
python examples/gitlab_example.py
```

## Testing

Run tests:

```bash
# Run all connector tests
pytest tests/test_connectors_*.py -v

# Run specific test file
pytest tests/test_connectors_models.py -v

# Run with coverage
pytest tests/test_connectors_*.py --cov=connectors --cov-report=html
```

## Integration with Existing Storage

The connectors can be integrated with the existing storage system:

```python
from connectors import GitHubConnector
from storage import SQLAlchemyStore
from models.git import Repo, GitCommit

# Initialize connector and storage
connector = GitHubConnector(token="your_token")
async with SQLAlchemyStore(conn_string="postgresql+asyncpg://...") as store:
    # Get repositories
    repos = connector.list_repositories(max_repos=10)
    
    # Store in database
    for repo in repos:
        db_repo = Repo(
            repo_path=None,
            id=repo.id,
            repo=repo.full_name,
            settings={},
            tags=[]
        )
        await store.insert_repo(db_repo)
```

## Performance Considerations

- **Pagination**: Default page size is 100 (maximum for both GitHub and GitLab)
- **Rate Limits**: 
  - GitHub: 5000 requests/hour (authenticated)
  - GitLab: 10 requests/second (self-hosted may vary)
- **Concurrency**: Use `max_workers` parameter for parallel operations
- **Caching**: Consider caching results for frequently accessed data

## Best Practices

1. **Always use authentication tokens** for higher rate limits
2. **Handle rate limit exceptions** gracefully with retries
3. **Limit results** when testing to avoid unnecessary API calls
4. **Close connectors** when done to clean up resources
5. **Use context managers** where applicable
6. **Monitor rate limits** to avoid hitting limits

## License

This code is part of the mergestat-syncs project.
