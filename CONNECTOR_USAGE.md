# Using git_mergestat.py with GitHub and GitLab Connectors

The `git_mergestat.py` script now supports three modes of operation:

1. **Local Repository Mode** (default) - Analyzes a local git repository
2. **GitHub Connector Mode** - Fetches data from GitHub API
3. **GitLab Connector Mode** - Fetches data from GitLab API

## Local Repository Mode

This is the original mode that analyzes a local git repository.

```bash
# Using environment variables
export DB_CONN_STRING="postgresql+asyncpg://localhost:5432/mergestat"
export REPO_PATH="/path/to/repo"
python git_mergestat.py

# Using command-line arguments
python git_mergestat.py --db "postgresql+asyncpg://localhost:5432/mergestat" --repo-path "/path/to/repo"
```

## GitHub Connector Mode

Fetch repository data directly from GitHub without cloning. **Fully supports both public and private repositories**.

### Requirements

- GitHub personal access token with appropriate permissions
  - **For public repositories**: Any valid token (for higher rate limits)
  - **For private repositories**: Token must have `repo` scope
- Repository owner and name

### Usage

```bash
# Public repository
export DB_CONN_STRING="postgresql+asyncpg://localhost:5432/mergestat"
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
python git_mergestat.py --github-owner torvalds --github-repo linux

# Private repository (token must have 'repo' scope)
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"  # Token with 'repo' scope
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --github-owner your-org \
  --github-repo your-private-repo

# Using command-line arguments
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --github-token "ghp_xxxxxxxxxxxx" \
  --github-owner torvalds \
  --github-repo linux
```

### What Gets Stored

- Repository metadata (name, URL, default branch)
- Commits (up to 100 most recent)
- Commit statistics (additions, deletions per file for last 50 commits)

### Example with MongoDB

```bash
python git_mergestat.py \
  --db-type mongo \
  --db "mongodb://localhost:27017" \
  --github-token "ghp_xxxxxxxxxxxx" \
  --github-owner kubernetes \
  --github-repo kubernetes
```

## GitLab Connector Mode

Fetch project data directly from GitLab (including self-hosted instances). **Fully supports both public and private projects**.

### Requirements

- GitLab private token with appropriate permissions
  - **For public projects**: Token optional (but recommended for rate limits)
  - **For private projects**: Token must have `read_api` and `read_repository` scopes
- Project ID (numeric ID, not path)

### Usage

```bash
# Public project on GitLab.com
export DB_CONN_STRING="postgresql+asyncpg://localhost:5432/mergestat"
export GITLAB_TOKEN="glpat-xxxxxxxxxxxx"
python git_mergestat.py --gitlab-project-id 278964

# Private project (token must have 'read_api' and 'read_repository' scopes)
export GITLAB_TOKEN="glpat-xxxxxxxxxxxx"  # Token with required scopes
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --gitlab-project-id 12345

# Self-hosted GitLab
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --gitlab-token "glpat-xxxxxxxxxxxx" \
  --gitlab-url "https://gitlab.example.com" \
  --gitlab-project-id 123
```

### What Gets Stored

- Project metadata (name, URL, default branch)
- Commits (up to 100 most recent)
- Commit statistics (aggregate additions/deletions for last 50 commits)

### Finding Your Project ID

The project ID is the numeric identifier for your GitLab project:

1. Go to your project page on GitLab
2. Look under the project name - you'll see "Project ID: 12345"
3. Or use the GitLab API: `curl "https://gitlab.com/api/v4/projects/owner%2Fproject" --header "PRIVATE-TOKEN: <token>"`

## Environment Variables

All modes support these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_CONN_STRING` | Database connection string | None (required) |
| `DB_TYPE` | Database type (`postgres` or `mongo`) | `postgres` |
| `DB_ECHO` | Enable SQL logging | `false` |
| `MONGO_DB_NAME` | MongoDB database name | None |
| `BATCH_SIZE` | Records per batch insert | `100` |
| `MAX_WORKERS` | Parallel workers | `4` |
| `GITHUB_TOKEN` | GitHub personal access token | None |
| `GITLAB_TOKEN` | GitLab private token | None |
| `REPO_PATH` | Path to local repository | `.` |

## Command-Line Arguments

```
usage: git_mergestat.py [-h] [--db DB] [--repo-path REPO_PATH] 
                        [--db-type {postgres,mongo}] [--start-date START_DATE]
                        [--end-date END_DATE] [--github-token GITHUB_TOKEN] 
                        [--github-owner GITHUB_OWNER] [--github-repo GITHUB_REPO]
                        [--gitlab-token GITLAB_TOKEN] [--gitlab-url GITLAB_URL]
                        [--gitlab-project-id GITLAB_PROJECT_ID]

Options:
  --db DB                       Database connection string
  --repo-path REPO_PATH        Path to the local git repository
  --db-type {postgres,mongo}   Database backend to use
  --github-token TOKEN         GitHub personal access token
  --github-owner OWNER         GitHub repository owner/organization
  --github-repo REPO           GitHub repository name
  --gitlab-token TOKEN         GitLab private token
  --gitlab-url URL             GitLab instance URL (default: https://gitlab.com)
  --gitlab-project-id ID       GitLab project ID (numeric)
```

## Examples

### Analyze Linux Kernel from GitHub

```bash
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --github-token "$GITHUB_TOKEN" \
  --github-owner torvalds \
  --github-repo linux
```

### Analyze GitLab's GitLab from GitLab.com

```bash
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --gitlab-token "$GITLAB_TOKEN" \
  --gitlab-project-id 278964
```

### Analyze Local Repository (Original Mode)

```bash
python git_mergestat.py \
  --db "postgresql+asyncpg://localhost:5432/mergestat" \
  --repo-path "/home/user/my-project"
```

## Limitations

### GitHub Connector Mode

- Fetches up to 100 most recent commits
- Commit stats limited to last 50 commits (API rate limits)
- Does not fetch blame data (requires per-file API calls)
- Does not fetch file contents

### GitLab Connector Mode

- Fetches up to 100 most recent commits
- Commit stats limited to last 50 commits (API rate limits)
- Stores aggregate stats per commit (not per-file breakdowns)
- Does not fetch blame data (requires per-file API calls)
- Does not fetch file contents

### Rate Limits

- **GitHub**: 5,000 requests/hour for authenticated users
- **GitLab**: 10 requests/second (self-hosted may vary)

Both connectors include automatic retry with exponential backoff for rate limit handling.

## Troubleshooting

### "Connectors are not available"

The connectors require additional dependencies. Install them:

```bash
pip install PyGithub python-gitlab
```

Or install all requirements:

```bash
pip install -r requirements.txt
```

### GitHub Authentication Errors

Ensure your token has the appropriate scopes:
- **`repo`** - Full control of private repositories (REQUIRED for private repos)
- **`read:org`** - Read org and team membership (recommended for organization repos)

**For private repositories**: The `repo` scope is mandatory. Without it, you'll receive 404 errors when trying to access private repositories.

**To verify your token scopes**:
1. Go to https://github.com/settings/tokens
2. Find your token and check which scopes are selected
3. If `repo` is not checked, generate a new token with this scope

### GitLab Authentication Errors

Ensure your token has the appropriate scopes:
- **`read_api`** - Read access to API (REQUIRED for private projects)
- **`read_repository`** - Read repository data (REQUIRED for private projects)

**For private projects**: Both scopes are mandatory. Without them, you'll receive authentication or permission errors.

**To verify your token permissions**:
1. Go to your GitLab instance Settings → Access Tokens
2. Review the token's scopes
3. If needed, create a new token with `read_api` and `read_repository` scopes

### Finding Repository/Project IDs

**GitHub**: Use owner/repo format (e.g., `torvalds/linux`)
**GitLab**: Use the numeric project ID (e.g., `278964`)

## Integration with Existing Workflows

The connector modes integrate seamlessly with the existing storage system:

- All data is stored in the same database schema
- Repository metadata tagged with source (`github` or `gitlab`)
- Can mix local and remote repositories in the same database
- Query data the same way regardless of source

## Performance Considerations

- **Local mode**: Best for comprehensive analysis (files, blame, full history)
- **GitHub/GitLab modes**: Faster for basic commits and stats
- **API rate limits**: GitHub and GitLab have rate limits; local mode has none
- **Network dependency**: Connector modes require internet access

Choose the appropriate mode based on your needs:
- Use **local mode** for complete repository analysis
- Use **connector modes** for quick commit and stats analysis without cloning
