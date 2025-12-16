# AGENTS.md — Guidance for AI coding agents

Purpose: compact, actionable rules for an AI coding agent (Copilot-like) working in this repository.

- Start by reading `git_mergestat.py` and `connectors/__init__.py` to understand boundaries.
- Prefer minimal, surgical changes. Use `apply_patch` for edits and keep surrounding style.
- Use `manage_todo_list` to create and track multi-step tasks; mark steps as you go.

## Architecture & flows

- `git_mergestat.py` dispatches `local`, `github`, and `gitlab` flows.
- Connectors live in `connectors/` and must handle pagination, rate-limits, and provide batch helpers.
- Processors in `processors/` implement the pipeline: commits → PRs → commit-stats → files/blame.

## Developer workflows

- Run the sync:

```bash
python git_mergestat.py --db "<DB_CONN>" --connector local --repo-path /path/to/repo
```

- Run tests: `pytest -q` or `pytest tests/test_github_connector.py -q`.
- Apply Postgres migrations: `alembic upgrade head` (use docker compose if needed).

## Conventions & rules for agents

- CLI args override env vars (`DB_CONN_STRING`, `DB_TYPE`, `GITHUB_TOKEN`, `GITLAB_TOKEN`, `REPO_PATH`).
- Performance knobs: `BATCH_SIZE` and `MAX_WORKERS`.
- Prefer async batch helpers for network I/O. Respect `RateLimitGate` backoff in connectors/processors.
- Do not commit secrets. Use environment variables for tokens in examples only.

## When adding code

- Export new connectors in `connectors/__init__.py`.
- Add unit tests under `tests/` and run `pytest` locally.
- If changing DB models, add/adjust Alembic migrations and run `alembic upgrade head` in dev.

If you'd like, I can insert short code examples from `connectors/github.py` or `processors/github.py` into this file. Which would you prefer?

### Quick code snippets

- `match_repo_pattern` example (connectors/github.py):

```py
from connectors.github import match_repo_pattern
assert match_repo_pattern('chrisgeo/mergestat-syncs', 'chrisgeo/m*')
```

- `process_github_repos_batch` usage (processors/github.py):

```py
await process_github_repos_batch(store, token="$GITHUB_TOKEN", org_name="myorg", pattern="myorg/*", batch_size=10, max_concurrent=4)
```

### Debugging tips

- If a test fails intermittently, check network-dependent tests and toggle `CONNECTORS_AVAILABLE` or mock connector clients to isolate logic.
- For rate-limit issues, inspect logs for `Retry-After` or `X-RateLimit-Reset` and ensure code uses `RateLimitGate`/`RateLimitConfig` to backoff.
