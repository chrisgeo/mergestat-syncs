## Purpose

This file orients AI coding agents to the mergestat-syncs repository: key architecture, developer workflows, conventions, and integration points so you can be productive immediately.

**Quick start & workflows**

**Big-picture architecture**

**Project-specific conventions & patterns**

**How to add or modify connectors**

**Connectors — concrete examples & notes**

Concrete examples & quick commands

- Pattern match example (from `connectors/github.py`):

```py
from connectors.github import match_repo_pattern
assert match_repo_pattern('chrisgeo/mergestat-syncs', 'chrisgeo/m*')
```

- Batch processing example (use the processor helper):

```py
from processors.github import process_github_repos_batch
await process_github_repos_batch(store, token="$GITHUB_TOKEN", org_name="myorg", pattern="myorg/*", batch_size=10, max_concurrent=4, use_async=True)
```

- Quick sqlite local run (no migrations):

```bash
export DB_CONN_STRING="sqlite+aiosqlite:///mergestat.db"
python git_mergestat.py --db "$DB_CONN_STRING" --connector local --repo-path .
```

- Quick Postgres run (dev):

```bash
docker compose up postgres -d
export DB_CONN_STRING="postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
alembic upgrade head
python git_mergestat.py --db "$DB_CONN_STRING" --connector local --repo-path .
```

Common pitfalls & heuristics

- Tests sometimes toggle `CONNECTORS_AVAILABLE` to exercise local-only code paths — prefer adding connector unit tests that mock connector clients instead of changing that flag widely.
- PyGithub lazy properties (e.g., `commit.stats`) may trigger per-commit API calls — processors try to avoid those in batch mode; follow existing patterns in `processors/github.py` when adding commit-stat logic.
- When iterating large result sets (PRs, commits), use `RateLimitGate`/`RateLimitConfig` to apply backoff rather than naive sleeps; see `processors/github.py` for examples.
- Keep DB writes batched (use `BATCH_SIZE`) and avoid concurrent writes on SQLite.

Where to look first

- For onboarding: `git_mergestat.py`, `connectors/__init__.py`, `processors/github.py`, `storage.py`, `tests/test_github_connector.py`.
- For connector patterns: `connectors/github.py` and `connectors/gitlab.py`.
- For DB model shapes: `models/` and `alembic/versions`.

**DB & migrations (expanded)**

- Postgres requires Alembic migrations before first use. From project root run:

```bash
docker compose up postgres -d
alembic upgrade head
```

- If using a local Postgres, set `DB_CONN_STRING` to a `postgresql+asyncpg://` URL and run the same `alembic upgrade head` command.
- For quick development, `sqlite+aiosqlite:///mergestat.db` requires no migrations; set `DB_TYPE=sqlite` and `DB_CONN_STRING` accordingly.

**Testing (expanded)**

- Run the whole test suite with `pytest -q` from the repository root.
- Run a focused test (fast feedback) example:

```bash
pytest tests/test_github_connector.py -q
```

- CI-like checks: run linters/formatters listed in `requirements.txt` (e.g., `black`, `ruff`) if present. Run `black .` and `ruff .` locally.

**Implementation notes discovered in code**

- `processors/github.py` uses a `CONNECTORS_AVAILABLE` flag to fall back when `connectors` are not installed — tests and local runs may toggle this. If you add a new connector, ensure tests import hooks or mocks account for this flag.
- `git_mergestat.py` demonstrates how local mode composes the pipeline: `process_git_commits`, `process_local_pull_requests`, `process_git_commit_stats`, then `process_files_and_blame`. Keep that ordering for correctness when adding or modifying flow.

**Key files to inspect for common tasks**

- Entry / CLI: [git_mergestat.py](../git_mergestat.py#L1-L120)
- Connectors surface: [connectors/**init**.py](../connectors/__init__.py#L1-L40)
- Core processors: [processors/](../processors/)
- Storage/backends: [storage.py](../storage.py#L1-L200)
- DB migrations: [alembic/](../alembic/)
- Tests: [tests/](../tests/)

**Testing and quick checks**

- Unit tests: `pytest -q`.
- Run single test file: `pytest tests/test_github_connector.py -q`.
- Lint/format: project uses standard Python tooling (see `requirements.txt`). Run `black`/`isort` if present in your environment.

**Integration points & external dependencies**

- GitHub / GitLab APIs: tokens required (`GITHUB_TOKEN`, `GITLAB_TOKEN`). Tokens require repository scopes listed in README.
- Databases: supports `postgres`, `mongo`, `sqlite`, `clickhouse` (auto-detected from URL). Use `alembic` for Postgres migrations.
- Local git: uses `gitpython` for local-mode iteration.
