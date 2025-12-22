## Purpose

This file orients AI coding agents to the dev-health-ops repository: key architecture, developer workflows, conventions, and integration points so you can be productive immediately.

**Quick start & workflows**

- Local sync: `python cli.py sync local --db "$DB_CONN_STRING" --repo-path .`
- GitHub sync: `python cli.py sync github --db "$DB_CONN_STRING" --owner <owner> --repo <repo>`
- GitLab sync: `python cli.py sync gitlab --db "$DB_CONN_STRING" --project-id <id>`
- Work items sync: `python cli.py sync work-items --provider jira|github|gitlab|synthetic|all -s "org/*" --date YYYY-MM-DD --db "$DB_CONN_STRING"` (use `--auth` for GitHub/GitLab token override)
- Planned: repo filtering for `sync work-items` by tags/settings (beyond name glob).
- Metrics: `python cli.py metrics daily --date YYYY-MM-DD --db "$DB_CONN_STRING"` (uses already-synced work items; `--provider` remains for backward-compatible fetch+compute)
- Complexity: `python cli.py metrics complexity -s "*" --backfill 30`
- Fixtures: `python cli.py fixtures generate --db "$DB_CONN_STRING" --days 30`

**Big-picture architecture**

- `cli.py` dispatches sync/metrics flows and calls processors directly.
- `processors/` implement data pipelines; `processors/local.py` orchestrates local sync.
- `connectors/` wrap GitHub/GitLab API access with pagination + rate limits.
- `storage.py` abstracts DB backends (Postgres/Mongo/SQLite/ClickHouse), including unified reads like `get_complexity_snapshots`.
- `utils.py` holds shared helpers (parsing, git iteration, file filtering).
- `fixtures/` generates synthetic data for testing.

**Project-specific conventions & patterns**

- CLI args override env vars (`DB_CONN_STRING`, `DB_TYPE`, `GITHUB_TOKEN`, `GITLAB_TOKEN`, `REPO_PATH`).
- Performance knobs: `BATCH_SIZE` and `MAX_WORKERS`.
- Prefer async batch helpers for network I/O; respect `RateLimitGate` backoff.
- Keep DB writes batched; avoid concurrent writes with SQLite.
- Implementation plans, metrics inventory, and requirement details: `docs/project.md`, `docs/metrics-inventory.md`, `docs/roadmap.md`.

**How to add or modify connectors**

- Export new connectors in `connectors/__init__.py`.
- Follow existing pagination/rate limit patterns in `connectors/github.py` and `connectors/gitlab.py`.

**Connectors — concrete examples & notes**

Concrete examples & quick commands

- Pattern match example (from `connectors/github.py`):

```py
from connectors.github import match_repo_pattern
assert match_repo_pattern('chrisgeo/dev-health-ops', 'chrisgeo/m*')
```

- Batch processing example (use the processor helper):

```py
from processors.github import process_github_repos_batch
await process_github_repos_batch(store, token="$GITHUB_TOKEN", org_name="myorg", pattern="myorg/*", batch_size=10, max_concurrent=4, use_async=True)
```

- Quick sqlite local run (no migrations):

```bash
export DB_CONN_STRING="sqlite+aiosqlite:///mergestat.db"
python cli.py sync local --db "$DB_CONN_STRING" --repo-path .
```

- Quick Postgres run (dev):

```bash
docker compose up postgres -d
export DB_CONN_STRING="postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
alembic upgrade head
python cli.py sync local --db "$DB_CONN_STRING" --repo-path .
```

Common pitfalls & heuristics

- Tests sometimes toggle `CONNECTORS_AVAILABLE` to exercise local-only code paths — prefer adding connector unit tests that mock connector clients instead of changing that flag widely.
- PyGithub lazy properties (e.g., `commit.stats`) may trigger per-commit API calls — processors try to avoid those in batch mode; follow existing patterns in `processors/github.py` when adding commit-stat logic.
- When iterating large result sets (PRs, commits), use `RateLimitGate`/`RateLimitConfig` to apply backoff rather than naive sleeps; see `processors/github.py` for examples.
- Keep DB writes batched (use `BATCH_SIZE`) and avoid concurrent writes on SQLite.

Where to look first

- For onboarding: `cli.py`, `processors/local.py`, `connectors/__init__.py`, `processors/github.py`, `storage.py`, `tests/test_github_connector.py`.
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

- CI-like checks: run linters/formatters listed in `requirements.txt` (e.g., `black`, `isort`, `flake8`) if present.

**Implementation notes discovered in code**

- `processors/github.py` uses a `CONNECTORS_AVAILABLE` flag to fall back when `connectors` are not installed — tests and local runs may toggle this. If you add a new connector, ensure tests import hooks or mocks account for this flag.
- `processors/local.py` composes the local pipeline: `process_git_commits`, `process_local_pull_requests`, `process_git_commit_stats`, then `process_files_and_blame`. Keep that ordering for correctness when changing flow.

**Key files to inspect for common tasks**

- Entry / CLI: [cli.py](../cli.py#L1-L200)
- Local orchestration: [processors/local.py](../processors/local.py#L1-L200)
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

---
**Note for AI Agents**: Always update this document, along with `GEMINI.md`, `AGENTS.md`, `docs/roadmap.md`, `docs/project.md`, and `docs/metrics-inventory.md` whenever a task is completed or a feature is modified to maintain an accurate system context.
