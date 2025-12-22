# AGENTS.md — Guidance for AI coding agents

Purpose: compact, actionable rules for an AI coding agent (Copilot-like) working in this repository.

- Start by reading `cli.py`, `processors/local.py`, and `connectors/__init__.py` to understand boundaries.
- Prefer minimal, surgical changes. Use `replace` or `write_file` for edits and keep surrounding style.
- Use `codebase_investigator` for planning complex changes or understanding the system.

## Architecture & flows

- `cli.py` dispatches `local`, `github`, and `gitlab` sync flows and calls the processors.
- Local sync orchestration lives in `processors/local.py` (`process_local_repo`).
- Connectors live in `connectors/` and must handle pagination, rate-limits, and provide batch helpers.
- Processors in `processors/` implement the pipeline: commits → PRs → commit-stats → files/blame.
- Fixtures in `fixtures/` generate synthetic data for testing/demos.
- Implementation plans, metrics inventory, and requirement details live in `docs/project.md`, `docs/metrics-inventory.md`, and `docs/roadmap.md`.

## Developer workflows

- Run the sync:

```bash
python cli.py sync local --db "<DB_CONN>" --repo-path /path/to/repo
```

- Generate synthetic data:

```bash
python cli.py fixtures generate --db "<DB_CONN>" --days 30
```

- Compute complexity metrics (batch mode):

```bash
python cli.py metrics complexity --repo-path . -s "*"
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
assert match_repo_pattern('chrisgeo/dev-health-ops', 'chrisgeo/m*')
```

- `process_github_repos_batch` usage (processors/github.py):

```py
await process_github_repos_batch(store, token="$GITHUB_TOKEN", org_name="myorg", pattern="myorg/*", batch_size=10, max_concurrent=4)
```

### Debugging tips

- If a test fails intermittently, check network-dependent tests and toggle `CONNECTORS_AVAILABLE` or mock connector clients to isolate logic.
- For rate-limit issues, inspect logs for `Retry-After` or `X-RateLimit-Reset` and ensure code uses `RateLimitGate`/`RateLimitConfig` to backoff.

---
**Note for AI Agents**: Always update this document, along with `GEMINI.md`, `.github/copilot-instructions.md`, `docs/roadmap.md`, `docs/project.md`, and `docs/metrics-inventory.md` whenever a task is completed or a feature is modified to maintain an accurate system context.
