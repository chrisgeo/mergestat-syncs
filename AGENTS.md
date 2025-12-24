# AGENTS.md — Guidance for AI coding agents

Purpose: compact, actionable rules for an AI coding agent (Copilot-like) working in this repository.

- Start by reading `cli.py`, `processors/local.py`, and `connectors/__init__.py` to understand boundaries.
- Prefer minimal, surgical changes. Use `replace` or `write_file` for edits and keep surrounding style.
- Use `codebase_investigator` for planning complex changes or understanding the system.

## Architecture & flows

# Gemini Context: dev-health-ops

This file provides a high-level overview and context for Gemini to understand the `dev-health-ops` project.

## Project Mission

`dev-health-ops` is an open-source development team operation analytics platform. Its goal is to provide tools and implementations for tracking developer health and operational metrics by integrating with popular tools like GitHub, GitLab, Jira, and local Git repositories.

## Architecture Overview

The project follows a pipeline-like architecture:

1.  **Connectors (`connectors/`)**: Fetch raw data from providers (GitHub, GitLab, Jira).
2.  **Processors (`processors/`)**: Normalize and process the raw data.
3.  **Storage (`storage.py`, `models/`)**: Persist processed data into various backends (also provides unified read helpers like `get_complexity_snapshots`).
4.  **Metrics (`metrics/`)**: Compute high-level metrics (e.g., throughput, cycle time, rework, bus factor, predictability) from the stored data.
5.  **Visualization (`grafana/`)**: Provision Grafana dashboards to visualize the computed metrics.
    - Investment Areas dashboard filters teams via `match(..., '${team_id:regex}')`.
    - Dashboard team filters normalize `team_id` with `ifNull(nullIf(team_id, ''), 'unassigned')` to include legacy NULL/empty values.
    - Investment metrics store NULL team_id for unassigned; the investment flow view casts with `toNullable(team_id)`.
    - Hotspot Explorer queries should use table format and order by day to satisfy Grafana time sorting.
    - Hotspot Explorer panel selects the facts frame by requiring `churn_loc_30d` to avoid binding to the sparkline frame.
    - Hotspot ownership concentration uses `git_blame` max-lines share per file.
    - Synthetic fixtures cover a broader file set to improve blame/ownership coverage.
    - IC Drilldown includes a Churn vs Throughput panel filtered by `identity_id`.
    - Blame-only sync is available via `cli.py sync <local|github|gitlab> --blame-only`.
    - GitHub/GitLab backfills (`--date/--backfill`) default to unlimited commits unless `--max-commits-per-repo` is set.
    - Dev Health panel plugin applies a plugin-local Grafana theme (via `createTheme`) with a custom visualization palette.

## Key Technologies

- **Language**: Python 3.10+
- **CLI Framework**: Python's `argparse` (implemented in `cli.py`)
- **Databases**:
  - **PostgreSQL**: Primary relational store (uses `SQLAlchemy` + `Alembic`).
  - **ClickHouse**: Analytics store for high-performance metric queries.
  - **MongoDB**: Document-based store (uses `motor`).
  - **SQLite**: Local file-based store (uses `aiosqlite`).
- **APIs**: `PyGithub`, `python-gitlab`, `jira`.
- **Git**: `GitPython` for local repository analysis.
- **Validation**: `pydantic`.

## Project Structure

- `cli.py`: Main entry point for the tool.
- `storage.py`: Unified storage interface for all supported databases.
- `connectors/`: Provider-specific logic for data fetching.
- `metrics/`: Core logic for computing DORA and other team health metrics.
- `models/`: SQLAlchemy and Pydantic models for data structures (includes `models/teams.py`).
- `processors/`: Logic to bridge connectors and storage.
- `providers/`: Mapping and identity management logic.
- `grafana/`: Configuration for automated Grafana setup.
- `grafana/plugins/dev-health-panels`: Panel plugin with Developer Landscape, Hotspot Explorer, and Investment Flow views; mode selection happens in panel settings, backed by ClickHouse views in the `stats` schema.
- ClickHouse view definitions use `WITH ... AS` aliasing (avoid `WITH name = expr` syntax).
- `alembic/`: Database migration scripts for PostgreSQL.
- `fixtures/`: Synthetic data generation for testing and demos.
- `tests/`: Comprehensive test suite covering connectors, metrics, and storage.

## Development Workflow

- **Syncing Data**: `python cli.py sync <provider> --db <connection_string> ...`
- **Syncing Teams**: `python cli.py sync teams --provider <config|jira|synthetic> --db <connection_string> ...`
- **Syncing Work Items**: `python cli.py sync work-items --provider <jira|github|gitlab|synthetic|all> -s "<org/*>" --db <connection_string> ...` (use `--auth` to override `GITHUB_TOKEN`/`GITLAB_TOKEN`)
- **Planned**: repo filtering for `sync work-items` by tags/settings (beyond name glob).
- **Computing Metrics**: `python cli.py metrics daily --db <connection_string> ...` (expects work items already synced unless `--provider` is set)
- **Complexity Metrics**: `python cli.py metrics complexity --repo-path . -s "*" --db <connection_string> ...`
- **Generating Data**: `python cli.py fixtures generate --db <connection_string> ...`
- **Visualization**: `python cli.py grafana up` to start the dashboard stack.
- **Testing**: Run `pytest` to execute the test suite.

## Important Context

- **Private Repos**: Full support for private GitHub/GitLab repos via tokens.
- **Batch Processing**: Connectors support pattern matching and concurrent batch processing.
- **Database Agnostic**: Most commands support switching between DB types using the `--db` flag or `DB_CONN_STRING` env var.
- **Metrics Computation**: Can be run daily or backfilled for a period.
- **Plans & Requirements**: Implementation plans in `docs/project.md`, metrics inventory in `docs/metrics-inventory.md`, the roadmap in `docs/roadmap.md`.

## Developer workflows

- Run the sync:

```bash
python cli.py sync git --provider local --db "<DB_CONN>" --repo-path /path/to/repo
```

- Generate synthetic data:

```bash
python cli.py fixtures generate --db "<DB_CONN>" --days 30
```

- Sync work items (provider APIs → work item tables):

```bash
python cli.py sync work-items --provider github --auth "$GITHUB_TOKEN" -s "org/*" --db "<DB_CONN>" --date 2025-02-01 --backfill 30
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
- Close SQLAlchemyStore engines in tests (or use context managers) to avoid aiosqlite event-loop teardown warnings.

## When adding code

- Export new connectors in `connectors/__init__.py`.
- Add unit tests under `tests/` and run `pytest` locally.
- If changing DB models, add/adjust Alembic migrations and run `alembic upgrade head` in dev.
