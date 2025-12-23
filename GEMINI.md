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
- `models/`: SQLAlchemy and Pydantic models for data structures.
- `processors/`: Logic to bridge connectors and storage.
- `providers/`: Mapping and identity management logic.
- `grafana/`: Configuration for automated Grafana setup.
- `grafana/plugins/dev-health-panels`: Panel plugin with Developer Landscape, Hotspot Explorer, and Investment Flow views, backed by ClickHouse views in the `stats` schema.
- ClickHouse view definitions use `WITH ... AS` aliasing (avoid `WITH name = expr` syntax).
- `alembic/`: Database migration scripts for PostgreSQL.
- `fixtures/`: Synthetic data generation for testing and demos.
- `tests/`: Comprehensive test suite covering connectors, metrics, and storage.

## Development Workflow
- **Syncing Data**: `python cli.py sync <provider> --db <connection_string> ...`
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
- **Plans & Requirements**: Implementation plans in `docs/project.md`, metrics inventory in `docs/metrics-inventory.md`, requirements/roadmap in `docs/roadmap.md`.

---
**Note for AI Agents**: Always update this document, along with `AGENTS.md`, `.github/copilot-instructions.md`, `docs/roadmap.md`, `docs/project.md`, and `docs/metrics-inventory.md` whenever a task is completed or a feature is modified to maintain an accurate system context.
