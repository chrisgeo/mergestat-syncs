# Daily Metrics (v1)

This repository can compute daily, developer-oriented metrics from the synced Git data (`git_commits`, `git_commit_stats`, `git_pull_requests`) and write the derived time-series to **ClickHouse** (preferred) and/or **MongoDB** (alternative) for Grafana querying.

## What Gets Computed

All metrics are computed per UTC day.

### Commit size bucketing
- `total_loc = additions + deletions` (summed from `git_commit_stats`)
- bucket:
  - `small`: `total_loc <= 50`
  - `medium`: `51..300`
  - `large`: `> 300`

### Daily user metrics (`user_metrics_daily`)
Keyed by `(repo_id, author_email, day)` where `author_email` falls back to `author_name` when email is missing.
- Commits: counts, LOC added/deleted, distinct files changed (union across the day), large commits, avg commit size
- PRs: authored (created that day), merged (merged that day), avg/median PR cycle hours (for PRs merged that day)
- Review-response fields are placeholders for future work and are stored as `0`.

### Daily repo metrics (`repo_metrics_daily`)
Keyed by `(repo_id, day)`.
- Commits: count, LOC touched, avg size, large commit ratio
- PRs: merged count, median PR cycle hours (for PRs merged that day)

### Optional per-commit metrics (`commit_metrics`)
Keyed by `(repo_id, day, author_email, commit_hash)`.

## Storage Targets

### ClickHouse (tables)
Tables are created automatically if missing:
- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`

They are `MergeTree` tables partitioned by `toYYYYMM(day)` and ordered by the natural keys for Grafana queries.

Re-computations are **append-only** and distinguished by `computed_at`. To query the latest metrics for a key/day, use `argMax(..., computed_at)` in ClickHouse.

### MongoDB (collections)
Collections are created automatically:
- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`

Documents use stable compound `_id` keys and are written via upserts, so recomputation is safe.

### SQLite (tables)
Tables are created automatically in the same `.db` file:
- `repo_metrics_daily`
- `user_metrics_daily`
- `commit_metrics`

## Running The Daily Job

The job reads source data from the **same backend** you point it at (ClickHouse or MongoDB), using the synced tables/collections:
- `git_commits`
- `git_commit_stats`
- `git_pull_requests`
It also supports SQLite, reading the same tables and writing metrics tables into the same `.db` file.

### Environment variables
- `DB_CONN_STRING` (or `DATABASE_URL`): ClickHouse or MongoDB URI for both reading source data and writing derived metrics.
- `MONGO_DB_NAME` (optional): MongoDB database name if not provided in the URI (defaults to the URI default db or `mergestat`).

### Examples
- Compute one day (backend inferred from `--db` or `DB_CONN_STRING`):
  - `python scripts/compute_metrics_daily.py --date 2025-02-01 --db clickhouse://localhost:8123/default`
  - `python scripts/compute_metrics_daily.py --date 2025-02-01 --db mongodb://localhost:27017/mergestat`
  - `python scripts/compute_metrics_daily.py --date 2025-02-01 --db sqlite:///./mergestat.db`
- Compute 7-day backfill ending at a date:
  - `python scripts/compute_metrics_daily.py --date 2025-02-01 --backfill 7 --db clickhouse://localhost:8123/default`
- Filter to one repository:
  - `python scripts/compute_metrics_daily.py --date 2025-02-01 --repo-id <uuid> --db clickhouse://localhost:8123/default`

## Dependencies

- ClickHouse uses `clickhouse-connect` (already in `requirements.txt`).
- MongoDB uses `pymongo` (available via the `motor` dependency in `requirements.txt`).
- SQLite uses `sqlalchemy` (already in `requirements.txt`).
