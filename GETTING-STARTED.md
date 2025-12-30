# Getting Started

## Demo data (synthetic, end-to-end)

Generate a complete demo dataset: teams, git facts, work items, and derived metrics
(including issue type, investment, hotspots, IC, CI/CD, and complexity).

```bash
dev-hops fixtures generate \
  --db "<DB_CONN>" \
  --days 30 \
  --with-metrics
```

Notes:
- `dev-hops` is available after `pip install dev-health-ops` or `pip install -e .`.
- Synthetic teams are inserted automatically.
- `--with-metrics` writes the same derived tables as `metrics daily`, plus
  complexity snapshots and hotspot risk metrics for demo dashboards.

## Performance tuning

Fixture generation uses batched inserts with concurrency for large datasets.
Tune these environment variables when you need faster loads:

- `BATCH_SIZE` (default: 1000) controls insert batch size.
- `MAX_WORKERS` (default: 4) controls concurrent insert workers (non-SQL backends).
