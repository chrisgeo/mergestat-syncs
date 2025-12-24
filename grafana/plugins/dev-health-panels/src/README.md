# Dev Health Panels

Opinionated Grafana panel plugin for teaching developer health through visual signals (not scores).

## Panels

- **Developer Landscape**: quadrant scatter for normalized tradeoffs (0.5 / 0.5 boundaries).
- **Hotspot Explorer**: table with sparkline trends and donut drivers per file.
- **Investment Flow**: Sankey-style flow of effort from investment areas to outcomes.

## Dashboard Configuration

The following queries and variables are recommended for use with the standard `dev-health-ops` dashboards.

### Developer Landscape

**Panel Query:**
*Set Format to **Table** in the query options.*
```sql
SELECT
  max(v.as_of_day) as as_of_day,
  map_name,
  team_id,
  identity_id,
  argMax(x_raw, v.as_of_day) as x_raw,
  argMax(y_raw, v.as_of_day) as y_raw,
  argMax(x_norm, v.as_of_day) as x_norm,
  argMax(y_norm, v.as_of_day) as y_norm
FROM stats.v_ic_landscape_points AS v
WHERE map_name = 'churn_throughput' -- REPLACE with 'cycle_throughput' or 'wip_throughput'
  AND v.as_of_day >= toDate(toDateTime(intDiv($__from, 1000)))
  AND v.as_of_day < toDate(toDateTime(intDiv($__to, 1000)))
  AND team_id IN (${team_id:sqlstring})
GROUP BY map_name, team_id, identity_id
ORDER BY team_id, identity_id
```

### Hotspot Explorer (table + sparklines)

**Variables:**
- `repo_id` (Query): `SELECT repo AS __text, repo AS __value FROM repos ORDER BY repo` (Multi + IncludeAll + Regex)

**Panel Query A (Facts):**
```sql
SELECT
    metrics.repo_id AS repo_id,
    metrics.path AS file_path,
    sum(metrics.churn) AS churn_loc_30d, -- Corrected Alias
    lookup.cyclomatic_total AS cyclomatic_total,
    lookup.ownership_concentration AS ownership_concentration,
    0 AS incident_count,
    log1p(churn_loc_30d) AS churn_signal,
    log1p(coalesce(cyclomatic_total, 0)) AS complexity_signal,
    coalesce(ownership_concentration, 0) AS ownership_signal,
    log1p(incident_count) AS incident_signal,
    (0.5 * churn_signal + 0.3 * complexity_signal + 0.2 * ownership_signal) AS risk_score
FROM stats.file_metrics_daily AS metrics
INNER JOIN repos AS r ON r.id = metrics.repo_id
LEFT JOIN (
    SELECT
        repo_id,
        file_path,
        argMax(cyclomatic_total, computed_at) AS cyclomatic_total,
        argMax(blame_concentration, computed_at) AS ownership_concentration
    FROM stats.file_hotspot_daily
    GROUP BY repo_id, file_path
) AS lookup
    ON lookup.repo_id = metrics.repo_id
    AND lookup.file_path = metrics.path
WHERE match(r.repo, '${repo_id:regex}')
  AND metrics.day >= toDate(toDateTime(intDiv($__from, 1000)))
  AND metrics.day < toDate(toDateTime(intDiv($__to, 1000)))
GROUP BY metrics.repo_id, metrics.path, lookup.cyclomatic_total, lookup.ownership_concentration
ORDER BY risk_score DESC
LIMIT 50
```

**Panel Query B (Sparklines):**
```sql
SELECT
    metrics.path AS file_path,
    metrics.day,
    sum(metrics.churn) AS churn_loc
FROM stats.file_metrics_daily AS metrics
INNER JOIN repos AS r ON r.id = metrics.repo_id
WHERE match(r.repo, '${repo_id:regex}')
  AND metrics.day >= toDate(toDateTime(intDiv($__from, 1000)))
  AND metrics.day < toDate(toDateTime(intDiv($__to, 1000)))
GROUP BY metrics.path, metrics.day
ORDER BY file_path, day
```

### Investment Flow (Sankey)
 
 **Variables:**
 - `team_id` (Query): `SELECT DISTINCT team_id AS __text, team_id AS __value FROM (SELECT team_id FROM stats.team_metrics_daily UNION ALL SELECT team_id FROM stats.investment_metrics_daily) ORDER BY team_id` (Multi + IncludeAll)
 
 **Panel Query:**
 ```sql
 SELECT
     source,
     target,
     sum(delivery_units) AS value
 FROM stats.v_investment_flow_edges
 WHERE team_id IN (${team_id:sqlstring})
   AND day >= toDate(toDateTime(intDiv($__from, 1000)))
   AND day < toDate(toDateTime(intDiv($__to, 1000)))
 GROUP BY source, target
 HAVING value > 0
 ORDER BY value DESC
 ```

## Development

```bash
npm install
npm run dev
```

## Build

```bash
npm run build
```