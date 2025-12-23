# Dev Health Panels

Opinionated Grafana panel plugin for teaching developer health through visual signals (not scores).

## Panels

- Developer Landscape: quadrant scatter for normalized tradeoffs (0.5 / 0.5 boundaries).
- Hotspot Explorer: table with sparkline trends and donut drivers per file.
- Investment Flow: Sankey-style flow of effort from investment areas to outcomes.

## Development

```bash
npm install
npm run dev
```

## Build

```bash
npm run build
```

## Unsigned plugin

This plugin loads unsigned. Ensure Grafana has `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=devhealth-dev-health-panels-panel`.

## ClickHouse schema contracts (stats)

These panels read from ClickHouse views in the `stats` schema. In the current schema, `file_metrics_daily.path` maps to `file_path`, `file_metrics_daily.churn` maps to `churn_loc`, and `file_hotspot_daily.blame_concentration` maps to `ownership_concentration`.

### Developer Landscape (quadrant scatter)

View:

```sql
CREATE OR REPLACE VIEW stats.v_ic_landscape_points AS
SELECT as_of_day, map_name, team_id, identity_id, x_raw, y_raw, x_norm, y_norm
FROM stats.ic_landscape_rolling_30d;
```

Panel query template:

```sql
SELECT as_of_day, map_name, team_id, identity_id, x_raw, y_raw, x_norm, y_norm
FROM stats.v_ic_landscape_points
WHERE map_name = {map_name:String}
  AND as_of_day >= toDate({from:DateTime}) AND as_of_day < toDate({to:DateTime})
  AND ({team_id:String} = '' OR team_id = {team_id:String})
ORDER BY team_id, identity_id;
```

### Hotspot Explorer (table + sparklines)

Windowed facts view (30d rolling window, using existing schema mappings):

```sql
CREATE OR REPLACE VIEW stats.v_file_hotspots_windowed AS
WITH toDate(now()) - toIntervalDay(30) AS start_day, toDate(now()) AS end_day
SELECT
  metrics.repo_id,
  metrics.path AS file_path,
  sumIf(metrics.churn, metrics.day >= start_day AND metrics.day < end_day) AS churn_loc_window,
  lookup.cyclomatic_total,
  lookup.ownership_concentration,
  0 AS incident_count,
  log1p(churn_loc_window) AS churn_signal,
  log1p(coalesce(cyclomatic_total, 0)) AS complexity_signal,
  coalesce(ownership_concentration, 0) AS ownership_signal,
  log1p(incident_count) AS incident_signal,
  (0.5 * churn_signal + 0.3 * complexity_signal + 0.2 * ownership_signal) AS risk_score
FROM stats.file_metrics_daily AS metrics
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
WHERE metrics.day >= start_day AND metrics.day < end_day
GROUP BY metrics.repo_id, metrics.path, lookup.cyclomatic_total, lookup.ownership_concentration;
```

Panel Query A (facts) template:

```sql
 WITH
     toDate({to:DateTime}) - toIntervalDay({window_days:Int32}) AS start_day,
     toDate({to:DateTime}) AS end_day
SELECT repo_id, file_path, churn_loc_window, cyclomatic_total, ownership_concentration, incident_count,
       churn_signal, complexity_signal, ownership_signal, incident_signal, risk_score
FROM (
  SELECT
    metrics.repo_id AS repo_id,
    metrics.path AS file_path,
    sumIf(metrics.churn, metrics.day >= start_day AND metrics.day < end_day) AS churn_loc_window,
    lookup.cyclomatic_total AS cyclomatic_total,
    lookup.ownership_concentration AS ownership_concentration,
    0 AS incident_count,
    log1p(churn_loc_window) AS churn_signal,
    log1p(coalesce(cyclomatic_total, 0)) AS complexity_signal,
    coalesce(ownership_concentration, 0) AS ownership_signal,
    log1p(incident_count) AS incident_signal,
    (0.5 * churn_signal + 0.3 * complexity_signal + 0.2 * ownership_signal) AS risk_score
  FROM stats.file_metrics_daily AS metrics
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
  WHERE metrics.repo_id = {repo_id:String} AND metrics.day >= start_day AND metrics.day < end_day
  GROUP BY metrics.repo_id, metrics.path, lookup.cyclomatic_total, lookup.ownership_concentration
)
ORDER BY risk_score DESC
LIMIT {limit:Int32};
```

Panel Query B (sparklines) template:

```sql
WITH
     toDate({to:DateTime}) - toIntervalDay({window_days:Int32}) AS start_day,
     toDate({to:DateTime}) AS end_day
SELECT metrics.path AS file_path, metrics.day, sum(metrics.churn) AS churn_loc
FROM stats.file_metrics_daily AS metrics
WHERE metrics.repo_id = {repo_id:String}
  AND metrics.day >= start_day AND metrics.day < end_day
  AND metrics.path IN ({file_paths:Array(String)})
GROUP BY metrics.path, metrics.day
ORDER BY file_path, day;
```

### Investment Flow (Sankey)

View:

```sql
CREATE OR REPLACE VIEW stats.v_investment_flow_edges AS
SELECT day, team_id, investment_area AS source, project_stream AS target,
       delivery_units, churn_loc, work_items_completed, '' AS provider
FROM stats.investment_metrics_daily;
```

Panel query template:

```sql
WITH
     toDate({to:DateTime}) - toIntervalDay({window_days:Int32}) AS start_day,
     toDate({to:DateTime}) AS end_day
SELECT source, target,
  sum(CASE {value_metric:String}
        WHEN 'delivery_units' THEN delivery_units
        WHEN 'churn_loc' THEN churn_loc
        WHEN 'work_items' THEN work_items_completed
        ELSE delivery_units END) AS value
FROM stats.v_investment_flow_edges
WHERE day >= start_day AND day < end_day
  AND ({team_id:String} = '' OR team_id = {team_id:String})
  AND ({provider:String} = '' OR provider = {provider:String})
GROUP BY source, target
HAVING value > 0
ORDER BY value DESC;
```
