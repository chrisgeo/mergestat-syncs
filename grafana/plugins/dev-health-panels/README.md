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

## Expected schemas

Developer Landscape (single query):
- identity_id (string, optional)
- x_raw (number, optional)
- y_raw (number, optional)
- x_norm (number, required, 0-1)
- y_norm (number, required, 0-1)
- map_name (string, optional: churn_throughput | cycle_throughput | wip_throughput)
- team_id (string, optional)
- as_of_day (string/date, optional)

Hotspot Explorer:
- Query A (table facts):
  - file_path (string, required)
  - risk_score (number, optional)
  - churn_loc_30d (number, optional)
  - cyclomatic_total (number, optional)
  - ownership_concentration (number, optional)
  - incident_count (number, optional)
  - review_friction (number, optional)
- Query B (sparkline trend):
  - file_path (string, required)
  - day (string/date, required)
  - churn_loc (number, required)

Investment Flow (single query):
- source (string, required; investment_area)
- target (string, optional; project_stream or outcome)
- project_stream (string, optional if using group by)
- outcome (string, optional if using group by)
- value (number, required; effort metric)
- day (string/date, optional; for time window)
