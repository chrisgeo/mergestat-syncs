# Roadmap & Remaining Tasks

This checklist tracks what is complete and what remains to finalize `dev-health-ops`.

## Completed

- [x] **Core Git + PR Metrics**: Commit size, churn, PR cycle/pickup/review times, review load, PR size stats.
- [x] **Work Item Flow Metrics**: Cycle/lead time p50/p90, WIP count/age, flow efficiency, state duration + avg WIP.
- [x] **Quality + Risk Metrics**: Defect introduction rate, WIP congestion, rework churn ratio, single-owner file ratio proxy.
- [x] **DORA Metrics**: MTTR, change failure rate, deployment/incident daily rollups.
- [x] **Wellbeing Signals**: After-hours and weekend commit ratios; weekend active users (derived from user activity).
- [x] **Connectors + Pipelines**: GitHub CI/CD + deployments + incidents ingestion; async batch helpers.
- [x] **Storage + Schema**: ClickHouse migrations and sink support for new metrics tables/columns.
- [x] **CLI Controls**: Flags for `--sync-cicd`, `--sync-deployments`, `--sync-incidents`.
- [x] **Synthetic fixtures**: CI/CD + deployments + incidents with metrics rollups for ClickHouse.
- [x] **IC Metrics + Landscape**: Identity resolution, unified UserMetricsDailyRecord, and landscape rolling stats (Churn/Cycle/WIP vs Throughput).

## Remaining

### Data + Fixtures
- [ ] **Fixtures validation**: Ensure synthetic work items + commits generate non-zero Phase 2 metrics.

### Dashboards
- [ ] **Dashboards for CI/CD, deployments, incidents** (panels for success rate, duration, deploy counts, MTTR).
- [ ] **Work tracking dashboards audit**: Validate filters and table joins for synthetic + real providers.
- [ ] **Fix dashboard templating filters**: Ensure variable regex and `match(...)` filters do not return empty results.

### Metrics Enhancements
- [ ] **Bus factor (true)**: Replace single-owner proxy with contributor coverage model (e.g., 80% rule).
- [ ] **Predictability index**: Estimated vs. actual story points/time.
- [ ] **Capacity planning**: Forecast completion using historical throughput.

### Testing + Docs
- [ ] **Tests for new sinks columns** (ClickHouse + SQLite write paths for Phase 2 + wellbeing).
- [ ] **Docs refresh**: Usage examples for new CLI flags and fixture generation steps.
