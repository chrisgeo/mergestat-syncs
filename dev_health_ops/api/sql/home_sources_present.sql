WITH
  toDate(%(start_day)s) AS start_day,
  toDate(%(end_day)s) AS end_day,
  toDateTime(start_day) AS start_ts,
  toDateTime(end_day) AS end_ts,
  toDateTime(%(cutoff)s) AS cutoff,
  repos_with_source AS (
    SELECT
      id,
      JSONExtractString(ifNull(settings, '{}'), 'source') AS source
    FROM repos
  ),
  source_sync AS (
    SELECT
      'github' AS key,
      if(count() = 0, NULL, max(_mergestat_synced_at)) AS last_sync
    FROM repos
    WHERE JSONExtractString(ifNull(settings, '{}'), 'source') = 'github'
    UNION ALL
    SELECT
      'gitlab' AS key,
      if(count() = 0, NULL, max(_mergestat_synced_at)) AS last_sync
    FROM repos
    WHERE JSONExtractString(ifNull(settings, '{}'), 'source') = 'gitlab'
    UNION ALL
    SELECT
      'jira' AS key,
      if(count() = 0, NULL, max(_mergestat_synced_at)) AS last_sync
    FROM work_items
    WHERE provider = 'jira'
    UNION ALL
    SELECT
      'ci' AS key,
      if(count() = 0, NULL, max(_mergestat_synced_at)) AS last_sync
    FROM (
      SELECT _mergestat_synced_at FROM ci_pipeline_runs
      UNION ALL
      SELECT _mergestat_synced_at FROM deployments
      UNION ALL
      SELECT _mergestat_synced_at FROM incidents
    )
  ),
  source_range AS (
    SELECT
      'github' AS key,
      if(count() = 0, NULL, max(author_when)) AS last_seen
    FROM git_commits
    INNER JOIN repos_with_source AS repos ON git_commits.repo_id = repos.id
    WHERE repos.source = 'github'
      AND author_when >= start_ts
      AND author_when < end_ts
    UNION ALL
    SELECT
      'gitlab' AS key,
      if(count() = 0, NULL, max(author_when)) AS last_seen
    FROM git_commits
    INNER JOIN repos_with_source AS repos ON git_commits.repo_id = repos.id
    WHERE repos.source = 'gitlab'
      AND author_when >= start_ts
      AND author_when < end_ts
    UNION ALL
    SELECT
      'jira' AS key,
      if(count() = 0, NULL, max(updated_at)) AS last_seen
    FROM work_items
    WHERE provider = 'jira'
      AND updated_at >= start_ts
      AND updated_at < end_ts
    UNION ALL
    SELECT
      'ci' AS key,
      if(count() = 0, NULL, max(event_at)) AS last_seen
    FROM (
      SELECT started_at AS event_at
      FROM ci_pipeline_runs
      WHERE started_at >= start_ts AND started_at < end_ts
      UNION ALL
      SELECT ifNull(deployed_at, ifNull(started_at, finished_at)) AS event_at
      FROM deployments
      WHERE ifNull(deployed_at, ifNull(started_at, finished_at)) >= start_ts
        AND ifNull(deployed_at, ifNull(started_at, finished_at)) < end_ts
      UNION ALL
      SELECT started_at AS event_at
      FROM incidents
      WHERE started_at >= start_ts AND started_at < end_ts
    )
  ),
  sources AS (
    SELECT 'github' AS key, 'GitHub' AS label
    UNION ALL
    SELECT 'gitlab' AS key, 'GitLab' AS label
    UNION ALL
    SELECT 'jira' AS key, 'Jira' AS label
    UNION ALL
    SELECT 'ci' AS key, 'CI' AS label
  )
SELECT
  sources.key AS key,
  sources.label AS label,
  if(
    isNull(source_sync.last_sync),
    source_range.last_seen,
    if(
      isNull(source_range.last_seen),
      source_sync.last_sync,
      if(
        source_sync.last_sync >= source_range.last_seen,
        source_sync.last_sync,
        source_range.last_seen
      )
    )
  ) AS last_seen_at,
  'ok' AS status
FROM sources
LEFT JOIN source_sync ON sources.key = source_sync.key
LEFT JOIN source_range ON sources.key = source_range.key
WHERE
  (source_sync.last_sync >= cutoff)
  OR (source_range.last_seen IS NOT NULL)
ORDER BY sources.key
