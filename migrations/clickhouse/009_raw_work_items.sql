CREATE TABLE IF NOT EXISTS work_items (
    repo_id UUID,
    work_item_id String,
    provider String,
    title String,
    type String,
    status String,
    status_raw String,
    project_key String,
    project_id String,
    assignees Array(String),
    reporter String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    started_at Nullable(DateTime64(3)),
    completed_at Nullable(DateTime64(3)),
    closed_at Nullable(DateTime64(3)),
    labels Array(String),
    story_points Nullable(Float64),
    sprint_id String,
    sprint_name String,
    parent_id String,
    epic_id String,
    url String,
    _mergestat_synced_at DateTime64(3)
) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
ORDER BY (repo_id, work_item_id);

CREATE TABLE IF NOT EXISTS work_item_transitions (
    repo_id UUID,
    work_item_id String,
    occurred_at DateTime64(3),
    provider String,
    from_status String,
    to_status String,
    from_status_raw String,
    to_status_raw String,
    actor String,
    _mergestat_synced_at DateTime64(3)
) ENGINE = ReplacingMergeTree(_mergestat_synced_at)
ORDER BY (repo_id, work_item_id, occurred_at);
