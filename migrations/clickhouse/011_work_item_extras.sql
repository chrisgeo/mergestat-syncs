ALTER TABLE work_items
    ADD COLUMN IF NOT EXISTS priority_raw Nullable(String),
    ADD COLUMN IF NOT EXISTS service_class Nullable(String),
    ADD COLUMN IF NOT EXISTS due_at Nullable(DateTime64(3));

CREATE TABLE IF NOT EXISTS work_item_dependencies (
    source_work_item_id String,
    target_work_item_id String,
    relationship_type String,
    relationship_type_raw String
) ENGINE = MergeTree()
ORDER BY (source_work_item_id, target_work_item_id, relationship_type);

CREATE TABLE IF NOT EXISTS work_item_reopen_events (
    work_item_id String,
    occurred_at DateTime64(3),
    from_status String,
    to_status String,
    from_status_raw Nullable(String),
    to_status_raw Nullable(String),
    actor Nullable(String)
) ENGINE = MergeTree()
ORDER BY (work_item_id, occurred_at);

CREATE TABLE IF NOT EXISTS work_item_interactions (
    work_item_id String,
    provider String,
    interaction_type String,
    occurred_at DateTime64(3),
    actor Nullable(String),
    body_length UInt32
) ENGINE = MergeTree()
ORDER BY (work_item_id, occurred_at);

CREATE TABLE IF NOT EXISTS sprints (
    provider String,
    sprint_id String,
    name Nullable(String),
    state Nullable(String),
    started_at Nullable(DateTime64(3)),
    ended_at Nullable(DateTime64(3)),
    completed_at Nullable(DateTime64(3))
) ENGINE = MergeTree()
ORDER BY (provider, sprint_id);
