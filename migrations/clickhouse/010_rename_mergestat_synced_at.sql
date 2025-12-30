-- Migration for tables where _mergestat_synced_at is the version column

-- 1. repos
CREATE TABLE IF NOT EXISTS repos_new (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);

INSERT INTO repos_new SELECT id, repo, ref, created_at, settings, tags, _mergestat_synced_at as last_synced FROM repos;
EXCHANGE TABLES repos AND repos_new;
DROP TABLE repos_new;

-- 2. git_files
CREATE TABLE IF NOT EXISTS git_files_new (
    repo_id UUID,
    path String,
    executable UInt8,
    contents Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, path);

INSERT INTO git_files_new SELECT repo_id, path, executable, contents, _mergestat_synced_at as last_synced FROM git_files;
EXCHANGE TABLES git_files AND git_files_new;
DROP TABLE git_files_new;

-- 3. git_commits
CREATE TABLE IF NOT EXISTS git_commits_new (
    repo_id UUID,
    hash String,
    message Nullable(String),
    author_name Nullable(String),
    author_email Nullable(String),
    author_when DateTime64(3, 'UTC'),
    committer_name Nullable(String),
    committer_email Nullable(String),
    committer_when DateTime64(3, 'UTC'),
    parents UInt32,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, hash);

INSERT INTO git_commits_new SELECT repo_id, hash, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents, _mergestat_synced_at as last_synced FROM git_commits;
EXCHANGE TABLES git_commits AND git_commits_new;
DROP TABLE git_commits_new;

-- 4. git_commit_stats
CREATE TABLE IF NOT EXISTS git_commit_stats_new (
    repo_id UUID,
    commit_hash String,
    file_path String,
    additions Int32,
    deletions Int32,
    old_file_mode String,
    new_file_mode String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, commit_hash, file_path);

INSERT INTO git_commit_stats_new SELECT repo_id, commit_hash, file_path, additions, deletions, old_file_mode, new_file_mode, _mergestat_synced_at as last_synced FROM git_commit_stats;
EXCHANGE TABLES git_commit_stats AND git_commit_stats_new;
DROP TABLE git_commit_stats_new;

-- 5. git_blame
CREATE TABLE IF NOT EXISTS git_blame_new (
    repo_id UUID,
    path String,
    line_no UInt32,
    author_email Nullable(String),
    author_name Nullable(String),
    author_when Nullable(DateTime64(3, 'UTC')),
    commit_hash Nullable(String),
    line Nullable(String),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, path, line_no);

INSERT INTO git_blame_new SELECT repo_id, path, line_no, author_email, author_name, author_when, commit_hash, line, _mergestat_synced_at as last_synced FROM git_blame;
EXCHANGE TABLES git_blame AND git_blame_new;
DROP TABLE git_blame_new;

-- 6. git_pull_requests
CREATE TABLE IF NOT EXISTS git_pull_requests_new (
    repo_id UUID,
    number UInt32,
    title Nullable(String),
    state Nullable(String),
    author_name Nullable(String),
    author_email Nullable(String),
    created_at DateTime64(3, 'UTC'),
    merged_at Nullable(DateTime64(3, 'UTC')),
    closed_at Nullable(DateTime64(3, 'UTC')),
    head_branch Nullable(String),
    base_branch Nullable(String),
    additions Nullable(UInt32),
    deletions Nullable(UInt32),
    changed_files Nullable(UInt32),
    first_review_at Nullable(DateTime64(3, 'UTC')),
    first_comment_at Nullable(DateTime64(3, 'UTC')),
    changes_requested_count UInt32 DEFAULT 0,
    reviews_count UInt32 DEFAULT 0,
    comments_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, number);

INSERT INTO git_pull_requests_new SELECT repo_id, number, title, state, author_name, author_email, created_at, merged_at, closed_at, head_branch, base_branch, additions, deletions, changed_files, first_review_at, first_comment_at, changes_requested_count, reviews_count, comments_count, _mergestat_synced_at as last_synced FROM git_pull_requests;
EXCHANGE TABLES git_pull_requests AND git_pull_requests_new;
DROP TABLE git_pull_requests_new;

-- 7. git_pull_request_reviews
CREATE TABLE IF NOT EXISTS git_pull_request_reviews_new (
    repo_id UUID,
    number UInt32,
    review_id String,
    reviewer String,
    state String,
    submitted_at DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, number, review_id);

INSERT INTO git_pull_request_reviews_new SELECT repo_id, number, review_id, reviewer, state, submitted_at, _mergestat_synced_at as last_synced FROM git_pull_request_reviews;
EXCHANGE TABLES git_pull_request_reviews AND git_pull_request_reviews_new;
DROP TABLE git_pull_request_reviews_new;

-- 8. ci_pipeline_runs
CREATE TABLE IF NOT EXISTS ci_pipeline_runs_new (
    repo_id UUID,
    run_id String,
    status Nullable(String),
    queued_at Nullable(DateTime64(3, 'UTC')),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, run_id);

INSERT INTO ci_pipeline_runs_new SELECT repo_id, run_id, status, queued_at, started_at, finished_at, _mergestat_synced_at as last_synced FROM ci_pipeline_runs;
EXCHANGE TABLES ci_pipeline_runs AND ci_pipeline_runs_new;
DROP TABLE ci_pipeline_runs_new;

-- 9. deployments
CREATE TABLE IF NOT EXISTS deployments_new (
    repo_id UUID,
    deployment_id String,
    status Nullable(String),
    environment Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')),
    finished_at Nullable(DateTime64(3, 'UTC')),
    deployed_at Nullable(DateTime64(3, 'UTC')),
    merged_at Nullable(DateTime64(3, 'UTC')),
    pull_request_number Nullable(UInt32),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, deployment_id);

INSERT INTO deployments_new SELECT repo_id, deployment_id, status, environment, started_at, finished_at, deployed_at, merged_at, pull_request_number, _mergestat_synced_at as last_synced FROM deployments;
EXCHANGE TABLES deployments AND deployments_new;
DROP TABLE deployments_new;

-- 10. incidents
CREATE TABLE IF NOT EXISTS incidents_new (
    repo_id UUID,
    incident_id String,
    status Nullable(String),
    started_at DateTime64(3, 'UTC'),
    resolved_at Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, incident_id);

INSERT INTO incidents_new SELECT repo_id, incident_id, status, started_at, resolved_at, _mergestat_synced_at as last_synced FROM incidents;
EXCHANGE TABLES incidents AND incidents_new;
DROP TABLE incidents_new;

-- 11. work_items
CREATE TABLE IF NOT EXISTS work_items_new (
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
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, work_item_id);

INSERT INTO work_items_new SELECT repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, _mergestat_synced_at as last_synced FROM work_items;
EXCHANGE TABLES work_items AND work_items_new;
DROP TABLE work_items_new;

-- 12. work_item_transitions
CREATE TABLE IF NOT EXISTS work_item_transitions_new (
    repo_id UUID,
    work_item_id String,
    occurred_at DateTime64(3),
    provider String,
    from_status String,
    to_status String,
    from_status_raw String,
    to_status_raw String,
    actor String,
    last_synced DateTime64(3)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (repo_id, work_item_id, occurred_at);

INSERT INTO work_item_transitions_new SELECT repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, _mergestat_synced_at as last_synced FROM work_item_transitions;
EXCHANGE TABLES work_item_transitions AND work_item_transitions_new;
DROP TABLE work_item_transitions_new;

-- Teams table (simple rename as it doesn't use version column in engine)
ALTER TABLE teams RENAME COLUMN IF EXISTS _mergestat_synced_at TO last_synced;