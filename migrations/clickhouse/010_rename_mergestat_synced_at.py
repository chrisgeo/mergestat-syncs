import logging

def upgrade(client):
    """
    Renames _mergestat_synced_at to last_synced for ClickHouse tables.
    Handles ReplacingMergeTree version column rename via EXCHANGE TABLES.
    """
    
    # List of tables to migrate that use ReplacingMergeTree
    tables_with_version = [
        'repos',
        'git_files',
        'git_commits',
        'git_commit_stats',
        'git_blame',
        'git_pull_requests',
        'git_pull_request_reviews',
        'ci_pipeline_runs',
        'deployments',
        'incidents',
        'work_items',
        'work_item_transitions'
    ]

    # Tables that are simple renames (not using version column in engine)
    tables_simple = [
        'teams'
    ]

    def column_exists(table, column):
        try:
            query = f"SELECT count() FROM system.columns WHERE table = '{table}' AND name = '{column}' AND database = currentDatabase()"
            res = client.query(query)
            return res.result_rows[0][0] > 0
        except Exception as e:
            logging.warning(f"Error checking column existence for {table}.{column}: {e}")
            return False

    def table_exists(table):
        try:
            query = f"SELECT count() FROM system.tables WHERE name = '{table}' AND database = currentDatabase()"
            res = client.query(query)
            return res.result_rows[0][0] > 0
        except Exception as e:
            logging.warning(f"Error checking table existence for {table}: {e}")
            return False

    for table in tables_with_version:
        if not table_exists(table):
            continue
            
        if column_exists(table, 'last_synced'):
            logging.info(f"Table {table} already has last_synced column. Skipping.")
            continue
            
        if not column_exists(table, '_mergestat_synced_at'):
            logging.warning(f"Table {table} missing _mergestat_synced_at column and last_synced column. Skipping.")
            continue

        logging.info(f"Migrating table {table}...")
        
        # Schemas based on current definitions (excluding version column which we handle separately)
        # Note: We must ensure these match exactly what's in the DB or what we want in the DB.
        
        schemas = {
            'repos': """
                id UUID,
                repo String,
                ref Nullable(String),
                created_at DateTime64(3, 'UTC'),
                settings Nullable(String),
                tags Nullable(String),
                last_synced DateTime64(3, 'UTC')
            """,
            'git_files': """
                repo_id UUID,
                path String,
                executable UInt8,
                contents Nullable(String),
                last_synced DateTime64(3, 'UTC')
            """,
            'git_commits': """
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
            """,
            'git_commit_stats': """
                repo_id UUID,
                commit_hash String,
                file_path String,
                additions Int32,
                deletions Int32,
                old_file_mode String,
                new_file_mode String,
                last_synced DateTime64(3, 'UTC')
            """,
            'git_blame': """
                repo_id UUID,
                path String,
                line_no UInt32,
                author_email Nullable(String),
                author_name Nullable(String),
                author_when Nullable(DateTime64(3, 'UTC')),
                commit_hash Nullable(String),
                line Nullable(String),
                last_synced DateTime64(3, 'UTC')
            """,
            'git_pull_requests': """
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
            """,
            'git_pull_request_reviews': """
                repo_id UUID,
                number UInt32,
                review_id String,
                reviewer String,
                state String,
                submitted_at DateTime64(3, 'UTC'),
                last_synced DateTime64(3, 'UTC')
            """,
            'ci_pipeline_runs': """
                repo_id UUID,
                run_id String,
                status Nullable(String),
                queued_at Nullable(DateTime64(3, 'UTC')),
                started_at DateTime64(3, 'UTC'),
                finished_at Nullable(DateTime64(3, 'UTC')),
                last_synced DateTime64(3, 'UTC')
            """,
            'deployments': """
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
            """,
            'incidents': """
                repo_id UUID,
                incident_id String,
                status Nullable(String),
                started_at DateTime64(3, 'UTC'),
                resolved_at Nullable(DateTime64(3, 'UTC')),
                last_synced DateTime64(3, 'UTC')
            """,
            'work_items': """
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
            """,
            'work_item_transitions': """
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
            """
        }
        
        order_by = {
            'repos': '(id)',
            'git_files': '(repo_id, path)',
            'git_commits': '(repo_id, hash)',
            'git_commit_stats': '(repo_id, commit_hash, file_path)',
            'git_blame': '(repo_id, path, line_no)',
            'git_pull_requests': '(repo_id, number)',
            'git_pull_request_reviews': '(repo_id, number, review_id)',
            'ci_pipeline_runs': '(repo_id, run_id)',
            'deployments': '(repo_id, deployment_id)',
            'incidents': '(repo_id, incident_id)',
            'work_items': '(repo_id, work_item_id)',
            'work_item_transitions': '(repo_id, work_item_id, occurred_at)'
        }

        schema = schemas[table]
        order = order_by[table]
        
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {table}_new (
                {schema}
            ) ENGINE = ReplacingMergeTree(last_synced)
            ORDER BY {order}
        """)
        
        # Construct INSERT statement
        columns = []
        for line in schema.split('\n'):
            line = line.strip()
            if not line or line.startswith(')'): continue
            col_name = line.split()[0]
            columns.append(col_name)
            
        select_cols = []
        for col in columns:
            if col == 'last_synced':
                select_cols.append('_mergestat_synced_at as last_synced')
            else:
                select_cols.append(col)
        
        select_query = f"SELECT {', '.join(select_cols)} FROM {table}"
        insert_query = f"INSERT INTO {table}_new ({', '.join(columns)}) {select_query}"
        
        try:
            client.command(insert_query)
            client.command(f"EXCHANGE TABLES {table} AND {table}_new")
            client.command(f"DROP TABLE {table}_new")
        except Exception as e:
            logging.error(f"Failed to migrate table {table}: {e}")
            raise

    for table in tables_simple:
        if not table_exists(table):
            continue
        if column_exists(table, 'last_synced'):
            logging.info(f"Table {table} already has last_synced column. Skipping.")
            continue
        
        client.command(f"ALTER TABLE {table} RENAME COLUMN IF EXISTS _mergestat_synced_at TO last_synced")
