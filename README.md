# Git Metrics

Using Mergestat's database schema, this set of syncs will use any git repository locally, or from gitlab (github ones already exist) to allow you to more quickly add data without the complicated setup that mergestat and entails.

## Why?

Mostly because using mergestat's syncs are great but take a lot of time to understand. The goal of this was for a personal project to understand how my teams are doing, and with a limited budget.

## Database Configuration

This project supports both PostgreSQL and MongoDB as storage backends. You can configure the database backend using environment variables or command-line arguments.

### Environment Variables

- **`DB_TYPE`** (optional): Specifies the database backend to use. Valid values are `postgres` or `mongo`. Default: `postgres`
- **`DB_CONN_STRING`** (required): The connection string for your database.
  - For PostgreSQL: `postgresql+asyncpg://user:password@host:port/database`
  - For MongoDB: `mongodb://host:port` or `mongodb://user:password@host:port`
- **`DB_ECHO`** (optional): Enable SQL query logging for PostgreSQL. Set to `true`, `1`, or `yes` (case-insensitive) to enable. Any other value (including `false`, `0`, `no`, or unset) disables it. Default: `false`. Note: Enabling this in production can expose sensitive data and impact performance.
- **`MONGO_DB_NAME`** (optional): The name of the MongoDB database to use. If not specified, the script will use the database specified in the connection string, or default to `mergestat`.
- **`REPO_PATH`** (optional): Path to the git repository to analyze. Default: `.` (current directory)
- **`REPO_UUID`** (optional): UUID for the repository. If not provided, one will be generated.
- **`GIT_IO_WORKERS`** (optional): Max worker threads for file/blame processing. Defaults to `max(4, cpu_count)` and capped at 32.
- **`BATCH_SIZE`** (optional): Insert batch size for files, commits, commit stats, and blame rows. Default: `50`.
- **`FILE_CONTENT_MAX_BYTES`** (optional): Max file size (bytes) to load into memory when storing contents. Larger files will skip content read. Default: `1_000_000`.
- **`BLAME_MAX_FILE_SIZE`** (optional): Max file size (bytes) to include in blame processing. Files above this size are skipped. Default: `5_000_000`.

### Command-Line Arguments

You can also configure the database using command-line arguments, which will override environment variables:

- **`--db-type`**: Database backend to use (`postgres` or `mongo`)
- **`--db`**: Database connection string
- **`--repo-path`**: Path to the git repository

Example usage:

```bash
# Using PostgreSQL
python git_mergestat.py --db-type postgres --db "postgresql+asyncpg://user:pass@localhost:5432/mergestat"

# Using MongoDB
python git_mergestat.py --db-type mongo --db "mongodb://localhost:27017"
```

### MongoDB Connection String Format

MongoDB connection strings follow the standard MongoDB URI format:

- **Basic**: `mongodb://host:port`
- **With authentication**: `mongodb://username:password@host:port`
- **With database**: `mongodb://username:password@host:port/database_name`
- **With options**: `mongodb://host:port/?authSource=admin&retryWrites=true`

You can also set the database name separately using the `MONGO_DB_NAME` environment variable instead of including it in the connection string.

### PostgreSQL vs MongoDB: Setup and Migration Considerations

#### Using PostgreSQL

- Requires running database migrations with Alembic before first use
- Provides strong relational data structure
- Best for complex queries and joins
- Example setup:

  ```bash
  # Start PostgreSQL with Docker Compose
  docker compose up postgres -d

  # Run migrations
  alembic upgrade head

  # Set environment variables
  export DB_TYPE=postgres
  export DB_CONN_STRING="postgresql+asyncpg://postgres:postgres@localhost:5333/postgres"

  # Run the script
  python git_mergestat.py
  ```

#### Using MongoDB

- No migrations required - collections are created automatically
- Schema-less design allows for flexible data structures
- Best for quick setup and document-based storage
- Example setup:

  ```bash
  # Start MongoDB with Docker Compose
  docker compose up mongo -d

  # Set environment variables
  export DB_TYPE=mongo
  export DB_CONN_STRING="mongodb://localhost:27017"
  export MONGO_DB_NAME="mergestat"

  # Run the script
  python git_mergestat.py
  ```

#### Switching Between Databases

- The two backends use different storage mechanisms and are not directly compatible
- Data is not automatically migrated when switching between PostgreSQL and MongoDB
- If you need to switch backends, you'll need to re-run the analysis to populate the new database
- Both databases can run simultaneously on the same machine using different ports (see `compose.yml`)
