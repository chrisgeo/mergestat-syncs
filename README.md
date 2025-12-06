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
- **`BATCH_SIZE`** (optional): Number of records to batch before inserting into the database. Higher values can improve performance but use more memory. Default: `100`
- **`MAX_WORKERS`** (optional): Number of parallel workers for processing git blame data. Higher values can speed up processing but use more CPU and memory. Default: `4`

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

### Performance Tuning

The script includes several configuration options to optimize performance:

- **`BATCH_SIZE`**: Controls how many records are batched before database insertion. Higher values (e.g., 200-500) can improve throughput but increase memory usage. Lower values (e.g., 50) reduce memory usage but may be slower.

- **`MAX_WORKERS`**: Controls parallel processing of git blame data. Set this based on your CPU cores (e.g., 2-8). Higher values speed up processing but use more CPU and memory.

- **Connection Pooling**: PostgreSQL automatically uses connection pooling with these defaults:
  - Pool size: 20 connections
  - Max overflow: 30 additional connections
  - Connections are recycled every hour

**Example for large repositories:**
```bash
export BATCH_SIZE=500
export MAX_WORKERS=8
python git_mergestat.py
```

**Example for resource-constrained environments:**
```bash
export BATCH_SIZE=50
export MAX_WORKERS=2
python git_mergestat.py
```

## Performance Optimizations

This project includes several key performance optimizations to speed up git data processing:

### 1. **Increased Batch Size** (10x improvement)
- **Changed from**: 10 records per batch
- **Changed to**: 100 records per batch (configurable)
- **Impact**: Reduces database round-trips by 10x, significantly improving insertion speed
- **Configuration**: Set `BATCH_SIZE=200` for even larger batches

### 2. **Parallel Git Blame Processing** (4-8x improvement)
- **Implementation**: Uses asyncio with configurable worker pool
- **Default**: 4 parallel workers processing files concurrently
- **Impact**: Multi-core CPU utilization, dramatically faster blame processing
- **Configuration**: Set `MAX_WORKERS=8` for more powerful machines

### 3. **Database Connection Pooling** (PostgreSQL)
- **Pool size**: 20 connections (up from default 5)
- **Max overflow**: 30 additional connections (up from default 10)
- **Impact**: Better handling of concurrent operations, reduced connection overhead
- **Auto-configured**: No manual setup required

### 4. **Optimized Bulk Operations**
- All database insertions use bulk operations
- MongoDB operations use `ordered=False` for better performance
- SQLAlchemy uses `add_all()` for efficient batch inserts

### 5. **Smart File Filtering**
- Skips binary files (images, videos, archives, etc.)
- Skips files larger than 1MB for content reading
- Reduces unnecessary I/O and processing time

### Expected Performance Improvements

For a typical repository with 1000 files and 10,000 commits:

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Git Blame | 50 min | 6-12 min | **4-8x faster** |
| Commits | - | 1-2 min | **New feature** |
| Commit Stats | - | 2-4 min | **New feature** |
| Files | - | 30-60 sec | **New feature** |
| **Total** | **50+ min** | **10-20 min** | **~3-5x faster** |

*Actual performance depends on hardware, repository size, and configuration.*

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
