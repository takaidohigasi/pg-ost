# pg-ost

PostgreSQL Online Schema Transformation - a trigger-less online schema change tool for PostgreSQL.

Inspired by [gh-ost](https://github.com/github/gh-ost), pg-ost performs schema migrations with minimal impact on production traffic by:

1. Using PostgreSQL logical replication instead of triggers
2. Copying data to a ghost table in small chunks
3. Applying live DML changes from the replication stream
4. Performing an atomic table swap at the end

## Features

- **Trigger-less**: Uses PostgreSQL's native logical replication (pgoutput plugin)
- **Online schema change**: Minimal blocking during migration
- **Runtime configuration**: Adjust parameters via Unix socket or TCP
- **Throttling**: Lag-based and load-based controls
- **Hooks**: Execute scripts at migration lifecycle points

## Requirements

- PostgreSQL 14 or later
- `wal_level = 'logical'` in postgresql.conf
- User with REPLICATION privilege

## Installation

```bash
go install github.com/takaidohigasi/pg-ost/cmd/pg-ost@latest
```

Or build from source:

```bash
git clone https://github.com/takaidohigasi/pg-ost.git
cd pg-ost
make build
```

## Usage

### Basic Usage

```bash
pg-ost \
  --host=localhost \
  --port=5432 \
  --user=postgres \
  --password=secret \
  --database=mydb \
  --table=users \
  --alter="ADD COLUMN email_verified BOOLEAN DEFAULT false" \
  --execute
```

### Dry Run (Default)

Without `--execute`, pg-ost validates the migration without making changes:

```bash
pg-ost \
  --user=postgres \
  --database=mydb \
  --table=users \
  --alter="ADD COLUMN email_verified BOOLEAN DEFAULT false"
```

### With Throttling

```bash
pg-ost \
  --user=postgres \
  --database=mydb \
  --table=users \
  --alter="ADD COLUMN status VARCHAR(50)" \
  --chunk-size=500 \
  --max-lag-millis=1000 \
  --max-load="active_connections=100" \
  --throttle-flag-file=/tmp/pg-ost.throttle \
  --execute
```

### With Online Control

Enable runtime control via Unix socket:

```bash
pg-ost \
  --user=postgres \
  --database=mydb \
  --table=users \
  --alter="ADD COLUMN created_at TIMESTAMP" \
  --serve-socket-file=/tmp/pg-ost.sock \
  --execute
```

Then control the migration:

```bash
echo "status" | nc -U /tmp/pg-ost.sock
echo "chunk-size=2000" | nc -U /tmp/pg-ost.sock
echo "throttle" | nc -U /tmp/pg-ost.sock
echo "no-throttle" | nc -U /tmp/pg-ost.sock
```

## Command-Line Flags

### Required

| Flag | Description |
|------|-------------|
| `--database` | Database name |
| `--table` | Table name |
| `--alter` | ALTER statement (without ALTER TABLE prefix) |
| `--user` | PostgreSQL user |

### Connection

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | localhost | PostgreSQL host |
| `--port` | 5432 | PostgreSQL port |
| `--password` | | PostgreSQL password |
| `--sslmode` | prefer | SSL mode |
| `--schema` | public | Schema name |

### Execution

| Flag | Default | Description |
|------|---------|-------------|
| `--execute` | false | Actually perform migration |
| `--chunk-size` | 1000 | Rows per chunk (100-100000) |
| `--dml-batch-size` | 100 | DML events per batch |
| `--nice-ratio` | 0 | Sleep ratio (0=aggressive) |

### Throttling

| Flag | Default | Description |
|------|---------|-------------|
| `--max-lag-millis` | 1500 | Max replication lag |
| `--max-load` | | Throttle thresholds (e.g., "active_connections=100") |
| `--critical-load` | | Hibernate thresholds |
| `--throttle-flag-file` | | Throttle when file exists |
| `--throttle-http` | | Throttle on non-200 response |
| `--throttle-query` | | Throttle when query returns > 0 |

### Online Control

| Flag | Default | Description |
|------|---------|-------------|
| `--serve-socket-file` | | Unix socket path |
| `--serve-tcp-port` | 0 | TCP port (0=disabled) |

### Table Management

| Flag | Default | Description |
|------|---------|-------------|
| `--ok-to-drop-table` | false | Drop old table after cutover |
| `--initially-drop-old-table` | false | Drop existing _old table |
| `--initially-drop-ghost-table` | false | Drop existing ghost table |
| `--timestamp-old-table` | false | Add timestamp to old table name |

### Cutover

| Flag | Default | Description |
|------|---------|-------------|
| `--cut-over-lock-timeout-seconds` | 10 | Lock timeout for cutover |
| `--postpone-cut-over-flag-file` | | Postpone cutover when file exists |

## How It Works

1. **Validate** - Parse and validate the ALTER statement
2. **Inspect** - Examine table structure and find unique key
3. **Setup Replication** - Set REPLICA IDENTITY FULL, create publication and slot
4. **Create Ghost Table** - Create a copy of the table with ALTER applied
5. **Stream & Copy** - Copy rows in chunks while applying DML from replication
6. **Cutover** - Lock table and atomically rename ghost to original
7. **Cleanup** - Drop old table, publication, and slot

## Online Commands

When using `--serve-socket-file` or `--serve-tcp-port`:

| Command | Description |
|---------|-------------|
| `status` | Show migration progress |
| `help` | Show available commands |
| `chunk-size=N` | Set chunk size |
| `max-lag-millis=N` | Set max lag threshold |
| `nice-ratio=N` | Set nice ratio |
| `throttle` | Pause migration |
| `no-throttle` | Resume migration |
| `unpostpone` | Proceed with cutover |
| `panic` | Abort migration |

## Hooks

Place executable scripts in the hooks directory:

- `on-startup` - Migration started
- `on-validated` - ALTER validated
- `on-row-count-complete` - Row count finished
- `on-before-row-copy` - Before row copy starts
- `on-row-copy-complete` - Row copy finished
- `on-before-cut-over` - Before cutover
- `on-success` - Migration succeeded
- `on-failure` - Migration failed
- `on-status` - Periodic status (every 60s)

Environment variables available in hooks:
- `PGOST_DATABASE_NAME`
- `PGOST_TABLE_NAME`
- `PGOST_GHOST_TABLE_NAME`
- `PGOST_ALTER_STATEMENT`
- `PGOST_TOTAL_ROWS_COPIED`
- `PGOST_PROGRESS_PCT`
- etc.

## Differences from gh-ost

| Feature | gh-ost (MySQL) | pg-ost (PostgreSQL) |
|---------|---------------|---------------------|
| Change capture | Binary log | Logical replication |
| Position tracking | Binlog file/offset | LSN |
| Cutover | Magic table trick | Simple LOCK + RENAME |
| Row format | Requires ROW format | Uses pgoutput |

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
