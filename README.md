# pg_flashback

A PostgreSQL extension that adds a **recycle bin** for dropped tables. Instead of permanently losing data when a `DROP TABLE` is executed, pg_flashback intercepts the command, moves the table to a safe schema, and lets you restore it later.

> Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

---

## 🔥 Quick Start

```sql
-- Enable extension
CREATE EXTENSION pg_flashback;

-- Create test table
CREATE TABLE test (id int);
INSERT INTO test VALUES (1), (2);

-- Drop it
DROP TABLE test;

-- See recycle bin
SELECT * FROM flashback_list_recycled_tables();

-- Restore it
SELECT flashback_restore('test');

-- Verify
SELECT * FROM test;
```

---

## Features

- Automatically captures `DROP TABLE` (including multi-table `DROP TABLE t1, t2`), `DROP TABLE ... CASCADE`, and `TRUNCATE TABLE`
- Captures `DROP SCHEMA ... CASCADE` — all tables in the schema are saved before deletion
- Stores dropped/truncated tables in a `flashback_recycle` schema until restored or expired
- Restore by table name or by specific operation ID (useful when a table was dropped multiple times)
- **Full metadata capture and restore:**
  - Dependent views are dropped and recreated on restore
  - Incoming FK constraints (from other tables pointing at the dropped table) are captured and re-applied
  - RLS policies are captured and re-applied, with `ENABLE ROW LEVEL SECURITY` if needed
  - Triggers survive restore automatically (they travel with the physical table)
  - Partitioned tables: children are co-moved and re-attached; missing partitions are recreated as empty shells with a warning
- Quoted identifiers fully supported: table and schema names with hyphens, spaces, or special characters work correctly (e.g. `DROP TABLE "my-table"`, `DROP TABLE "My Schema"."My Table"`)
- Sequence / IDENTITY column state automatically restored after DROP and TRUNCATE recovery
- FIFO eviction when table count or total size limits are reached
- Background worker that automatically purges expired entries
- Permission model: superusers see and manage all tables; regular users can only see and restore their own
- Configurable via PostgreSQL GUCs (no config files needed)
- Skips temporary tables, internal schemas, and user-defined excluded schemas
- Race condition safe: advisory locks keyed by `op_id` prevent concurrent restores of the same operation

---

## Compatibility

| PostgreSQL | Status |
|---|---|
| 18 | Supported |
| 17 | Supported |
| 16 | Supported |
| 15 | Supported |
| 14 | Supported |

## Requirements

- PostgreSQL 14 – 18
- pgrx 0.16.1
- Rust (stable)

---

## Installation

```bash
git clone https://github.com/CaghanTU/pg_flashback.git
cd pg_flashback
./install.sh  # builds with pgrx and installs to PostgreSQL

# Alternative (manual/pro)
cargo pgrx install --release
```

### First-time setup checklist

1. Set `shared_preload_libraries` in `postgresql.conf`:

```conf
shared_preload_libraries = 'pg_flashback'
```

Requires restart.

2. Restart PostgreSQL.
3. Extension is created in the target database:

```sql
CREATE EXTENSION pg_flashback;
```

4. Optional but recommended for non-`postgres` databases: configure the worker connection DB.

```sql
ALTER SYSTEM SET flashback.database_name = 'your_database_name';
SELECT pg_reload_conf();
```

---

## How It Works

A DDL hook fires on every `DROP TABLE` and `TRUNCATE TABLE`. On `DROP`, the table is moved to `flashback_recycle` and renamed with a unique op_id suffix. On `TRUNCATE`, the data is copied to `flashback_recycle` before truncation proceeds. Metadata (original schema, owner, operation type, retention deadline) is written to `flashback.operations`. A background worker periodically purges expired entries; FIFO eviction runs before each capture if a size or count limit is reached.

---

## SQL Functions

### `flashback_list_recycled_tables()`

Lists all tables currently in the recycle bin.

```sql
SELECT * FROM flashback_list_recycled_tables();
```

| Column | Type | Description |
|---|---|---|
| `schema_name` | text | Original schema |
| `table_name` | text | Original table name |
| `recycled_name` | text | Internal name in `flashback_recycle` schema |
| `dropped_at` | text | Timestamp when the table was dropped/truncated |
| `role_name` | text | The user who dropped/truncated the table |
| `retention_until` | text | Expiry date (auto-purged after this) |
| `op_id` | bigint | Unique operation ID |
| `operation_type` | text | `DROP` or `TRUNCATE` |

Superusers see all entries. Regular users see only the tables they dropped.

---

### `flashback_restore(table_name text, target_schema text DEFAULT NULL)`

Restores the most recently dropped or truncated version of a table.

```sql
-- Restore to the original schema (two ways)
SELECT flashback_restore('orders');
SELECT flashback_restore('orders', NULL);

-- Restore to a different schema
SELECT flashback_restore('orders', 'archive');
```

Returns `true` on success, `false` on failure (with a `WARNING` message explaining why).

Regular users can only restore tables they dropped themselves.

---

### `flashback_restore_by_id(op_id bigint, target_schema text DEFAULT NULL)`

Restores a specific version of a table by its operation ID. Useful when the same table was dropped multiple times and you need a specific version.

```sql
-- Find which version you want
SELECT op_id, dropped_at FROM flashback_list_recycled_tables() WHERE table_name = 'orders';

-- Restore that specific version
SELECT flashback_restore_by_id(3);

-- Or restore to a different schema
SELECT flashback_restore_by_id(3, 'archive');
```

---

### `flashback_purge(table_name text)`

Permanently removes the most recently dropped/truncated version of a table from the recycle bin without restoring it.

```sql
SELECT flashback_purge('orders');
```

Returns `true` on success. Regular users can only purge tables they dropped.

---

### `flashback_purge_by_id(op_id bigint)`

Permanently removes a specific entry from the recycle bin by operation ID.

```sql
SELECT flashback_purge_by_id(42);
```

Returns `true` on success. Regular users can only purge their own entries.

---

### `flashback_purge_all()`

Permanently removes all entries from the recycle bin.

```sql
SELECT flashback_purge_all();
```

Returns the number of tables that were purged. Superusers purge everything; regular users purge only their own tables.

---

### `flashback_restore_all()`

Restores **all** tables currently in the recycle bin to their original schemas in a single call. Useful after a mass-drop incident (e.g. accidental `DROP SCHEMA ... CASCADE` across multiple schemas).

```sql
SELECT flashback_restore_all();
```

Returns the count of successfully restored tables. Schemas that no longer exist are automatically recreated. Superusers restore all entries; regular users restore only the tables they dropped themselves. Tables are restored newest-first so that a DROP followed by a TRUNCATE on the same table is always handled in the correct order.

---

### `flashback_restore_schema(schema_name text, target_schema text DEFAULT NULL)`

Restores all unrecovered entries that originally belong to one schema.

```sql
-- Restore everything back to its original schema name
SELECT flashback_restore_schema('sales', NULL);

-- Restore everything from one source schema into another destination schema
SELECT flashback_restore_schema('sales', 'sales_restored');
```

Returns the number of successfully restored tables.

---

### `flashback_status()`

Shows the current state of the recycle bin alongside the configured limits.

```sql
SELECT * FROM flashback_status();
```

| Column | Type | Description |
|---|---|---|
| `table_count` | bigint | Number of tables currently in the recycle bin |
| `table_limit` | int | Maximum allowed tables (`flashback.max_tables`) |
| `total_size_bytes` | bigint | Total physical size of recycled tables |
| `size_limit_mb` | int | Maximum allowed size in MB (`flashback.max_size`) |
| `retention_days` | int | Retention period in days (`flashback.retention_days`) |
| `worker_interval_seconds` | int | Background worker run interval (`flashback.worker_interval_seconds`) |
| `oldest_entry` | text | Timestamp of the oldest entry in the recycle bin |
| `newest_entry` | text | Timestamp of the newest entry in the recycle bin |

---

## Configuration (GUCs)

All settings are adjustable at runtime without restarting PostgreSQL (requires superuser or `pg_suset` role).

| GUC | Default | Range | Description |
|---|---|---|---|
| `flashback.retention_days` | `7` | 1 – 365 | Days to keep dropped tables before auto-purge |
| `flashback.max_tables` | `100` | 1 – 10000 | Maximum number of tables in the recycle bin |
| `flashback.max_size` | `102400` (100 GB) | 1 – 1048576 MB | Maximum total size of the recycle bin |
| `flashback.worker_interval_seconds` | `60` | 10 – 86400 | How often the cleanup worker runs |
| `flashback.excluded_schemas` | *(empty)* | — | Comma-separated list of schemas to exclude |
| `flashback.database_name` | `postgres` | — | Database the background worker connects to; set this when pg_flashback is installed in a database other than `postgres` |

### Examples

```sql
-- Keep tables for 30 days
ALTER SYSTEM SET flashback.retention_days = 30;

-- Allow at most 50 tables in the recycle bin
ALTER SYSTEM SET flashback.max_tables = 50;

-- Set a 10 GB size cap
ALTER SYSTEM SET flashback.max_size = 10240;

-- Run the cleanup worker every 10 minutes
ALTER SYSTEM SET flashback.worker_interval_seconds = 600;

-- Exclude staging and temp schemas from the recycle bin
ALTER SYSTEM SET flashback.excluded_schemas = 'staging,temp_data';

-- Tell the background worker to connect to a non-default database
ALTER SYSTEM SET flashback.database_name = 'myapp';

-- Apply without restart
SELECT pg_reload_conf();
```

---

## Permissions

| Scenario | Behavior |
|---|---|
| Superuser drops a table | Captured; superuser can restore, purge, or list it |
| Regular user drops a table | Captured; only that user (or a superuser) can restore or purge it |
| Regular user calls `flashback_list_recycled_tables()` | Returns only their own dropped tables |
| Regular user tries to restore another user's table | Returns `false` with a `WARNING` |

All public functions are `SECURITY DEFINER` so that regular users can access the `flashback` schema without being granted direct schema privileges.

---

## TRUNCATE Support

pg_flashback intercepts `TRUNCATE TABLE`: before the truncation runs, the current data is copied into a backup table in `flashback_recycle`. The actual `TRUNCATE` executes normally, leaving the table empty.

```sql
TRUNCATE TABLE orders;

SELECT * FROM flashback_list_recycled_tables();  -- backup entry visible

SELECT flashback_restore('orders', NULL);        -- data restored
SELECT * FROM orders;                            -- rows are back
```

---

## What Gets Skipped

The following `DROP TABLE` commands are silently ignored by pg_flashback (the table is dropped normally):

- Temporary tables (`CREATE TEMP TABLE ... DROP TABLE ...`)
- Tables in the `flashback` or `flashback_recycle` schemas
- Tables in schemas matching `pg_temp*`
- Tables in schemas listed in `flashback.excluded_schemas`

---

## Internal Schema

pg_flashback creates two schemas on install:

- **`flashback`** — contains the `operations` metadata table and all public SQL functions
- **`flashback_recycle`** — physical storage for dropped/truncated tables, named `<original_name>_<op_id>`

---

## Limitations

- `flashback_purge` removes only the most recently dropped version; use `flashback_purge_by_id` to remove a specific version.
- **TRUNCATE on large tables**: the backup is a full data copy (`CREATE TABLE ... AS SELECT *`). On very large tables this will consume additional disk space equal to the original table size and may take noticeable time. For tables in that size range consider excluding the schema via `flashback.excluded_schemas`.
- **TRUNCATE restore constraints**: restoring a TRUNCATE entry requires the target table to still exist, and it fails if the table is currently referenced by incoming foreign keys from other tables.
- When a partitioned table's child partitions are missing after restore (edge case: child was in a different schema that was also dropped), pg_flashback recreates the partition shell but the original data is gone — a `WARNING` is emitted.
- Materialized views dependent on a dropped table are dropped along with it (on `CASCADE`) and recreated from captured definitions on restore. Mview-specific runtime state (for example last refresh timing/history) is not preserved.

---

## Function Name Variants

For operation-ID based flows, both names are available and equivalent:

- `flashback_restore_by_id` and `flashback_restore_by_op_id`
- `flashback_purge_by_id` and `flashback_purge_by_op_id`
