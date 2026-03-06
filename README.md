# pg_flashback

A PostgreSQL extension that adds a **recycle bin** for dropped tables. Instead of permanently losing data when a `DROP TABLE` is executed, pg_flashback intercepts the command, moves the table to a safe schema, and lets you restore it later.

> Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

---

## Features

- Automatically captures `DROP TABLE` (including `DROP TABLE ... CASCADE`) and `TRUNCATE TABLE`
- Stores dropped/truncated tables in a `flashback_recycle` schema until restored or expired
- Restore by table name or by specific operation ID (useful when a table was dropped multiple times)
- Sequence state automatically restored after TRUNCATE recovery
- FIFO eviction when table count or total size limits are reached
- Background worker that automatically purges expired entries
- Permission model: superusers see and manage all tables; regular users can only see and restore their own
- Configurable via PostgreSQL GUCs (no config files needed)
- Skips temporary tables, internal schemas, and user-defined excluded schemas
- Race condition safe: advisory locks keyed by `op_id` prevent concurrent restores of the same operation

---

## Requirements

- PostgreSQL 17
- pgrx 0.16.1
- Rust (stable)

---

## Installation

```bash
git clone https://github.com/CaghanTU/pg_flashback.git
cd pg_flashback
./install.sh
```

Then load the extension in PostgreSQL:

```sql
-- postgresql.conf (add this line):
shared_preload_libraries = 'pg_flashback'

-- After restarting PostgreSQL:
CREATE EXTENSION pg_flashback;
```

---

## How It Works

1. pg_flashback registers a DDL hook that fires on every `DROP TABLE` and `TRUNCATE TABLE`.
2. On `DROP TABLE`: the table is moved to the `flashback_recycle` schema and renamed with a unique op_id suffix (e.g. `orders_42`).
3. On `TRUNCATE TABLE`: the data is copied into a backup table in `flashback_recycle` before truncation runs.
4. Metadata is recorded in the `flashback.operations` table (original schema, table name, owner, operation type, retention deadline, etc.).
5. A background worker periodically scans `flashback.operations` and purges entries that have exceeded their retention period.
6. FIFO eviction runs before each capture: if the table count or total size limit is reached, the oldest entry is removed to make room.

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

pg_flashback also intercepts `TRUNCATE TABLE`. Before the truncation runs, the current data is copied into a backup table in the `flashback_recycle` schema. The actual `TRUNCATE` then executes normally, leaving the table empty.

```sql
TRUNCATE TABLE orders;

SELECT * FROM flashback_list_recycled_tables();  -- backup entry visible

SELECT flashback_restore('orders', NULL);        -- data restored
SELECT * FROM orders;                            -- rows are back
```

| | DROP TABLE | TRUNCATE TABLE |
|---|---|---|
| Table after operation | Gone | Empty, still exists |
| What pg_flashback saves | The whole table | A copy of the data |
| How restore works | Moves table back | `INSERT INTO ... SELECT * FROM backup` |

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

## Limitations / Roadmap

- **No dependency tracking**: if `DROP TABLE ... CASCADE` drops dependent views or foreign keys, those are not captured and cannot be restored.
- `flashback_purge` removes only the most recently dropped version; use `flashback_purge_by_id` to remove a specific version.
- `DROP SCHEMA ... CASCADE` is not intercepted; individual table drops within a schema cascade are not captured.

---

## License

MIT
