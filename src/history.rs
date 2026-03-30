use pgrx::prelude::*;
use crate::context::qi;

// ---------------------------------------------------------------------------
// Table tracking management
// ---------------------------------------------------------------------------

/// Install a row-history trigger on `table_name`.
///
/// After calling this, every INSERT, UPDATE, and DELETE on the target table
/// is logged to `flashback.row_history`.  The trigger is an AFTER / FOR EACH ROW
/// trigger that calls the shared `flashback.history_trigger_fn()` function.
///
/// `table_name` should be schema-qualified, e.g. `'public.orders'`.
///
/// ```sql
/// SELECT flashback_track_table('public.orders');
/// ```
#[pg_extern]
pub fn flashback_track_table(table_name: &str) -> bool {
    let (schema, table) = split_schema_table(table_name);

    // Validate the table exists
    let exists = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS ( \
            SELECT 1 FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            WHERE n.nspname = {} AND c.relname = {} AND c.relkind = 'r')",
        quote_literal(&schema),
        quote_literal(&table)
    ))
    .unwrap_or(None)
    .unwrap_or(false);

    if !exists {
        pgrx::warning!(
            "pg_recyclebin: table '{}.{}' does not exist — cannot track it",
            schema, table
        );
        return false;
    }

    // Drop any existing trigger first (idempotent re-install)
    let drop_sql = format!(
        "DROP TRIGGER IF EXISTS flashback_history ON {}.{}",
        qi(&schema), qi(&table)
    );
    if let Err(e) = Spi::run(&drop_sql) {
        pgrx::warning!(
            "pg_recyclebin: could not drop existing trigger on '{}.{}': {}",
            schema, table, e
        );
        return false;
    }

    let create_sql = format!(
        "CREATE TRIGGER flashback_history \
         AFTER INSERT OR UPDATE OR DELETE ON {}.{} \
         FOR EACH ROW EXECUTE FUNCTION flashback.history_trigger_fn()",
        qi(&schema), qi(&table)
    );

    if let Err(e) = Spi::run(&create_sql) {
        pgrx::warning!(
            "pg_recyclebin: could not install history trigger on '{}.{}': {}",
            schema, table, e
        );
        return false;
    }

    pgrx::log!(
        "pg_recyclebin: history tracking enabled for '{}.{}'",
        schema, table
    );
    true
}

/// Remove the row-history trigger from `table_name`.
///
/// ```sql
/// SELECT flashback_untrack_table('public.orders');
/// ```
#[pg_extern]
pub fn flashback_untrack_table(table_name: &str) -> bool {
    let (schema, table) = split_schema_table(table_name);

    let drop_sql = format!(
        "DROP TRIGGER IF EXISTS flashback_history ON {}.{}",
        qi(&schema), qi(&table)
    );

    if let Err(e) = Spi::run(&drop_sql) {
        pgrx::warning!(
            "pg_recyclebin: could not remove history trigger from '{}.{}': {}",
            schema, table, e
        );
        return false;
    }

    pgrx::log!(
        "pg_recyclebin: history tracking disabled for '{}.{}'",
        schema, table
    );
    true
}

// ---------------------------------------------------------------------------
// Query functions
// ---------------------------------------------------------------------------

/// Return rows deleted from `table_name` within the last `since` interval.
///
/// `since` is a PostgreSQL interval string, e.g. `'1 hour'`, `'30 minutes'`, `'2 days'`.
/// Default is `'1 hour'`.
///
/// ```sql
/// SELECT * FROM flashback_deleted_since('public.orders', '1 hour');
/// SELECT * FROM flashback_deleted_since('orders');   -- uses default 1-hour window
/// ```
#[pg_extern]
pub fn flashback_deleted_since(
    table_name: &str,
    since: default!(&str, "'1 hour'"),
) -> TableIterator<
    'static,
    (
        name!(changed_at, String),
        name!(old_data, pgrx::JsonB),
        name!(txid, i64),
    ),
> {
    let (schema, table) = split_schema_table(table_name);

    let query = format!(
        "SELECT changed_at::text, old_data, COALESCE(txid, 0)::bigint \
         FROM flashback.row_history \
         WHERE schema_name = {} \
           AND table_name = {} \
           AND operation = 'DELETE' \
           AND changed_at >= now() - {}::interval \
         ORDER BY changed_at DESC",
        quote_literal(&schema),
        quote_literal(&table),
        quote_literal(since)
    );

    let rows = Spi::connect(|client| {
        let mut results: Vec<(String, pgrx::JsonB, i64)> = Vec::new();
        let tup_table = client.select(&query, None, &[])?;
        for row in tup_table {
            let changed_at = row.get::<String>(1)?.unwrap_or_default();
            let old_data = row.get::<pgrx::JsonB>(2)?.unwrap_or(pgrx::JsonB(serde_json::Value::Null));
            let txid = row.get::<i64>(3)?.unwrap_or(0);
            results.push((changed_at, old_data, txid));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_default();

    TableIterator::new(rows.into_iter())
}

/// Return all change history rows for `table_name` within the last `since` interval.
///
/// `ops` filters by operation type: `'ALL'` (default), `'INSERT'`, `'UPDATE'`, `'DELETE'`.
///
/// ```sql
/// SELECT * FROM flashback_row_history('public.orders', '2 hours');
/// SELECT * FROM flashback_row_history('public.orders', '1 day', 'DELETE');
/// ```
#[pg_extern]
pub fn flashback_row_history(
    table_name: &str,
    since: default!(&str, "'1 hour'"),
    ops: default!(&str, "'ALL'"),
) -> TableIterator<
    'static,
    (
        name!(changed_at, String),
        name!(operation, String),
        name!(old_data, Option<pgrx::JsonB>),
        name!(new_data, Option<pgrx::JsonB>),
        name!(txid, i64),
    ),
> {
    let (schema, table) = split_schema_table(table_name);

    let ops_upper = ops.to_uppercase();
    let op_filter = if ops_upper == "ALL" {
        String::new()
    } else {
        format!("AND operation = {}", quote_literal(&ops_upper))
    };

    let query = format!(
        "SELECT changed_at::text, operation, old_data, new_data, COALESCE(txid, 0)::bigint \
         FROM flashback.row_history \
         WHERE schema_name = {} \
           AND table_name = {} \
           AND changed_at >= now() - {}::interval \
           {} \
         ORDER BY changed_at DESC",
        quote_literal(&schema),
        quote_literal(&table),
        quote_literal(since),
        op_filter
    );

    let rows = Spi::connect(|client| {
        let mut results: Vec<(String, String, Option<pgrx::JsonB>, Option<pgrx::JsonB>, i64)> = Vec::new();
        let tup_table = client.select(&query, None, &[])?;
        for row in tup_table {
            let changed_at = row.get::<String>(1)?.unwrap_or_default();
            let operation = row.get::<String>(2)?.unwrap_or_default();
            let old_data = row.get::<pgrx::JsonB>(3)?;
            let new_data = row.get::<pgrx::JsonB>(4)?;
            let txid = row.get::<i64>(5)?.unwrap_or(0);
            results.push((changed_at, operation, old_data, new_data, txid));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_default();

    TableIterator::new(rows.into_iter())
}

/// Return a summary of tracked tables (those with a `flashback_history` trigger installed).
///
/// ```sql
/// SELECT * FROM flashback_tracked_tables();
/// ```
#[pg_extern]
pub fn flashback_tracked_tables() -> TableIterator<
    'static,
    (
        name!(schema_name, String),
        name!(table_name, String),
        name!(history_rows, i64),
        name!(oldest_change, String),
        name!(newest_change, String),
    ),
> {
    let query = "\
        SELECT n.nspname::text, c.relname::text, \
               COALESCE(h.cnt, 0)::bigint, \
               COALESCE(h.oldest, '-'), \
               COALESCE(h.newest, '-') \
        FROM pg_trigger t \
        JOIN pg_class c ON c.oid = t.tgrelid \
        JOIN pg_namespace n ON n.oid = c.relnamespace \
        LEFT JOIN LATERAL ( \
            SELECT COUNT(*)::bigint AS cnt, \
                   MIN(changed_at)::text AS oldest, \
                   MAX(changed_at)::text AS newest \
            FROM flashback.row_history rh \
            WHERE rh.schema_name = n.nspname \
              AND rh.table_name = c.relname \
        ) h ON true \
        WHERE t.tgname = 'flashback_history' \
        ORDER BY n.nspname, c.relname";

    let rows = Spi::connect(|client| {
        let mut results: Vec<(String, String, i64, String, String)> = Vec::new();
        let tup_table = client.select(query, None, &[])?;
        for row in tup_table {
            let schema = row.get::<String>(1)?.unwrap_or_default();
            let table = row.get::<String>(2)?.unwrap_or_default();
            let cnt = row.get::<i64>(3)?.unwrap_or(0);
            let oldest = row.get::<String>(4)?.unwrap_or_else(|| "-".to_string());
            let newest = row.get::<String>(5)?.unwrap_or_else(|| "-".to_string());
            results.push((schema, table, cnt, oldest, newest));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_default();

    TableIterator::new(rows.into_iter())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Split `schema.table` into `(schema, table)`.
/// If no schema is given, defaults to `public`.
fn split_schema_table(input: &str) -> (String, String) {
    let parts: Vec<&str> = input.splitn(2, '.').collect();
    if parts.len() == 2 {
        (parts[0].trim().to_string(), parts[1].trim().to_string())
    } else {
        ("public".to_string(), input.trim().to_string())
    }
}

/// Wrap a string in single-quotes for safe embedding in SQL literals.
/// Escapes any internal single quotes by doubling them.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
