use pgrx::prelude::*;
use chrono::Local;
use crate::guc;

// Deletes the oldest table in the recycle bin (both the physical table and the operations record)
fn evict_oldest() {
    let find_sql = "SELECT recycled_name FROM flashback.operations ORDER BY retention_until ASC LIMIT 1";

    let oldest = Spi::get_one::<String>(find_sql);

    match oldest {
        Ok(Some(recycled_name)) => {
            let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{}", recycled_name);
            let delete_sql = format!("DELETE FROM flashback.operations WHERE recycled_name = '{}'", recycled_name);

            if let Err(e) = Spi::run(&drop_sql) {
                pgrx::warning!("Evict drop error: {}", e);
                return;
            }
            if let Err(e) = Spi::run(&delete_sql) {
                pgrx::warning!("Evict delete error: {}", e);
            }

            pgrx::log!("FIFO evict: {} removed", recycled_name);
        }
        _ => {
            pgrx::warning!("Evict: no table found to remove");
        }
    }
}

// If table count exceeds max_tables, delete the oldest
fn enforce_table_limit() {
    let count_sql = "SELECT COUNT(*)::int FROM flashback.operations";

    if let Ok(Some(count)) = Spi::get_one::<i32>(count_sql) {
        let max = guc::get_max_tables();
        if count >= max {
            pgrx::log!("Table limit reached ({}/{}), removing oldest", count, max);
            evict_oldest();
        }
    }
}

// If total size exceeds max_size, delete the oldest (comparison in MB)
fn enforce_size_limit() {
    let size_sql = "SELECT COALESCE(SUM(pg_total_relation_size(quote_ident('flashback_recycle') || '.' || quote_ident(recycled_name))), 0)::bigint FROM flashback.operations WHERE restored = false";

    if let Ok(Some(total_bytes)) = Spi::get_one::<i64>(size_sql) {
        let total_mb = total_bytes / (1024 * 1024);
        let max_mb = guc::get_max_size() as i64;
        if total_mb >= max_mb {
            pgrx::log!("Size limit reached ({}/{} MB), removing oldest", total_mb, max_mb);
            evict_oldest();
        }
    }
}

pub fn handle_drop_table(query: &str) -> bool {
    enforce_table_limit();
    enforce_size_limit();

    let table_name = query
        .trim()
        .trim_end_matches(';')
        .trim()
        .split_whitespace()
        .last()
        .unwrap_or("");

    // Can be schema.table or just table
    let (schema, bare_table) = if table_name.contains('.') {
        let (s, t) = table_name.split_once('.').unwrap();
        (s.to_string(), t.to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    // SQL injection protection
    if bare_table.contains('\'') || bare_table.contains(';') {
        pgrx::warning!("Invalid table name: {}", bare_table);
        return false;
    }
    if schema.contains('\'') || schema.contains(';') {
        pgrx::warning!("Invalid schema name: {}", schema);
        return false;
    }

    pgrx::log!("Table name: {}.{}", schema, bare_table);
    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();

    let recycled_name = format!("{}_{}", bare_table, timestamp);

    let move_sql = format!("ALTER TABLE {}.{} SET SCHEMA flashback_recycle", schema, bare_table);
    let rename_sql = format!("ALTER TABLE flashback_recycle.{} RENAME TO {}", bare_table, recycled_name);

    if let Err(e) = Spi::run(&move_sql) {
    pgrx::warning!("Move error: {}", e);
    return false;
    }

    if let Err(e) = Spi::run(&rename_sql) {
    pgrx::warning!("Rename error: {}", e);
    return false;
    }

    let insert_sql = format!(
    "INSERT INTO flashback.operations (operation_type, schema_name, table_name, recycled_name, role_name, retention_until)
     VALUES ('DROP', '{}', '{}', '{}', current_user, now() + interval '{} days')",
    schema, bare_table, recycled_name, guc::get_retention_days()
    );

    if let Err(e) = Spi::run(&insert_sql) {
        pgrx::warning!("Insertion error: {}", e);
        return false;
    }

    true
}