use pgrx::prelude::*;
use crate::guc;

// Deletes the oldest table in the recycle bin (both the physical table and the operations record)
fn evict_oldest() {
    let find_sql = "SELECT recycled_name FROM flashback.operations WHERE restored = false ORDER BY retention_until ASC LIMIT 1";

    let oldest = Spi::get_one::<String>(find_sql);

    match oldest {
        Ok(Some(recycled_name)) => {
            let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{}", recycled_name);
            let delete_sql = format!("DELETE FROM flashback.operations WHERE recycled_name = '{}' AND restored = false", recycled_name);

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
    let count_sql = "SELECT COUNT(*)::int FROM flashback.operations WHERE restored = false";

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
    // Skip silently if the extension is not yet installed
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')").unwrap_or(None) != Some(true) {
        return false;
    }
    enforce_table_limit();
    enforce_size_limit();

    let table_name = query
        .trim()
        .trim_end_matches(';')
        .trim()
        .split_whitespace()
        .filter(|w| {
            let u = w.to_uppercase();
            u != "CASCADE" && u != "RESTRICT"
        })
        .last()
        .unwrap_or("");

    // Can be schema.table or just table
    let (schema, bare_table) = if table_name.contains('.') {
        let (s, t) = table_name.split_once('.').unwrap();
        (s.to_string(), t.to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    // Skip internal and system schemas
    if schema == "flashback" || schema == "flashback_recycle" || schema.starts_with("pg_temp") {
        return false;
    }

    // Skip if schema is in excluded list
    let excluded = crate::guc::get_excluded_schemas();
    if excluded.iter().any(|s| s == &schema) {
        return false;
    }

    // SQL injection protection
    if bare_table.contains('\'') || bare_table.contains(';') {
        pgrx::warning!("Invalid table name: {}", bare_table);
        return false;
    }
    if schema.contains('\'') || schema.contains(';') {
        pgrx::warning!("Invalid schema name: {}", schema);
        return false;
    }

    // Skip temp tables: relpersistence = 't' means temporary
    let is_temp = Spi::get_one::<String>(&format!(
        "SELECT relpersistence::text FROM pg_class WHERE relname = '{}' LIMIT 1",
        bare_table
    ))
    .unwrap_or(None)
    .unwrap_or_default();

    if is_temp == "t" || is_temp.is_empty() {
        return false;
    }

    pgrx::log!("Table name: {}.{}", schema, bare_table);

    // Reserve a unique op_id first; use it as the recycled_name suffix to guarantee uniqueness.
    let reserve_sql = format!(
        "INSERT INTO flashback.operations \
         (operation_type, schema_name, table_name, recycled_name, role_name, retention_until) \
         VALUES ('DROP', '{}', '{}', '', current_user, now() + interval '{} days') \
         RETURNING op_id",
        schema, bare_table, guc::get_retention_days()
    );
    let op_id = match Spi::get_one::<i64>(&reserve_sql) {
        Ok(Some(id)) => id,
        _ => {
            pgrx::warning!("Failed to reserve op_id for '{}'", bare_table);
            return false;
        }
    };

    let recycled_name = format!("{}_{}", bare_table, op_id);

    let move_sql = format!("ALTER TABLE {}.{} SET SCHEMA flashback_recycle", schema, bare_table);
    let rename_sql = format!("ALTER TABLE flashback_recycle.{} RENAME TO {}", bare_table, recycled_name);

    if let Err(e) = Spi::run(&move_sql) {
        pgrx::warning!("Move error: {}", e);
        let _ = Spi::run(&format!("DELETE FROM flashback.operations WHERE op_id = {}", op_id));
        return false;
    }

    if let Err(e) = Spi::run(&rename_sql) {
        pgrx::warning!("Rename error: {}", e);
        let _ = Spi::run(&format!("DELETE FROM flashback.operations WHERE op_id = {}", op_id));
        return false;
    }

    // Rename owned sequences with op_id suffix to prevent name collisions when the
    // same table is dropped again before being restored (e.g. orders_id_seq -> orders_id_seq_42).
    let mut seq_names: Vec<String> = Vec::new();
    let _ = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT s.relname \
                 FROM pg_depend d \
                 JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
                 JOIN pg_class t ON t.oid = d.refobjid AND t.relname = '{}' \
                 WHERE d.deptype IN ('a', 'i') \
                   AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flashback_recycle')",
                recycled_name
            ),
            None, &[],
        )?;
        for row in rows {
            if let Ok(Some(n)) = row.get::<String>(1) {
                seq_names.push(n);
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    });
    for seq_name in &seq_names {
        let new_seq_name = format!("{}_{}", seq_name, op_id);
        if let Err(e) = Spi::run(&format!(
            "ALTER SEQUENCE flashback_recycle.{} RENAME TO {}",
            seq_name, new_seq_name
        )) {
            pgrx::warning!("Failed to rename sequence '{}': {}", seq_name, e);
        }
    }

    if let Err(e) = Spi::run(&format!(
        "UPDATE flashback.operations SET recycled_name = '{}' WHERE op_id = {}",
        recycled_name, op_id
    )) {
        pgrx::warning!("Failed to update recycled_name: {}", e);
        return false;
    }

    true
}

pub fn handle_truncate_table(query: &str) -> bool {
    // Skip silently if the extension is not yet installed
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')").unwrap_or(None) != Some(true) {
        return false;
    }
    // Extract table name: "TRUNCATE [TABLE] orders [CASCADE|RESTRICT|...]"
    let table_name = query
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .filter(|w| {
            let u = w.to_uppercase();
            u != "TRUNCATE" && u != "TABLE" && u != "CASCADE"
                && u != "RESTRICT" && u != "ONLY"
        })
        .next()
        .unwrap_or("");

    let (schema, bare_table) = if table_name.contains('.') {
        let (s, t) = table_name.split_once('.').unwrap();
        (s.to_string(), t.to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    if schema == "flashback" || schema == "flashback_recycle" || schema.starts_with("pg_temp") {
        return false;
    }

    let excluded = crate::guc::get_excluded_schemas();
    if excluded.iter().any(|s| s == &schema) {
        return false;
    }

    if bare_table.contains('\'') || bare_table.contains(';') || schema.contains('\'') || schema.contains(';') {
        pgrx::warning!("Invalid table or schema name in TRUNCATE");
        return false;
    }

    // Check relpersistence — skip temp tables
    let is_temp = Spi::get_one::<String>(&format!(
        "SELECT relpersistence::text FROM pg_class WHERE relname = '{}' LIMIT 1",
        bare_table
    ))
    .unwrap_or(None)
    .unwrap_or_default();

    if is_temp == "t" || is_temp.is_empty() {
        return false;
    }

    enforce_table_limit();
    enforce_size_limit();

    // Reserve a unique op_id first; use it as the recycled_name suffix to guarantee uniqueness.
    let reserve_sql = format!(
        "INSERT INTO flashback.operations \
         (operation_type, schema_name, table_name, recycled_name, role_name, retention_until) \
         VALUES ('TRUNCATE', '{}', '{}', '', current_user, now() + interval '{} days') \
         RETURNING op_id",
        schema, bare_table, guc::get_retention_days()
    );
    let op_id = match Spi::get_one::<i64>(&reserve_sql) {
        Ok(Some(id)) => id,
        _ => {
            pgrx::warning!("Failed to reserve op_id for TRUNCATE of '{}'", bare_table);
            return false;
        }
    };

    let recycled_name = format!("{}_{}", bare_table, op_id);

    // Collect sequence info for all serial columns
    let seq_info = {
        let mut map = serde_json::Map::new();
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT column_name, pg_get_serial_sequence('{}.{}', column_name) \
                    FROM information_schema.columns \
                    WHERE table_schema = '{}' AND table_name = '{}' \
                    AND column_default LIKE 'nextval%%'",
                    schema, bare_table, schema, bare_table
                ),
                None,
                &[],
            )?;
            for row in rows {
                let col = row.get::<String>(1)?.unwrap_or_default();
                let seq = row.get::<String>(2)?.unwrap_or_default();
                if !seq.is_empty() {
                    if let Ok(Some(last_val)) = Spi::get_one::<i64>(
                        &format!("SELECT last_value FROM {}", seq)
                    ) {
                        let mut obj = serde_json::Map::new();
                        obj.insert("seq".into(), serde_json::Value::String(seq));
                        obj.insert("last_value".into(), serde_json::Value::Number(last_val.into()));
                        map.insert(col, serde_json::Value::Object(obj));
                    }
                }
            }
            Ok::<_, spi::Error>(())
        });
        serde_json::Value::Object(map)
    };
    let metadata_json = seq_info.to_string();

    // Copy data into a new backup table in flashback_recycle schema
    let create_sql = format!(
        "CREATE TABLE flashback_recycle.{} AS SELECT * FROM {}.{}",
        recycled_name, schema, bare_table
    );

    if let Err(e) = Spi::run(&create_sql) {
        pgrx::warning!("TRUNCATE backup error: {}", e);
        let _ = Spi::run(&format!("DELETE FROM flashback.operations WHERE op_id = {}", op_id));
        return false;
    }

    if let Err(e) = Spi::run(&format!(
        "UPDATE flashback.operations SET recycled_name = '{}', metadata = '{}' WHERE op_id = {}",
        recycled_name, metadata_json, op_id
    )) {
        pgrx::warning!("TRUNCATE metadata update error: {}", e);
        return false;
    }

    pgrx::log!("TRUNCATE captured: {}.{} -> {}", schema, bare_table, recycled_name);
    false  // false = don't suppress the actual TRUNCATE, let PostgreSQL execute it
}