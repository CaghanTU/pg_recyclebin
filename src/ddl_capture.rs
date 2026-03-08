use pgrx::prelude::*;
use crate::guc;

/// Returns JSON array of FK constraints defined on OTHER tables that reference this table.
/// Each entry: { "table": "schema.tblname", "def": "ALTER TABLE ... ADD CONSTRAINT ..." }
/// These are captured so we can warn or restore them after the table is brought back.
/// Note: constraints ON the captured table itself travel with the physical table;
/// only constraints on FOREIGN tables referencing us are lost when we disappear.
fn capture_incoming_fks(schema: &str, table: &str) -> Vec<serde_json::Value> {
    let json_str = Spi::get_one::<String>(&format!(
        "SELECT COALESCE( \
             jsonb_agg(jsonb_build_object( \
                 'table', quote_ident(ns2.nspname) || '.' || quote_ident(rc.relname), \
                 'constraint', quote_ident(c.conname), \
                 'def', 'ALTER TABLE ' || quote_ident(ns2.nspname) || '.' || \
                         quote_ident(rc.relname) || ' ADD CONSTRAINT ' || \
                         quote_ident(c.conname) || ' ' || \
                         pg_get_constraintdef(c.oid) \
             )), '[]'::jsonb)::text \
         FROM pg_constraint c \
         JOIN pg_class rc   ON rc.oid = c.conrelid \
         JOIN pg_namespace ns2 ON ns2.oid = rc.relnamespace \
         WHERE c.confrelid = ( \
             SELECT t.oid FROM pg_class t \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE t.relname = '{}' AND n.nspname = '{}' LIMIT 1 \
         ) AND c.contype = 'f'",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or_else(|| "[]".to_string());

    serde_json::from_str::<Vec<serde_json::Value>>(&json_str).unwrap_or_default()
}

/// Returns JSON array of RLS policies on this table.
/// Each entry: { "name": "...", "cmd": "...", "roles": "...", "qual": "...", "with_check": "..." }
/// PostgreSQL drops policies when the table is dropped; we need to recreate them on restore.
fn capture_rls_policies(schema: &str, table: &str) -> Vec<serde_json::Value> {
    let json_str = Spi::get_one::<String>(&format!(
        "SELECT COALESCE( \
             jsonb_agg(jsonb_build_object( \
                 'name',       polname, \
                 'permissive', polpermissive, \
                 'cmd',        CASE polcmd \
                                 WHEN 'r' THEN 'SELECT' \
                                 WHEN 'a' THEN 'INSERT' \
                                 WHEN 'w' THEN 'UPDATE' \
                                 WHEN 'd' THEN 'DELETE' \
                                 ELSE 'ALL' END, \
                 'roles',      (SELECT string_agg(rolname, ', ') \
                                FROM pg_roles \
                                WHERE oid = ANY(polroles)), \
                 'qual',       COALESCE(pg_get_expr(polqual, polrelid), ''), \
                 'with_check', COALESCE(pg_get_expr(polwithcheck, polrelid), '') \
             )), '[]'::jsonb)::text \
         FROM pg_policy \
         WHERE polrelid = ( \
             SELECT c.oid FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{}' AND n.nspname = '{}' LIMIT 1 \
         )",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or_else(|| "[]".to_string());

    serde_json::from_str::<Vec<serde_json::Value>>(&json_str).unwrap_or_default()
}

/// Returns partition info if this table is a partitioned parent.
/// { "is_partitioned": true, "strategy": "RANGE|LIST|HASH",
///   "children": [ { "schema": "...", "name": "...",
///                   "def": "CREATE TABLE ... PARTITION OF ... FOR VALUES ..." } ] }
fn capture_partition_info(schema: &str, table: &str) -> Option<serde_json::Value> {
    // Check if this is a partitioned parent (relkind = 'p')
    let is_partitioned = Spi::get_one::<bool>(&format!(
        "SELECT relkind = 'p' FROM pg_class \
         WHERE relname = '{}' \
           AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}' LIMIT 1)",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or(false);

    if !is_partitioned {
        return None;
    }

    // Get partitioning strategy from pg_partitioned_table
    let strategy = Spi::get_one::<String>(&format!(
        "SELECT CASE partstrat \
                  WHEN 'r' THEN 'RANGE' \
                  WHEN 'l' THEN 'LIST' \
                  WHEN 'h' THEN 'HASH' \
                  ELSE 'UNKNOWN' END \
         FROM pg_partitioned_table pt \
         JOIN pg_class c ON c.oid = pt.partrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = '{}' AND n.nspname = '{}' LIMIT 1",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or_else(|| "UNKNOWN".to_string());

    // Get child partitions with their bound definitions
    let children_json = Spi::get_one::<String>(&format!(
        "SELECT COALESCE( \
             jsonb_agg(jsonb_build_object( \
                 'schema', quote_ident(cn.nspname), \
                 'name',   quote_ident(cc.relname), \
                 'def',    pg_get_expr(cc.relpartbound, cc.oid, true) \
             ) ORDER BY cc.relname), '[]'::jsonb)::text \
         FROM pg_inherits i \
         JOIN pg_class cc   ON cc.oid = i.inhrelid \
         JOIN pg_namespace cn ON cn.oid = cc.relnamespace \
         WHERE i.inhparent = ( \
             SELECT c.oid FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{}' AND n.nspname = '{}' LIMIT 1 \
         )",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or_else(|| "[]".to_string());

    let children: Vec<serde_json::Value> =
        serde_json::from_str(&children_json).unwrap_or_default();

    Some(serde_json::json!({
        "is_partitioned": true,
        "strategy": strategy,
        "children": children
    }))
}

/// Queries pg_depend for all views that directly depend on `table` in `schema`.
/// PostgreSQL stores view→table dependencies via pg_rewrite entries, so we join:
///   pg_depend.objid → pg_rewrite.oid → pg_rewrite.ev_class → pg_class (view)
/// Returns a serde_json array (may be empty) — called before the table is moved.
fn capture_dependent_views(schema: &str, table: &str) -> Vec<serde_json::Value> {
    // Use string_agg → Spi::get_one so we avoid Spi::connect inside a hook context.
    let json_str = Spi::get_one::<String>(&format!(
        "SELECT COALESCE( \
             jsonb_agg(jsonb_build_object( \
                 'schema', n.nspname, \
                 'name',   v.relname, \
                 'def',    pg_get_viewdef(v.oid, true) \
             )), '[]'::jsonb)::text \
         FROM pg_depend d \
         JOIN pg_rewrite r  ON r.oid = d.objid \
                           AND d.classid = 'pg_rewrite'::regclass \
         JOIN pg_class   v  ON v.oid = r.ev_class AND v.relkind = 'v' \
         JOIN pg_namespace n ON n.oid = v.relnamespace \
         WHERE d.refobjid = ( \
             SELECT c.oid FROM pg_class c \
             JOIN pg_namespace ns ON ns.oid = c.relnamespace \
             WHERE c.relname = '{}' AND ns.nspname = '{}' \
             LIMIT 1 \
         ) AND d.deptype = 'n' \
           AND v.relname != '{}'",
        table, schema, table
    ))
    .unwrap_or(None)
    .unwrap_or_else(|| "[]".to_string());

    serde_json::from_str::<Vec<serde_json::Value>>(&json_str).unwrap_or_default()
}

/// Core inner function: moves a single verified regular table into flashback_recycle.
/// Callers MUST have already done: extension check, injection check, temp-table skip,
/// partition skip, and excluded-schema skip.
fn capture_drop_inner(schema: &str, bare_table: &str) -> bool {
    enforce_table_limit();
    enforce_size_limit();

    // Capture dependent views before the table is moved (they'll be dropped by PG otherwise).
    let dep_views = capture_dependent_views(schema, bare_table);
    // Capture FK constraints from OTHER tables that reference this table.
    let incoming_fks = capture_incoming_fks(schema, bare_table);
    // Capture RLS policies (dropped by PG when the table is dropped).
    let rls_policies = capture_rls_policies(schema, bare_table);
    // Capture partition metadata if this is a partitioned parent.
    let partition_info = capture_partition_info(schema, bare_table);

    // Reserve a unique op_id; use it as the recycled_name suffix.
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

    let move_sql   = format!("ALTER TABLE {}.{} SET SCHEMA flashback_recycle", schema, bare_table);
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

    // Rename owned sequences with op_id suffix to prevent collisions on re-drop.
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

    // Build metadata object: always include all keys (empty arrays are fine).
    let mut meta_obj = serde_json::Map::new();
    meta_obj.insert("views".into(), serde_json::Value::Array(dep_views));
    meta_obj.insert("incoming_fks".into(), serde_json::Value::Array(incoming_fks));
    meta_obj.insert("rls_policies".into(), serde_json::Value::Array(rls_policies));
    if let Some(pi) = partition_info {
        meta_obj.insert("partition_info".into(), pi);
    }
    let meta = serde_json::Value::Object(meta_obj);
    let meta_str = meta.to_string().replace('\'', "''");
    let update_sql = format!(
        "UPDATE flashback.operations SET recycled_name = '{}', metadata = '{}'::jsonb WHERE op_id = {}",
        recycled_name, meta_str, op_id
    );
    if let Err(e) = Spi::run(&update_sql) {
        pgrx::warning!("Failed to update recycled_name: {}", e);
        return false;
    }

    true
}

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

/// Common safety checks for a single (schema, table) pair.
/// Returns Some(()) when the table is capturable, None when it should be skipped.
fn check_capturable(schema: &str, bare_table: &str) -> Option<()> {
    if schema == "flashback" || schema == "flashback_recycle" || schema.starts_with("pg_temp") {
        return None;
    }
    let excluded = crate::guc::get_excluded_schemas();
    if excluded.iter().any(|s| s == schema) {
        return None;
    }
    if bare_table.contains('\'') || bare_table.contains(';') {
        pgrx::warning!("Invalid table name: {}", bare_table);
        return None;
    }
    if schema.contains('\'') || schema.contains(';') {
        pgrx::warning!("Invalid schema name: {}", schema);
        return None;
    }
    Some(())
}

/// Strip double-quotes from a single SQL identifier token.
/// e.g. `"My Schema"` → `My Schema`, `public` → `public`.
fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        // Unescape doubled double-quotes inside: "" → "
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        s.to_string()
    }
}

/// Split a potentially quoted `schema.table` or bare `table` token into (schema, table).
/// Handles: `public.orders`, `"My NS"."My Tbl"`, `orders`, `"orders"`
fn split_schema_table(token: &str) -> (String, String) {
    // We need to split on the DOT that is NOT inside double-quotes.
    let mut depth = 0usize;
    let mut dot_pos = None;
    for (i, ch) in token.char_indices() {
        match ch {
            '"' => depth = if depth == 0 { 1 } else { 0 },
            '.' if depth == 0 => { dot_pos = Some(i); break; }
            _ => {}
        }
    }
    if let Some(pos) = dot_pos {
        let schema = strip_quotes(&token[..pos]);
        let table  = strip_quotes(&token[pos + 1..]);
        (schema, table)
    } else {
        ("public".to_string(), strip_quotes(token))
    }
}

/// Tokenise the table list portion of a DROP TABLE statement while respecting
/// double-quoted identifiers (which may contain commas or spaces).
/// Input example: `orders, "My Schema"."My Table", public.customers`
fn tokenise_table_list(list_str: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in list_str.chars() {
        match ch {
            '"' => { in_quotes = !in_quotes; current.push(ch); }
            ',' if !in_quotes => {
                let t = current.trim().to_string();
                if !t.is_empty() { tokens.push(t); }
                current = String::new();
            }
            _ => { current.push(ch); }
        }
    }
    let t = current.trim().to_string();
    if !t.is_empty() { tokens.push(t); }
    tokens
}

/// Called from the ProcessUtility hook with names extracted from the DropStmt AST.
/// Using the AST avoids mis-parsing the query string when fired inside a multi-statement batch.
pub fn handle_drop_table(tables: &[(String, String)], if_exists: bool, cascade: bool) -> bool {
    // Skip silently if the extension is not yet installed
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')").unwrap_or(None) != Some(true) {
        return false;
    }

    if tables.is_empty() {
        return false;
    }

    let mut any_seen = false;
    let mut skipped_drops: Vec<String> = Vec::new();

    for (schema, bare_table) in tables {

        // Quick existence + kind check so we know whether to pass to PG or absorb.
        let row_info = Spi::get_one::<String>(&format!(
            "SELECT relpersistence::text || ':' || relkind::text || ':' || relispartition::text \
             FROM pg_class \
             WHERE relname = '{}' \
               AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}' LIMIT 1) \
             LIMIT 1",
            bare_table, schema
        ))
        .unwrap_or(None)
        .unwrap_or_default();

        if row_info.is_empty() {
            // Non-existent table — IF EXISTS no-op, just skip.
            continue;
        }
        any_seen = true;

        let parts: Vec<&str> = row_info.split(':').collect();
        let relpersistence = parts.first().copied().unwrap_or("");
        let is_partition   = parts.get(2).copied().unwrap_or("") == "t";

        if relpersistence == "t" || is_partition
            || check_capturable(&schema, &bare_table).is_none()
        {
            // Temp table, child partition, or excluded schema: can't be captured.
            // We'll issue a direct DROP for this table ourselves so PG doesn't error.
            let if_e  = if if_exists { "IF EXISTS " } else { "" };
            let casc  = if cascade   { " CASCADE"  } else { "" };
            // The /* pg_flashback_internal */ comment causes hooks.rs to skip this statement,
            // preventing a re-entrant infinite loop.
            // Quote identifiers so names with special chars are handled correctly.
            skipped_drops.push(format!(
                "DROP TABLE {}\"{}\".\"{}\"{} /* pg_flashback_internal */",
                if_e,
                schema.replace('"', "\"\""),
                bare_table.replace('"', "\"\""),
                casc
            ));
        } else {
            // Capturable — move it to flashback_recycle.
            capture_drop_inner(&schema, &bare_table);
        }
    }

    // Execute individual drops for tables we couldn't capture.
    for drop_sql in &skipped_drops {
        if let Err(e) = Spi::run(drop_sql) {
            pgrx::warning!("pg_flashback: fallback DROP failed: {}", e);
        }
    }

    // Return true (absorb the original DROP command) if we saw at least one table.
    // We've already handled everything — either moved to flashback_recycle or explicitly dropped.
    // Returning false would cause PG to run the original DROP again, which would error because
    // the tables are already gone.
    any_seen
}

/// Capture data for a single TRUNCATE'd table into flashback_recycle.
/// Returns silently on errors (warnings are emitted).
fn capture_single_truncate(schema: &str, bare_table: &str) {
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
            return;
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
        return;
    }

    if let Err(e) = Spi::run(&format!(
        "UPDATE flashback.operations SET recycled_name = '{}', metadata = '{}' WHERE op_id = {}",
        recycled_name, metadata_json, op_id
    )) {
        pgrx::warning!("TRUNCATE metadata update error: {}", e);
        return;
    }

    pgrx::log!("TRUNCATE captured: {}.{} -> {}", schema, bare_table, recycled_name);
}

/// Called from the ProcessUtility hook with names extracted from the TruncateStmt AST.
pub fn handle_truncate_table(tables: &[(String, String)]) -> bool {
    // Skip silently if the extension is not yet installed
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')").unwrap_or(None) != Some(true) {
        return false;
    }

    for (schema, bare_table) in tables {
        if check_capturable(schema, bare_table).is_none() {
            continue;
        }

        // Skip temp and non-existent tables; also skip partitioned tables.
        let row_info = Spi::get_one::<String>(&format!(
            "SELECT relpersistence::text || ':' || relkind::text || ':' || relispartition::text \
             FROM pg_class \
             WHERE relname = '{}' \
               AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}' LIMIT 1) \
             LIMIT 1",
            bare_table, schema
        ))
        .unwrap_or(None)
        .unwrap_or_default();

        if row_info.is_empty() {
            continue;
        }

        let parts: Vec<&str> = row_info.split(':').collect();
        let relpersistence = parts.first().copied().unwrap_or("");
        let relkind        = parts.get(1).copied().unwrap_or("");
        let is_partition   = parts.get(2).copied().unwrap_or("") == "t";

        if relpersistence == "t" { continue; }
        if relkind == "p" {
            pgrx::warning!(
                "pg_flashback: '{}.{}' is a partitioned table — TRUNCATE capture skipped.",
                schema, bare_table
            );
            continue;
        }
        if is_partition { continue; }

        capture_single_truncate(schema, bare_table);
    }

    false  // false = let PostgreSQL execute the actual TRUNCATE
}

/// Called when DROP SCHEMA [IF EXISTS] ... CASCADE is intercepted.
/// Best-effort: capture all regular tables in the schema before they are removed.
pub fn handle_drop_schema_cascade(query: &str) {
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')")
        .unwrap_or(None) != Some(true)
    {
        return;
    }

    // Parse schema name: DROP SCHEMA [IF EXISTS] schema_name [CASCADE|RESTRICT]
    let schema_name = query
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .filter(|w| {
            let u = w.to_uppercase();
            u != "DROP" && u != "SCHEMA" && u != "IF" && u != "EXISTS"
                && u != "CASCADE" && u != "RESTRICT"
        })
        .next()
        .unwrap_or("")
        .to_string();

    if schema_name.is_empty()
        || schema_name == "flashback"
        || schema_name == "flashback_recycle"
        || schema_name.starts_with("pg_")
        || schema_name == "information_schema"
        || schema_name.contains('\'')
        || schema_name.contains(';')
    {
        return;
    }

    let excluded = crate::guc::get_excluded_schemas();
    if excluded.iter().any(|s| s == &schema_name) {
        return;
    }

    // Enumerate all regular (non-partition-child) tables in the schema.
    // Use Spi::get_one+string_agg to avoid Spi::connect inside a hook context.
    let tables_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(relname, ',' ORDER BY relname), '') \
         FROM pg_class \
         WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}') \
           AND relkind = 'r' AND NOT relispartition",
        schema_name
    ))
    .unwrap_or(None)
    .unwrap_or_default();

    let tables: Vec<String> = if tables_csv.is_empty() {
        Vec::new()
    } else {
        tables_csv.split(',').map(String::from).collect()
    };

    if tables.is_empty() {
        return;
    }

    let mut captured = 0i32;
    for table in &tables {
        if capture_drop_inner(&schema_name, table) {
            captured += 1;
        }
    }

    if captured > 0 {
        pgrx::warning!(
            "pg_flashback: captured {}/{} tables from DROP SCHEMA '{}'. \
             Use flashback_list_recycled_tables() to see them and flashback_restore() to recover.",
            captured, tables.len(), schema_name
        );
    }
}