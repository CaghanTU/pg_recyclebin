use pgrx::prelude::*;
use crate::guc;
use crate::context::qi;

const FLASHBACK_BIN_LOCK_KEY: i64 = 846271;

fn acquire_flashback_bin_lock() {
    let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", FLASHBACK_BIN_LOCK_KEY));
}

fn with_suffix_63(base: &str, suffix: &str) -> String {
    let max_ident_len = 63usize;
    if base.len() + suffix.len() <= max_ident_len {
        return format!("{}{}", base, suffix);
    }

    let keep_bytes = max_ident_len.saturating_sub(suffix.len());
    let mut kept = String::new();
    for ch in base.chars() {
        let ch_len = ch.len_utf8();
        if kept.len() + ch_len > keep_bytes {
            break;
        }
        kept.push(ch);
    }
    format!("{}{}", kept, suffix)
}

// FK constraints from other tables pointing at this table. The table's own constraints
// travel with it on SET SCHEMA; only incoming FKs need to be recaptured.
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

fn capture_dependent_views(schema: &str, table: &str) -> Vec<serde_json::Value> {
    let json_str = Spi::get_one::<String>(&format!(
        "SELECT COALESCE( \
             jsonb_agg(jsonb_build_object( \
                 'schema', n.nspname, \
                 'name',   v.relname, \
                 'kind',   v.relkind::text, \
                 'def',    pg_get_viewdef(v.oid, true) \
             )), '[]'::jsonb)::text \
         FROM pg_depend d \
         JOIN pg_rewrite r  ON r.oid = d.objid \
                           AND d.classid = 'pg_rewrite'::regclass \
         JOIN pg_class   v  ON v.oid = r.ev_class AND v.relkind IN ('v', 'm') \
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

fn capture_drop_inner(schema: &str, bare_table: &str, cascade: bool) -> bool {
    enforce_table_limit();
    enforce_size_limit();

    let dep_views     = capture_dependent_views(schema, bare_table);
    let incoming_fks  = capture_incoming_fks(schema, bare_table);
    let rls_policies  = capture_rls_policies(schema, bare_table);
    let partition_info = capture_partition_info(schema, bare_table);

    // If dependent views exist and the user did NOT specify CASCADE, let PostgreSQL handle it
    // naturally — it will emit "cannot drop ... because other objects depend on it".
    // For CASCADE: we proceed with capture (table saved to recycle bin), then explicitly drop
    // the views so they don't survive pointing at flashback_recycle (Bug I fix).
    // View definitions are preserved in metadata and recreated on restore.
    if !dep_views.is_empty() && !cascade {
        return false;
    }

    // Reserve a unique op_id; use it as the recycled_name suffix.
    let reserve_sql = format!(
        "INSERT INTO flashback.operations \
         (operation_type, schema_name, table_name, recycled_name, role_name, retention_until, \
          query_text, application_name, client_addr) \
         VALUES ('DROP', '{}', '{}', '', current_user, now() + interval '{} days', \
          COALESCE(current_query(), ''), \
          COALESCE(current_setting('application_name', true), ''), \
          COALESCE(inet_client_addr()::text, '')) \
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

    let op_suffix = format!("_{}", op_id);
    let recycled_name = with_suffix_63(bare_table, &op_suffix);

    // Capture backup-related metadata BEFORE SET SCHEMA — pg_relation_filepath()
    // and relfilenode reference the original location which becomes invalid after move.
    // Uses qualified catalog lookup (relname + nspname) instead of regclass to avoid
    // ambiguity if another object shares the same unqualified name.
    let backup_meta: Option<serde_json::Value> = Spi::get_one::<String>(&format!(
        "SELECT jsonb_build_object(\
             'relfilenode', c.relfilenode::bigint,\
             'toast_relfilenode', c.reltoastrelid::bigint,\
             'db_oid', d.oid::bigint,\
             'filepath', pg_relation_filepath(c.oid),\
             'wal_lsn', pg_current_wal_lsn()::text\
         )::text \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN pg_database d \
         WHERE c.relname = '{}' AND n.nspname = '{}' \
           AND d.datname = current_database()",
        bare_table, schema
    ))
    .ok()
    .flatten()
    .and_then(|s| serde_json::from_str(&s).ok());

    let move_sql   = format!("ALTER TABLE {}.{} SET SCHEMA flashback_recycle", qi(schema), qi(bare_table));
    let rename_sql = format!("ALTER TABLE flashback_recycle.{} RENAME TO {}", qi(bare_table), qi(&recycled_name));

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

    // Append op_id suffix to owned sequences to avoid name collisions on re-drop.
    // Use Spi::get_one with string_agg to avoid Spi::connect visibility issues after DDL.
    // chr(1) (SOH) separator: PG identifiers cannot contain SOH bytes, safe even for
    // quoted names that include commas (e.g. "a,b").
    let seq_names_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(s.relname, chr(1)), '') \
         FROM pg_depend d \
         JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
         JOIN pg_class t ON t.oid = d.refobjid AND t.relname = '{}' \
         WHERE d.deptype IN ('a', 'i') \
           AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flashback_recycle')",
        recycled_name
    ))
    .unwrap_or(None)
    .unwrap_or_default();
    for seq_name in seq_names_csv.split('\x01').filter(|s| !s.is_empty()) {
        let new_seq_name = with_suffix_63(seq_name, &op_suffix);
        if let Err(e) = Spi::run(&format!(
            "ALTER SEQUENCE flashback_recycle.{} RENAME TO {}",
            qi(seq_name), qi(&new_seq_name)
        )) {
            pgrx::warning!("Failed to rename sequence '{}': {}", seq_name, e);
        }
    }

    // Append op_id suffix to indexes to avoid name collisions in flashback_recycle on re-drop.
    let idx_names_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(indexname, chr(1)), '') \
         FROM pg_indexes \
         WHERE schemaname = 'flashback_recycle' AND tablename = '{}'",
        recycled_name
    ))
    .unwrap_or(None)
    .unwrap_or_default();
    for idx_name in idx_names_csv.split('\x01').filter(|s| !s.is_empty()) {
        let new_idx_name = with_suffix_63(idx_name, &op_suffix);
        if let Err(e) = Spi::run(&format!(
            "ALTER INDEX flashback_recycle.{} RENAME TO {}",
            qi(idx_name), qi(&new_idx_name)
        )) {
            pgrx::warning!("Failed to rename index '{}': {}", idx_name, e);
        }
    }

    // Persist metadata.
    // Clone dep_views before moving into meta_obj so we can drop the views after saving.
    let dep_views_to_drop = dep_views.clone();
    let mut meta_obj = serde_json::Map::new();
    meta_obj.insert("views".into(), serde_json::Value::Array(dep_views));
    meta_obj.insert("incoming_fks".into(), serde_json::Value::Array(incoming_fks));
    meta_obj.insert("rls_policies".into(), serde_json::Value::Array(rls_policies));
    if let Some(pi) = partition_info {
        meta_obj.insert("partition_info".into(), pi);
    }
    if let Some(serde_json::Value::Object(bm)) = backup_meta {
        for (k, v) in bm {
            meta_obj.insert(k, v);
        }
    }
    let meta = serde_json::Value::Object(meta_obj);
    let meta_str = meta.to_string().replace('\'', "''");
    let update_sql = format!(
        "UPDATE flashback.operations SET recycled_name = '{}', metadata = '{}'::jsonb WHERE op_id = {}",
        recycled_name, meta_str, op_id
    );
    if let Err(e) = Spi::run(&update_sql) {
        pgrx::warning!("Failed to update recycled_name: {}", e);
        let _ = Spi::run(&format!("DELETE FROM flashback.operations WHERE op_id = {}", op_id));
        return false;
    }

    // CASCADE: table is now safely in flashback_recycle with metadata saved.
    // Explicitly drop each dependent view/mview so they are no longer queryable
    // (they would otherwise survive by OID reference — Bug I fix).
    // Their definitions are in metadata["views"] and will be recreated on restore.
    if cascade {
        for view in &dep_views_to_drop {
            let vs = view.get("schema").and_then(|s| s.as_str()).unwrap_or("public");
            let vn = view.get("name").and_then(|s| s.as_str()).unwrap_or("");
            let vk = view.get("kind").and_then(|s| s.as_str()).unwrap_or("v");
            if vn.is_empty() { continue; }
            let obj_type = if vk == "m" { "MATERIALIZED VIEW" } else { "VIEW" };
            let drop_sql = format!(
                "DROP {} IF EXISTS \"{}\".\"{}\" CASCADE /* pg_recyclebin_internal */",
                obj_type,
                vs.replace('"', "\"\""),
                vn.replace('"', "\"\"")
            );
            if let Err(e) = Spi::run(&drop_sql) {
                pgrx::warning!("pg_recyclebin: failed to drop view '{}.{}': {}", vs, vn, e);
            }
        }
    }

    true
}

// Deletes the oldest table in the recycle bin (both the physical table and the operations record)
fn evict_oldest() {
    acquire_flashback_bin_lock();

    let find_sql = "SELECT recycled_name FROM flashback.operations WHERE restored = false ORDER BY retention_until ASC, op_id ASC LIMIT 1";

    let oldest = Spi::get_one::<String>(find_sql);

    match oldest {
        Ok(Some(recycled_name)) => {
            let drop_sql = format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{}", qi(&recycled_name));
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

// If table count exceeds max_tables, delete the oldest (loop until under limit)
fn enforce_table_limit() {
    let count_sql = "SELECT COUNT(*)::int FROM flashback.operations WHERE restored = false";
    loop {
        if let Ok(Some(count)) = Spi::get_one::<i32>(count_sql) {
            let max = guc::get_max_tables();
            if count >= max {
                pgrx::log!("Table limit reached ({}/{}), removing oldest", count, max);
                evict_oldest();
                continue;
            }
        }
        break;
    }
}

// If total size exceeds max_size, delete the oldest (loop until under limit, comparison in MB)
fn enforce_size_limit() {
    acquire_flashback_bin_lock();

    let size_sql = "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint \
                    FROM flashback.operations o \
                    JOIN LATERAL to_regclass(format('flashback_recycle.%I', o.recycled_name)) AS r(oid) ON true \
                    JOIN pg_class c ON c.oid = r.oid \
                    WHERE o.restored = false AND o.recycled_name IS NOT NULL";
    loop {
        if let Ok(Some(total_bytes)) = Spi::get_one::<i64>(size_sql) {
            let total_mb = total_bytes / (1024 * 1024);
            let max_mb = guc::get_max_size() as i64;
            if total_mb >= max_mb {
                pgrx::log!("Size limit reached ({}/{} MB), removing oldest", total_mb, max_mb);
                evict_oldest();
                continue;
            }
        }
        break;
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

    for (schema_in, bare_table) in tables {
        let mut schema = schema_in.clone();

        // Quick existence + kind check so we know whether to pass to PG or absorb.
        let row_info = if schema.is_empty() {
            let visible = Spi::get_one::<String>(&format!(
                "SELECT n.nspname || chr(1) || c.relpersistence::text || ':' || c.relkind::text || ':' || c.relispartition::text \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname = '{}' AND pg_table_is_visible(c.oid) \
                 ORDER BY c.oid DESC \
                 LIMIT 1",
                bare_table
            ))
            .unwrap_or(None)
            .unwrap_or_default();

            if let Some((resolved_schema, info)) = visible.split_once('\x01') {
                schema = resolved_schema.to_string();
                info.to_string()
            } else {
                String::new()
            }
        } else {
            Spi::get_one::<String>(&format!(
                "SELECT relpersistence::text || ':' || relkind::text || ':' || relispartition::text \
                 FROM pg_class \
                 WHERE relname = '{}' \
                   AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}' LIMIT 1) \
                 LIMIT 1",
                bare_table, schema
            ))
            .unwrap_or(None)
            .unwrap_or_default()
        };

        if row_info.is_empty() {
            if !if_exists {
                // No IF EXISTS and table doesn't exist — raise a PostgreSQL-compatible error.
                // Any SPI work already done this call will roll back with the transaction.
                pgrx::error!("table \"{}\" does not exist", bare_table);
            }
            // IF EXISTS no-op, just skip.
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
            // pg_recyclebin_internal marker prevents the hook from re-intercepting this DROP.
            skipped_drops.push(format!(
                "DROP TABLE {}\"{}\".\"{}\"{} /* pg_recyclebin_internal */",
                if_e,
                schema.replace('"', "\"\""),
                bare_table.replace('"', "\"\""),
                casc
            ));
        } else {
            let captured = capture_drop_inner(&schema, &bare_table, cascade);
            if !captured {
                // capture_drop_inner declined (views exist but no CASCADE).
                // Let PostgreSQL handle this DROP natively so it fires the correct error.
                let if_e = if if_exists { "IF EXISTS " } else { "" };
                let casc = if cascade   { " CASCADE"  } else { "" };
                skipped_drops.push(format!(
                    "DROP TABLE {}\"{}\".\"{}\"{} /* pg_recyclebin_internal */",
                    if_e,
                    schema.replace('"', "\"\""),
                    bare_table.replace('"', "\"\""),
                    casc
                ));
            }
        }
    }

    for drop_sql in &skipped_drops {
        if let Err(e) = Spi::run(drop_sql) {
            pgrx::warning!("pg_recyclebin: fallback DROP failed: {}", e);
        }
    }

    // true = absorb the original DROP; we've already handled every table.
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
         (operation_type, schema_name, table_name, recycled_name, role_name, retention_until, \
          query_text, application_name, client_addr) \
         VALUES ('TRUNCATE', '{}', '{}', '', current_user, now() + interval '{} days', \
          COALESCE(current_query(), ''), \
          COALESCE(current_setting('application_name', true), ''), \
          COALESCE(inet_client_addr()::text, '')) \
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

    let recycled_name = with_suffix_63(bare_table, &format!("_{}", op_id));

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
    let metadata_json = seq_info.to_string().replace('\'', "''");

    // Copy data into a new backup table in flashback_recycle schema
    let create_sql = format!(
        "CREATE TABLE flashback_recycle.{} AS SELECT * FROM {}.{}",
        qi(&recycled_name), qi(schema), qi(bare_table)
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
        let _ = Spi::run(&format!("DELETE FROM flashback.operations WHERE op_id = {}", op_id));
        return;
    }

    pgrx::log!("TRUNCATE captured: {}.{} -> {}", schema, bare_table, recycled_name);
}

/// Called from the ProcessUtility hook with names extracted from the TruncateStmt AST.
pub fn handle_truncate_table(tables: &[(String, String)]) {
    // Skip silently if the extension is not yet installed
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')").unwrap_or(None) != Some(true) {
        return;
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
                "pg_recyclebin: '{}.{}' is a partitioned table — TRUNCATE capture skipped.",
                schema, bare_table
            );
            continue;
        }
        if is_partition { continue; }

        capture_single_truncate(schema, bare_table);
    }
}

/// Called when DROP SCHEMA [IF EXISTS] ... CASCADE is intercepted.
/// `schema_name` is extracted from the DropStmt AST by the caller.
/// Best-effort: capture all regular tables in the schema before they are removed.
pub fn handle_drop_schema_cascade(schema_name: &str) {
    if Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'flashback')")
        .unwrap_or(None) != Some(true)
    {
        return;
    }
    let schema_name = schema_name.to_string();

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
    // chr(1) (SOH) separator: PG identifiers cannot contain SOH bytes, safe even for
    // quoted names that include commas (e.g. "a,b").
    let tables_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(relname, chr(1) ORDER BY relname), '') \
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
        tables_csv.split('\x01').map(String::from).collect()
    };

    if tables.is_empty() {
        return;
    }

    let mut captured = 0i32;
    for table in &tables {
        if capture_drop_inner(&schema_name, table, true) {
            captured += 1;
        }
    }

    if captured > 0 {
        pgrx::warning!(
            "pg_recyclebin: captured {}/{} tables from DROP SCHEMA '{}'. \
             Use flashback_list_recycled_tables() to see them and flashback_restore() to recover.",
            captured, tables.len(), schema_name
        );
    }
}