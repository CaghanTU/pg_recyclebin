use pgrx::prelude::*;

/// Find (column_name, sequence_name) pairs for a table via pg_depend.
/// Covers both SERIAL (deptype='a') and IDENTITY (deptype='i') columns.
fn find_serial_sequences(schema: &str, table: &str) -> Vec<(String, String)> {
    let mut cols = Vec::new();
    let _ = pgrx::Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT a.attname, s.oid::regclass::text \
                 FROM pg_class t \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 \
                 JOIN pg_depend d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum \
                 JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
                 WHERE t.relname = '{}' AND n.nspname = '{}' \
                 AND d.deptype IN ('a', 'i')",
                table, schema
            ),
            None, &[],
        )?;
        for row in rows {
            let col = row.get::<String>(1)?.unwrap_or_default();
            let seq = row.get::<String>(2)?.unwrap_or_default();
            if !col.is_empty() && !seq.is_empty() {
                cols.push((col, seq));
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    });
    cols
}

/// After a DROP-type restore, recreate all objects saved in the metadata JSON.
/// Processing order: views → FK constraints → RLS policies.
/// Failures emit warnings but do NOT abort the restore.
fn restore_from_metadata(metadata_json: &str, table_name: &str, restore_schema: &str) {
    let meta: serde_json::Value = match serde_json::from_str(metadata_json) {
        Ok(v) => v,
        Err(_) => return,
    };

    // 1. Views
    if let Some(views) = meta.get("views").and_then(|v| v.as_array()) {
        for view in views {
            let view_schema = view.get("schema").and_then(|s| s.as_str()).unwrap_or("public");
            let view_name   = view.get("name").and_then(|s| s.as_str()).unwrap_or("");
            let view_def    = view.get("def").and_then(|s| s.as_str()).unwrap_or("");

            if view_name.is_empty() || view_def.is_empty() {
                continue;
            }
            let create_sql = format!(
                "CREATE OR REPLACE VIEW {}.{} AS {}",
                view_schema, view_name, view_def
            );
            if let Err(e) = Spi::run(&create_sql) {
                pgrx::warning!(
                    "pg_flashback: failed to recreate view '{}.{}' for '{}': {}",
                    view_schema, view_name, table_name, e
                );
            } else {
                pgrx::log!("pg_flashback: recreated view '{}.{}' for '{}'", view_schema, view_name, table_name);
            }
        }
    }

    // 2. Incoming FK constraints (defined on OTHER tables pointing at this table)
    if let Some(fks) = meta.get("incoming_fks").and_then(|v| v.as_array()) {
        for fk in fks {
            let def             = fk.get("def").and_then(|s| s.as_str()).unwrap_or("");
            let constraint_q    = fk.get("constraint").and_then(|s| s.as_str()).unwrap_or("");
            if def.is_empty() { continue; }

            // Strip outer double-quotes from the constraint name for catalog lookup.
            let bare_name = if constraint_q.starts_with('"') && constraint_q.ends_with('"') && constraint_q.len() >= 2 {
                &constraint_q[1..constraint_q.len()-1]
            } else {
                constraint_q
            };

            // When pg_flashback captures a DROP by moving the table via SET SCHEMA,
            // the FK constraint on the child is preserved (it references by OID).
            // Skip re-adding if it already exists — this is the normal case.
            let already_exists = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname = '{}')",
                bare_name.replace('\'', "''")
            )).unwrap_or(None).unwrap_or(false);

            if already_exists {
                pgrx::log!(
                    "pg_flashback: FK constraint '{}' already exists on child table — skipping re-add",
                    bare_name
                );
                continue;
            }

            if let Err(e) = Spi::run(def) {
                pgrx::warning!(
                    "pg_flashback: failed to restore FK constraint for '{}': {}",
                    table_name, e
                );
            } else {
                pgrx::log!("pg_flashback: restored FK constraint for '{}'", table_name);
            }
        }
    }

    // 3. RLS policies
    if let Some(policies) = meta.get("rls_policies").and_then(|v| v.as_array()) {
        if !policies.is_empty() {
            // Enable RLS on the (restored) table first.
            let _ = Spi::run(&format!(
                "ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY",
                restore_schema, table_name
            ));
            for policy in policies {
                let name       = policy.get("name").and_then(|s| s.as_str()).unwrap_or("");
                let cmd        = policy.get("cmd").and_then(|s| s.as_str()).unwrap_or("ALL");
                let permissive = policy.get("permissive").and_then(|s| s.as_bool()).unwrap_or(true);
                let roles      = policy.get("roles").and_then(|s| s.as_str()).unwrap_or("PUBLIC");
                let qual       = policy.get("qual").and_then(|s| s.as_str()).unwrap_or("");
                let with_check = policy.get("with_check").and_then(|s| s.as_str()).unwrap_or("");

                if name.is_empty() { continue; }

                // Policies travel with the table when SET SCHEMA is used, so drop first
                // to avoid "already exists" errors before recreating.
                let _ = Spi::run(&format!(
                    "DROP POLICY IF EXISTS {} ON {}.{}",
                    name, restore_schema, table_name
                ));

                let permissive_str = if permissive { "PERMISSIVE" } else { "RESTRICTIVE" };
                let roles_clause   = if roles.is_empty() { "PUBLIC".to_string() } else { roles.to_string() };
                let using_clause   = if qual.is_empty() { String::new() } else { format!(" USING ({})", qual) };
                let check_clause   = if with_check.is_empty() { String::new() } else { format!(" WITH CHECK ({})", with_check) };

                let policy_sql = format!(
                    "CREATE POLICY {} ON {}.{} AS {} FOR {} TO {}{}{}",
                    name, restore_schema, table_name,
                    permissive_str, cmd, roles_clause,
                    using_clause, check_clause
                );
                if let Err(e) = Spi::run(&policy_sql) {
                    pgrx::warning!(
                        "pg_flashback: failed to restore RLS policy '{}' for '{}': {}",
                        name, table_name, e
                    );
                } else {
                    pgrx::log!("pg_flashback: restored RLS policy '{}' for '{}'", name, table_name);
                }
            }
        }
    }
}

/// Restore partition children after the parent table has been restored.
/// PostgreSQL's ALTER TABLE parent SET SCHEMA ... co-moves child partitions automatically,
/// so they are usually already back in the target schema.  We verify each child and, for
/// any that are missing (edge-case), attempt CREATE TABLE child PARTITION OF parent.
fn restore_partition_info_from_metadata(metadata_json: &str, table_name: &str, restore_schema: &str) {
    let meta: serde_json::Value = match serde_json::from_str(metadata_json) {
        Ok(v) => v,
        Err(_) => return,
    };
    let pi = match meta.get("partition_info") {
        Some(v) => v,
        None => return,
    };
    if pi.get("is_partitioned").and_then(|v| v.as_bool()) != Some(true) {
        return;
    }
    let strategy  = pi.get("strategy").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
    let children  = pi.get("children").and_then(|v| v.as_array()).cloned().unwrap_or_default();

    if children.is_empty() {
        return;
    }

    // Build a properly-quoted reference to the restored parent.
    let parent_ref = format!(
        "\"{}\".\"{}\"",
        restore_schema.replace('"', "\"\""),
        table_name.replace('"', "\"\"")
    );

    let mut co_moved = 0usize;
    let mut created  = 0usize;
    let mut failed   = 0usize;

    for child in &children {
        // Values stored via quote_ident(): simple names are unquoted, special ones are quoted.
        let child_schema_q = child.get("schema").and_then(|s| s.as_str()).unwrap_or("public");
        let child_name_q   = child.get("name").and_then(|s| s.as_str()).unwrap_or("");
        let bound_def      = child.get("def").and_then(|s| s.as_str()).unwrap_or("");

        if child_name_q.is_empty() || bound_def.is_empty() {
            continue;
        }

        // Strip outer double-quotes for catalog lookups.
        let child_schema_bare = if child_schema_q.starts_with('"') && child_schema_q.ends_with('"') && child_schema_q.len() >= 2 {
            &child_schema_q[1..child_schema_q.len()-1]
        } else {
            child_schema_q
        };
        let child_name_bare = if child_name_q.starts_with('"') && child_name_q.ends_with('"') && child_name_q.len() >= 2 {
            &child_name_q[1..child_name_q.len()-1]
        } else {
            child_name_q
        };

        // Check if the child was already co-moved with the parent.
        let exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS( \
               SELECT 1 FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               WHERE c.relname = '{}' AND n.nspname = '{}')",
            child_name_bare.replace('\'', "''"),
            child_schema_bare.replace('\'', "''")
        )).unwrap_or(None).unwrap_or(false);

        if exists {
            co_moved += 1;
            pgrx::log!(
                "pg_flashback: child partition {}.{} co-moved with parent '{}'",
                child_schema_q, child_name_q, table_name
            );
            continue;
        }

        // Child is missing — attempt to recreate an empty partition shell.
        let create_sql = format!(
            "CREATE TABLE {}.{} PARTITION OF {} {}",
            child_schema_q, child_name_q, parent_ref, bound_def
        );
        match Spi::run(&create_sql) {
            Ok(_) => {
                created += 1;
                pgrx::warning!(
                    "pg_flashback: recreated empty partition {}.{} for '{}' — \
                     original data was lost when the parent was dropped. Restore from backup if needed.",
                    child_schema_q, child_name_q, table_name
                );
            }
            Err(e) => {
                failed += 1;
                pgrx::warning!(
                    "pg_flashback: could not recreate partition {}.{} for '{}': {}. \
                     Recreate manually: {}",
                    child_schema_q, child_name_q, table_name, e, create_sql
                );
            }
        }
    }

    if failed > 0 {
        pgrx::warning!(
            "pg_flashback: '{}' {} partitioned table — {}/{} partitions OK, {} failed. \
             See warnings above.",
            table_name, strategy,
            co_moved + created, children.len(), failed
        );
    } else {
        pgrx::log!(
            "pg_flashback: '{}' {} partitioned table — {}/{} partitions OK \
             ({} co-moved, {} recreated as empty shells)",
            table_name, strategy,
            co_moved + created, children.len(), co_moved, created
        );
    }
}

#[pg_extern]

fn flashback_restore(table_name: &str, target_schema: Option<&str>) -> bool {
    pgrx::log!("Restoring table: {}", table_name);

    // SQL injection protection
    if table_name.contains('\'') || table_name.contains(';') {
        pgrx::warning!("Invalid table name: {}", table_name);
        return false;
    }
    if let Some(s) = target_schema {
        if s.contains('\'') || s.contains(';') {
            pgrx::warning!("Invalid target schema: {}", s);
            return false;
        }
    }

    let (recycled_name, schema_name, role_name, operation_type, metadata_json) = {
        // Get op_id first, then acquire an advisory lock on it to prevent concurrent restores
        let op_id_val = Spi::get_one::<i64>(&format!(
            "SELECT op_id FROM flashback.operations \
             WHERE table_name = '{}' AND restored = false \
             ORDER BY timestamp DESC LIMIT 1",
            table_name
        )).unwrap_or(None);
        let op_id_val = match op_id_val {
            Some(id) => id,
            None => {
                pgrx::warning!("Table not found in recycle bin: {}", table_name);
                return false;
            }
        };
        let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", op_id_val));
        let mut found = None;
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT recycled_name, schema_name, role_name, operation_type, COALESCE(metadata::text, '') \
                     FROM flashback.operations \
                     WHERE op_id = {} AND restored = false",
                    op_id_val
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r  = row.get::<String>(1)?.unwrap_or_default();
                let s  = row.get::<String>(2)?.unwrap_or_default();
                let o  = row.get::<String>(3)?.unwrap_or_default();
                let op = row.get::<String>(4)?.unwrap_or_default();
                let m  = row.get::<String>(5)?.unwrap_or_default();
                found = Some((r, s, o, op, m));
            }
            Ok::<_, spi::Error>(())
        });
        match found {
            Some(t) => t,
            None => {
                pgrx::warning!("Table not found in recycle bin: {}", table_name);
                return false;
            }
        }
    };

    // Permission check: only the original owner or a superuser can restore.
    // Use GetSessionUserId() + superuser_arg() so SECURITY DEFINER doesn't
    // bypass the check (superuser() checks the effective user, not session user).
    let is_session_superuser = unsafe {
        pg_sys::superuser_arg(pg_sys::GetSessionUserId())
    };
    if !is_session_superuser {
        let session_user = Spi::get_one::<String>("SELECT session_user")
            .unwrap_or(None)
            .unwrap_or_default();
        if session_user != role_name {
            pgrx::warning!(
                "Permission denied: table '{}' was dropped by '{}'",
                table_name, role_name
            );
            return false;
        }
    }

    let restore_schema = target_schema.unwrap_or(&schema_name);

    if operation_type == "TRUNCATE" {
    // Table still exists, restore data back into it.
    // OVERRIDING SYSTEM VALUE is required for GENERATED ALWAYS AS IDENTITY columns;
    // it is a harmless no-op for SERIAL / GENERATED BY DEFAULT columns.
    let sql = format!(
        "INSERT INTO {}.{} OVERRIDING SYSTEM VALUE SELECT * FROM flashback_recycle.{}",
        restore_schema, table_name, recycled_name
    );
    match Spi::run(&sql) {
        Ok(_) => {
            // Restore sequences to max values present in the restored data
            let serial_cols = find_serial_sequences(restore_schema, table_name);
            for (col, seq) in &serial_cols {
                if let Ok(Some(max_val)) = Spi::get_one::<i64>(
                    &format!("SELECT COALESCE(MAX(\"{}\"), 0)::bigint FROM \"{}\".\"{}\"" ,
                             col, restore_schema, table_name)
                ) {
                    let _ = Spi::run(&format!("SELECT setval('{}', {})", seq, max_val.max(1)));
                }
            }
            let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{}", recycled_name);
            let _ = Spi::run(&drop_sql);
            let update_sql = format!(
                "UPDATE flashback.operations SET restored = true \
                 WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
                table_name, recycled_name
            );
            let _ = Spi::run(&update_sql);
            return true;
        }
        Err(e) => {
            pgrx::warning!("TRUNCATE restore error: {}", e);
            return false;
        }
    }
}

    // operation_type == "DROP" — existing logic below
    if let Ok(Some(true)) = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}')",
        restore_schema, table_name
    )) {
        pgrx::warning!("Target table already exists: {}", table_name);
        return false;
    }

    let sql = format!("ALTER TABLE flashback_recycle.{} SET SCHEMA {}", recycled_name, restore_schema);
    match Spi::run(&sql) {
    Ok(_) => {
        let rename_sql = format!("ALTER TABLE {}.{} RENAME TO {}", restore_schema, recycled_name, table_name);
        if let Err(e) = Spi::run(&rename_sql) {
            pgrx::warning!("Rename error: {}", e);
            return false;
        }
        let update_sql = format!(
            "UPDATE flashback.operations SET restored = true WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
            table_name, recycled_name
        );
        if let Err(e) = Spi::run(&update_sql) {
            pgrx::warning!("Update error: {}", e);
        }
        // Restore views, incoming FK constraints, RLS policies, and log partition info.
        if !metadata_json.is_empty() {
            restore_from_metadata(&metadata_json, table_name, restore_schema);
            restore_partition_info_from_metadata(&metadata_json, table_name, restore_schema);
        }
        true
    }
    Err(e) => {
        pgrx::warning!("Restore error: {}", e);
        false
    }
}}

#[pg_extern]
fn flashback_restore_by_id(op_id: i64, target_schema: Option<&str>) -> bool {
    let (recycled_name, table_name, schema_name, role_name, operation_type, metadata_json) = {
        // Acquire advisory lock on this op_id to prevent concurrent restores
        let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", op_id));
        let mut found = None;
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT recycled_name, table_name, schema_name, role_name, operation_type, COALESCE(metadata::text, '') \
                     FROM flashback.operations \
                     WHERE op_id = {} AND restored = false",
                    op_id
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r  = row.get::<String>(1)?.unwrap_or_default();
                let t  = row.get::<String>(2)?.unwrap_or_default();
                let s  = row.get::<String>(3)?.unwrap_or_default();
                let o  = row.get::<String>(4)?.unwrap_or_default();
                let op = row.get::<String>(5)?.unwrap_or_default();
                let m  = row.get::<String>(6)?.unwrap_or_default();
                found = Some((r, t, s, o, op, m));
            }
            Ok::<_, spi::Error>(())
        });
        match found {
            Some(t) => t,
            None => {
                pgrx::warning!("Operation ID not found in recycle bin: {}", op_id);
                return false;
            }
        }
    };

    // Permission check: only the original owner or a superuser can restore.
    let is_session_superuser = unsafe {
        pg_sys::superuser_arg(pg_sys::GetSessionUserId())
    };
    if !is_session_superuser {
        let session_user = Spi::get_one::<String>("SELECT session_user")
            .unwrap_or(None)
            .unwrap_or_default();
        if session_user != role_name {
            pgrx::warning!(
                "Permission denied: table '{}' was dropped by '{}'",table_name, role_name
            );
            return false;
        }
    }

    let restore_schema = target_schema.unwrap_or(&schema_name);

    if operation_type == "TRUNCATE" {
    // Table still exists, restore data back into it.
    // OVERRIDING SYSTEM VALUE is required for GENERATED ALWAYS AS IDENTITY columns;
    // it is a harmless no-op for SERIAL / GENERATED BY DEFAULT columns.
    let sql = format!(
        "INSERT INTO {}.{} OVERRIDING SYSTEM VALUE SELECT * FROM flashback_recycle.{}",
        restore_schema, table_name, recycled_name
    );
    match Spi::run(&sql) {
        Ok(_) => {
            // Restore sequences to max values present in the restored data
            let serial_cols = find_serial_sequences(restore_schema, &table_name);
            for (col, seq) in &serial_cols {
                if let Ok(Some(max_val)) = Spi::get_one::<i64>(
                    &format!("SELECT COALESCE(MAX(\"{}\"), 0)::bigint FROM \"{}\".\"{}\"" ,
                             col, restore_schema, table_name)
                ) {
                    let _ = Spi::run(&format!("SELECT setval('{}', {})", seq, max_val.max(1)));
                }
            }
            let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{}", recycled_name);
            let _ = Spi::run(&drop_sql);
            let update_sql = format!(
                "UPDATE flashback.operations SET restored = true \
                 WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
                table_name, recycled_name
            );
            let _ = Spi::run(&update_sql);
            return true;
        }
        Err(e) => {
            pgrx::warning!("TRUNCATE restore error: {}", e);
            return false;
        }
    }
}

    // operation_type == "DROP" — existing logic below
    if let Ok(Some(true)) = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}')",
        restore_schema, &table_name
    )) {
        pgrx::warning!("Target table already exists: {}", table_name);
        return false;
    }

    let sql = format!("ALTER TABLE flashback_recycle.{} SET SCHEMA {}", recycled_name, restore_schema);
    match Spi::run(&sql) {
    Ok(_) => {
        let rename_sql = format!("ALTER TABLE {}.{} RENAME TO {}", restore_schema, recycled_name, table_name);
        if let Err(e) = Spi::run(&rename_sql) {
            pgrx::warning!("Rename error: {}", e);
            return false;
        }
        let update_sql = format!(
            "UPDATE flashback.operations SET restored = true WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
            table_name, recycled_name
        );
        if let Err(e) = Spi::run(&update_sql) {
            pgrx::warning!("Update error: {}", e);
        }
        // Restore views, incoming FK constraints, RLS policies, and log partition info.
        if !metadata_json.is_empty() {
            restore_from_metadata(&metadata_json, &table_name, restore_schema);
            restore_partition_info_from_metadata(&metadata_json, &table_name, restore_schema);
        }
        true
    }
    Err(e) => {
        pgrx::warning!("Restore error: {}", e);
        false
    }
}}

#[pg_extern]
fn flashback_purge(table_name: &str) -> bool {
    // SQL injection protection
    if table_name.contains('\'') || table_name.contains(';') {
        pgrx::warning!("Invalid table name: {}", table_name);
        return false;
    }

    // Find the most recently dropped entry for this table
    let (recycled_name, role_name) = {
        let mut found = None;
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT recycled_name, role_name \
                     FROM flashback.operations \
                     WHERE table_name = '{}' AND restored = false \
                     ORDER BY timestamp DESC LIMIT 1",
                    table_name
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r = row.get::<String>(1)?.unwrap_or_default();
                let o = row.get::<String>(2)?.unwrap_or_default();
                found = Some((r, o));
            }
            Ok::<_, spi::Error>(())
        });
        match found {
            Some(t) => t,
            None => {
                pgrx::warning!("Table not found in recycle bin: {}", table_name);
                return false;
            }
        }
    };

    // Permission check: only the original owner or a superuser can purge.
    let is_session_superuser = unsafe {
        pg_sys::superuser_arg(pg_sys::GetSessionUserId())
    };
    if !is_session_superuser {
        let session_user = Spi::get_one::<String>("SELECT session_user")
            .unwrap_or(None)
            .unwrap_or_default();
        if session_user != role_name {
            pgrx::warning!(
                "Permission denied: table '{}' was dropped by '{}'",
                table_name, role_name
            );
            return false;
        }
    }

    let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", recycled_name);
    if let Err(e) = Spi::run(&drop_sql) {
        pgrx::warning!("Failed to drop recycled table '{}': {}", recycled_name, e);
        return false;
    }

    let delete_sql = format!(
        "DELETE FROM flashback.operations WHERE recycled_name = '{}' AND restored = false",
        recycled_name
    );
    if let Err(e) = Spi::run(&delete_sql) {
        pgrx::warning!("Failed to delete metadata for '{}': {}", table_name, e);
        return false;
    }

    pgrx::log!("Purged '{}' from recycle bin", table_name);
    true
}

#[pg_extern]
fn flashback_purge_by_id(op_id: i64) -> bool {
    // Look up the entry
    let (recycled_name, role_name) = {
        let mut found = None;
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT recycled_name, role_name \
                     FROM flashback.operations \
                     WHERE op_id = {} AND restored = false",
                    op_id
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r = row.get::<String>(1)?.unwrap_or_default();
                let o = row.get::<String>(2)?.unwrap_or_default();
                found = Some((r, o));
            }
            Ok::<_, spi::Error>(())
        });
        match found {
            Some(t) => t,
            None => {
                pgrx::warning!("op_id {} not found in recycle bin", op_id);
                return false;
            }
        }
    };

    // Permission check
    let is_session_superuser = unsafe { pg_sys::superuser_arg(pg_sys::GetSessionUserId()) };
    if !is_session_superuser {
        let session_user = Spi::get_one::<String>("SELECT session_user")
            .unwrap_or(None)
            .unwrap_or_default();
        if session_user != role_name {
            pgrx::warning!("Permission denied: op_id {} was created by '{}'", op_id, role_name);
            return false;
        }
    }

    let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", recycled_name);
    if let Err(e) = Spi::run(&drop_sql) {
        pgrx::warning!("Failed to drop '{}': {}", recycled_name, e);
        return false;
    }

    let delete_sql = format!(
        "DELETE FROM flashback.operations WHERE op_id = {} AND restored = false",
        op_id
    );
    if let Err(e) = Spi::run(&delete_sql) {
        pgrx::warning!("Failed to delete metadata for op_id {}: {}", op_id, e);
        return false;
    }

    pgrx::log!("Purged op_id {} from recycle bin", op_id);
    true
}

#[pg_extern(name = "flashback_restore")]
fn flashback_restore_no_schema(table_name: &str) -> bool {
    flashback_restore(table_name, None)
}

#[pg_extern]
fn flashback_purge_all() -> i64 {
    let is_super = unsafe { pg_sys::superuser_arg(pg_sys::GetSessionUserId()) };

    let session_user = if !is_super {
        Spi::get_one::<String>("SELECT session_user")
            .unwrap_or(None)
            .unwrap_or_default()
    } else {
        String::new()
    };

    let filter = if is_super {
        "WHERE restored = false".to_string()
    } else {
        format!("WHERE restored = false AND role_name = '{}'", session_user)
    };

    let mut recycled_names: Vec<String> = Vec::new();
    let _ = Spi::connect(|client| {
        let rows = client.select(
            &format!("SELECT recycled_name FROM flashback.operations {}", filter),
            None,
            &[],
        )?;
        for row in rows {
            if let Ok(Some(name)) = row.get::<String>(1) {
                recycled_names.push(name);
            }
        }
        Ok::<_, spi::Error>(())
    });

    let count = recycled_names.len() as i64;

    for name in &recycled_names {
        let sql = format!("DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", name);
        if let Err(e) = Spi::run(&sql) {
            pgrx::warning!("Failed to drop '{}': {}", name, e);
        }
    }

    if let Err(e) = Spi::run(&format!(
        "DELETE FROM flashback.operations {}",
        filter
    )) {
        pgrx::warning!("Failed to delete operations: {}", e);
    }

    count
}

#[pg_extern]
fn flashback_status() -> TableIterator<
    'static,
    (
        name!(table_count, i64),
        name!(table_limit, i32),
        name!(total_size_bytes, i64),
        name!(size_limit_mb, i32),
        name!(retention_days, i32),
        name!(worker_interval_seconds, i32),
        name!(oldest_entry, String),
        name!(newest_entry, String),
    ),
> {
    let table_count = Spi::get_one::<i64>(
        "SELECT COUNT(*)::bigint FROM flashback.operations WHERE restored = false",
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    let total_size_bytes = Spi::get_one::<i64>(
        "SELECT COALESCE(SUM(pg_total_relation_size(format('flashback_recycle.%I', recycled_name))), 0)::bigint \
         FROM flashback.operations WHERE restored = false",
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    let oldest_entry = Spi::get_one::<String>(
        "SELECT MIN(timestamp)::text FROM flashback.operations WHERE restored = false",
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "-".to_string());

    let newest_entry = Spi::get_one::<String>(
        "SELECT MAX(timestamp)::text FROM flashback.operations WHERE restored = false",
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "-".to_string());

    let table_limit = crate::guc::get_max_tables();
    let size_limit_mb = crate::guc::get_max_size();
    let retention_days = crate::guc::get_retention_days();
    let worker_interval_seconds = crate::guc::worker_interval_seconds();

    TableIterator::new(
        vec![(
            table_count,
            table_limit,
            total_size_bytes,
            size_limit_mb,
            retention_days,
            worker_interval_seconds,
            oldest_entry,
            newest_entry,
        )]
        .into_iter(),
    )
}

/// Restore all tables that were dropped from `schema_name` in a single call.
/// Tables are restored in oldest-first order (so earlier drops come back first).
/// Each table's own permission checks still apply.
/// Returns the count of successfully restored tables.
#[pg_extern]
fn flashback_restore_schema(schema_name: &str, target_schema: Option<&str>) -> i64 {
    // SQL injection protection
    if schema_name.contains('\'') || schema_name.contains(';') {
        pgrx::warning!("pg_flashback: invalid schema name: {}", schema_name);
        return 0;
    }
    if let Some(s) = target_schema {
        if s.contains('\'') || s.contains(';') {
            pgrx::warning!("pg_flashback: invalid target schema: {}", s);
            return 0;
        }
    }

    // Collect all unrestored op_ids for this schema, oldest-first.
    let mut op_ids: Vec<i64> = Vec::new();
    let _ = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT op_id FROM flashback.operations \
                 WHERE schema_name = '{}' AND restored = false \
                 ORDER BY timestamp ASC",
                schema_name
            ),
            None,
            &[],
        )?;
        for row in rows {
            if let Ok(Some(id)) = row.get::<i64>(1) {
                op_ids.push(id);
            }
        }
        Ok::<_, spi::Error>(())
    });

    if op_ids.is_empty() {
        pgrx::warning!(
            "pg_flashback: no tables found in recycle bin for schema '{}'",
            schema_name
        );
        return 0;
    }

    let total = op_ids.len() as i64;
    let mut restored_count = 0i64;
    for op_id in &op_ids {
        if flashback_restore_by_id(*op_id, target_schema) {
            restored_count += 1;
        }
    }

    pgrx::log!(
        "pg_flashback: schema restore '{}': {}/{} tables restored",
        schema_name, restored_count, total
    );
    restored_count
}

#[pg_extern]
fn flashback_list_recycled_tables() -> TableIterator<'static, (name!(schema_name, String), name!(table_name, String), name!(recycled_name, String), name!(dropped_at, String), name!(role_name, String), name!(retention_until, String), name!(op_id, i64), name!(operation_type, String))> {
    let mut results = Vec::new();
    
    // Superuser sees all entries; regular users see only their own.
    // Use GetSessionUserId() so SECURITY DEFINER doesn't bypass the check.
    let is_superuser = unsafe { pg_sys::superuser_arg(pg_sys::GetSessionUserId()) };
    let sql = if is_superuser {
        "SELECT schema_name, table_name, recycled_name, timestamp::text, role_name, retention_until::text, op_id, operation_type \
         FROM flashback.operations \
         WHERE restored = false \
         ORDER BY timestamp DESC".to_string()
    } else {
        "SELECT schema_name, table_name, recycled_name, timestamp::text, role_name, retention_until::text, op_id, operation_type \
         FROM flashback.operations \
         WHERE restored = false AND role_name = current_user \
         ORDER BY timestamp DESC".to_string()
    };
    
    let _ = Spi::connect(|client| {
        let tup_table = client.select(&sql, None, &[])?;
        for row in tup_table {
            let schema_name = row.get::<String>(1)?.unwrap_or_default();
            let table_name = row.get::<String>(2)?.unwrap_or_default();
            let recycled_name = row.get::<String>(3)?.unwrap_or_default();
            let dropped_at = row.get::<String>(4)?.unwrap_or_default();
            let role_name = row.get::<String>(5)?.unwrap_or_default();
            let retention_until = row.get::<String>(6)?.unwrap_or_default();
            let op_id = row.get::<i64>(7)?.unwrap_or_default();
            let operation_type = row.get::<String>(8)?.unwrap_or_default();
            results.push((schema_name, table_name, recycled_name, dropped_at, role_name, retention_until, op_id, operation_type));
        }
        Ok::<_, spi::Error>(())
    });
    
    TableIterator::new(results.into_iter())
}