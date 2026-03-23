use pgrx::prelude::*;
use crate::context::qi;

const FLASHBACK_BIN_LOCK_KEY: i64 = 846271;

fn acquire_flashback_bin_lock() {
    let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", FLASHBACK_BIN_LOCK_KEY));
}

fn is_in_recovery() -> bool {
    unsafe { pg_sys::RecoveryInProgress() }
}

// Rename indexes back to their original names after restoring a table.
// During capture, indexes were renamed from `idx_name` to `idx_name_{op_id}` to prevent
// name collisions in flashback_recycle when the same table is dropped multiple times.
fn rename_indexes_back(schema: &str, table: &str, recycled_name: &str) {
    // Extract the numeric op_id suffix from recycled_name (format: {table}_{op_id}).
    let op_id_suffix = recycled_name
        .rfind('_')
        .map(|i| &recycled_name[i + 1..])
        .filter(|s| s.chars().all(|c| c.is_ascii_digit()))
        .unwrap_or("");
    if op_id_suffix.is_empty() {
        return;
    }
    let suffix = format!("_{}", op_id_suffix);
    // Use Spi::get_one with string_agg to avoid Spi::connect visibility issues after DDL.
    // chr(1) (SOH) separator: PG identifiers cannot contain SOH bytes, safe even for
    // quoted names that include commas (e.g. "a,b").
    let idx_names_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(indexname, chr(1)), '') \
         FROM pg_indexes \
         WHERE schemaname = '{}' AND tablename = '{}'",
        schema, table
    ))
    .unwrap_or(None)
    .unwrap_or_default();
    for idx in idx_names_csv.split('\x01').filter(|s| !s.is_empty()) {
        if idx.ends_with(&suffix) {
            let orig = &idx[..idx.len() - suffix.len()];
            if let Err(e) = Spi::run(&format!(
                "ALTER INDEX {}.{} RENAME TO {}",
                qi(schema), qi(idx), qi(orig)
            )) {
                pgrx::warning!(
                    "pg_flashback: failed to rename index '{}' back to '{}': {}",
                    idx, orig, e
                );
            }
        }
    }
}

// Rename sequences back to their original names after restoring a table.
// During capture, sequences were renamed from `seq_name` to `seq_name_{op_id}` to prevent
// name collisions in flashback_recycle when the same table is dropped multiple times.
fn rename_sequences_back(schema: &str, table: &str, recycled_name: &str) {
    let op_id_suffix = recycled_name
        .rfind('_')
        .map(|i| &recycled_name[i + 1..])
        .filter(|s| s.chars().all(|c| c.is_ascii_digit()))
        .unwrap_or("");
    if op_id_suffix.is_empty() {
        return;
    }
    let suffix = format!("_{}", op_id_suffix);
    // Use Spi::get_one with string_agg to avoid Spi::connect visibility issues after DDL.
    // chr(1) (SOH) separator: PG identifiers cannot contain SOH bytes, safe even for
    // quoted names that include commas (e.g. "a,b").
    let seq_names_csv = Spi::get_one::<String>(&format!(
        "SELECT COALESCE(string_agg(s.relname, chr(1)), '') \
         FROM pg_depend d \
         JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
         JOIN pg_class t ON t.oid = d.refobjid AND t.relname = '{}' \
         WHERE d.deptype IN ('a', 'i') \
           AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}')",
        table, schema
    ))
    .unwrap_or(None)
    .unwrap_or_default();
    for seq in seq_names_csv.split('\x01').filter(|s| !s.is_empty()) {
        if seq.ends_with(&suffix) {
            let orig = &seq[..seq.len() - suffix.len()];
            if let Err(e) = Spi::run(&format!(
                "ALTER SEQUENCE {}.{} RENAME TO {}",
                qi(schema), qi(seq), qi(orig)
            )) {
                pgrx::warning!(
                    "pg_flashback: failed to rename sequence '{}' back to '{}': {}",
                    seq, orig, e
                );
            }
        }
    }
}

fn find_serial_sequences(schema: &str, table: &str) -> Vec<(String, String)> {
    let mut cols = Vec::new();
    let result = pgrx::Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT a.attname::text, s.oid::regclass::text \
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
    if let Err(e) = result {
        pgrx::warning!("pg_flashback: find_serial_sequences error: {:?}", e);
    }
    cols
}

fn has_incoming_foreign_keys(schema: &str, table: &str) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS( \
             SELECT 1 \
             FROM pg_constraint c \
             JOIN pg_class target ON target.oid = c.confrelid \
             JOIN pg_namespace ns ON ns.oid = target.relnamespace \
             WHERE c.contype = 'f' \
               AND ns.nspname = '{}' \
               AND target.relname = '{}' \
               AND c.conrelid <> c.confrelid \
         )",
        schema, table
    ))
    .unwrap_or(None)
    .unwrap_or(false)
}

fn restore_from_metadata(metadata_json: &str, table_name: &str, restore_schema: &str) {
    let meta: serde_json::Value = match serde_json::from_str(metadata_json) {
        Ok(v) => v,
        Err(_) => return,
    };

    // 1. Views (regular 'v' and materialized 'm')
    if let Some(views) = meta.get("views").and_then(|v| v.as_array()) {
        for view in views {
            let view_schema = view.get("schema").and_then(|s| s.as_str()).unwrap_or("public");
            let view_name   = view.get("name").and_then(|s| s.as_str()).unwrap_or("");
            let view_kind   = view.get("kind").and_then(|s| s.as_str()).unwrap_or("v");
            let view_def    = view.get("def").and_then(|s| s.as_str()).unwrap_or("");

            if view_name.is_empty() || view_def.is_empty() {
                continue;
            }

            let create_sql = if view_kind == "m" {
                // Materialized views: drop first (may still exist if restore runs before explicit
                // drop propagates), then recreate.
                let drop_sql = format!(
                    "DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE",
                    qi(view_schema), qi(view_name)
                );
                let _ = Spi::run(&drop_sql);
                format!("CREATE MATERIALIZED VIEW {}.{} AS {}", qi(view_schema), qi(view_name), view_def)
            } else {
                format!("CREATE OR REPLACE VIEW {}.{} AS {}", qi(view_schema), qi(view_name), view_def)
            };

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
                qi(restore_schema), qi(table_name)
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
                    qi(name), qi(restore_schema), qi(table_name)
                ));

                let permissive_str = if permissive { "PERMISSIVE" } else { "RESTRICTIVE" };
                let roles_clause   = if roles.is_empty() { "PUBLIC".to_string() } else { roles.to_string() };
                let using_clause   = if qual.is_empty() { String::new() } else { format!(" USING ({})", qual) };
                let check_clause   = if with_check.is_empty() { String::new() } else { format!(" WITH CHECK ({})", with_check) };

                let policy_sql = format!(
                    "CREATE POLICY {} ON {}.{} AS {} FOR {} TO {}{}{}",
                    qi(name), qi(restore_schema), qi(table_name),
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

// SET SCHEMA co-moves child partitions with the parent; this verifies they landed correctly
// and recreates any that are missing as empty shells with a warning.
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

    let parent_ref = format!(
        "\"{}\".\"{}\"",
        restore_schema.replace('"', "\"\""),
        table_name.replace('"', "\"\"")
    );

    let mut co_moved = 0usize;
    let mut created  = 0usize;
    let mut failed   = 0usize;

    for child in &children {
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

/// Shared core restore logic — called after the caller has resolved the recycle-bin entry
/// by name or by op_id, performed its advisory lock, and extracted all fields.
fn perform_restore(
    recycled_name: &str,
    table_name: &str,
    schema_name: &str,
    role_name: &str,
    operation_type: &str,
    metadata_json: &str,
    target_schema: Option<&str>,
) -> bool {
    acquire_flashback_bin_lock();

    // Use session user (not effective user) so SECURITY DEFINER can't bypass the check.
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

    let restore_schema = target_schema.unwrap_or(schema_name);
    let recycled_name_sql = recycled_name.replace('\'', "''");

    // Race safety: another session may purge this recycle table after we've selected
    // the operation row but before we execute restore DDL.
    let recycle_exists = Spi::get_one::<bool>(&format!(
        "SELECT to_regclass(format('flashback_recycle.%I', '{}')) IS NOT NULL",
        recycled_name_sql
    ))
    .unwrap_or(None)
    .unwrap_or(false);
    if !recycle_exists {
        let _ = Spi::run(&format!(
            "DELETE FROM flashback.operations \
             WHERE recycled_name = '{}' AND restored = false",
            recycled_name_sql
        ));
        pgrx::warning!(
            "Recycle entry disappeared before restore: {} (likely purged concurrently)",
            recycled_name
        );
        return false;
    }

    // Spi DDL errors surface as PG ERROR rather than Rust Err, so validate upfront.
    let schema_exists = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{}')",
        restore_schema
    )).unwrap_or(None).unwrap_or(false);
    if !schema_exists {
        if operation_type == "TRUNCATE" {
            // TRUNCATE restore requires the table to already exist; schema must be present.
            pgrx::warning!("Target schema does not exist: {}", restore_schema);
            return false;
        }
        // For DROP operations, auto-create the missing schema so the table can be restored.
        if let Err(e) = Spi::run(&format!("CREATE SCHEMA {}", qi(restore_schema))) {
            pgrx::warning!(
                "pg_flashback: could not create schema '{}': {}",
                restore_schema, e
            );
            return false;
        }
        pgrx::log!("pg_flashback: created schema '{}' for restore", restore_schema);
    }

    if operation_type == "TRUNCATE" {
        // For TRUNCATE restore, the target table must already exist (it was only emptied, not dropped).
        // If it doesn't exist (e.g. a subsequent DROP), fail gracefully instead of crashing.
        let table_exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}')",
            restore_schema, table_name
        )).unwrap_or(None).unwrap_or(false);
        if !table_exists {
            pgrx::warning!(
                "TRUNCATE restore: target table '{}.{}' does not exist. \
                 If the table was also dropped, restore the DROP entry first.",
                restore_schema, table_name
            );
            return false;
        }
        if has_incoming_foreign_keys(restore_schema, table_name) {
            pgrx::warning!(
                "TRUNCATE restore: target table '{}.{}' is referenced by foreign keys from other tables",
                restore_schema, table_name
            );
            return false;
        }
        // Clear existing rows before inserting backed-up data (idempotent restore).
        // The PG_FLASHBACK_INTERNAL marker prevents our own ProcessUtility hook from
        // re-capturing this internal TRUNCATE as a new bin entry.
        if let Err(e) = Spi::run(&format!(
            "/* PG_FLASHBACK_INTERNAL */ TRUNCATE TABLE {}.{}",
            qi(restore_schema), qi(table_name)
        )) {
            pgrx::warning!("TRUNCATE restore: failed to clear target table '{}': {}", table_name, e);
            return false;
        }
        // OVERRIDING SYSTEM VALUE is needed for GENERATED ALWAYS AS IDENTITY columns.
        let sql = format!(
            "INSERT INTO {}.{} OVERRIDING SYSTEM VALUE SELECT * FROM flashback_recycle.{}",
            qi(restore_schema), qi(table_name), qi(recycled_name)
        );
        match Spi::run(&sql) {
            Ok(_) => {
                let serial_cols = find_serial_sequences(restore_schema, table_name);
                for (col, seq) in &serial_cols {
                    if let Ok(Some(max_val)) = Spi::get_one::<i64>(
                        &format!("SELECT COALESCE(MAX(\"{}\"), 0)::bigint FROM \"{}\".\"{}\"",
                                 col, restore_schema, table_name)
                    ) {
                        let _ = Spi::run(&format!("SELECT setval('{}', {})", seq, max_val.max(1)));
                    }
                }
                let _ = Spi::run(&format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{}", qi(recycled_name)));
                let _ = Spi::run(&format!(
                    "UPDATE flashback.operations SET restored = true \
                     WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
                    table_name, recycled_name
                ));
                return true;
            }
            Err(e) => {
                pgrx::warning!("TRUNCATE restore error: {}", e);
                return false;
            }
        }
    }

    // operation_type == "DROP"
    if let Ok(Some(true)) = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}')",
        restore_schema, table_name
    )) {
        pgrx::warning!("Target table already exists: {}", table_name);
        return false;
    }

    // Use a savepoint so that if RENAME fails after SET SCHEMA the table is not
    // left orphaned in restore_schema under its recycled name.
    let sp = format!("flashback_restore_{}", recycled_name.replace(|c: char| !c.is_alphanumeric(), "_"));
    let _ = Spi::run(&format!("SAVEPOINT {}", sp));

    let set_schema_sql = format!(
        "ALTER TABLE IF EXISTS flashback_recycle.{} SET SCHEMA {}",
        qi(recycled_name), qi(restore_schema)
    );
    if let Err(e) = Spi::run(&set_schema_sql) {
        let _ = Spi::run(&format!("ROLLBACK TO SAVEPOINT {}", sp));
        pgrx::warning!("Restore error: {}", e);
        return false;
    }
    let moved = Spi::get_one::<bool>(&format!(
        "SELECT to_regclass(format('%I.%I', '{}', '{}')) IS NOT NULL",
        restore_schema.replace('\'', "''"),
        recycled_name_sql
    ))
    .unwrap_or(None)
    .unwrap_or(false);
    if !moved {
        let _ = Spi::run(&format!("ROLLBACK TO SAVEPOINT {}", sp));
        let _ = Spi::run(&format!(
            "DELETE FROM flashback.operations WHERE recycled_name = '{}' AND restored = false",
            recycled_name_sql
        ));
        pgrx::warning!(
            "Recycle entry disappeared during restore: {} (likely purged concurrently)",
            recycled_name
        );
        return false;
    }

    let rename_sql = format!("ALTER TABLE IF EXISTS {}.{} RENAME TO {}",
        qi(restore_schema), qi(recycled_name), qi(table_name));
    if let Err(e) = Spi::run(&rename_sql) {
        let _ = Spi::run(&format!("ROLLBACK TO SAVEPOINT {}", sp));
        pgrx::warning!("Rename error: {}", e);
        return false;
    }

    let renamed = Spi::get_one::<bool>(&format!(
        "SELECT to_regclass(format('%I.%I', '{}', '{}')) IS NOT NULL",
        restore_schema.replace('\'', "''"),
        table_name.replace('\'', "''")
    ))
    .unwrap_or(None)
    .unwrap_or(false);
    if !renamed {
        let _ = Spi::run(&format!("ROLLBACK TO SAVEPOINT {}", sp));
        return false;
    }

    let _ = Spi::run(&format!("RELEASE SAVEPOINT {}", sp));

    rename_indexes_back(restore_schema, table_name, recycled_name);
    rename_sequences_back(restore_schema, table_name, recycled_name);
    let update_sql = format!(
        "UPDATE flashback.operations SET restored = true \
         WHERE table_name = '{}' AND recycled_name = '{}' AND restored = false",
        table_name, recycled_name
    );
    if let Err(e) = Spi::run(&update_sql) {
        pgrx::warning!("Update error: {}", e);
    }
    if !metadata_json.is_empty() {
        restore_from_metadata(metadata_json, table_name, restore_schema);
        restore_partition_info_from_metadata(metadata_json, table_name, restore_schema);
    }
    true
}

#[pg_extern]
fn flashback_restore(table_name: &str, target_schema: default!(Option<&str>, NULL)) -> bool {
    pgrx::log!("Restoring table: {}", table_name);

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
        // Advisory lock on op_id to prevent concurrent restores of the same entry.
        // When target_schema is provided, prefer the record whose original schema_name
        // matches target_schema so that two tables with the same name from different
        // schemas are not swapped. Fall back to the most-recent record if no match.
        let schema_priority = match target_schema {
            Some(s) => format!("(schema_name = '{}') DESC, ", s),
            None => String::new(),
        };
        let op_id_val = Spi::get_one::<i64>(&format!(
            "SELECT op_id FROM flashback.operations \
             WHERE table_name = '{}' AND restored = false \
             ORDER BY {}timestamp DESC, op_id DESC LIMIT 1",
            table_name, schema_priority
        )).unwrap_or(None);
        let op_id_val = match op_id_val {
            Some(id) => id,
            None => {
                // Phase 4.3 fallback: try pgBackRest backup restore if stanza is configured
                if try_backup_restore_fallback(table_name, target_schema) {
                    return true;
                }
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

    perform_restore(&recycled_name, table_name, &schema_name, &role_name, &operation_type, &metadata_json, target_schema)
}

/// Fallback path: attempt backup restore when a table is NOT in the recycle bin but
/// `flashback.pgbackrest_stanza` is configured and the operation record has relfilenode metadata.
fn try_backup_restore_fallback(table_name: &str, target_schema: Option<&str>) -> bool {
    // Check if pgBackRest integration is configured
    if crate::guc::get_pgbackrest_stanza().is_none() {
        return false;
    }

    // Look for a (possibly already-restored or old) operation record that has relfilenode
    let row = pgrx::Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT schema_name, COALESCE(metadata::text, '') \
                 FROM flashback.operations \
                 WHERE table_name = '{}' \
                   AND metadata->>'relfilenode' IS NOT NULL \
                 ORDER BY timestamp DESC, op_id DESC LIMIT 1",
                table_name.replace('\'', "''")
            ),
            None,
            &[],
        )?;
        let mut found: Option<(String, String)> = None;
        for r in rows {
            let s = r.get::<String>(1)?.unwrap_or_default();
            let m = r.get::<String>(2)?.unwrap_or_default();
            found = Some((s, m));
        }
        Ok::<_, pgrx::spi::Error>(found)
    });

    match row {
        Ok(Some((schema, meta))) if !meta.is_empty() => {
            let restore_schema = target_schema.unwrap_or(&schema);
            pgrx::warning!(
                "pg_flashback: '{}' not in recycle bin — attempting pgBackRest backup restore",
                table_name
            );
            match crate::backup_restore::restore_table_from_backup(
                restore_schema,
                table_name,
                &meta,
                false,
            ) {
                Ok(()) => {
                    let _ = pgrx::Spi::run(&format!(
                        "UPDATE flashback.operations \
                         SET restored = true, restored_at = now() \
                         WHERE table_name = '{}' AND restored = false",
                        table_name.replace('\'', "''")
                    ));
                    true
                }
                Err(e) => {
                    pgrx::warning!(
                        "pg_flashback: backup restore failed for '{}': {}",
                        table_name,
                        e
                    );
                    false
                }
            }
        }
        _ => false,
    }
}

#[pg_extern]
fn flashback_restore_by_id(op_id: i64, target_schema: default!(Option<&str>, NULL)) -> bool {
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

    perform_restore(&recycled_name, &table_name, &schema_name, &role_name, &operation_type, &metadata_json, target_schema)
}

#[pg_extern]
fn flashback_restore_by_op_id(op_id: i64, target_schema: default!(Option<&str>, NULL)) -> bool {
    flashback_restore_by_id(op_id, target_schema)
}

#[pg_extern]
fn flashback_purge(table_name: &str) -> bool {
    acquire_flashback_bin_lock();

    // SQL injection protection
    if table_name.contains('\'') || table_name.contains(';') {
        pgrx::warning!("Invalid table name: {}", table_name);
        return false;
    }

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

    let drop_sql = format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", qi(&recycled_name));
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
    acquire_flashback_bin_lock();

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

    let drop_sql = format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", qi(&recycled_name));
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

#[pg_extern]
fn flashback_purge_by_op_id(op_id: i64) -> bool {
    flashback_purge_by_id(op_id)
}

#[pg_extern]
fn flashback_purge_all() -> i64 {
    acquire_flashback_bin_lock();

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

    let mut purged_count = 0i64;

    for name in &recycled_names {
        let sql = format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", qi(name));
        if let Err(e) = Spi::run(&sql) {
            pgrx::warning!("Failed to drop '{}': {}", name, e);
            continue;
        }
        purged_count += 1;
    }

    if let Err(e) = Spi::run(&format!(
        "DELETE FROM flashback.operations {}",
        filter
    )) {
        pgrx::warning!("Failed to delete operations: {}", e);
    }

    purged_count
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
    // On hot-standby, SPI + xact-scoped lock paths can fail with
    // "cannot assign TransactionIds during recovery". Return a safe read-only fallback.
    if is_in_recovery() {
        let table_limit = crate::guc::get_max_tables();
        let size_limit_mb = crate::guc::get_max_size();
        let retention_days = crate::guc::get_retention_days();
        let worker_interval_seconds = crate::guc::worker_interval_seconds();
        return TableIterator::new(
            vec![(
                0,
                table_limit,
                0,
                size_limit_mb,
                retention_days,
                worker_interval_seconds,
                "-".to_string(),
                "-".to_string(),
            )]
            .into_iter(),
        );
    }

    acquire_flashback_bin_lock();

    let table_count = Spi::get_one::<i64>(
        "SELECT COUNT(*)::bigint FROM flashback.operations WHERE restored = false",
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    let total_size_bytes = Spi::get_one::<i64>(
           "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint \
            FROM flashback.operations o \
            JOIN LATERAL to_regclass(format('flashback_recycle.%I', o.recycled_name)) AS r(oid) ON true \
            JOIN pg_class c ON c.oid = r.oid \
            WHERE o.restored = false",
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
/// Tables are restored in newest-first order (DROP entries before TRUNCATE entries),
/// so that a DROP followed by a TRUNCATE on the same table is handled correctly.
/// Each table's own permission checks still apply.
/// Returns the count of successfully restored tables.
#[pg_extern]
fn flashback_restore_schema(schema_name: &str, target_schema: Option<&str>) -> i64 {
    acquire_flashback_bin_lock();

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

    // Collect all unrestored op_ids for this schema, newest-first (DESC).
    let mut op_ids: Vec<i64> = Vec::new();
    let _ = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT op_id FROM flashback.operations \
                 WHERE schema_name = '{}' AND restored = false \
                 ORDER BY timestamp DESC, op_id DESC",
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

    // When restoring a CASCADE-dropped schema, the schema itself was deleted.
    // If the destination schema does not exist, create it automatically so the
    // individual restore calls don't fail with "Target schema does not exist".
    let effective_schema = target_schema.unwrap_or(schema_name);
    let schema_exists = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{}')",
        effective_schema
    )).unwrap_or(None).unwrap_or(false);
    if !schema_exists {
        if let Err(e) = Spi::run(&format!("CREATE SCHEMA {}", qi(effective_schema))) {
            pgrx::warning!(
                "pg_flashback: could not create schema '{}': {}",
                effective_schema, e
            );
            return 0;
        }
        pgrx::log!("pg_flashback: created schema '{}' for restore", effective_schema);
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
         ORDER BY op_id DESC".to_string()
    } else {
        "SELECT schema_name, table_name, recycled_name, timestamp::text, role_name, retention_until::text, op_id, operation_type \
         FROM flashback.operations \
         WHERE restored = false AND role_name = current_user \
         ORDER BY op_id DESC".to_string()
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

#[pg_extern]
fn flashback_restore_all() -> i64 {
    acquire_flashback_bin_lock();

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

    // Collect (op_id, schema_name) newest-first so DROP-then-TRUNCATE on the same table is
    // handled correctly (same ordering strategy as flashback_restore_schema).
    let mut entries: Vec<(i64, String)> = Vec::new();
    let _ = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT op_id, schema_name FROM flashback.operations {} \
                 ORDER BY timestamp DESC, op_id DESC",
                filter
            ),
            None,
            &[],
        )?;
        for row in rows {
            if let (Ok(Some(id)), Ok(Some(schema))) = (row.get::<i64>(1), row.get::<String>(2)) {
                entries.push((id, schema));
            }
        }
        Ok::<_, spi::Error>(())
    });

    if entries.is_empty() {
        return 0;
    }

    let total = entries.len() as i64;

    // Auto-create any schemas that no longer exist (e.g. after DROP SCHEMA CASCADE).
    let mut seen_schemas = std::collections::HashSet::<String>::new();
    for (_, schema) in &entries {
        if seen_schemas.insert(schema.clone()) {
            let exists = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{}')",
                schema
            )).unwrap_or(None).unwrap_or(false);
            if !exists {
                if let Err(e) = Spi::run(&format!("CREATE SCHEMA {}", qi(schema))) {
                    pgrx::warning!("pg_flashback: restore_all: could not create schema '{}': {}", schema, e);
                }
            }
        }
    }

    let mut restored_count = 0i64;
    for (op_id, _) in &entries {
        if flashback_restore_by_id(*op_id, None) {
            restored_count += 1;
        }
    }

    pgrx::log!("pg_flashback: restore_all: {}/{} tables restored", restored_count, total);
    restored_count
}