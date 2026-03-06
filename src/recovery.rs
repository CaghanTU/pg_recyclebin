use pgrx::prelude::*;

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

    let (recycled_name, schema_name, role_name, operation_type) = {
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
                    "SELECT recycled_name, schema_name, role_name, operation_type \
                     FROM flashback.operations \
                     WHERE op_id = {} AND restored = false",
                    op_id_val
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r = row.get::<String>(1)?.unwrap_or_default();
                let s = row.get::<String>(2)?.unwrap_or_default();
                let o = row.get::<String>(3)?.unwrap_or_default();
                let op = row.get::<String>(4)?.unwrap_or_default();
                found = Some((r, s, o, op));
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
    // Table still exists, restore data back into it
    let sql = format!(
        "INSERT INTO {}.{} SELECT * FROM flashback_recycle.{}",
        restore_schema, table_name, recycled_name
    );
    match Spi::run(&sql) {
        Ok(_) => {
            // Restore sequences to max values present in the restored data
            let serial_cols: Vec<(String, String)> = {
                let mut cols = Vec::new();
                let _ = Spi::connect(|client| {
                    let rows = client.select(
                        &format!(
                            "SELECT column_name, \
                             pg_get_serial_sequence('{}.{}', column_name) \
                             FROM information_schema.columns \
                             WHERE table_schema = '{}' AND table_name = '{}' \
                             AND column_default LIKE 'nextval%%' \
                             AND pg_get_serial_sequence('{}.{}', column_name) IS NOT NULL",
                            restore_schema, table_name, restore_schema, table_name,
                            restore_schema, table_name
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
                    Ok::<_, spi::Error>(())
                });
                cols
            };
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
        true
    }
    Err(e) => {
        pgrx::warning!("Restore error: {}", e);
        false
    }
}}

#[pg_extern]
fn flashback_restore_by_id(op_id: i64, target_schema: Option<&str>) -> bool {
    let (recycled_name, table_name, schema_name, role_name, operation_type) = {
        // Acquire advisory lock on this op_id to prevent concurrent restores
        let _ = Spi::run(&format!("SELECT pg_advisory_xact_lock({})", op_id));
        let mut found = None;
        let _ = Spi::connect(|client| {
            let rows = client.select(
                &format!(
                    "SELECT recycled_name, table_name, schema_name, role_name, operation_type \
                     FROM flashback.operations \
                     WHERE op_id = {} AND restored = false",
                    op_id
                ),
                None,
                &[],
            )?;
            for row in rows {
                let r = row.get::<String>(1)?.unwrap_or_default();
                let t = row.get::<String>(2)?.unwrap_or_default();
                let s = row.get::<String>(3)?.unwrap_or_default();
                let o = row.get::<String>(4)?.unwrap_or_default();
                let op = row.get::<String>(5)?.unwrap_or_default();
                found = Some((r, t, s, o, op));
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
    // Table still exists, restore data back into it
    let sql = format!(
        "INSERT INTO {}.{} SELECT * FROM flashback_recycle.{}",
        restore_schema, table_name, recycled_name
    );
    match Spi::run(&sql) {
        Ok(_) => {
            // Restore sequences to max values present in the restored data
            let serial_cols: Vec<(String, String)> = {
                let mut cols = Vec::new();
                let _ = Spi::connect(|client| {
                    let rows = client.select(
                        &format!(
                            "SELECT column_name, \
                             pg_get_serial_sequence('{}.{}', column_name) \
                             FROM information_schema.columns \
                             WHERE table_schema = '{}' AND table_name = '{}' \
                             AND column_default LIKE 'nextval%%' \
                             AND pg_get_serial_sequence('{}.{}', column_name) IS NOT NULL",
                            restore_schema, table_name, restore_schema, table_name,
                            restore_schema, table_name
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
                    Ok::<_, spi::Error>(())
                });
                cols
            };
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

#[pg_extern]
fn flashback_restore_table(table_name: &str) -> bool {
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

#[pg_extern]
fn flashback_list_recycled_tables() -> TableIterator<'static, (name!(schema_name, String), name!(table_name, String), name!(recycled_name, String), name!(dropped_at, String), name!(role_name, String), name!(retention_until, String), name!(op_id, i64))> {
    let mut results = Vec::new();
    
    // Superuser sees all entries; regular users see only their own.
    // Use GetSessionUserId() so SECURITY DEFINER doesn't bypass the check.
    let is_superuser = unsafe { pg_sys::superuser_arg(pg_sys::GetSessionUserId()) };
    let sql = if is_superuser {
        "SELECT schema_name, table_name, recycled_name, timestamp::text, role_name, retention_until::text, op_id \
         FROM flashback.operations \
         WHERE restored = false \
         ORDER BY timestamp DESC".to_string()
    } else {
        "SELECT schema_name, table_name, recycled_name, timestamp::text, role_name, retention_until::text, op_id \
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
            results.push((schema_name, table_name, recycled_name, dropped_at, role_name, retention_until, op_id));
        }
        Ok::<_, spi::Error>(())
    });
    
    TableIterator::new(results.into_iter())
}