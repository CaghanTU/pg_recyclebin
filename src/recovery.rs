use pgrx::prelude::*;

#[pg_extern]

fn flashback_restore(table_name: &str) -> bool {
    pgrx::log!("Restoring table: {}", table_name);

    // SQL injection protection
    if table_name.contains('\'') || table_name.contains(';') {
        pgrx::warning!("Invalid table name: {}", table_name);
        return false;
    }

    let (recycled_name, schema_name) = match Spi::get_two::<String, String>(&format!(
    "SELECT recycled_name, schema_name FROM flashback.operations 
     WHERE table_name = '{}' AND restored = false 
     ORDER BY timestamp DESC LIMIT 1",
    table_name
    )) {
    Ok((Some(r), Some(s))) => (r, s),
    Ok(_) => {
        pgrx::warning!("Table not found: {}", table_name);
        return false;
    }
    Err(e) => {
        pgrx::warning!("Query error: {}", e);
        return false;
    }
    };

     // Return error if target table already exists
    if let Ok(Some(true)) = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}')",
        schema_name, table_name
    )) {
        pgrx::warning!("Target table already exists: {}", table_name);
        return false;
    }

    let sql = format!("ALTER TABLE flashback_recycle.{} SET SCHEMA {}", recycled_name, schema_name);
    match Spi::run(&sql) {
    Ok(_) => {
        // First rename
        let rename_sql = format!("ALTER TABLE {}.{} RENAME TO {}", schema_name, recycled_name, table_name);
        if let Err(e) = Spi::run(&rename_sql) {
            pgrx::warning!("Rename error: {}", e);
            return false;
        }
        
        // Then update metadata
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
}
    
}


#[pg_extern]
fn flashback_list_recycled_tables() -> TableIterator<'static, (name!(table_name, String), name!(recycled_name, String), name!(dropped_at, String), name!(role_name, String), name!(retention_until, String))> {
    let mut results = Vec::new();
    
    let sql = "SELECT table_name, recycled_name, timestamp::text, role_name, retention_until::text 
               FROM flashback.operations 
               WHERE restored = false 
               ORDER BY timestamp DESC";
    
    let _ = Spi::connect(|client| {
        let tup_table = client.select(sql, None, &[])?;
        for row in tup_table {
            let table_name = row.get::<String>(1)?.unwrap_or_default();
            let recycled_name = row.get::<String>(2)?.unwrap_or_default();
            let dropped_at = row.get::<String>(3)?.unwrap_or_default();
            let role_name = row.get::<String>(4)?.unwrap_or_default();
            let retention_until = row.get::<String>(5)?.unwrap_or_default();
            results.push((table_name, recycled_name, dropped_at, role_name, retention_until));
        }
        Ok::<_, spi::Error>(())
    });
    
    TableIterator::new(results.into_iter())
}