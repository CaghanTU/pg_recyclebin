use pgrx::prelude::*;
use pgrx::bgworkers::*;
use std::time::Duration;

pub fn register() {
    BackgroundWorkerBuilder::new("pg_flashback cleanup worker")
        .set_function("flashback_cleanup_worker_main")
        .set_library("pg_flashback")
        .set_argument(None::<i32>.into_datum())
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
}


#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn flashback_cleanup_worker_main(_arg: pg_sys::Datum) {
    
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    
    let dbname = crate::guc::get_database_name();
    BackgroundWorker::connect_worker_to_spi(Some(dbname.as_str()), None);

    pgrx::log!("Flashback Auto-Cleanup Worker started");
    
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(crate::guc::worker_interval_seconds() as u64))) {
        if BackgroundWorker::sighup_received() {
            pgrx::log!("Flashback worker: configuration reloaded");
        }

        BackgroundWorker::transaction(|| {
            cleanup_expired_tables();
        });
    }

    pgrx::log!("Flashback Auto-Cleanup Worker Stopped");
}

/// Cleanup tables that have exceeded their retention period
fn cleanup_expired_tables() {
    let retention_days = crate::guc::get_retention_days();
    
    if retention_days <= 0 {
        return; // Cleanup disabled
    }

    let result: Result<(), spi::Error> = Spi::connect(|client| {
        // Skip if extension is not installed in this database.
        // Check for the operations TABLE specifically (not just the schema), because
        // DROP EXTENSION drops the table but may leave the schema behind if the schema
        // was created before the extension was first installed.  Checking only the schema
        // would let the worker proceed and crash on "relation does not exist".
        let schema_exists = client
            .select(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_class c \
                    JOIN pg_namespace n ON n.oid = c.relnamespace \
                    WHERE n.nspname = 'flashback' \
                      AND c.relname = 'operations' \
                      AND c.relkind = 'r')",
                None,
                &[] as &[pgrx::datum::DatumWithOid],
            )?
            .first()
            .get::<bool>(1)?
            .unwrap_or(false);

        if !schema_exists {
            return Ok(());
        }

        let query = "SELECT op_id, recycled_name, table_name \
             FROM flashback.operations \
             WHERE restored = false \
             AND retention_until < NOW() \
             ORDER BY timestamp";

        let results = client.select(query, None, &[] as &[pgrx::datum::DatumWithOid])?;
        
        let mut cleaned_count = 0;
        
        for row in results {
            let op_id = row.get::<i64>(1)?.unwrap_or(0);
            let recycled_name = row.get::<String>(2)?.unwrap_or_default();
            let table_name = row.get::<String>(3)?.unwrap_or_default();
            
            if recycled_name.is_empty() {
                continue;
            }

            let drop_sql = format!("/* PG_FLASHBACK_INTERNAL */ DROP TABLE IF EXISTS flashback_recycle.{} CASCADE", crate::context::qi(&recycled_name));
            
            if let Err(e) = Spi::run(&drop_sql) {
                pgrx::warning!("Failed to drop expired table '{}': {}", recycled_name, e);
                continue;
            }

            let delete_sql = format!(
                "DELETE FROM flashback.operations WHERE op_id = {}",
                op_id
            );
            
            if let Err(e) = Spi::run(&delete_sql) {
                pgrx::warning!("Failed to delete metadata for op_id {}: {}", op_id, e);
                continue;
            }

            cleaned_count += 1;
            pgrx::log!(
                "Auto-cleanup: Removed expired table '{}' (was: {})",
                table_name, recycled_name
            );
        }

        if cleaned_count > 0 {
            pgrx::log!(
                "Auto-cleanup completed: {} expired table(s) removed",
                cleaned_count
            );
        }

        Ok(())
    });

    if let Err(e) = result {
        pgrx::warning!("Cleanup cycle failed: {}", e);
    }
}