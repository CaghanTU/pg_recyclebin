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
    
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("Flashback Auto-Cleanup Worker started");
    
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(crate::guc::worker_interval_seconds() as u64))) {
        // Check if we received SIGHUP (config reload)
        if BackgroundWorker::sighup_received() {
            // Reload configuration
            pgrx::log!("Flashback Worker: Reloading configuration");
        }

        // Run cleanup in a transaction
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
        // Find expired tables
        let query = format!(
            "SELECT op_id, recycled_name, table_name 
             FROM flashback.operations 
             WHERE restored = false 
             AND retention_until < NOW() 
             ORDER BY timestamp"
        );

        let results = client.select(&query, None, &[] as &[pgrx::datum::DatumWithOid])?;
        
        let mut cleaned_count = 0;
        
        for row in results {
            let op_id = row.get::<i64>(1)?.unwrap_or(0);
            let recycled_name = row.get::<String>(2)?.unwrap_or_default();
            let table_name = row.get::<String>(3)?.unwrap_or_default();
            
            if recycled_name.is_empty() {
                continue;
            }

            // Drop the expired table
            let drop_sql = format!(
                "DROP TABLE IF EXISTS flashback_recycle.{} CASCADE",
                recycled_name
            );
            
            if let Err(e) = Spi::run(&drop_sql) {
                pgrx::warning!(
                    "Failed to drop expired table '{}': {}", 
                    recycled_name, e
                );
                continue;
            }

            // Delete metadata record
            let delete_sql = format!(
                "DELETE FROM flashback.operations WHERE op_id = {}",
                op_id
            );
            
            if let Err(e) = Spi::run(&delete_sql) {
                pgrx::warning!(
                    "Failed to delete metadata for op_id {}: {}", 
                    op_id, e
                );
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