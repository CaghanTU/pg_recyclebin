use pgrx::prelude::*;
use chrono::Local;
use crate::guc;

// Recycle bin'deki en eski tabloyu siler (hem fiziksel tablo hem operations kaydı)
fn evict_oldest() {
    let find_sql = "SELECT recycled_name FROM flashback.operations ORDER BY retention_until ASC LIMIT 1";

    let oldest = Spi::get_one::<String>(find_sql);

    match oldest {
        Ok(Some(recycled_name)) => {
            let drop_sql = format!("DROP TABLE IF EXISTS flashback_recycle.{}", recycled_name);
            let delete_sql = format!("DELETE FROM flashback.operations WHERE recycled_name = '{}'", recycled_name);

            if let Err(e) = Spi::run(&drop_sql) {
                pgrx::warning!("Evict drop hatası: {}", e);
                return;
            }
            if let Err(e) = Spi::run(&delete_sql) {
                pgrx::warning!("Evict delete hatası: {}", e);
            }

            pgrx::log!("FIFO evict: {} silindi", recycled_name);
        }
        _ => {
            pgrx::warning!("Evict: silinecek tablo bulunamadı");
        }
    }
}

// Tablo sayısı max_tables'ı geçtiyse en eskiyi sil
fn enforce_table_limit() {
    let count_sql = "SELECT COUNT(*)::int FROM flashback.operations";

    if let Ok(Some(count)) = Spi::get_one::<i32>(count_sql) {
        let max = guc::get_max_tables();
        if count >= max {
            pgrx::log!("Tablo limiti doldu ({}/{}), en eski siliniyor", count, max);
            evict_oldest();
        }
    }
}

// Toplam boyut max_size'ı geçtiyse en eskiyi sil (MB cinsinden karşılaştırır)
fn enforce_size_limit() {
    let size_sql = "SELECT COALESCE(SUM(pg_total_relation_size(quote_ident('flashback_recycle') || '.' || quote_ident(recycled_name))), 0)::bigint FROM flashback.operations";

    if let Ok(Some(total_bytes)) = Spi::get_one::<i64>(size_sql) {
        let total_mb = total_bytes / (1024 * 1024);
        let max_mb = guc::get_max_size() as i64;
        if total_mb >= max_mb {
            pgrx::log!("Boyut limiti doldu ({}/{} MB), en eski siliniyor", total_mb, max_mb);
            evict_oldest();
        }
    }
}

pub fn handle_drop_table(query: &str) -> bool {
    // Limitler dolmuşsa FIFO — en eski gider, yenisi girer
    enforce_table_limit();
    enforce_size_limit();

    let table_name = query
    .trim_end_matches(';')
    .trim()
    .split_whitespace()
    .last()
    .unwrap_or("");

    pgrx::log!("Tablo adı: {}", table_name);
    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    
    let recycled_name = format!("{}_{}", table_name, timestamp);

    let move_sql = format!("ALTER TABLE public.{} SET SCHEMA flashback_recycle", table_name);
    let rename_sql = format!("ALTER TABLE flashback_recycle.{} RENAME TO {}", table_name, recycled_name);

    if let Err(e) = Spi::run(&move_sql) {
    pgrx::warning!("Taşıma hatası: {}", e);
    return false;
    }

    if let Err(e) = Spi::run(&rename_sql) {
    pgrx::warning!("Rename hatası: {}", e);
    return false;
    }

    let insert_sql = format!(
    "INSERT INTO flashback.operations (operation_type, schema_name, table_name, recycled_name, role_name, retention_until)
     VALUES ('DROP', 'public', '{}', '{}', current_user, now() + interval '{} days')",
    table_name, recycled_name, guc::get_retention_days()
    );

    if let Err(e) = Spi::run(&insert_sql) {
        pgrx::warning!("Insertion hatası: {}", e);
        return false;
    }

    true
}