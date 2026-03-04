use pgrx::prelude::*;

#[pg_extern]

fn flashback_restore(table_name: &str) -> bool {
    pgrx::log!("Restoring table: {}", table_name);
    let recycled_name = match Spi::get_one::<String>(&format!(
    "SELECT recycled_name FROM flashback.operations 
     WHERE table_name = '{}' AND restored = false 
     ORDER BY timestamp DESC LIMIT 1",
    table_name
    )) {
    Ok(Some(name)) => name,
    Ok(None) => {
        pgrx::warning!("Tablo bulunamadı: {}", table_name);
        return false;
    }
    Err(e) => {
        pgrx::warning!("Sorgu hatası: {}", e);
        return false;
    }
    };

    let sql = format!("ALTER TABLE flashback_recycle.{} SET SCHEMA public", recycled_name);
    match Spi::run(&sql) {
    Ok(_) => {
        // Önce rename
        let rename_sql = format!("ALTER TABLE public.{} RENAME TO {}", recycled_name, table_name);
        if let Err(e) = Spi::run(&rename_sql) {
            pgrx::warning!("Rename hatası: {}", e);
            return false;
        }
        
        // Sonra metadata güncelle
        let update_sql = format!(
            "UPDATE flashback.operations SET restored = true WHERE table_name = '{}' AND recycled_name = '{}'",
            table_name, recycled_name
        );
        if let Err(e) = Spi::run(&update_sql) {
            pgrx::warning!("Güncelleme hatası: {}", e);
        }
        true
    }
    Err(e) => {
        pgrx::warning!("Geri yükleme hatası: {}", e);
        false
    }
}
    
}