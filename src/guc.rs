use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};


static RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(7);
static MAX_TABLES: GucSetting<i32> = GucSetting::<i32>::new(100);
static MAX_SIZE: GucSetting<i32> = GucSetting::<i32>::new(102400);

/// _PG_init içinde çağrılır. GUC'ları PostgreSQL'e kayıt eder.
pub fn register_gucs() {
    GucRegistry::define_int_guc(
        c"flashback.retention_days",
        c"Recycle bin'deki tablolarin kac gun saklanacagi",
        c"DROP edilen tablolar bu sure sonunda otomatik olarak silinir.",
        &RETENTION_DAYS,
        1,
        365,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"flashback.max_tables",
        c"Recycle bin'de tutulabilecek maksimum tablo sayisi",
        c"Bu limite ulasildiginda en eski tablolar once silinir.",
        &MAX_TABLES,
        1,
        10000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"flashback.max_size",
        c"Recycle bin icin maksimum toplam boyut",
        c"Bu limite ulasildiginda en eski tablolar once silinir. Ornek: 100GB, 512MB",
        &MAX_SIZE,
        1,
        1048576,
        GucContext::Suset,
        GucFlags::UNIT_MB,
    );
}


pub fn get_retention_days() -> i32 {
    RETENTION_DAYS.get()
}

pub fn get_max_tables() -> i32 {
    MAX_TABLES.get()
}

pub fn get_max_size() -> i32 {
    MAX_SIZE.get()
}
