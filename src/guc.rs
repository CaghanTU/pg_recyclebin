use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};


static RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(7);
static MAX_TABLES: GucSetting<i32> = GucSetting::<i32>::new(100);
static MAX_SIZE: GucSetting<i32> = GucSetting::<i32>::new(102400);
static WORKER_INTERVAL_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

/// Called in _PG_init. Registers GUCs with PostgreSQL.
pub fn register_gucs() {
    GucRegistry::define_int_guc(
        c"flashback.retention_days",
        c"How many days tables in the recycle bin will be retained",
        c"Tables that are DROPped will be automatically deleted after this period.",
        &RETENTION_DAYS,
        1,
        365,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"flashback.max_tables",
        c"Maximum number of tables that can be kept in the recycle bin",
        c"When this limit is reached, the oldest tables are deleted first.",
        &MAX_TABLES,
        1,
        10000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"flashback.max_size",
        c"Maximum total size for the recycle bin",
        c"When this limit is reached, the oldest tables are deleted first. Example: 100GB, 512MB",
        &MAX_SIZE,
        1,
        1048576,
        GucContext::Suset,
        GucFlags::UNIT_MB,
    );

    GucRegistry::define_int_guc(
        c"flashback.worker_interval_seconds",
        c"How often the cleanup worker will run, in seconds",
        c"Expired tables will be cleaned up at this interval.",
        &WORKER_INTERVAL_SECONDS,
        10,
        86400,
        GucContext::Suset,
        GucFlags::default(),
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

pub fn worker_interval_seconds() -> i32 {
    WORKER_INTERVAL_SECONDS.get()
}
