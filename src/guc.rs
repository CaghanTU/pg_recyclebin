use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;


static RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(7);
static MAX_TABLES: GucSetting<i32> = GucSetting::<i32>::new(100);
static MAX_SIZE: GucSetting<i32> = GucSetting::<i32>::new(102400);
static WORKER_INTERVAL_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);
static EXCLUDED_SCHEMAS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static DATABASE_NAME: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

// pgBackRest integration GUCs
static PGBACKREST_REPO_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static PGBACKREST_STANZA: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static PGBACKREST_TEMP_DIR: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static PGBACKREST_BIN_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static PGBACKREST_PG_BIN_DIR: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
// Cipher pass: PGC_SUSET + GUC_SUPERUSER_ONLY + GUC_NOT_IN_SAMPLE to prevent key leakage
static PGBACKREST_CIPHER_PASS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
// Repo index: 1 = default. Set to 2, 3, … for multi-repo configs.
static PGBACKREST_REPO: GucSetting<i32> = GucSetting::<i32>::new(1);

// Row history GUCs
static HISTORY_RETENTION_HOURS: GucSetting<i32> = GucSetting::<i32>::new(24);
static MAX_HISTORY_ROWS: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);

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

    GucRegistry::define_string_guc(
        c"flashback.excluded_schemas",
        c"Comma-separated list of schemas to exclude from the recycle bin",
        c"Tables in these schemas will not be captured when dropped.",
        &EXCLUDED_SCHEMAS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.database_name",
        c"Database that the flashback cleanup background worker will connect to",
        c"Set this to the name of the database where pg_recyclebin is installed. Defaults to 'postgres'.",
        &DATABASE_NAME,
        GucContext::Suset,
        GucFlags::default(),
    );

    // pgBackRest integration GUCs
    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_repo_path",
        c"Path to the pgBackRest repository root",
        c"Used for single-table restore from backup. Example: /var/lib/pgbackrest",
        &PGBACKREST_REPO_PATH,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_stanza",
        c"pgBackRest stanza name",
        c"The stanza name configured in pgBackRest (e.g. main, db1).",
        &PGBACKREST_STANZA,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_temp_dir",
        c"Temporary directory for pg_recyclebin backup restore operations",
        c"A temporary PostgreSQL instance will be created here during single-table restore.",
        &PGBACKREST_TEMP_DIR,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_bin_path",
        c"Full path to the pgbackrest binary",
        c"Used in restore_command for the temporary instance. Default: /usr/bin/pgbackrest",
        &PGBACKREST_BIN_PATH,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_pg_bin_dir",
        c"Directory containing pg_ctl, pg_dump and related PostgreSQL binaries",
        c"Used to start the temporary PostgreSQL instance. Example: /usr/lib/postgresql/16/bin",
        &PGBACKREST_PG_BIN_DIR,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"flashback.pgbackrest_cipher_pass",
        c"pgBackRest repository cipher passphrase",
        c"Repo-level encryption passphrase. Only superusers can set or read this.",
        &PGBACKREST_CIPHER_PASS,
        GucContext::Suset,
        // GUC_SUPERUSER_ONLY prevents non-superusers from reading via pg_settings.
        // NO_SHOW_ALL includes GUC_NOT_IN_SAMPLE — hides from SHOW ALL and postgresql.conf.sample.
        GucFlags::SUPERUSER_ONLY | GucFlags::NO_SHOW_ALL,
    );

    GucRegistry::define_int_guc(
        c"flashback.pgbackrest_repo",
        c"pgBackRest repository index to use (1-based)",
        c"When multiple repos are configured, set this to the target repo number. Default: 1.",
        &PGBACKREST_REPO,
        1,
        99,
        GucContext::Suset,
        GucFlags::default(),
    );

    // Row history GUCs
    GucRegistry::define_int_guc(
        c"flashback.history_retention_hours",
        c"How many hours to keep row-level change history",
        c"Rows in flashback.row_history older than this will be purged by the background worker.",
        &HISTORY_RETENTION_HOURS,
        1,
        8760,  // 1 year max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"flashback.max_history_rows",
        c"Maximum number of rows to keep in flashback.row_history",
        c"When this limit is reached, the oldest rows are deleted first as a safety cap.",
        &MAX_HISTORY_ROWS,
        1000,
        100_000_000,
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

pub fn get_excluded_schemas() -> Vec<String> {
    EXCLUDED_SCHEMAS
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn get_database_name() -> String {
    DATABASE_NAME
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "postgres".to_string())
}

fn cstring_guc_get(guc: &GucSetting<Option<CString>>) -> Option<String> {
    guc.get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

pub fn get_pgbackrest_repo_path() -> Option<String> {
    cstring_guc_get(&PGBACKREST_REPO_PATH)
}

pub fn get_pgbackrest_stanza() -> Option<String> {
    cstring_guc_get(&PGBACKREST_STANZA)
}

pub fn get_pgbackrest_temp_dir() -> String {
    cstring_guc_get(&PGBACKREST_TEMP_DIR)
        .unwrap_or_else(|| "/tmp/pg_recyclebin_restore".to_string())
}

pub fn get_pgbackrest_bin_path() -> String {
    cstring_guc_get(&PGBACKREST_BIN_PATH)
        .unwrap_or_else(|| "/usr/bin/pgbackrest".to_string())
}

pub fn get_pgbackrest_pg_bin_dir() -> Option<String> {
    cstring_guc_get(&PGBACKREST_PG_BIN_DIR)
}

pub fn get_pgbackrest_cipher_pass() -> Option<String> {
    cstring_guc_get(&PGBACKREST_CIPHER_PASS)
}

pub fn get_pgbackrest_repo() -> i32 {
    PGBACKREST_REPO.get()
}

pub fn get_history_retention_hours() -> i32 {
    HISTORY_RETENTION_HOURS.get()
}

pub fn get_max_history_rows() -> i32 {
    MAX_HISTORY_ROWS.get()
}