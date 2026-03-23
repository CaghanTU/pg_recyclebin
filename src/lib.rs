use pgrx::prelude::*;

pgrx::pg_module_magic!();

pgrx::extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS flashback;
CREATE SCHEMA IF NOT EXISTS flashback_recycle;
CREATE TABLE IF NOT EXISTS flashback.operations (
    op_id            BIGSERIAL PRIMARY KEY,
    operation_type   TEXT NOT NULL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT now(),
    database_name    TEXT NOT NULL DEFAULT current_database(),
    schema_name      TEXT NOT NULL,
    table_name       TEXT NOT NULL,
    recycled_name    TEXT,
    role_name        TEXT NOT NULL,
    query_text       TEXT,
    application_name TEXT,
    client_addr      TEXT,
    retention_until  TIMESTAMPTZ,
    restored         BOOLEAN DEFAULT false,
    restored_at      TIMESTAMPTZ,
    metadata         JSONB
);
CREATE INDEX IF NOT EXISTS idx_flashback_ops_table ON flashback.operations (table_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_flashback_ops_retention ON flashback.operations (retention_until);
-- The ProcessUtility hook runs as the calling user; grant the minimum set of
-- privileges so that non-superusers can also DROP / TRUNCATE tracked tables
-- and call flashback_restore*() functions.
-- Row Level Security restricts each user to their own operations,
-- while superusers bypass RLS by default (no FORCE needed).
GRANT USAGE ON SCHEMA flashback TO PUBLIC;
GRANT INSERT, SELECT, UPDATE ON flashback.operations TO PUBLIC;
GRANT USAGE ON SEQUENCE flashback.operations_op_id_seq TO PUBLIC;
GRANT USAGE, CREATE ON SCHEMA flashback_recycle TO PUBLIC;
ALTER TABLE flashback.operations ENABLE ROW LEVEL SECURITY;
CREATE POLICY flashback_ops_own_rows ON flashback.operations
    USING (role_name = current_user);
"#,
    name = "flashback_schema_setup",
    bootstrap
);

mod hooks;
mod ddl_capture;
mod recovery;
mod background_worker;
pub mod guc;
pub(crate) mod context;
mod backup_restore;
mod tests;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    hooks::install();
    pgrx::log!("pg_flashback loaded");
    background_worker::register();
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    hooks::uninstall();
    pgrx::log!("pg_flashback unloaded");
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}


