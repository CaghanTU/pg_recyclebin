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
-- Explicit permission hardening: block PUBLIC access to internal schemas/tables.
-- PostgreSQL defaults don't grant PUBLIC access to non-public schemas, but we
-- make this explicit so the intent is clear and survives pg_dump/restore cycles.
REVOKE ALL ON SCHEMA flashback FROM PUBLIC;
REVOKE ALL ON SCHEMA flashback_recycle FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA flashback FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback_recycle REVOKE ALL ON TABLES FROM PUBLIC;
"#,
    name = "flashback_schema_setup",
    bootstrap
);

mod hooks;
mod ddl_capture;
mod recovery;
mod background_worker;
pub mod guc;
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


