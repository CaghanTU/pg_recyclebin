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
-- Row-level change history table
CREATE TABLE IF NOT EXISTS flashback.row_history (
    id          BIGSERIAL PRIMARY KEY,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    operation   TEXT NOT NULL,
    old_data    JSONB,
    new_data    JSONB,
    txid        BIGINT
);
CREATE INDEX IF NOT EXISTS idx_flashback_row_history_lookup
    ON flashback.row_history (schema_name, table_name, changed_at);
-- Shared trigger function used by all tracked tables
CREATE OR REPLACE FUNCTION flashback.history_trigger_fn()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO flashback.row_history(schema_name, table_name, operation, old_data, new_data, txid)
    VALUES (
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        TG_OP,
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN row_to_json(OLD)::jsonb END,
        CASE WHEN TG_OP IN ('UPDATE', 'INSERT') THEN row_to_json(NEW)::jsonb END,
        txid_current()
    );
    RETURN NULL;
END;
$$;
GRANT INSERT ON flashback.row_history TO PUBLIC;
GRANT SELECT ON flashback.row_history TO PUBLIC;
GRANT USAGE ON SEQUENCE flashback.row_history_id_seq TO PUBLIC;
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
mod history;
mod tests;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    hooks::install();
    pgrx::log!("pg_recyclebin loaded");
    background_worker::register();
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    hooks::uninstall();
    pgrx::log!("pg_recyclebin unloaded");
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}


