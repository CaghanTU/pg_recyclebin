

CREATE SCHEMA IF NOT EXISTS flashback;

CREATE SCHEMA IF NOT EXISTS flashback_recycle;

CREATE TABLE IF NOT EXISTS flashback.operations(
    op_id          BIGSERIAL PRIMARY KEY,
    operation_type TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT now(),
    database_name  TEXT NOT NULL,
    schema_name    TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    recycled_name  TEXT,
    role_name      TEXT NOT NULL,
    query_text     TEXT,
    application_name TEXT,
    client_addr    TEXT,
    retention_until TIMESTAMPTZ,
    restored       BOOLEAN DEFAULT false,
    restored_at    TIMESTAMPTZ,
    metadata       JSONB
);

CREATE INDEX IF NOT EXISTS idx_flashback_operations_timestamp ON flashback.operations (table_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_flashback_operations_database ON flashback.operations (retention_until);

-- Row-level change history (populated by per-table triggers installed via flashback_track_table)
CREATE TABLE IF NOT EXISTS flashback.row_history (
    id          BIGSERIAL PRIMARY KEY,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    operation   TEXT NOT NULL,  -- INSERT, UPDATE, DELETE
    old_data    JSONB,
    new_data    JSONB,
    txid        BIGINT
);
CREATE INDEX IF NOT EXISTS idx_flashback_row_history_lookup
    ON flashback.row_history (schema_name, table_name, changed_at);

-- Trigger function shared by all tracked tables
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