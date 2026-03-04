

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