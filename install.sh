#!/bin/bash
cargo pgrx install && \
cat >> /usr/local/pgsql-17/share/extension/pg_flashback--0.1.0.sql << 'EOF'

CREATE SCHEMA IF NOT EXISTS flashback;
CREATE SCHEMA IF NOT EXISTS flashback_recycle;

CREATE TABLE IF NOT EXISTS flashback.operations(
    op_id          BIGSERIAL PRIMARY KEY,
    operation_type TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_name    TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    recycled_name  TEXT,
    role_name      TEXT NOT NULL,
    retention_until TIMESTAMPTZ,
    restored       BOOLEAN DEFAULT false,
    restored_at    TIMESTAMPTZ,
    metadata       JSONB
);
EOF
