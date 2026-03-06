#!/bin/bash
set -e

SQL_FILE=/usr/local/pgsql-17/share/extension/pg_flashback--0.1.0.sql

cargo pgrx install

# Fix: add DEFAULT NULL for target_schema (pgrx doesn't generate it for Option<T>)
sed -i 's/"target_schema" TEXT \/\* core::option::Option<[^*]*> \*\//"target_schema" TEXT DEFAULT NULL/' "$SQL_FILE"

# Fix: add SECURITY DEFINER to all pg_flashback functions so non-superusers can call them
# (they need to access flashback schema which regular users don't have privileges on)
sed -i '/flashback_restore_wrapper\|flashback_purge_wrapper\|flashback_list_recycled_tables_wrapper\|flashback_status_wrapper/{
    /SECURITY DEFINER/! {
        s/AS '\''MODULE_PATHNAME'\''/SECURITY DEFINER\nAS '\''MODULE_PATHNAME'\''/
    }
}' "$SQL_FILE"

cat >> "$SQL_FILE" << 'EOF'

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
