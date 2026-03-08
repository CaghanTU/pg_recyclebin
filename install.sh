#!/bin/bash
set -e

SQL_FILE=/usr/local/pgsql-17/share/extension/pg_flashback--0.1.0.sql

cargo pgrx install

# Fix: add DEFAULT NULL for target_schema (pgrx doesn't generate it for Option<T>)
sed -i 's/"target_schema" TEXT \/\* core::option::Option<[^*]*> \*\//"target_schema" TEXT DEFAULT NULL/' "$SQL_FILE"

# Fix: remove duplicate single-arg overloads for functions that also have a 2-arg DEFAULT NULL form.
# pgrx generates both forms for Option<T> parameters, causing PostgreSQL "not unique" errors
# when the function is called with one argument (both forms match).
# We keep only the 2-arg DEFAULT NULL form; PostgreSQL resolves single-arg calls via the default.
python3 - << 'PYEOF' "$SQL_FILE"
import sys
path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()

# Symbols that are the DUPLICATE (no-schema) wrappers to remove
no_schema_syms = {
    "'flashback_restore_no_schema_wrapper'",
    "'flashback_restore_by_id_no_schema_wrapper'",
}

begin_marker = "/* <begin connected objects> */"
end_marker   = "/* </end connected objects> */"

result = []
i = 0
while i < len(lines):
    if begin_marker in lines[i]:
        # Accumulate the whole block
        block = [lines[i]]
        j = i + 1
        while j < len(lines) and end_marker not in lines[j]:
            block.append(lines[j])
            j += 1
        if j < len(lines):
            block.append(lines[j])  # include end marker line
        block_text = "".join(block)
        # Drop the block if it contains one of the duplicate no-schema symbols
        if any(sym in block_text for sym in no_schema_syms):
            i = j + 1
            continue
        result.extend(block)
        i = j + 1
    else:
        result.append(lines[i])
        i += 1

with open(path, "w") as f:
    f.writelines(result)
PYEOF

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

# Restart PostgreSQL so it loads the new .so instead of the cached old one.
# Without restart, CREATE EXTENSION fails with "could not find function" because
# PostgreSQL holds the old shared library in memory via dlopen cache.
echo "Restarting PostgreSQL to load new shared library..."
if command -v pg_ctl &>/dev/null; then
    PGDATA="${PGDATA:-/usr/local/pgsql-17/data}"
    pg_ctl -D "$PGDATA" restart -w -s && echo "PostgreSQL restarted." || echo "pg_ctl restart failed — restart PostgreSQL manually before running CREATE EXTENSION."
elif command -v systemctl &>/dev/null && systemctl is-active --quiet postgresql 2>/dev/null; then
    systemctl restart postgresql && echo "PostgreSQL restarted." || echo "systemctl restart failed — restart PostgreSQL manually."
else
    echo "WARNING: Could not restart PostgreSQL automatically."
    echo "         Please restart it manually before running CREATE EXTENSION pg_flashback."
fi
