#!/usr/bin/env bash
# =============================================================================
# pg_recyclebin — pgBackRest backup restore test matrix
# =============================================================================
# Senaryolar: büyük tablo (~PGFB_TARGET_GB), full+diff zinciri, çoklu tablo,
#             FK / partition / inheritance, isteğe bağlı şifreli repo.
#
# Kullanım:
#   cp scripts/backup_restore_matrix.env.example scripts/backup_restore_matrix.env
#   # düzenle
#   source scripts/backup_restore_matrix.env
#   ./scripts/backup_restore_test_matrix.sh all
#
# Komutlar: schema | seed | load-only | backup-full | mutate | backup-diff |
#           backup-chain | drop-public | strip-recycle | restore-all | verify | all
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCHEMA_SQL="$SCRIPT_DIR/setup_backup_matrix_schema.sql"
ENV_FILE="${PGFB_ENV_FILE:-$SCRIPT_DIR/backup_restore_matrix.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$ENV_FILE"
fi

: "${PGHOST:=/tmp}"
: "${PGPORT:=5432}"
: "${PGUSER:=postgres}"
: "${PGDATABASE:=postgres}"
: "${PGFB_STANZA:=testdb}"
: "${PGFB_REPO_PATH:=/var/lib/pgbackrest}"
: "${PGFB_PGBACKREST_BIN:=pgbackrest}"
: "${PGFB_PG_BIN:=}"
: "${PGFB_TEMP_DIR:=/tmp/pgfb_matrix_restore}"
: "${PGFB_TARGET_GB:=0.05}"
: "${PGFB_ENCRYPTED_REPO:=0}"
: "${PGFB_CIPHER_PASS:=}"
: "${PGFB_SKIP_WAL_REPLAY:=true}"

PSQL=(psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE")

log() { echo "[pgfb-matrix] $(date +%H:%M:%S) $*"; }
die() { echo "[pgfb-matrix] ERROR: $*" >&2; exit 1; }

# Ortalama ~800 byte/satır (payload); toplam hedef boyut PGFB_TARGET_GB
bytes_per_row=800
target_bytes=$(awk "BEGIN { printf \"%d\", ($PGFB_TARGET_GB * 1000000000) }")
total_rows=$((target_bytes / bytes_per_row))
[[ $total_rows -lt 1000 ]] && total_rows=1000
batch_rows=500000
num_batches=$(( (total_rows + batch_rows - 1) / batch_rows ))

pgbr() {
  "$PGFB_PGBACKREST_BIN" --stanza="$PGFB_STANZA" "$@"
}

run_sql() { "${PSQL[@]}" -q "$@"; }

cmd_schema() {
  log "Applying matrix schema ($SCHEMA_SQL)"
  "${PSQL[@]}" -f "$SCHEMA_SQL"
}

cmd_seed() {
  log "Seeding FK / partition / inheritance (small)"
  "${PSQL[@]}" -q <<'SQL'
TRUNCATE fb_matrix_order_lines, fb_matrix_orders, fb_matrix_part, fb_matrix_child, fb_matrix_parent RESTART IDENTITY CASCADE;

INSERT INTO fb_matrix_orders (customer_id) SELECT g FROM generate_series(1, 500) g;
INSERT INTO fb_matrix_order_lines (order_id, sku, qty)
SELECT id, 'SKU-' || id, (id % 5) + 1 FROM fb_matrix_orders;

INSERT INTO fb_matrix_part (pkey, data) SELECT i, repeat('p', 200) FROM generate_series(1, 15000) i;

INSERT INTO fb_matrix_parent (id, name) VALUES (1, 'root-a'), (2, 'root-b');
INSERT INTO fb_matrix_child (id, name, extra) VALUES (3, 'child-x', 1.5), (4, 'child-y', 2.5);
SQL
}

cmd_load_only() {
  cmd_schema
  cmd_seed
  log "Loading fb_matrix_big: target ~${PGFB_TARGET_GB}GB, ~${total_rows} rows, ${num_batches} batches × ${batch_rows}"
  local b=0
  for ((b = 1; b <= num_batches; b++)); do
    log "  batch $b / $num_batches"
    "${PSQL[@]}" -q -c "
      INSERT INTO fb_matrix_big (payload)
      SELECT repeat(md5((g + $((b * batch_rows)))::text), 25)
      FROM generate_series(1, $batch_rows) g;
    "
  done
  run_sql -c "SELECT pg_size_pretty(pg_total_relation_size('fb_matrix_big')) AS fb_matrix_big_size, COUNT(*) AS rows FROM fb_matrix_big;"
}

cmd_backup_full() {
  log "pgBackRest FULL backup"
  pgbr backup --type=full --log-level-console=info
}

cmd_mutate() {
  log "Post-full mutations (diff backup anlamlı olsun)"
  "${PSQL[@]}" -q <<'SQL'
INSERT INTO fb_matrix_orders (customer_id) VALUES (999999);
INSERT INTO fb_matrix_order_lines (order_id, sku, qty)
SELECT id, 'POST-FULL', 1 FROM fb_matrix_orders WHERE customer_id = 999999;
UPDATE fb_matrix_big SET payload = payload || 'X' WHERE id IN (SELECT MIN(id) FROM fb_matrix_big);
SQL
}

cmd_backup_diff() {
  log "pgBackRest DIFF backup"
  pgbr backup --type=diff --log-level-console=info
}

cmd_backup_chain() {
  cmd_backup_full
  cmd_mutate
  cmd_backup_diff
}

# Public tabloları DROP et → recycle + operations metadata
cmd_drop_public() {
  log "DROP public matrix tables (capture to flashback_recycle)"
  "${PSQL[@]}" -q <<'SQL'
SET client_min_messages = WARNING;
DROP TABLE IF EXISTS fb_matrix_order_lines CASCADE;
DROP TABLE IF EXISTS fb_matrix_orders CASCADE;
DROP TABLE IF EXISTS fb_matrix_big CASCADE;
DROP TABLE IF EXISTS fb_matrix_part CASCADE;
DROP TABLE IF EXISTS fb_matrix_child CASCADE;
DROP TABLE IF EXISTS fb_matrix_parent CASCADE;
SQL
}

# Recycle içindeki kopyayı sil; operations satırları kalsın (backup restore için şart)
cmd_strip_recycle() {
  log "Strip recycle heap copies, KEEP flashback.operations (saves disk for large tests)"
  "${PSQL[@]}" -q <<'SQL'
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT recycled_name FROM flashback.operations
    WHERE restored = false
      AND table_name = ANY (ARRAY[
        'fb_matrix_big',
        'fb_matrix_orders',
        'fb_matrix_order_lines',
        'fb_matrix_part',
        'fb_matrix_parent',
        'fb_matrix_child'
      ])
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS flashback_recycle.%I CASCADE', r.recycled_name);
  END LOOP;
END$$;
SQL
}

guc_prefix() {
  local skip="$PGFB_SKIP_WAL_REPLAY"
  echo "SET flashback.pgbackrest_repo_path = '$PGFB_REPO_PATH';"
  echo "SET flashback.pgbackrest_stanza = '$PGFB_STANZA';"
  echo "SET flashback.pgbackrest_temp_dir = '$PGFB_TEMP_DIR';"
  if [[ -n "$PGFB_PG_BIN" ]]; then
    echo "SET flashback.pgbackrest_pg_bin_dir = '$PGFB_PG_BIN';"
  fi
  echo "SET flashback.pgbackrest_bin_path = '$PGFB_PGBACKREST_BIN';"
  if [[ "$PGFB_ENCRYPTED_REPO" == "1" || "$PGFB_ENCRYPTED_REPO" == "true" ]]; then
    [[ -n "$PGFB_CIPHER_PASS" ]] || die "PGFB_CIPHER_PASS empty but PGFB_ENCRYPTED_REPO set"
    echo "SET flashback.pgbackrest_cipher_pass = '$PGFB_CIPHER_PASS';"
  fi
}

cmd_restore_all() {
  local skip_sql=true
  [[ "$PGFB_SKIP_WAL_REPLAY" == "false" || "$PGFB_SKIP_WAL_REPLAY" == "0" ]] && skip_sql=false

  log "Restore from backup (skip_wal_replay=$skip_sql). Order: big → orders → lines → part → parent → child"
  rm -rf "$PGFB_TEMP_DIR"
  mkdir -p "$PGFB_TEMP_DIR"

  "${PSQL[@]}" -q <<EOF
$(guc_prefix)
SELECT flashback_restore_from_backup('fb_matrix_big', NULL, $skip_sql);
SELECT flashback_restore_from_backup('fb_matrix_orders', NULL, $skip_sql);
SELECT flashback_restore_from_backup('fb_matrix_order_lines', NULL, $skip_sql);
SELECT flashback_restore_from_backup('fb_matrix_part', NULL, $skip_sql);
SELECT flashback_restore_from_backup('fb_matrix_parent', NULL, $skip_sql);
SELECT flashback_restore_from_backup('fb_matrix_child', NULL, $skip_sql);
EOF
}

cmd_verify() {
  log "Verify row counts / sanity"
  "${PSQL[@]}" -c "
SELECT 'fb_matrix_big' AS t, COUNT(*) AS n, pg_size_pretty(pg_total_relation_size('fb_matrix_big')) AS sz FROM fb_matrix_big
UNION ALL
SELECT 'fb_matrix_orders', COUNT(*)::bigint, NULL::text FROM fb_matrix_orders
UNION ALL
SELECT 'fb_matrix_order_lines', COUNT(*)::bigint, NULL::text FROM fb_matrix_order_lines
UNION ALL
SELECT 'fb_matrix_part', COUNT(*)::bigint, NULL::text FROM fb_matrix_part
UNION ALL
SELECT 'fb_matrix_parent_ONLY', COUNT(*)::bigint, NULL::text FROM ONLY fb_matrix_parent
UNION ALL
SELECT 'fb_matrix_child_ONLY', COUNT(*)::bigint, NULL::text FROM ONLY fb_matrix_child;
"
  "${PSQL[@]}" -c "SELECT sku, COUNT(*) FROM fb_matrix_order_lines WHERE sku = 'POST-FULL' GROUP BY sku;"
}

cmd_all() {
  cmd_schema
  cmd_seed
  if awk "BEGIN { exit !($PGFB_TARGET_GB > 0) }"; then
    local b=0
    log "Loading fb_matrix_big: ~${PGFB_TARGET_GB}GB"
    for ((b = 1; b <= num_batches; b++)); do
      log "  batch $b / $num_batches"
      "${PSQL[@]}" -q -c "
        INSERT INTO fb_matrix_big (payload)
        SELECT repeat(md5((g + $((b * batch_rows)))::text), 25)
        FROM generate_series(1, $batch_rows) g;
      "
    done
  fi
  cmd_backup_chain
  cmd_drop_public
  cmd_strip_recycle
  cmd_restore_all
  cmd_verify
  log "DONE matrix all"
}

cmd_help() {
  cat <<EOF
Usage: $0 <command>

  schema         — DDL only
  seed           — small data for FK/partition/inheritance
  load-only      — schema + seed + load fb_matrix_big to PGFB_TARGET_GB
  backup-full    — pgBackRest full
  mutate         — small changes after full
  backup-diff    — pgBackRest diff
  backup-chain   — full + mutate + diff
  drop-public    — DROP tables (recycle capture)
  strip-recycle  — drop recycle copies, keep operations (large test friendly)
  restore-all    — flashback_restore_from_backup × 6
  verify         — counts + POST-FULL marker
  all            — full matrix (schema, load, chain, drop, strip, restore, verify)

Env: see scripts/backup_restore_matrix.env.example
EOF
}

main() {
  local c="${1:-help}"
  case "$c" in
    schema)       cmd_schema ;;
    seed)         cmd_seed ;;
    load-only)    cmd_load_only ;;
    backup-full)  cmd_backup_full ;;
    mutate)       cmd_mutate ;;
    backup-diff)  cmd_backup_diff ;;
    backup-chain) cmd_backup_chain ;;
    drop-public)  cmd_drop_public ;;
    strip-recycle) cmd_strip_recycle ;;
    restore-all)  cmd_restore_all ;;
    verify)       cmd_verify ;;
    all)          cmd_all ;;
    help|-h|--help) cmd_help ;;
    *) die "unknown command: $c (try: help)" ;;
  esac
}

main "$@"
