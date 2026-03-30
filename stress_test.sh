#!/bin/bash
# pg_recyclebin concurrent stress test
# Tests that advisory locks prevent double-restore under concurrent load.

PSQL="psql -U postgres -d postgres"
PASS=0
FAIL=0

log()  { echo "[$(date +%H:%M:%S)] $*"; }
ok()   { echo "  ✓ $*"; ((PASS++)); }
fail() { echo "  ✗ $*"; ((FAIL++)); }

# Cleanup on unexpected exit (Ctrl-C, crash, etc.) so reruns always start clean.
_emergency_cleanup() {
    echo ""
    log "Emergency cleanup triggered..."
    $PSQL -q -c "RESET flashback.max_tables;" 2>/dev/null || true
    $PSQL -q -c "RESET flashback.excluded_schemas;" 2>/dev/null || true
    for t in stress_orders stress_big stress_identity \
              stress_race stress_cycle \
              stress_lim_1 stress_lim_2 stress_lim_3 stress_lim_4 stress_lim_5 \
              stress_concurrent_1 stress_concurrent_2 stress_concurrent_3 \
              stress_concurrent_4 stress_concurrent_5; do
        $PSQL -q -c "DROP TABLE IF EXISTS $t CASCADE;" 2>/dev/null || true
    done
    $PSQL -q -c "SELECT flashback_purge_all();" > /dev/null 2>/dev/null || true
}
trap _emergency_cleanup EXIT INT TERM

# ── helpers ──────────────────────────────────────────────────────────────────
# Temporarily exclude 'public' so setup/cleanup DROPs don't trigger the hook.
# Also nuke ALL stress_orders* objects from flashback_recycle (including orphans
# left by previous test runs before the sequence-rename fix was applied).
_purge_stress_recycle() {
    $PSQL -q <<'SQL'
DO $$
DECLARE r RECORD;
BEGIN
  -- Drop any tables in flashback_recycle matching stress_orders* (CASCADE drops owned seqs too)
  FOR r IN SELECT relname FROM pg_class
           WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flashback_recycle')
             AND relkind = 'r'
             AND relname LIKE 'stress_orders%'
  LOOP
    EXECUTE 'DROP TABLE IF EXISTS flashback_recycle.' || quote_ident(r.relname) || ' CASCADE';
  END LOOP;
  -- Drop any stray sequences that survived without a table
  FOR r IN SELECT relname FROM pg_class
           WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flashback_recycle')
             AND relkind = 'S'
             AND relname LIKE 'stress_orders%'
  LOOP
    EXECUTE 'DROP SEQUENCE IF EXISTS flashback_recycle.' || quote_ident(r.relname) || ' CASCADE';
  END LOOP;
  DELETE FROM flashback.operations WHERE table_name = 'stress_orders';
END$$;
SQL
}

setup() {
    _purge_stress_recycle
    $PSQL -q <<'SQL'
SET flashback.excluded_schemas = 'public';
DROP TABLE IF EXISTS stress_orders CASCADE;
RESET flashback.excluded_schemas;
CREATE TABLE stress_orders (id SERIAL, amount numeric);
INSERT INTO stress_orders (amount) SELECT random()*1000 FROM generate_series(1,1000);
SQL
}

cleanup() {
    $PSQL -q <<'SQL'
SET flashback.excluded_schemas = 'public';
DROP TABLE IF EXISTS stress_orders CASCADE;
RESET flashback.excluded_schemas;
SQL
    _purge_stress_recycle
}

# ── Test 1: Concurrent DROP restore (10 sessions, same op_id) ────────────────
log "Test 1: 10 concurrent sessions restoring the same DROP op_id"
setup
$PSQL -q -c "DROP TABLE stress_orders"

OP_ID=$($PSQL -tAq -c "SELECT op_id FROM flashback.operations WHERE table_name='stress_orders' AND restored=false LIMIT 1")
log "  op_id = $OP_ID"

pids=()
results=()
for i in $(seq 1 10); do
    result=$($PSQL -tAq -c "SELECT flashback_restore_by_id($OP_ID, NULL)" 2>/dev/null) &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid"
    results+=($?)
done

# Count how many rows stress_orders has now
ROW_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM stress_orders" 2>/dev/null)
RESTORED_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM flashback.operations WHERE op_id=$OP_ID AND restored=true")

if [[ "$RESTORED_COUNT" -eq 1 ]]; then
    ok "Exactly 1 restore succeeded (restored=true count: $RESTORED_COUNT)"
else
    fail "Expected 1 restore, got restored=true count: $RESTORED_COUNT"
fi

if [[ "$ROW_COUNT" -eq 1000 ]]; then
    ok "Row count correct: $ROW_COUNT"
else
    fail "Row count wrong: expected 1000, got $ROW_COUNT"
fi

cleanup

# ── Test 2: Concurrent TRUNCATE restore (10 sessions) ────────────────────────
log "Test 2: 10 concurrent sessions restoring the same TRUNCATE op_id"
setup
$PSQL -q -c "TRUNCATE stress_orders"

OP_ID=$($PSQL -tAq -c "SELECT op_id FROM flashback.operations WHERE table_name='stress_orders' AND restored=false LIMIT 1")
log "  op_id = $OP_ID"

pids=()
for i in $(seq 1 10); do
    $PSQL -tAq -c "SELECT flashback_restore_by_id($OP_ID, NULL)" 2>/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

ROW_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM stress_orders" 2>/dev/null)
RESTORED_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM flashback.operations WHERE op_id=$OP_ID AND restored=true")

if [[ "$RESTORED_COUNT" -eq 1 ]]; then
    ok "Exactly 1 restore succeeded (restored=true count: $RESTORED_COUNT)"
else
    fail "Expected 1 restore, got restored=true count: $RESTORED_COUNT"
fi

if [[ "$ROW_COUNT" -eq 1000 ]]; then
    ok "Row count correct: $ROW_COUNT"
else
    fail "Row count wrong: expected 1000, got $ROW_COUNT"
fi

cleanup

# ── Test 3: Rapid DROP → restore → DROP → restore (serial integrity) ─────────
log "Test 3: Rapid DROP → restore → DROP → restore cycle (10 rounds)"
$PSQL -q -c "CREATE TABLE stress_orders (id SERIAL, amount numeric)"
$PSQL -q -c "INSERT INTO stress_orders (amount) SELECT random()*1000 FROM generate_series(1,100)"

for i in $(seq 1 10); do
    $PSQL -q -c "DROP TABLE stress_orders" 2>/dev/null
    RESULT=$($PSQL -tAq -c "SELECT flashback_restore('stress_orders', NULL)" 2>/dev/null)
    if [[ "$RESULT" != "t" ]]; then
        fail "Round $i: restore returned '$RESULT'"
    fi
done

ROW_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM stress_orders" 2>/dev/null)
if [[ "$ROW_COUNT" -eq 100 ]]; then
    ok "10 rounds of DROP→restore completed, row count: $ROW_COUNT"
else
    fail "Row count after 10 rounds: expected 100, got $ROW_COUNT"
fi

cleanup

# ── Test 4: purge_all while concurrent drops are happening ───────────────────
log "Test 4: flashback_purge_all while 5 tables are being dropped concurrently"
pids=()
for i in $(seq 1 5); do
    ($PSQL -q -c "CREATE TABLE stress_concurrent_$i (id int)" 2>/dev/null
     $PSQL -q -c "DROP TABLE stress_concurrent_$i" 2>/dev/null) &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

PURGED=$($PSQL -tAq -c "SELECT flashback_purge_all()" 2>/dev/null)
log "  purge_all returned: $PURGED"

REMAINING=$($PSQL -tAq -c "SELECT COUNT(*) FROM flashback.operations WHERE restored=false")
if [[ "$REMAINING" -eq 0 ]]; then
    ok "Recycle bin empty after purge_all (purged: $PURGED)"
else
    fail "Recycle bin not empty after purge_all: $REMAINING remaining"
fi

# ── Test 5: 100k row TRUNCATE restore — performance + row integrity ───────────
log "Test 5: 100k row TRUNCATE restore (scale + timing + sequence)"
$PSQL -q -c "DROP TABLE IF EXISTS stress_big CASCADE;"
$PSQL -q -c "CREATE TABLE stress_big (id SERIAL PRIMARY KEY, amount numeric, note text);"
$PSQL -q -c "INSERT INTO stress_big (amount, note) SELECT i, repeat('x',100) FROM generate_series(1,100000) i;"

SIZE_BEFORE=$($PSQL -tAq -c "SELECT pg_size_pretty(pg_total_relation_size('stress_big'));")
log "  Table size before TRUNCATE: $SIZE_BEFORE"

T0=$(date +%s%N)
$PSQL -q -c "TRUNCATE TABLE stress_big;"
TRUNC_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

T0=$(date +%s%N)
$PSQL -q -c "SELECT flashback_restore('stress_big', NULL);" > /dev/null
RESTORE_MS=$(( ($(date +%s%N) - T0) / 1000000 ))

COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM stress_big;")
if [[ "$COUNT" -eq 100000 ]]; then
    ok "100 000 rows restored correctly  [TRUNCATE: ${TRUNC_MS}ms  RESTORE: ${RESTORE_MS}ms]"
else
    fail "Expected 100 000 rows, got $COUNT"
fi

# Sequence must continue from 100001
$PSQL -q -c "INSERT INTO stress_big (amount, note) VALUES (0, 'new');"
MAX_ID=$($PSQL -tAq -c "SELECT MAX(id) FROM stress_big;")
if [[ "$MAX_ID" -eq 100001 ]]; then
    ok "SERIAL sequence continues from 100001 after TRUNCATE restore"
else
    fail "Expected MAX(id)=100001, got $MAX_ID"
fi

SIZE_AFTER=$($PSQL -tAq -c "SELECT pg_size_pretty(pg_total_relation_size('stress_big'));")
log "  Table size after restore: $SIZE_AFTER"

$PSQL -q -c "DROP TABLE IF EXISTS stress_big CASCADE;"
$PSQL -q -c "SELECT flashback_purge_all();" > /dev/null 2>/dev/null || true

# ── Test 6: GENERATED ALWAYS AS IDENTITY — TRUNCATE restore ──────────────────
log "Test 6: GENERATED ALWAYS AS IDENTITY column — TRUNCATE restore"
$PSQL -q -c "DROP TABLE IF EXISTS stress_identity CASCADE;"
$PSQL -q -c "CREATE TABLE stress_identity (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val text);"
$PSQL -q -c "INSERT INTO stress_identity (val) SELECT 'row'||i FROM generate_series(1,10000) i;"

$PSQL -q -c "TRUNCATE TABLE stress_identity;"
$PSQL -q -c "SELECT flashback_restore('stress_identity', NULL);" > /dev/null

COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM stress_identity;")
if [[ "$COUNT" -eq 10000 ]]; then
    ok "10 000 rows restored for GENERATED ALWAYS AS IDENTITY table"
else
    fail "Expected 10 000 rows, got $COUNT"
fi

$PSQL -q -c "INSERT INTO stress_identity (val) VALUES ('new');"
MAX_ID=$($PSQL -tAq -c "SELECT MAX(id) FROM stress_identity;")
if [[ "$MAX_ID" -eq 10001 ]]; then
    ok "IDENTITY sequence continues from 10001 after TRUNCATE restore"
else
    fail "Expected MAX(id)=10001, got $MAX_ID"
fi

$PSQL -q -c "DROP TABLE IF EXISTS stress_identity CASCADE;"
$PSQL -q -c "SELECT flashback_purge_all();" > /dev/null 2>/dev/null || true

# ── Test 7: flashback.max_tables GUC — FIFO eviction under load ──────────────
log "Test 7: flashback.max_tables=3, drop 5 tables — verify FIFO eviction"
$PSQL -q -c "SELECT flashback_purge_all();" > /dev/null 2>/dev/null || true
# ALTER SYSTEM SET so every subsequent psql connection (new backend) picks it up
$PSQL -q -c "ALTER SYSTEM SET flashback.max_tables = 3;"
$PSQL -q -c "SELECT pg_reload_conf();"
sleep 0.2  # give reload a moment
for i in 1 2 3 4 5; do
    $PSQL -q -c "DROP TABLE IF EXISTS stress_lim_$i CASCADE;"
    $PSQL -q -c "CREATE TABLE stress_lim_$i (id int);"
    $PSQL -q -c "DROP TABLE stress_lim_$i;" 2>/dev/null || true
done
BIN_COUNT=$($PSQL -tAq -c "SELECT COUNT(*) FROM flashback.operations WHERE table_name LIKE 'stress_lim_%' AND restored=false;")
if [[ "$BIN_COUNT" -le 3 ]]; then
    ok "Table limit enforced: $BIN_COUNT entries in bin (max_tables=3)"
else
    fail "Expected ≤3 entries with max_tables=3, got $BIN_COUNT"
fi
$PSQL -q -c "ALTER SYSTEM RESET flashback.max_tables;"
$PSQL -q -c "SELECT pg_reload_conf();"
$PSQL -q -c "SELECT flashback_purge_all();" > /dev/null 2>/dev/null || true

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════"
echo "  Stress test: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
