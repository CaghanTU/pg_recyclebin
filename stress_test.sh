#!/bin/bash
# pg_flashback concurrent stress test
# Tests that advisory locks prevent double-restore under concurrent load.

PSQL="psql -U postgres -d postgres"
PASS=0
FAIL=0

log()  { echo "[$(date +%H:%M:%S)] $*"; }
ok()   { echo "  ✓ $*"; ((PASS++)); }
fail() { echo "  ✗ $*"; ((FAIL++)); }

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

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════"
echo "  Stress test: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
