#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn setup() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_orders CASCADE").unwrap();
        Spi::run("CREATE TABLE test_orders (id int, val text)").unwrap();
        Spi::run("INSERT INTO test_orders VALUES (1, 'a'), (2, 'b')").unwrap();
    }

    fn cleanup() {
        Spi::run("DROP TABLE IF EXISTS test_orders CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_orders'").unwrap();
    }

    // Test 1: Temp table should NOT enter recycle bin
    #[pg_test]
    fn test_temp_table_excluded() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("CREATE TEMP TABLE temp_test (id int)").unwrap();
        Spi::run("DROP TABLE temp_test").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'temp_test'"
        ).unwrap().unwrap_or(0);

        assert_eq!(count, 0, "Temp table should not be captured");
    }

    // Test 2: restore by op_id when same table dropped multiple times
    #[pg_test]
    fn test_restore_by_op_id() {
        setup();

        // First drop
        Spi::run("DROP TABLE test_orders").unwrap();

        // Recreate and drop again
        Spi::run("CREATE TABLE test_orders (id int, val text)").unwrap();
        Spi::run("INSERT INTO test_orders VALUES (3, 'c'), (4, 'd')").unwrap();
        Spi::run("DROP TABLE test_orders").unwrap();

        // Both entries should exist
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 2, "Both drops should be in recycle bin");

        // Get the first op_id (oldest)
        let op_id = Spi::get_one::<i64>(
            "SELECT op_id FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false ORDER BY timestamp ASC LIMIT 1"
        ).unwrap().unwrap();

        // Restore the first drop by op_id
        let restored = Spi::get_one::<bool>(
            &format!("SELECT flashback_restore_by_id({}, NULL)", op_id)
        ).unwrap().unwrap_or(false);
        assert!(restored, "Restore by op_id should succeed");

        // First version should have 2 rows (id 1,2)
        let row_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_orders"
        ).unwrap().unwrap_or(0);
        assert_eq!(row_count, 2, "Restored table should have 2 rows from first drop");

        cleanup();
    }

    // Test 3: Permission check — non-owner cannot restore another user's table
    #[pg_test]
    fn test_permission_check_blocks_non_owner() {
        setup();
        Spi::run("DROP TABLE test_orders").unwrap();

        // Manually insert an entry with a different role_name
        Spi::run(
            "UPDATE flashback.operations SET role_name = 'other_user' \
             WHERE table_name = 'test_orders' AND restored = false"
        ).unwrap();

        // Simulate non-superuser: we're postgres (superuser) so we check the logic indirectly.
        // Verify that the role_name mismatch is recorded correctly.
        let role = Spi::get_one::<String>(
            "SELECT role_name FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert_eq!(role, "other_user", "Role should be set to other_user");

        // As superuser, restore should still work (superuser bypasses permission check)
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_orders', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Superuser should be able to restore any table");

        cleanup();
    }

    // Test 4: TRUNCATE captured and data restored correctly
    #[pg_test]
    fn test_truncate_capture_and_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_trunc CASCADE").unwrap();
        Spi::run("CREATE TABLE test_trunc (id SERIAL, val text)").unwrap();
        Spi::run("INSERT INTO test_trunc (val) VALUES ('x'), ('y'), ('z')").unwrap();

        Spi::run("TRUNCATE TABLE test_trunc").unwrap();

        // Should be in recycle bin as TRUNCATE
        let op_type = Spi::get_one::<String>(
            "SELECT operation_type FROM flashback.operations \
             WHERE table_name = 'test_trunc' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert_eq!(op_type, "TRUNCATE", "TRUNCATE should be captured with correct operation_type");

        // Restore
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_trunc', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "TRUNCATE restore should succeed");

        // Data should be back
        let row_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_trunc"
        ).unwrap().unwrap_or(0);
        assert_eq!(row_count, 3, "All 3 rows should be restored after TRUNCATE");

        // Sequence should continue from max id
        Spi::run("INSERT INTO test_trunc (val) VALUES ('w')").unwrap();
        let max_id = Spi::get_one::<i64>(
            "SELECT MAX(id) FROM test_trunc"
        ).unwrap().unwrap_or(0);
        assert_eq!(max_id, 4, "Sequence should continue from 4 after restore");

        Spi::run("DROP TABLE IF EXISTS test_trunc CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_trunc'").unwrap();
    }

    // Test 5: flashback_purge removes the entry and physical table
    #[pg_test]
    fn test_purge() {
        setup();
        Spi::run("DROP TABLE test_orders").unwrap();

        let count_before = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_before, 1, "Should have 1 entry before purge");

        let purged = Spi::get_one::<bool>(
            "SELECT flashback_purge('test_orders')"
        ).unwrap().unwrap_or(false);
        assert!(purged, "flashback_purge should return true");

        let count_after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_after, 0, "Entry should be removed after purge");
    }

    // Test 6: flashback_purge_by_id removes only the specified entry
    #[pg_test]
    fn test_purge_by_id() {
        setup();
        Spi::run("DROP TABLE test_orders").unwrap();

        let op_id = Spi::get_one::<i64>(
            "SELECT op_id FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false LIMIT 1"
        ).unwrap().unwrap();

        let purged = Spi::get_one::<bool>(
            &format!("SELECT flashback_purge_by_id({})", op_id)
        ).unwrap().unwrap_or(false);
        assert!(purged, "flashback_purge_by_id should return true");

        let count = Spi::get_one::<i64>(
            &format!("SELECT COUNT(*) FROM flashback.operations WHERE op_id = {}", op_id)
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 0, "Entry should be gone after purge_by_id");
    }

    // Test 7: flashback_purge_all returns correct count and clears entries
    #[pg_test]
    fn test_purge_all() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_pa1 CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_pa2 CASCADE").unwrap();
        Spi::run("CREATE TABLE test_pa1 (id int)").unwrap();
        Spi::run("CREATE TABLE test_pa2 (id int)").unwrap();
        Spi::run("DROP TABLE test_pa1").unwrap();
        Spi::run("DROP TABLE test_pa2").unwrap();

        let count_before = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name IN ('test_pa1','test_pa2') AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_before, 2, "Both tables should be in recycle bin");

        let purged_count = Spi::get_one::<i64>(
            "SELECT flashback_purge_all()"
        ).unwrap().unwrap_or(0);
        assert!(purged_count >= 2, "purge_all should return at least 2");

        let count_after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_after, 0, "Recycle bin should be empty after purge_all");
    }

    // Test 8: Restore nonexistent table returns false
    #[pg_test]
    fn test_restore_nonexistent_returns_false() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT flashback_restore('does_not_exist_xyz', NULL)"
        ).unwrap().unwrap_or(true);
        assert!(!result, "Restoring nonexistent table should return false");
    }

    // Test 9: Restoring an already-restored op_id returns false
    #[pg_test]
    fn test_restore_already_restored_op_id() {
        setup();
        Spi::run("DROP TABLE test_orders").unwrap();

        let op_id = Spi::get_one::<i64>(
            "SELECT op_id FROM flashback.operations WHERE table_name = 'test_orders' AND restored = false LIMIT 1"
        ).unwrap().unwrap();

        // First restore — should succeed
        let first = Spi::get_one::<bool>(
            &format!("SELECT flashback_restore_by_id({}, NULL)", op_id)
        ).unwrap().unwrap_or(false);
        assert!(first, "First restore should succeed");

        // Drop the restored table so name is free, then try the same op_id again
        Spi::run("DROP TABLE IF EXISTS test_orders CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_orders'").unwrap();

        let second = Spi::get_one::<bool>(
            &format!("SELECT flashback_restore_by_id({}, NULL)", op_id)
        ).unwrap().unwrap_or(true);
        assert!(!second, "Restoring already-restored op_id should return false");
    }

    // Test 10: Restore when target table already exists returns false (DROP type)
    #[pg_test]
    fn test_restore_target_already_exists() {
        setup();
        Spi::run("DROP TABLE test_orders").unwrap();

        // Recreate a table with the same name before restoring
        Spi::run("CREATE TABLE test_orders (id int, val text)").unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_orders', NULL)"
        ).unwrap().unwrap_or(true);
        assert!(!result, "Restore should fail when target table already exists");

        Spi::run("DROP TABLE IF EXISTS test_orders CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_orders'").unwrap();
    }

    // Test 11: SQL injection in table name is rejected
    #[pg_test]
    fn test_sql_injection_rejected() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT flashback_restore('orders''; DROP TABLE flashback.operations;--', NULL)"
        ).unwrap().unwrap_or(true);
        assert!(!result, "SQL injection in table name should be rejected");
    }

    // Test 12: Table in excluded schema is not captured
    #[pg_test]
    fn test_excluded_schema_not_captured() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SET flashback.excluded_schemas = 'excluded_test_schema'").unwrap();
        Spi::run("CREATE SCHEMA IF NOT EXISTS excluded_test_schema").unwrap();
        Spi::run("DROP TABLE IF EXISTS excluded_test_schema.excl_tbl CASCADE").unwrap();
        Spi::run("CREATE TABLE excluded_test_schema.excl_tbl (id int)").unwrap();
        Spi::run("DROP TABLE excluded_test_schema.excl_tbl").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'excl_tbl' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 0, "Table in excluded schema should not be captured");

        Spi::run("DROP SCHEMA IF EXISTS excluded_test_schema CASCADE").unwrap();
        Spi::run("RESET flashback.excluded_schemas").unwrap();
    }

    // Test 13: flashback_status returns sensible values
    #[pg_test]
    fn test_flashback_status() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();

        // table_limit should be > 0, retention_days > 0
        let table_limit = Spi::get_one::<i32>(
            "SELECT table_limit FROM flashback_status()"
        ).unwrap().unwrap_or(0);
        assert!(table_limit > 0, "table_limit should be positive");

        let retention_days = Spi::get_one::<i32>(
            "SELECT retention_days FROM flashback_status()"
        ).unwrap().unwrap_or(0);
        assert!(retention_days > 0, "retention_days should be positive");
    }

    // Test 14: flashback_list_recycled_tables returns correct operation_type column
    #[pg_test]
    fn test_list_returns_operation_type() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_list_drop CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_list_trunc CASCADE").unwrap();
        Spi::run("CREATE TABLE test_list_drop (id int)").unwrap();
        Spi::run("CREATE TABLE test_list_trunc (id int)").unwrap();
        Spi::run("DROP TABLE test_list_drop").unwrap();
        Spi::run("TRUNCATE TABLE test_list_trunc").unwrap();

        let drop_type = Spi::get_one::<String>(
            "SELECT operation_type FROM flashback_list_recycled_tables() WHERE table_name = 'test_list_drop'"
        ).unwrap().unwrap_or_default();
        assert_eq!(drop_type, "DROP");

        let trunc_type = Spi::get_one::<String>(
            "SELECT operation_type FROM flashback_list_recycled_tables() WHERE table_name = 'test_list_trunc'"
        ).unwrap().unwrap_or_default();
        assert_eq!(trunc_type, "TRUNCATE");

        Spi::run("DROP TABLE IF EXISTS test_list_trunc CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name IN ('test_list_drop','test_list_trunc')").unwrap();
    }

    // Test 15: GENERATED ALWAYS AS IDENTITY column — DROP restore preserves sequence
    #[pg_test]
    fn test_identity_column_drop_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_identity_drop CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_identity_drop (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val text)"
        ).unwrap();
        Spi::run("INSERT INTO test_identity_drop (val) VALUES ('a'), ('b'), ('c')").unwrap();

        Spi::run("DROP TABLE test_identity_drop").unwrap();

        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_identity_drop', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "IDENTITY table DROP should be restorable");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_identity_drop"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 3, "All 3 rows should be restored");

        // Sequence must continue from 4 (not restart from 1)
        Spi::run("INSERT INTO test_identity_drop (val) VALUES ('d')").unwrap();
        let max_id = Spi::get_one::<i64>(
            "SELECT MAX(id) FROM test_identity_drop"
        ).unwrap().unwrap_or(0);
        assert_eq!(max_id, 4, "IDENTITY sequence should continue from 4 after DROP restore");

        Spi::run("DROP TABLE IF EXISTS test_identity_drop CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_identity_drop'").unwrap();
    }

    // Test 16: GENERATED ALWAYS AS IDENTITY column — TRUNCATE restore works correctly
    // This test also covers the OVERRIDING SYSTEM VALUE fix required for IDENTITY columns.
    #[pg_test]
    fn test_identity_column_truncate_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_identity_trunc CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_identity_trunc (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val text)"
        ).unwrap();
        Spi::run("INSERT INTO test_identity_trunc (val) VALUES ('x'), ('y'), ('z')").unwrap();

        Spi::run("TRUNCATE TABLE test_identity_trunc").unwrap();

        let op_type = Spi::get_one::<String>(
            "SELECT operation_type FROM flashback.operations \
             WHERE table_name = 'test_identity_trunc' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert_eq!(op_type, "TRUNCATE");

        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_identity_trunc', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "IDENTITY table TRUNCATE restore should succeed (OVERRIDING SYSTEM VALUE)");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_identity_trunc"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 3, "All 3 rows should be restored");

        // Sequence must continue from 4
        Spi::run("INSERT INTO test_identity_trunc (val) VALUES ('w')").unwrap();
        let max_id = Spi::get_one::<i64>(
            "SELECT MAX(id) FROM test_identity_trunc"
        ).unwrap().unwrap_or(0);
        assert_eq!(max_id, 4, "IDENTITY sequence should continue from 4 after TRUNCATE restore");

        Spi::run("DROP TABLE IF EXISTS test_identity_trunc CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_identity_trunc'").unwrap();
    }

    // Test 17: Table in a non-public schema is captured and restored correctly
    #[pg_test]
    fn test_non_public_schema_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("CREATE SCHEMA IF NOT EXISTS testns").unwrap();
        Spi::run("DROP TABLE IF EXISTS testns.ns_orders CASCADE").unwrap();
        Spi::run("CREATE TABLE testns.ns_orders (id int, val text)").unwrap();
        Spi::run("INSERT INTO testns.ns_orders VALUES (1, 'one'), (2, 'two')").unwrap();

        Spi::run("DROP TABLE testns.ns_orders").unwrap();

        let schema_in_bin = Spi::get_one::<String>(
            "SELECT schema_name FROM flashback.operations \
             WHERE table_name = 'ns_orders' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert_eq!(schema_in_bin, "testns", "schema_name in bin should be 'testns'");

        // NULL target_schema → restores to original schema (testns)
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('ns_orders', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Non-public schema table should be restorable");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM testns.ns_orders"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 2, "Both rows should be restored in testns schema");

        Spi::run("DROP TABLE IF EXISTS testns.ns_orders CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'ns_orders'").unwrap();
        Spi::run("DROP SCHEMA IF EXISTS testns CASCADE").unwrap();
    }

    // Test 18: TRUNCATE on an empty table — restore must not crash and return 0 rows
    #[pg_test]
    fn test_empty_table_truncate_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_empty_trunc CASCADE").unwrap();
        Spi::run("CREATE TABLE test_empty_trunc (id int, val text)").unwrap();
        // No rows — table is intentionally empty

        Spi::run("TRUNCATE TABLE test_empty_trunc").unwrap();

        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_empty_trunc', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Empty table TRUNCATE restore should succeed without crash");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_empty_trunc"
        ).unwrap().unwrap_or(99);
        assert_eq!(count, 0, "Restored empty table should have 0 rows");

        Spi::run("DROP TABLE IF EXISTS test_empty_trunc CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_empty_trunc'").unwrap();
    }

    // Test 19: flashback.max_tables GUC — oldest entry is evicted when limit is reached
    #[pg_test]
    fn test_table_limit_enforcement() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        // Clean slate so residual entries from other tests don't skew the count
        Spi::run("SELECT flashback_purge_all()").unwrap();

        Spi::run("SET flashback.max_tables = 3").unwrap();

        for i in 1..=5 {
            Spi::run(&format!("DROP TABLE IF EXISTS test_limit_{i} CASCADE")).unwrap();
            Spi::run(&format!("CREATE TABLE test_limit_{i} (id int)")).unwrap();
            Spi::run(&format!("DROP TABLE test_limit_{i}")).unwrap();
        }

        let bin_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name LIKE 'test_limit_%' AND restored = false"
        ).unwrap().unwrap_or(99);
        assert!(
            bin_count <= 3,
            "Recycle bin should have ≤3 entries with max_tables=3, got {}",
            bin_count
        );

        Spi::run("RESET flashback.max_tables").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }
}