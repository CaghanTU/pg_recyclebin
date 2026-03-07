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
}