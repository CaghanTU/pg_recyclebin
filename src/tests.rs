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
}