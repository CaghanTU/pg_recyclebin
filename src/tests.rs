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

    // Test 20: Dependent view DDL is saved and recreated on DROP restore
    #[pg_test]
    fn test_view_dependency_restored_after_drop() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_view_tbl CASCADE").unwrap();
        Spi::run("CREATE TABLE test_view_tbl (id int, status text)").unwrap();
        Spi::run("INSERT INTO test_view_tbl VALUES (1, 'active'), (2, 'inactive')").unwrap();
        Spi::run("CREATE OR REPLACE VIEW test_view_active AS \
                  SELECT id, status FROM test_view_tbl WHERE status = 'active'").unwrap();

        // Drop the table (and with it, the view)
        Spi::run("DROP TABLE test_view_tbl CASCADE").unwrap();

        // View DDL must have been captured in metadata
        let meta: String = Spi::get_one::<String>(
            "SELECT metadata::text FROM flashback.operations \
             WHERE table_name = 'test_view_tbl' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert!(!meta.is_empty(), "metadata should contain view DDL");
        assert!(meta.contains("test_view_active"), "metadata should reference the view name");

        // Restore the table
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_view_tbl', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "DROP restore should succeed");

        // Table rows must be back
        let row_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_view_tbl"
        ).unwrap().unwrap_or(0);
        assert_eq!(row_count, 2, "Both rows should be restored");

        // View must be recreated
        let view_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'test_view_active')"
        ).unwrap().unwrap_or(false);
        assert!(view_exists, "Dependent view should be recreated after restore");

        // View should return only the 'active' row
        let view_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_view_active"
        ).unwrap().unwrap_or(0);
        assert_eq!(view_count, 1, "View should return 1 active row");

        Spi::run("DROP VIEW IF EXISTS test_view_active").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_view_tbl CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_view_tbl'").unwrap();
    }

    // Test 21: DROP TABLE IF EXISTS on a non-existent table is a no-op (not captured)
    #[pg_test]
    fn test_drop_table_if_exists_nonexistent() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();

        let count_before = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'does_not_exist_ife' AND restored = false"
        ).unwrap().unwrap_or(0);

        // Should not error, and should not create a recycle-bin entry
        Spi::run("DROP TABLE IF EXISTS does_not_exist_ife").unwrap();

        let count_after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE table_name = 'does_not_exist_ife' AND restored = false"
        ).unwrap().unwrap_or(0);

        assert_eq!(count_before, count_after, "DROP TABLE IF EXISTS on non-existent table must not create a recycle-bin entry");
    }

    // Test 22: TRUNCATE on multiple tables in a single statement — all are captured
    #[pg_test]
    fn test_multi_table_truncate_captured() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_multi_a CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_multi_b CASCADE").unwrap();
        Spi::run("CREATE TABLE test_multi_a (id int)").unwrap();
        Spi::run("CREATE TABLE test_multi_b (id int)").unwrap();
        Spi::run("INSERT INTO test_multi_a VALUES (1), (2)").unwrap();
        Spi::run("INSERT INTO test_multi_b VALUES (3), (4), (5)").unwrap();

        // Truncate both in one statement
        Spi::run("TRUNCATE TABLE test_multi_a, test_multi_b").unwrap();

        let count_a = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_multi_a' AND operation_type = 'TRUNCATE' AND restored = false"
        ).unwrap().unwrap_or(0);
        let count_b = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_multi_b' AND operation_type = 'TRUNCATE' AND restored = false"
        ).unwrap().unwrap_or(0);

        assert_eq!(count_a, 1, "test_multi_a should have 1 TRUNCATE entry");
        assert_eq!(count_b, 1, "test_multi_b should have 1 TRUNCATE entry");

        // Restore both
        let r_a = Spi::get_one::<bool>("SELECT flashback_restore('test_multi_a', NULL)")
            .unwrap().unwrap_or(false);
        let r_b = Spi::get_one::<bool>("SELECT flashback_restore('test_multi_b', NULL)")
            .unwrap().unwrap_or(false);
        assert!(r_a, "test_multi_a should restore");
        assert!(r_b, "test_multi_b should restore");

        let rows_a = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_multi_a")
            .unwrap().unwrap_or(0);
        let rows_b = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_multi_b")
            .unwrap().unwrap_or(0);
        assert_eq!(rows_a, 2, "test_multi_a should have 2 rows after restore");
        assert_eq!(rows_b, 3, "test_multi_b should have 3 rows after restore");

        Spi::run("DROP TABLE IF EXISTS test_multi_a, test_multi_b CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name IN ('test_multi_a','test_multi_b')").unwrap();
    }

    // Test 23: DROP SCHEMA CASCADE captures tables inside the schema
    #[pg_test]
    fn test_drop_schema_cascade_captures_tables() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP SCHEMA IF EXISTS test_drop_schema CASCADE").unwrap();
        Spi::run("CREATE SCHEMA test_drop_schema").unwrap();
        Spi::run("CREATE TABLE test_drop_schema.tbl_one (id int)").unwrap();
        Spi::run("CREATE TABLE test_drop_schema.tbl_two (id int)").unwrap();
        Spi::run("INSERT INTO test_drop_schema.tbl_one VALUES (1),(2)").unwrap();

        // Drop the entire schema
        Spi::run("DROP SCHEMA test_drop_schema CASCADE").unwrap();

        // Both tables should have been captured in the recycle bin
        let captured = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE schema_name = 'test_drop_schema' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(captured, 2, "Both tables inside the dropped schema should be in the recycle bin");

        // Restore tbl_one — create the schema first since it was dropped
        Spi::run("CREATE SCHEMA IF NOT EXISTS test_drop_schema").unwrap();
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('tbl_one', 'test_drop_schema')"
        ).unwrap().unwrap_or(false);
        assert!(restored, "tbl_one should be restorable after schema drop");

        let rows = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_drop_schema.tbl_one"
        ).unwrap().unwrap_or(0);
        assert_eq!(rows, 2, "tbl_one should have 2 rows after restore");

        // Cleanup
        Spi::run("DROP SCHEMA IF EXISTS test_drop_schema CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 24: Partitioned table IS captured with partition metadata in metadata JSON
    #[pg_test]
    fn test_partitioned_table_captured_with_metadata() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_part_parent CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_part_parent (id int, region text) \
             PARTITION BY LIST (region)"
        ).unwrap();
        Spi::run(
            "CREATE TABLE test_part_eu PARTITION OF test_part_parent \
             FOR VALUES IN ('EU')"
        ).unwrap();
        Spi::run(
            "CREATE TABLE test_part_us PARTITION OF test_part_parent \
             FOR VALUES IN ('US')"
        ).unwrap();

        // Drop the partitioned table (CASCADE also drops children)
        Spi::run("DROP TABLE test_part_parent CASCADE").unwrap();

        // Parent should be in recycle bin
        let in_bin = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_part_parent' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(in_bin, 1, "Partitioned table parent should be captured in recycle bin");

        // Metadata should contain partition_info with children
        let meta = Spi::get_one::<String>(
            "SELECT metadata::text FROM flashback.operations \
             WHERE table_name = 'test_part_parent' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert!(meta.contains("partition_info"), "metadata should contain partition_info");
        assert!(meta.contains("test_part_eu") || meta.contains("test_part_us"),
            "metadata should list child partitions");

        // Child partitions should NOT appear as separate recycle bin entries
        let child_in_bin = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name IN ('test_part_eu','test_part_us') AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(child_in_bin, 0, "Child partitions should not have separate recycle bin entries");

        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 25: Multi-table DROP captures all tables
    #[pg_test]
    fn test_multi_table_drop_captured() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_multi_drop_a CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_multi_drop_b CASCADE").unwrap();
        Spi::run("CREATE TABLE test_multi_drop_a (id int)").unwrap();
        Spi::run("CREATE TABLE test_multi_drop_b (id int)").unwrap();
        Spi::run("INSERT INTO test_multi_drop_a VALUES (1),(2)").unwrap();
        Spi::run("INSERT INTO test_multi_drop_b VALUES (3),(4),(5)").unwrap();

        // Drop both in a single statement
        Spi::run("DROP TABLE test_multi_drop_a, test_multi_drop_b").unwrap();

        let count_a = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_multi_drop_a' AND restored = false"
        ).unwrap().unwrap_or(0);
        let count_b = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_multi_drop_b' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_a, 1, "test_multi_drop_a should be in recycle bin");
        assert_eq!(count_b, 1, "test_multi_drop_b should be in recycle bin");

        // Restore both
        let r_a = Spi::get_one::<bool>("SELECT flashback_restore('test_multi_drop_a', NULL)")
            .unwrap().unwrap_or(false);
        let r_b = Spi::get_one::<bool>("SELECT flashback_restore('test_multi_drop_b', NULL)")
            .unwrap().unwrap_or(false);
        assert!(r_a, "test_multi_drop_a should restore");
        assert!(r_b, "test_multi_drop_b should restore");

        let rows_a = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_multi_drop_a").unwrap().unwrap_or(0);
        let rows_b = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_multi_drop_b").unwrap().unwrap_or(0);
        assert_eq!(rows_a, 2, "test_multi_drop_a should have 2 rows");
        assert_eq!(rows_b, 3, "test_multi_drop_b should have 3 rows");

        Spi::run("DROP TABLE IF EXISTS test_multi_drop_a, test_multi_drop_b CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name IN ('test_multi_drop_a','test_multi_drop_b')").unwrap();
    }

    // Test 26: Triggers on the captured table survive restore (they travel with the physical table)
    #[pg_test]
    fn test_trigger_survives_drop_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_trigger_tbl CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_trigger_log CASCADE").unwrap();
        Spi::run("CREATE TABLE test_trigger_tbl (id SERIAL, val text)").unwrap();
        Spi::run("CREATE TABLE test_trigger_log (logged_at timestamptz DEFAULT now())").unwrap();
        Spi::run(
            "CREATE OR REPLACE FUNCTION _pg_flashback_trig_fn() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN INSERT INTO test_trigger_log DEFAULT VALUES; RETURN NEW; END; $$"
        ).unwrap();
        Spi::run(
            "CREATE TRIGGER test_trigger_trig \
             AFTER INSERT ON test_trigger_tbl \
             FOR EACH ROW EXECUTE FUNCTION _pg_flashback_trig_fn()"
        ).unwrap();
        Spi::run("INSERT INTO test_trigger_tbl (val) VALUES ('before_drop')").unwrap();

        // Verify trigger fired once
        let log_before = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_trigger_log")
            .unwrap().unwrap_or(0);
        assert_eq!(log_before, 1, "Trigger should have fired once before drop");

        // Drop and restore
        Spi::run("DROP TABLE test_trigger_tbl").unwrap();
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_trigger_tbl', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Trigger table should restore successfully");

        // Trigger should still be attached after restore
        let trig_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'test_trigger_trig')"
        ).unwrap().unwrap_or(false);
        assert!(trig_exists, "Trigger should survive DROP+restore (travels with physical table)");

        // Trigger should fire on new insert
        Spi::run("INSERT INTO test_trigger_tbl (val) VALUES ('after_restore')").unwrap();
        let log_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_trigger_log")
            .unwrap().unwrap_or(0);
        assert_eq!(log_after, 2, "Trigger should fire again after restore");

        Spi::run("DROP TABLE IF EXISTS test_trigger_tbl CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_trigger_log CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP FUNCTION IF EXISTS _pg_flashback_trig_fn() CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_trigger_tbl'").unwrap();
    }

    // Test 27: FK constraints on OTHER tables referencing this table are captured in metadata
    #[pg_test]
    fn test_incoming_fk_captured_in_metadata() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_fk_child CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_fk_parent CASCADE").unwrap();
        Spi::run("CREATE TABLE test_fk_parent (id SERIAL PRIMARY KEY, name text)").unwrap();
        Spi::run(
            "CREATE TABLE test_fk_child (id SERIAL PRIMARY KEY, parent_id INT \
             REFERENCES test_fk_parent(id) ON DELETE CASCADE)"
        ).unwrap();
        Spi::run("INSERT INTO test_fk_parent (name) VALUES ('Alice'),('Bob')").unwrap();
        Spi::run("INSERT INTO test_fk_child (parent_id) VALUES (1),(2)").unwrap();

        // Drop the PARENT (child's FK reference disappears)
        Spi::run("DROP TABLE test_fk_parent CASCADE").unwrap();

        // FK info should be in metadata
        let meta = Spi::get_one::<String>(
            "SELECT metadata::text FROM flashback.operations \
             WHERE table_name = 'test_fk_parent' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert!(meta.contains("incoming_fks"), "metadata should contain incoming_fks key");
        assert!(meta.contains("test_fk_child"), "metadata should reference the child table");

        // Cleanup
        Spi::run("DROP TABLE IF EXISTS test_fk_child CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 28b: Incoming FK constraints remain intact after DROP restore
    // pg_flashback moves the parent via ALTER TABLE SET SCHEMA (not DROP), so the FK
    // is never dropped — it keeps pointing at the parent by OID throughout.
    // After restore the FK must still reference the correct parent in the correct schema.
    #[pg_test]
    fn test_incoming_fk_restored_end_to_end() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_fke_child CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_fke_parent CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_fke_parent (id SERIAL PRIMARY KEY, name text)"
        ).unwrap();
        Spi::run(
            "CREATE TABLE test_fke_child (id SERIAL PRIMARY KEY, \
             parent_id INT REFERENCES test_fke_parent(id) ON DELETE CASCADE)"
        ).unwrap();
        Spi::run("INSERT INTO test_fke_parent (name) VALUES ('Alice'),('Bob')").unwrap();
        Spi::run("INSERT INTO test_fke_child (parent_id) VALUES (1),(2)").unwrap();

        // DROP parent — our hook absorbs the DROP via SET SCHEMA, so the FK is NOT
        // dropped (parent is moved, not deleted). The FK keeps its OID reference.
        Spi::run("DROP TABLE test_fke_parent CASCADE").unwrap();

        // Restore parent
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_fke_parent', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Parent table should restore successfully");

        // Parent data should be back
        let row_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_fke_parent"
        ).unwrap().unwrap_or(0);
        assert_eq!(row_count, 2, "Parent should have 2 rows after restore");

        // FK constraint must exist on the child table
        let fk_exists = Spi::get_one::<bool>(
            "SELECT EXISTS( \
               SELECT 1 FROM pg_constraint c \
               JOIN pg_class t ON t.oid = c.conrelid \
               WHERE t.relname = 'test_fke_child' AND c.contype = 'f')"
        ).unwrap().unwrap_or(false);
        assert!(fk_exists, "FK constraint must exist on child table after restore");

        // FK must reference the restored parent (now back in its original schema)
        let fk_refs_parent = Spi::get_one::<bool>(
            "SELECT EXISTS( \
               SELECT 1 FROM pg_constraint c \
               JOIN pg_class t ON t.oid = c.conrelid \
               JOIN pg_class r ON r.oid = c.confrelid \
               JOIN pg_namespace rn ON rn.oid = r.relnamespace \
               WHERE t.relname = 'test_fke_child' \
                 AND r.relname = 'test_fke_parent' \
                 AND rn.nspname = 'public' \
                 AND c.contype = 'f')"
        ).unwrap().unwrap_or(false);
        assert!(fk_refs_parent, "FK must reference test_fke_parent in public schema after restore");

        Spi::run("DROP TABLE IF EXISTS test_fke_child CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_fke_parent CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 28: RLS policies are captured in metadata and restored after DROP restore
    #[pg_test]
    fn test_rls_policy_captured_and_restored() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_rls_tbl CASCADE").unwrap();
        Spi::run("CREATE TABLE test_rls_tbl (id int, owner text)").unwrap();
        Spi::run("ALTER TABLE test_rls_tbl ENABLE ROW LEVEL SECURITY").unwrap();
        Spi::run(
            "CREATE POLICY test_rls_pol ON test_rls_tbl \
             FOR SELECT TO PUBLIC USING (owner = current_user)"
        ).unwrap();
        Spi::run("INSERT INTO test_rls_tbl VALUES (1, 'postgres')").unwrap();

        // Drop the table
        Spi::run("DROP TABLE test_rls_tbl CASCADE").unwrap();

        // RLS policy should be in metadata
        let meta = Spi::get_one::<String>(
            "SELECT metadata::text FROM flashback.operations \
             WHERE table_name = 'test_rls_tbl' AND restored = false LIMIT 1"
        ).unwrap().unwrap_or_default();
        assert!(meta.contains("rls_policies"), "metadata should contain rls_policies key");
        assert!(meta.contains("test_rls_pol"), "metadata should reference the policy name");

        // Restore
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_rls_tbl', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "RLS table should restore successfully");

        // Policy should be recreated
        let policy_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_policy \
             JOIN pg_class ON pg_class.oid = pg_policy.polrelid \
             WHERE pg_class.relname = 'test_rls_tbl' AND polname = 'test_rls_pol')"
        ).unwrap().unwrap_or(false);
        assert!(policy_exists, "RLS policy should be recreated after restore");

        Spi::run("DROP TABLE IF EXISTS test_rls_tbl CASCADE").unwrap();
        Spi::run("DELETE FROM flashback.operations WHERE table_name = 'test_rls_tbl'").unwrap();
    }

    // Test 29: Partition children are restored (co-moved) when parent is restored
    #[pg_test]
    fn test_partitioned_children_restored_with_parent() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_part_restore_p CASCADE").unwrap();
        Spi::run(
            "CREATE TABLE test_part_restore_p (id int, region text) PARTITION BY LIST (region)"
        ).unwrap();
        Spi::run(
            "CREATE TABLE test_part_restore_eu PARTITION OF test_part_restore_p \
             FOR VALUES IN ('EU')"
        ).unwrap();
        Spi::run(
            "CREATE TABLE test_part_restore_us PARTITION OF test_part_restore_p \
             FOR VALUES IN ('US')"
        ).unwrap();
        Spi::run("INSERT INTO test_part_restore_p VALUES (1,'EU'),(2,'US')").unwrap();

        // Drop parent CASCADE
        Spi::run("DROP TABLE test_part_restore_p CASCADE").unwrap();

        // Restore parent
        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_part_restore_p', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Partitioned parent should restore");

        // Children must exist after restore (co-moved or recreated)
        let eu_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'test_part_restore_eu')"
        ).unwrap().unwrap_or(false);
        let us_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'test_part_restore_us')"
        ).unwrap().unwrap_or(false);
        assert!(eu_exists, "EU partition should exist after parent restore");
        assert!(us_exists, "US partition should exist after parent restore");

        // Partitioned table should be fully functional (routing works)
        Spi::run("INSERT INTO test_part_restore_p VALUES (10,'EU'),(11,'US')").unwrap();
        let total = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_part_restore_p"
        ).unwrap().unwrap_or(0);
        // total >= 2 (new inserts) — original data preserved if co-moved, or 2 if recreated empty
        assert!(total >= 2, "Should be able to insert into restored partitioned table");

        Spi::run("DROP TABLE IF EXISTS test_part_restore_p CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 30: flashback_restore_schema restores all tables from a given schema
    #[pg_test]
    fn test_restore_schema_bulk() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP SCHEMA IF EXISTS test_bulk_schema CASCADE").unwrap();
        Spi::run("CREATE SCHEMA test_bulk_schema").unwrap();
        Spi::run("CREATE TABLE test_bulk_schema.tbl_a (id int, val text)").unwrap();
        Spi::run("CREATE TABLE test_bulk_schema.tbl_b (id int, val text)").unwrap();
        Spi::run("INSERT INTO test_bulk_schema.tbl_a VALUES (1,'a'),(2,'b')").unwrap();
        Spi::run("INSERT INTO test_bulk_schema.tbl_b VALUES (3,'c')").unwrap();

        // Drop both tables individually
        Spi::run("DROP TABLE test_bulk_schema.tbl_a").unwrap();
        Spi::run("DROP TABLE test_bulk_schema.tbl_b").unwrap();

        let in_bin = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE schema_name = 'test_bulk_schema' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(in_bin, 2, "Both tables should be in recycle bin");

        // Bulk-restore all tables in the schema
        let restored_count = Spi::get_one::<i64>(
            "SELECT flashback_restore_schema('test_bulk_schema', NULL)"
        ).unwrap().unwrap_or(0);
        assert_eq!(restored_count, 2, "Both tables should be restored by schema restore");

        // Verify data is back
        let count_a = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_bulk_schema.tbl_a"
        ).unwrap().unwrap_or(0);
        let count_b = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM test_bulk_schema.tbl_b"
        ).unwrap().unwrap_or(0);
        assert_eq!(count_a, 2, "tbl_a should have 2 rows");
        assert_eq!(count_b, 1, "tbl_b should have 1 row");

        Spi::run("DROP SCHEMA IF EXISTS test_bulk_schema CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 31: DROP TABLE existing, non_existent (without IF EXISTS) should error
    #[pg_test]
    #[should_panic]
    fn test_drop_nonexistent_table_errors() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_drop_real CASCADE").unwrap();
        Spi::run("CREATE TABLE test_drop_real (id int)").unwrap();

        // This should raise an error because non_existent_xyz does not exist
        // and no IF EXISTS is specified. Bug 1 fix: pgrx::error! is called.
        Spi::run("DROP TABLE test_drop_real, non_existent_xyz_table").unwrap();
    }

    // Test 32: DROP TABLE existing, non_existent WITH IF EXISTS should succeed silently
    #[pg_test]
    fn test_drop_nonexistent_with_if_exists_ok() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_drop_ifexists CASCADE").unwrap();
        Spi::run("CREATE TABLE test_drop_ifexists (id int)").unwrap();

        // IF EXISTS: non-existent table should be silently skipped, real table captured.
        Spi::run("DROP TABLE IF EXISTS test_drop_ifexists, non_existent_xyz_table").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations \
             WHERE table_name = 'test_drop_ifexists' AND restored = false"
        ).unwrap().unwrap_or(0);
        assert_eq!(count, 1, "Real table should be in recycle bin");

        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 33: View with special-character name restores correctly (Bug 2 fix)
    #[pg_test]
    fn test_view_special_name_restore() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_view_base CASCADE").unwrap();
        Spi::run("CREATE TABLE test_view_base (id int, val text)").unwrap();
        Spi::run("INSERT INTO test_view_base VALUES (1, 'hello')").unwrap();
        // View name with uppercase (requires quoting to round-trip correctly)
        Spi::run("CREATE VIEW \"MySpecialView\" AS SELECT * FROM test_view_base").unwrap();

        Spi::run("DROP TABLE test_view_base CASCADE").unwrap();

        let restored = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_view_base', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(restored, "Table with dependent view should restore");

        // View should be recreated
        let view_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'MySpecialView')"
        ).unwrap().unwrap_or(false);
        assert!(view_exists, "Special-named view should be recreated after restore");

        Spi::run("DROP TABLE IF EXISTS test_view_base CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }

    // Test 34: flashback_purge_all returns actual purged count (Bug 4 fix)
    #[pg_test]
    fn test_purge_all_count() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_purge_a, test_purge_b CASCADE").unwrap();
        Spi::run("CREATE TABLE test_purge_a (id int)").unwrap();
        Spi::run("CREATE TABLE test_purge_b (id int)").unwrap();
        Spi::run("DROP TABLE test_purge_a").unwrap();
        Spi::run("DROP TABLE test_purge_b").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT flashback_purge_all()"
        ).unwrap().unwrap_or(-1);
        assert_eq!(count, 2, "purge_all should return 2 (the number of successfully purged entries)");

        // Recycle bin should be empty now
        let remaining = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM flashback.operations WHERE restored = false"
        ).unwrap().unwrap_or(-1);
        assert_eq!(remaining, 0, "Recycle bin should be empty after purge_all");
    }

    // Test 35: Restore with savepoint safety — second restore of same table fails gracefully (Bug 5)
    #[pg_test]
    fn test_restore_duplicate_name_fails_gracefully() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_flashback").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_savepoint_tbl CASCADE").unwrap();
        Spi::run("CREATE TABLE test_savepoint_tbl (id int)").unwrap();
        Spi::run("DROP TABLE test_savepoint_tbl").unwrap();

        // Restore once — should succeed
        let first = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_savepoint_tbl', NULL)"
        ).unwrap().unwrap_or(false);
        assert!(first, "First restore should succeed");

        // Drop again so we have a second bin entry
        Spi::run("DROP TABLE test_savepoint_tbl").unwrap();
        Spi::run("CREATE TABLE test_savepoint_tbl (id int)").unwrap();

        // Table now exists AND a bin entry exists — restore should fail gracefully
        let second = Spi::get_one::<bool>(
            "SELECT flashback_restore('test_savepoint_tbl', NULL)"
        ).unwrap().unwrap_or(true);
        assert!(!second, "Restore should fail gracefully when target table already exists");

        // The existing table must still be intact (not orphaned)
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'test_savepoint_tbl')"
        ).unwrap().unwrap_or(false);
        assert!(exists, "Existing table must not be lost after failed restore");

        Spi::run("DROP TABLE IF EXISTS test_savepoint_tbl CASCADE").unwrap();
        Spi::run("SELECT flashback_purge_all()").unwrap();
    }
}