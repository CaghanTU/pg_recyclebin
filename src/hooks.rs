use pgrx::prelude::*;
use std::ffi::CStr;

static mut PREV_PROCESS_UTILITY: pg_sys::ProcessUtility_hook_type = None;

pub fn install() {
    unsafe {
        PREV_PROCESS_UTILITY = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_hook);
    }
}

pub fn uninstall() {
    unsafe {
        pg_sys::ProcessUtility_hook = PREV_PROCESS_UTILITY;
    }
}

fn cstr_to_string(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .ok()
            .map(|s| s.to_string())
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn process_utility_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::os::raw::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {

   // Only intercept DROP TABLE commands (checked via AST node type)
    let is_drop_table = unsafe {
        if pstmt.is_null() {
            false
        } else {
            let utility = (*pstmt).utilityStmt;
            if utility.is_null() {
                false
            } else {
                let tag = (*(utility as *mut pg_sys::Node)).type_;
                if tag == pg_sys::NodeTag::T_DropStmt {
                    let drop = utility as *mut pg_sys::DropStmt;
                    (*drop).removeType == pg_sys::ObjectType::OBJECT_TABLE
                } else {
                    false
                }
            }
        }
    };

    let is_truncate = unsafe {
        if pstmt.is_null() {
            false
        } else {
            let utility = (*pstmt).utilityStmt;
            if utility.is_null() {
                false
            } else {
                let tag = (*(utility as *mut pg_sys::Node)).type_;
                tag == pg_sys::NodeTag::T_TruncateStmt
            }
        }
    };

    // Detect DROP SCHEMA so we can capture tables before the schema is deleted.
    let is_drop_schema = unsafe {
        if pstmt.is_null() {
            false
        } else {
            let utility = (*pstmt).utilityStmt;
            if utility.is_null() {
                false
            } else {
                let tag = (*(utility as *mut pg_sys::Node)).type_;
                if tag == pg_sys::NodeTag::T_DropStmt {
                    let drop = utility as *mut pg_sys::DropStmt;
                    (*drop).removeType == pg_sys::ObjectType::OBJECT_SCHEMA
                } else {
                    false
                }
            }
        }
    };

    if is_drop_table {
        if let Some(query) = cstr_to_string(query_string) {
            let upper = query.to_uppercase();
            let is_internal = upper.contains("FLASHBACK_RECYCLE")
                || upper.contains("FLASHBACK.OPERATIONS")
                || upper.contains("PG_FLASHBACK_INTERNAL");
            if !is_internal {
                if crate::ddl_capture::handle_drop_table(&query) {
                    return;
                }
            }
        }
    }
    
    if is_truncate {
        if let Some(query) = cstr_to_string(query_string) {
            let upper = query.to_uppercase();
            let is_internal = upper.contains("FLASHBACK_RECYCLE") || upper.contains("FLASHBACK.OPERATIONS");
            if !is_internal {
                if crate::ddl_capture::handle_truncate_table(&query) {
                    return;
                }
            }
        }
    }

    // Capture tables from DROP SCHEMA CASCADE before the schema is removed.
    // We do this BEFORE delegating to standard_ProcessUtility so the tables still exist.
    if is_drop_schema {
        if let Some(query) = cstr_to_string(query_string) {
            let upper = query.to_uppercase();
            if !upper.contains("FLASHBACK") {
                crate::ddl_capture::handle_drop_schema_cascade(&query);
            }
        }
    }
    
    if let Some(prev) = PREV_PROCESS_UTILITY {
        prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
        pg_sys::standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }
}