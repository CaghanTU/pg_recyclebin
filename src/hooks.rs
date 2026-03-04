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

// Helper function to convert C string pointer to Rust String
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

   if let Some(query) = cstr_to_string(query_string) {
    let upper = query.to_uppercase();
    let is_drop_table = upper.contains("DROP") && upper.contains("TABLE");
    let is_internal = upper.contains("FLASHBACK_RECYCLE") || upper.contains("FLASHBACK.OPERATIONS");

    if is_drop_table && !is_internal {
        if crate::ddl_capture::handle_drop_table(&query) {
            return;
        }
    }
}
    pgrx::log!("ProcessUtility hook called");
    
    // Chain to previous hook or standard function
    if let Some(prev) = PREV_PROCESS_UTILITY {
        prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
        pg_sys::standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }
}