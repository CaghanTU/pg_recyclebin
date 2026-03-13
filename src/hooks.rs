use pgrx::prelude::*;
use std::ffi::CStr;

static mut PREV_PROCESS_UTILITY: pg_sys::ProcessUtility_hook_type = None;

/// Extract the C string pointer from a PostgreSQL String/Value node.
/// PG14 uses `pg_sys::Value` with `val.str_`; PG15+ uses `pg_sys::String` with `sval`.
unsafe fn pg_node_str(ptr: *mut std::ffi::c_void) -> *const std::os::raw::c_char {
    if ptr.is_null() {
        return std::ptr::null();
    }
    #[cfg(feature = "pg14")]
    {
        let val = ptr as *mut pg_sys::Value;
        (*val).val.str_
    }
    #[cfg(not(feature = "pg14"))]
    {
        let s = ptr as *mut pg_sys::String;
        (*s).sval
    }
}

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

unsafe fn drop_table_ast_names(
    pstmt: *mut pg_sys::PlannedStmt,
) -> (Vec<(String, String)>, bool, bool) {
    let mut tables = Vec::new();
    if pstmt.is_null() {
        return (tables, false, false);
    }
    let drop = (*pstmt).utilityStmt as *mut pg_sys::DropStmt;
    if drop.is_null() {
        return (tables, false, false);
    }
    let if_exists = (*drop).missing_ok;
    let cascade = (*drop).behavior == pg_sys::DropBehavior::DROP_CASCADE;
    let outer = (*drop).objects;
    if outer.is_null() {
        return (tables, if_exists, cascade);
    }
    let n_outer = (*outer).length as usize;
    let outer_elements = (*outer).elements;
    if outer_elements.is_null() {
        return (tables, if_exists, cascade);
    }
    for i in 0..n_outer {
        // Each outer element is a *mut pg_sys::List containing the qualified name as String nodes
        let outer_cell = &*outer_elements.add(i);
        let inner = outer_cell.ptr_value as *mut pg_sys::List;
        if inner.is_null() {
            continue;
        }
        let n_inner = (*inner).length as usize;
        let inner_elements = (*inner).elements;
        if inner_elements.is_null() || n_inner == 0 {
            continue;
        }
        // Collect the name parts (each is a String/Value node)
        let mut parts: Vec<String> = Vec::new();
        for j in 0..n_inner {
            let inner_cell = &*inner_elements.add(j);
            let sval = pg_node_str(inner_cell.ptr_value as *mut std::ffi::c_void);
            if sval.is_null() {
                continue;
            }
            let s = CStr::from_ptr(sval)
                .to_str()
                .unwrap_or("")
                .to_string();
            if !s.is_empty() {
                parts.push(s);
            }
        }
        let (schema, table) = match parts.len() {
            // Unqualified name: resolve via search_path in ddl_capture.
            // Do not force "public" here, otherwise TEMP tables are mis-resolved.
            1 => ("".to_string(), parts.remove(0)),
            n if n >= 2 => {
                let table = parts.remove(n - 1);
                let schema = parts.remove(n - 2);
                (schema, table)
            }
            _ => continue,
        };
        if !table.is_empty() {
            tables.push((schema, table));
        }
    }
    (tables, if_exists, cascade)
}

/// Extract the schema name from a DROP SCHEMA AST.
/// DropStmt.objects for OBJECT_SCHEMA is List<StringNode> (not List<List<StringNode>>).
unsafe fn drop_schema_ast_name(pstmt: *mut pg_sys::PlannedStmt) -> String {
    if pstmt.is_null() { return String::new(); }
    let drop = (*pstmt).utilityStmt as *mut pg_sys::DropStmt;
    if drop.is_null() { return String::new(); }
    let objects = (*drop).objects;
    if objects.is_null() || (*objects).length == 0 { return String::new(); }
    let cell = &*(*objects).elements.add(0);
    let sval = pg_node_str(cell.ptr_value as *mut std::ffi::c_void);
    if sval.is_null() { return String::new(); }
    CStr::from_ptr(sval)
        .to_str()
        .unwrap_or("")
        .to_string()
}

/// Extract (schema, table) pairs from a TRUNCATE statement AST.
unsafe fn truncate_ast_names(pstmt: *mut pg_sys::PlannedStmt) -> Vec<(String, String)> {
    let mut tables = Vec::new();
    if pstmt.is_null() {
        return tables;
    }
    let trunc = (*pstmt).utilityStmt as *mut pg_sys::TruncateStmt;
    if trunc.is_null() {
        return tables;
    }
    let relations = (*trunc).relations;
    if relations.is_null() {
        return tables;
    }
    let n = (*relations).length as usize;
    let elements = (*relations).elements;
    if elements.is_null() {
        return tables;
    }
    for i in 0..n {
        let cell = &*elements.add(i);
        let rv = cell.ptr_value as *mut pg_sys::RangeVar;
        if rv.is_null() || (*rv).relname.is_null() {
            continue;
        }
        let relname = CStr::from_ptr((*rv).relname)
            .to_str()
            .unwrap_or("")
            .to_string();
        let schemaname = if (*rv).schemaname.is_null() {
            "public".to_string()
        } else {
            CStr::from_ptr((*rv).schemaname)
                .to_str()
                .unwrap_or("public")
                .to_string()
        };
        if !relname.is_empty() {
            tables.push((schemaname, relname));
        }
    }
    tables
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
        // Skip our own internal DDL to prevent re-entrant interception.
        // Only check for the PG_FLASHBACK_INTERNAL marker that we explicitly inject into
        // internally-generated SQL. Broad schema-name checks (FLASHBACK_RECYCLE,
        // FLASHBACK.OPERATIONS) caused false positives when user query batches happened to
        // reference those schemas elsewhere in the same multi-statement string.
        let is_internal = cstr_to_string(query_string)
            .map(|q| q.to_uppercase().contains("PG_FLASHBACK_INTERNAL"))
            .unwrap_or(false);
        if !is_internal {
            let (tables, if_exists, cascade) = drop_table_ast_names(pstmt);
            if crate::ddl_capture::handle_drop_table(&tables, if_exists, cascade) {
                return;
            }
        }
    }

    if is_truncate {
        let is_internal = cstr_to_string(query_string)
            .map(|q| q.to_uppercase().contains("PG_FLASHBACK_INTERNAL"))
            .unwrap_or(false);
        if !is_internal {
            let tables = truncate_ast_names(pstmt);
            crate::ddl_capture::handle_truncate_table(&tables);
        }
    }

    // Capture tables from DROP SCHEMA CASCADE before the schema is removed.
    // We do this BEFORE delegating to standard_ProcessUtility so the tables still exist.
    // Schema name is read from the AST (DropStmt.objects is List<StringNode> for OBJECT_SCHEMA).
    // Only intercept CASCADE drops: a RESTRICT drop on a non-empty schema must still fail.
    if is_drop_schema {
        let schema_name = drop_schema_ast_name(pstmt);
        let is_cascade = unsafe {
            let drop = (*pstmt).utilityStmt as *mut pg_sys::DropStmt;
            (*drop).behavior == pg_sys::DropBehavior::DROP_CASCADE
        };
        if !schema_name.is_empty()
            && !schema_name.to_uppercase().contains("FLASHBACK")
            && is_cascade
        {
            crate::ddl_capture::handle_drop_schema_cascade(&schema_name);
        }
    }
    
    if let Some(prev) = PREV_PROCESS_UTILITY {
        prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    } else {
        pg_sys::standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    }
}