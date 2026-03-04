use pgrx::prelude::*;

pgrx::pg_module_magic!();

mod hooks;
mod ddl_capture;
mod recovery;
pub mod guc;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    hooks::install();
    pgrx::log!("pg_flashback loaded");
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    hooks::uninstall();
    pgrx::log!("pg_flashback unloaded");
}

#[pg_extern]
fn flashback_hello() -> &'static str {
    "pg_flashback loaded"
}

