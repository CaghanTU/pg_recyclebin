use pgrx::prelude::*;

pgrx::pg_module_magic!();

mod hooks;
mod ddl_capture;
mod recovery;
mod background_worker;
pub mod guc;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    hooks::install();
    pgrx::log!("pg_flashback loaded");
    background_worker::register();
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    hooks::uninstall();
    pgrx::log!("pg_flashback unloaded");
}


