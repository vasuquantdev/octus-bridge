use std::{
    panic::{self, PanicInfo},
    process, thread, time,
};

use backtrace::Backtrace;
use serde::Serialize;
use sled::Db;

#[derive(Debug, Serialize)]
pub struct CrashInfo {
    details: String,
    backtrace: String,
}

/// Invoke to ensure process exits on a thread panic.
///
/// Tokio's default behavior is to catch panics and ignore them.  Invoking this function will
/// ensure that all subsequent thread panics (even Tokio threads) will report the
/// details/backtrace and then exit.
pub fn setup_panic_handler(db: Db) {
    panic::set_hook(Box::new(move |pi: &PanicInfo<'_>| {
        handle_panic(pi, db.clone());
    }));
}

// Formats and logs panic information
fn handle_panic(panic_info: &PanicInfo<'_>, db: Db) {
    // The Display formatter for a PanicInfo contains the message, payload and location.
    let details = format!("{}", panic_info);
    let backtrace = format!("{:#?}", Backtrace::new());
    match db.flush(){
        Ok(a)=>log::info!("Flushed db on panic. Written {} bytes", a),
        Err(e) => log::error!("Failed flushing db on disk: {}", e),
    };
    let info = CrashInfo { details, backtrace };
    log::error!(
        "{}",
        crash_info = toml::to_string_pretty(&info).unwrap()
    );
    // Provide some time to save the log to disk
    thread::sleep(time::Duration::from_millis(100));

    // Kill the process
    process::exit(1);
}
