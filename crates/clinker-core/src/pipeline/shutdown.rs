//! Process-wide shutdown flag for graceful SIGINT/SIGTERM handling.
//!
//! `install_signal_handler()` registers for SIGINT + SIGTERM via `ctrlc`.
//! `shutdown_requested()` is checked at chunk boundaries (Phase 2)
//! and during Arena::build (Phase 1).

use std::sync::atomic::{AtomicBool, Ordering};

/// Process-wide shutdown flag, set by the signal handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Install the signal handler for SIGINT + SIGTERM. Safe to call multiple
/// times (uses `Once` internally). The handler sets `SHUTDOWN_REQUESTED`
/// to true; actual cleanup happens at the next chunk boundary check.
pub fn install_signal_handler() -> Result<(), String> {
    use std::sync::Once;
    static INIT: Once = Once::new();
    let mut result = Ok(());
    INIT.call_once(|| {
        // ctrlc with "termination" feature handles both SIGINT and SIGTERM
        if let Err(e) = ctrlc::set_handler(move || {
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        }) {
            result = Err(e.to_string());
        }
    });
    result
}

/// Check if shutdown has been requested. Called at chunk boundaries
/// and periodically during Arena::build.
pub fn shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Request shutdown programmatically (used by signal handler and tests).
pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Reset the shutdown flag (for testing only).
#[cfg(test)]
pub fn reset_shutdown_flag() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_flag_sets_atomic() {
        reset_shutdown_flag();
        assert!(!shutdown_requested());
        request_shutdown();
        assert!(shutdown_requested());
        reset_shutdown_flag();
        assert!(!shutdown_requested());
    }

    #[test]
    fn test_exit_codes_documented() {
        use crate::exit_codes::*;
        assert_eq!(EXIT_SUCCESS, 0);
        assert_eq!(EXIT_CONFIG_ERROR, 1);
        assert_eq!(EXIT_PARTIAL_DLQ, 2);
        assert_eq!(EXIT_FATAL_DATA, 3);
        assert_eq!(EXIT_IO_ERROR, 4);
        assert_eq!(EXIT_INTERRUPTED, 130);
    }
}
