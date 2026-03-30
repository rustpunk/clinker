//! Process-wide shutdown flag for graceful SIGINT/SIGTERM handling.
//!
//! The signal handler is installed in Task 6.5. This module provides
//! the `shutdown_requested()` check used at chunk boundaries (Phase 2)
//! and during Arena::build (Phase 1).

use std::sync::atomic::{AtomicBool, Ordering};

/// Process-wide shutdown flag, set by the signal handler.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

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
