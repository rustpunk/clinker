//! Pipeline exit codes per spec §10.2 + Unix signal convention.

/// Success — zero errors, all records processed.
pub const EXIT_SUCCESS: i32 = 0;

/// Config parse error or CXL compile error — no data processed.
pub const EXIT_CONFIG_ERROR: i32 = 1;

/// Partial success — some rows routed to DLQ, pipeline completed.
pub const EXIT_PARTIAL_DLQ: i32 = 2;

/// Fatal data error — fail_fast triggered, error threshold exceeded,
/// or NaN in group_by key.
pub const EXIT_FATAL_DATA: i32 = 3;

/// I/O or system error — file not found, permission denied, disk full.
pub const EXIT_IO_ERROR: i32 = 4;

/// Interrupted by SIGINT (Ctrl-C) or SIGTERM. Unix convention: 128 + signal.
pub const EXIT_INTERRUPTED: i32 = 130;
