//! Phase 14 splitting tests — `SplittingWriter` and `CountingWriter`.
//!
//! Tests: record count splitting, byte size splitting, key-group preservation,
//! oversize group policies, CSV header repetition, file naming, CountingWriter accuracy.

use super::*;
