//! Phase 14 correlated DLQ tests — grouped rejection with sort-and-group pattern.
//!
//! Tests: one-fail-DLQs-group, trigger flag, null key individual rejection,
//! compound keys, auto-sort injection, --explain output, large groups,
//! buffer overflow, threshold counting.

use super::*;
