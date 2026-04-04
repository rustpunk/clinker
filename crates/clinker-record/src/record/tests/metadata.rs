//! Phase 14 metadata tests — per-record `$meta.*` key-value map.
//!
//! Tests: get_meta/set_meta round-trip, lazy init, clone independence,
//! multiple keys, overwrite, Send+Sync.

use super::super::*;
