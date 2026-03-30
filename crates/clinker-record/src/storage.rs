//! Trait for indexed record storage.
//!
//! Defined in the foundation crate so `RecordView` and `WindowContext`
//! can reference it without depending on `clinker-core`.

use crate::Value;

/// Indexed record storage backend. Arena implements this in `clinker-core`.
///
/// Must be `Send + Sync` — the Arena is immutable after construction
/// and shared across rayon workers during Phase 2.
pub trait RecordStorage: Send + Sync {
    /// Resolve a field by name from the record at the given index.
    fn resolve_field(&self, index: u32, name: &str) -> Option<Value>;

    /// Resolve a qualified field (source.field) from the record at the given index.
    fn resolve_qualified(&self, index: u32, source: &str, field: &str) -> Option<Value>;

    /// List all available field names for the record at the given index.
    fn available_fields(&self, index: u32) -> Vec<&str>;

    /// Total number of records in this storage.
    fn record_count(&self) -> u32;
}
