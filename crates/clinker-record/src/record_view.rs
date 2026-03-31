//! Zero-allocation view into arena-backed record storage.

use crate::Value;
use crate::resolver::FieldResolver;
use crate::storage::RecordStorage;

/// Zero-allocation view into an arena-backed record.
///
/// 16 bytes: pointer + u32 index + padding. `Copy` and stack-allocated.
/// Implements `FieldResolver` by delegating to the underlying `RecordStorage`.
#[derive(Clone, Copy)]
pub struct RecordView<'a, S: RecordStorage + ?Sized> {
    storage: &'a S,
    index: u32,
}

impl<'a, S: RecordStorage + ?Sized> RecordView<'a, S> {
    /// Create a new view into the storage at the given record index.
    pub fn new(storage: &'a S, index: u32) -> Self {
        Self { storage, index }
    }

    /// The record index this view points to.
    pub fn index(&self) -> u32 {
        self.index
    }
}

impl<S: RecordStorage + ?Sized> FieldResolver for RecordView<'_, S> {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.storage.resolve_field(self.index, name)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.storage.resolve_qualified(self.index, source, field)
    }

    fn available_fields(&self) -> Vec<&str> {
        self.storage.available_fields(self.index)
    }
}

impl<S: RecordStorage + ?Sized> std::fmt::Debug for RecordView<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordView")
            .field("index", &self.index)
            .finish()
    }
}
