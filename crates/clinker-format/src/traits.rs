use std::sync::Arc;

use clinker_record::{Record, Schema};

use crate::error::FormatError;

/// Streaming record reader. Yields records one at a time.
///
/// `&mut self` on `schema()` because some formats (e.g. CSV) must read
/// the first row to discover column names. Must be `Send` for executor
/// ownership transfer; not `Sync` — single-threaded streaming.
pub trait FormatReader: Send {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError>;
    fn next_record(&mut self) -> Result<Option<Record>, FormatError>;
}

/// Streaming record writer. Consumes records one at a time.
///
/// Writer stores `Arc<Schema>` internally (passed at construction).
/// Must be `Send` for executor ownership transfer; not `Sync`.
pub trait FormatWriter: Send {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError>;
    fn flush(&mut self) -> Result<(), FormatError>;
}
