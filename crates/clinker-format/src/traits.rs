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

    /// Borrow the path of the file that produced the most-recently-
    /// emitted record. Returns `None` for single-file readers (the
    /// caller falls back to the source's static path); multi-file
    /// readers override this to expose the per-file `Arc<str>` that
    /// changes as the wrapper advances across file boundaries.
    ///
    /// Wrappers (e.g. `CoercingReader`) that hold an inner reader
    /// must delegate to it.
    fn current_source_file(&self) -> Option<&Arc<str>> {
        None
    }
}

/// Streaming record writer. Consumes records one at a time.
///
/// Writer stores `Arc<Schema>` internally (passed at construction).
/// Must be `Send` for executor ownership transfer; not `Sync`.
pub trait FormatWriter: Send {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError>;
    fn flush(&mut self) -> Result<(), FormatError>;

    /// Bytes written to the underlying I/O sink since this writer was created.
    /// Returns `None` if byte counting is not enabled for this writer.
    /// Used by `SplittingWriter` for byte-limit rotation and by `StageMetrics`
    /// for per-stage write accounting.
    fn bytes_written(&self) -> Option<u64> {
        None
    }
}
