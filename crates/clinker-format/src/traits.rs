use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

use crate::envelope::{EnvelopeConfig, EnvelopeEvent};
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

    /// One-time envelope pre-scan for the current file, run by the
    /// executor's source ingest before any `next_record` call. Each
    /// declared section in `config.sections` resolves to a
    /// [`Value::Map`] of typed field values keyed by the section's
    /// declared field names; the returned map is then attached to
    /// every body record's `Arc<DocumentContext>`.
    ///
    /// Default impl returns an empty map — readers that don't yet
    /// support envelope extraction (CSV, fixed-width pending #101) or
    /// that the config asked nothing of (no declared sections) take
    /// the no-op path. Format-specific implementations (XML, JSON) are
    /// added per-reader; if `config.sections` declares an extract rule
    /// the reader does not support, that reader returns a format
    /// error surfacing the mismatch at startup rather than mid-stream.
    fn prepare_document(
        &mut self,
        _config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        Ok(IndexMap::new())
    }

    /// Drain the envelope-nesting events the reader queued while serving
    /// the most recent `next_record` (or its end-of-input transition).
    /// The source ingest driver polls this after every `next_record` and
    /// once more at end-of-input, applying each [`EnvelopeEvent`] to its
    /// document-level stack — `OpenLevel` opens a nested document context,
    /// `CloseLevel` closes the innermost.
    ///
    /// Default impl returns an empty `Vec` — single-level envelope formats
    /// (CSV, fixed-width, XML, JSON, EDIFACT) never nest mid-file, so they
    /// open exactly one document per file via `prepare_document` and never
    /// queue an event. Multi-level formats (EDI X12 ISA/GS/ST) override
    /// this to surface their envelope boundaries. Wrappers holding an
    /// inner reader (`CoercingReader`, `MultiFileFormatReader`,
    /// `TakeReader`) must delegate so a nested-envelope source streamed
    /// through them keeps emitting boundaries.
    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        Vec::new()
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
