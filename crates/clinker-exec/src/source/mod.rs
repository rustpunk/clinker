//! Source-side multi-file ingestion.
//!
//! File-set discovery — resolving a [`clinker_plan::config::SourceConfig`]
//! matcher (`path` / `glob` / `regex` / `paths`) and applying the
//! post-discovery filters — lives in [`clinker_plan::config::discovery`], so
//! the planner's `--explain` spill-volume estimate and the runtime ingest
//! path resolve the same file set. This module owns the multi-file format
//! streaming that consumes that resolved set (header skipping across files,
//! per-file `RecordProvenance` Arc swaps).

pub mod multi_file;

use std::sync::Arc;

use clinker_format::traits::FormatReader;
use clinker_format::{EnvelopeConfig, EnvelopeEvent, FormatError};
use clinker_record::{Record, Schema, Value};
use indexmap::IndexMap;

/// Transport-agnostic record yielder driving one Source's ingest thread.
///
/// Generalizes the byte-oriented [`FormatReader`] contract: where a
/// `FormatReader` decodes a `Box<dyn Read>` byte stream, a `RecordSource`
/// makes no byte-stream assumption. A file transport reaches this contract
/// by wrapping its `FormatReader` (see the blanket impl below); a network
/// transport that yields rows without a byte body (a SQL `SELECT` cursor)
/// implements `RecordSource` directly. The executor's ingest loop drives
/// either through these four methods identically — the widening,
/// document-boundary, watermark, and channel-push work downstream of
/// `next_record` is shared by both arms.
///
/// Must be `Send` for the per-Source `std::thread` to own it; not `Sync`
/// — each source is single-threaded streaming.
pub trait RecordSource: Send {
    /// Resolve the record schema. `&mut self` because some sources (CSV,
    /// a cursor that must issue its query) discover columns only after
    /// reading the first row / executing the statement.
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError>;

    /// Yield the next record, or `None` at end of input. Finite by
    /// contract — every transport EOFs after exhausting its cursor.
    fn next_record(&mut self) -> Result<Option<Record>, FormatError>;

    /// Borrow the originating file path of the most-recently-emitted
    /// record, when the transport has a per-record file identity (a
    /// multi-file reader swaps this `Arc` as it crosses file boundaries).
    /// Returns `None` for single-file and pathless transports; the
    /// ingest loop then falls back to the source's stable synthetic id.
    fn current_source_file(&self) -> Option<&Arc<str>> {
        None
    }

    /// One-time envelope pre-scan for the current document, run before
    /// any `next_record` call. Mirrors [`FormatReader::prepare_document`]:
    /// each declared section resolves to a [`Value::Map`] of typed field
    /// values keyed by the section's field names. The default returns an
    /// empty map for transports without envelope semantics.
    fn prepare_document(
        &mut self,
        _config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        Ok(IndexMap::new())
    }

    /// Drain the envelope-nesting events the source queued while serving
    /// the most recent `next_record` (or its end-of-input transition).
    /// Mirrors [`FormatReader::take_envelope_events`]: the ingest driver
    /// polls this after every `next_record` and once at end-of-input,
    /// opening/closing nested document contexts per event. The default
    /// returns an empty `Vec` for transports without multi-level envelope
    /// semantics (every transport shipping today).
    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        Vec::new()
    }

    /// Hand the source its run shutdown handle so it can poll for
    /// cancellation at page/row-batch boundaries and stop cleanly
    /// (returning `Ok(None)` like a normal EOF). The ingest driver
    /// injects the token before the first `next_record`. The default is
    /// a no-op: the file arm relies on the dropped-receiver stop signal,
    /// so it ignores the token. Network readers holding a live socket
    /// override this to bound cancellation latency.
    fn set_shutdown_token(&mut self, _token: crate::pipeline::shutdown::ShutdownToken) {}

    /// Abandon the current file and advance to the next, returning `Ok(true)`
    /// when a next file opened or `Ok(false)` when none remain. Mirrors
    /// [`FormatReader::advance_to_next_file`]: the ingest driver calls this
    /// after dead-lettering a whole file for a structural-count failure under
    /// `dlq_granularity: document`, so a multi-file source keeps reading its
    /// remaining files past the malformed one. The default returns `Ok(false)`
    /// for single-file and pathless transports.
    ///
    /// # Errors
    ///
    /// Surfaces the next file's reader-construction or schema-mismatch error.
    fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
        Ok(false)
    }
}

/// Byte-stream transports reach the row-yielding contract by delegating
/// to their underlying [`FormatReader`]. This is the file-transport arm:
/// the executor builds a `MultiFileFormatReader` (itself a `FormatReader`),
/// wraps it for schema coercion, then hands the `Box<dyn FormatReader>` to
/// the shared ingest loop as a `RecordSource`.
impl RecordSource for Box<dyn FormatReader> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        (**self).schema()
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        (**self).next_record()
    }

    fn current_source_file(&self) -> Option<&Arc<str>> {
        (**self).current_source_file()
    }

    fn prepare_document(
        &mut self,
        config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        (**self).prepare_document(config)
    }

    fn take_envelope_events(&mut self) -> Vec<EnvelopeEvent> {
        (**self).take_envelope_events()
    }

    fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
        (**self).advance_to_next_file()
    }
}

/// What a declared Source feeds into its ingest thread, generalized off
/// the file-slot model so a non-file transport registers without being
/// forced through the file abstractions.
///
/// - `Files`: the byte-stream file transport. The discovery layer
///   produced one [`multi_file::FileSlot`] per matched file; the executor
///   concatenates them via [`multi_file::MultiFileFormatReader`] and
///   stamps each record with its originating file.
/// - `Records`: any non-file transport, already a row yielder. The
///   executor drives its [`RecordSource`] directly — no `Box<dyn Read>`,
///   no `MultiFileFormatReader`, no fs discovery.
///
/// Both arms feed the identical `SourceIngestChannel`; the dispatcher
/// sees only the paired `Receiver<StreamEvent>` and never branches on
/// transport.
pub enum SourceInput {
    /// File transport: ordered file slots concatenated into one stream.
    Files(Vec<multi_file::FileSlot>),
    /// Non-file transport: a ready-to-drive row yielder.
    Records(Box<dyn RecordSource>),
}
