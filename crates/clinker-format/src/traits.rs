use std::sync::Arc;

use clinker_record::owned_storage::{OwnedMap, SharedStorage};
use clinker_record::{DocumentContext, Record, Schema};
use indexmap::IndexMap;

use crate::envelope::{EnvelopeConfig, EnvelopeEvent};
use crate::error::FormatError;

/// Ordered structural events surfaced alongside a reader's record stream.
///
/// Most readers emit only nested [`EnvelopeEvent`] values. A multi-file
/// wrapper also emits the physical-file open/close transitions so an empty
/// file remains observable to execution barriers and document consumers.
#[derive(Debug, Clone)]
pub enum SourceLifecycleEvent {
    PhysicalFileOpen(Arc<str>),
    Envelope(EnvelopeEvent),
    PhysicalFileClose(Arc<str>),
}

/// Streaming record reader. Yields records one at a time.
///
/// `&mut self` on `schema()` because some formats (e.g. CSV) must read
/// the first row to discover column names. Must be `Send` for executor
/// ownership transfer; not `Sync` — single-threaded streaming.
pub trait FormatReader: Send {
    fn schema(&mut self) -> Result<SharedStorage<Schema>, FormatError>;
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
    /// [`Value::Map`](clinker_record::Value::Map) of typed field values keyed
    /// by the section's declared field names. The returned [`OwnedMap`] moves its backing
    /// allocation and section values to the caller, preserving any attached
    /// allocation grants. Wrappers must forward it without rebuilding or
    /// cloning it; ingest uses the sections to construct the
    /// `SharedStorage<DocumentContext>` attached to body records.
    ///
    /// Default impl returns an empty map — a reader takes the no-op
    /// path when the config asked nothing of it (no declared sections).
    /// The empty default uses legacy storage with no input-sized state.
    /// Multi-record CSV / fixed-width readers override this hook to extract
    /// their document sections. A *plain* single-schema CSV / fixed-width
    /// source that declares envelope sections never reaches this path:
    /// the planner rejects it (E356), because a plain flat file carries
    /// no header/trailer document to pre-scan. If
    /// `config.sections` declares an extract rule the reader does not
    /// support, that reader returns a format error surfacing the
    /// mismatch at startup rather than mid-stream.
    fn prepare_document(&mut self, _config: &EnvelopeConfig) -> Result<OwnedMap, FormatError> {
        Ok(OwnedMap::from_map(IndexMap::new()))
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

    /// Drain structural events queued while serving the most recent read.
    ///
    /// The default lifts the established nested-envelope stream into the
    /// unified lifecycle carrier. Wrappers must delegate this method; the
    /// multi-file wrapper additionally brackets every physical file, even
    /// when it contains no body records.
    fn take_source_lifecycle_events(&mut self) -> Vec<SourceLifecycleEvent> {
        self.take_envelope_events()
            .into_iter()
            .map(SourceLifecycleEvent::Envelope)
            .collect()
    }

    /// Abandon the file currently being read and advance to the next one,
    /// returning `Ok(true)` when a next file was opened (and
    /// [`Self::current_source_file`] now names it) or `Ok(false)` when no
    /// files remain.
    ///
    /// The ingest driver calls this after dead-lettering a whole file for a
    /// structural-integrity failure under `dlq_granularity: document`, to keep
    /// reading the remaining files of a multi-file source instead of stopping
    /// the source at the first malformed file. A trailer-count failure fires
    /// at the file's closing trailer, so the abandoned file is fully
    /// consumed; a mid-file structural failure (an unknown record-type
    /// discriminator under document granularity, or a body record after the
    /// trailer) abandons the file's unread remainder — either way the whole
    /// file is already condemned, so no record that should stream is lost.
    ///
    /// Default impl returns `Ok(false)`: a single-file reader has no next file.
    /// Wrappers holding an inner reader (`CoercingReader`) must delegate;
    /// [`MultiFileFormatReader`](crate) overrides it to advance its file cursor.
    ///
    /// # Errors
    ///
    /// Surfaces the next file's reader-construction or schema-mismatch error.
    fn advance_to_next_file(&mut self) -> Result<bool, FormatError> {
        Ok(false)
    }
}

/// Streaming record writer. Consumes records one at a time.
///
/// Writer stores `SharedStorage<Schema>` internally (passed at construction).
/// Must be `Send` for executor ownership transfer; not `Sync`.
pub trait FormatWriter: Send {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError>;
    fn flush(&mut self) -> Result<(), FormatError>;

    /// Push bytes buffered inside this writer through to the underlying I/O
    /// sink *without* emitting the document's closing framing. Called by
    /// [`SplittingWriter`](crate::splitting::SplittingWriter) after every
    /// record when a byte limit is configured, so the shared byte counter
    /// reflects the true on-disk size before the next rotation check.
    ///
    /// Distinct from [`Self::flush`] on purpose: for the whole-file-framed
    /// formats `flush` finalizes the document (JSON's closing `]`, XML's
    /// closing root element, an EDI interchange trailer), and finalizing after
    /// every record would close the document mid-stream — later records then
    /// land after the close and corrupt the file. The default forwards to
    /// [`Self::flush`], correct for record-oriented writers whose `flush`
    /// emits no closing framing (CSV, fixed-width); finalizing formats
    /// override it to drain only the underlying sink.
    ///
    /// Wrapper writers that hold an inner writer
    /// ([`CountedFormatWriter`](crate::counting::CountedFormatWriter),
    /// [`SplittingWriter`](crate::splitting::SplittingWriter)) must delegate to the inner writer's
    /// `flush_bytes` — taking this default would replace a finalizing inner
    /// writer's non-finalizing drain with its finalizing `flush`.
    ///
    /// # Errors
    ///
    /// Surfaces any I/O error draining the buffer.
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.flush()
    }

    /// Emit any per-document opening framing (an envelope header) before the
    /// document's first body record streams. Called by the Output dispatch
    /// arm on the first record of each document (boundaries are detected from
    /// each record's `doc_ctx().source_file()`), passing the same
    /// [`DocumentContext`] the body records carry so the writer can read its
    /// envelope sections. The body records then flow through
    /// [`Self::write_record`] one at a time, and [`Self::end_document`] closes
    /// the framing — no document is ever buffered, so a writer that renders an
    /// envelope still streams at O(1-record).
    ///
    /// Default impl is a no-op: a writer that does not reconstruct envelopes
    /// (every writer today) ignores document boundaries entirely, leaving its
    /// output byte-identical to the boundary-unaware path.
    ///
    /// Wrapper writers that hold an inner writer
    /// ([`CountedFormatWriter`](crate::counting::CountedFormatWriter),
    /// [`SplittingWriter`](crate::splitting::SplittingWriter)) must forward this hook to the inner writer,
    /// or an enveloped inner writer's per-document framing is silently dropped.
    ///
    /// # Errors
    ///
    /// Surfaces any I/O error emitting the opening framing.
    fn begin_document(&mut self, _doc: &DocumentContext) -> Result<(), FormatError> {
        Ok(())
    }

    /// Emit any per-document closing framing (an envelope footer / trailer)
    /// after the document's last body record has been written. Called by the
    /// Output dispatch arm when the document ends — the next record's
    /// `source_file` differs, or the input is exhausted — paired with the
    /// [`Self::begin_document`] that opened it.
    ///
    /// Default impl is a no-op, mirroring [`Self::begin_document`]; wrapper
    /// writers holding an inner writer must forward it for the same reason.
    ///
    /// # Errors
    ///
    /// Surfaces any I/O error emitting the closing framing.
    fn end_document(&mut self, _doc: &DocumentContext) -> Result<(), FormatError> {
        Ok(())
    }

    /// Bytes written to the underlying I/O sink since this writer was created.
    /// Returns `None` if byte counting is not enabled for this writer.
    /// Used by `SplittingWriter` for byte-limit rotation and by `StageMetrics`
    /// for per-stage write accounting.
    ///
    /// A wrapper writer holding an inner writer must forward this hook so a
    /// byte-counting inner writer's total is not masked by this `None` default.
    fn bytes_written(&self) -> Option<u64> {
        None
    }

    /// Values this writer cut to fit a column under `truncation: warn`, over
    /// every record it delivered. `None` for a writer with no truncation
    /// policy (every format but fixed-width) or one that truncated nothing.
    ///
    /// Reads storage the writer sized when it was built; building the summary
    /// allocates only its own schema-bounded result, so call it once, when the
    /// output is done.
    ///
    /// A wrapper writer holding an inner writer must forward this hook, or the
    /// inner writer's truncations never reach the run report.
    fn truncation_summary(&self) -> Option<crate::truncation::TruncationSummary> {
        None
    }
}

/// Unique writer owner that retains backing admission through deallocation.
///
/// Construction admits the concrete writer's layout before allocation. The
/// writer's internal buffers must have their own owners; this handle accounts
/// only for the outer box. Moving the handle does not allocate. Dropping it
/// destroys and frees the writer before releasing its backing charge.
///
/// No raw box or detachable lease is exposed:
/// ```compile_fail
/// fn extract(writer: clinker_format::FormatWriterHandle) {
///     let _ = writer.inner;
/// }
/// ```
pub struct FormatWriterHandle {
    // Field order is load-bearing: Box deallocation precedes lease release.
    inner: Box<dyn FormatWriter>,
    _allocation: Option<clinker_record::owned_storage::AllocationLease>,
}
impl FormatWriterHandle {
    /// Admit and allocate one concrete writer fallibly. On refusal the intact
    /// writer is dropped, retaining any resources its payload already owns.
    pub fn try_new<T: FormatWriter + 'static>(
        writer: T,
        scope: &clinker_record::owned_storage::AllocationScope,
    ) -> Result<Self, clinker_record::owned_storage::ResourceError> {
        let allocation = scope.reserve(std::alloc::Layout::new::<T>())?;
        let inner = clinker_record::owned_storage::try_box(writer).map_err(|(error, writer)| {
            drop(writer);
            error
        })?;
        Ok(Self {
            inner,
            _allocation: Some(allocation),
        })
    }

    /// Retain an existing legacy writer without claiming allocation admission.
    /// This migration boundary is for unchanged implementations; it cannot
    /// establish a finite-resource contract for a newly allocated writer.
    pub fn from_legacy(writer: Box<dyn FormatWriter>) -> Self {
        Self {
            inner: writer,
            _allocation: None,
        }
    }
}
impl std::ops::Deref for FormatWriterHandle {
    type Target = dyn FormatWriter;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}
impl std::ops::DerefMut for FormatWriterHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut()
    }
}
impl AsMut<dyn FormatWriter> for FormatWriterHandle {
    fn as_mut(&mut self) -> &mut (dyn FormatWriter + 'static) {
        self.inner.as_mut()
    }
}
impl FormatWriter for FormatWriterHandle {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.inner.write_record(record)
    }
    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()
    }
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.inner.flush_bytes()
    }
    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.begin_document(doc)
    }
    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.inner.end_document(doc)
    }
    fn bytes_written(&self) -> Option<u64> {
        self.inner.bytes_written()
    }
    fn truncation_summary(&self) -> Option<crate::truncation::TruncationSummary> {
        self.inner.truncation_summary()
    }
}

/// Shared test fixtures for the `FormatWriter` wrapper-delegation contract.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex};

    use clinker_record::{DocumentContext, Record};

    use crate::error::FormatError;
    use crate::traits::FormatWriter;

    /// A `FormatWriter` that appends a label for every lifecycle hook it
    /// receives into a shared log, so a wrapper test can prove the wrapper
    /// delegates each hook to its inner writer rather than silently taking a
    /// trait default. Labels: `write`, `flush`, `flush_bytes`, `begin:<file>`,
    /// `end:<file>`. Shared by the `CountedFormatWriter` and `SplittingWriter`
    /// delegation tests so both assert against one fixture.
    pub(crate) struct HookProbe {
        log: Arc<Mutex<Vec<String>>>,
        /// Records this probe (not the shared log) received.
        writes: u64,
    }

    impl HookProbe {
        /// Build a probe that records into `log`, letting a caller (e.g. a
        /// split writer factory) construct the probe while retaining its own
        /// handle on the shared log.
        pub(crate) fn with_log(log: Arc<Mutex<Vec<String>>>) -> Self {
            Self { log, writes: 0 }
        }

        fn record(&self, entry: impl Into<String>) {
            self.log.lock().unwrap().push(entry.into());
        }
    }

    impl FormatWriter for HookProbe {
        fn write_record(&mut self, _record: &Record) -> Result<(), FormatError> {
            self.record("write");
            self.writes += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<(), FormatError> {
            self.record("flush");
            Ok(())
        }

        fn flush_bytes(&mut self) -> Result<(), FormatError> {
            self.record("flush_bytes");
            Ok(())
        }

        fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
            self.record(format!("begin:{}", doc.source_file()));
            Ok(())
        }

        fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
            self.record(format!("end:{}", doc.source_file()));
            Ok(())
        }

        /// One `probe` column truncated once per record written, listing this
        /// writer's own 1-based record numbers.
        fn truncation_summary(&self) -> Option<crate::truncation::TruncationSummary> {
            use crate::truncation::{
                ColumnTruncation, TRUNCATION_EXAMPLE_LIMIT, TruncationSummary,
            };
            let writes = self.writes;
            (writes > 0).then(|| TruncationSummary {
                columns: vec![ColumnTruncation {
                    column: "probe".into(),
                    width: 1,
                    cells: writes,
                    longest_bytes: 2,
                    example_records: (1..=writes.min(TRUNCATION_EXAMPLE_LIMIT as u64)).collect(),
                    more_records: writes > TRUNCATION_EXAMPLE_LIMIT as u64,
                }],
            })
        }
    }
}

#[cfg(test)]
mod writer_owner_tests {
    use super::*;
    use crate::preparation::MemoryOnlyResources;
    use clinker_record::{DocumentId, EnvelopeRecord};
    use std::num::NonZeroUsize;

    struct Probe(u64);
    impl FormatWriter for Probe {
        fn write_record(&mut self, _: &Record) -> Result<(), FormatError> {
            self.0 |= 1;
            Ok(())
        }
        fn flush(&mut self) -> Result<(), FormatError> {
            self.0 |= 2;
            Ok(())
        }
        fn flush_bytes(&mut self) -> Result<(), FormatError> {
            self.0 |= 4;
            Ok(())
        }
        fn begin_document(&mut self, _: &DocumentContext) -> Result<(), FormatError> {
            self.0 |= 8;
            Ok(())
        }
        fn end_document(&mut self, _: &DocumentContext) -> Result<(), FormatError> {
            self.0 |= 16;
            Ok(())
        }
        fn bytes_written(&self) -> Option<u64> {
            Some(self.0)
        }
        fn truncation_summary(&self) -> Option<crate::truncation::TruncationSummary> {
            Some(crate::truncation::TruncationSummary {
                columns: vec![crate::truncation::ColumnTruncation {
                    column: "probe".into(),
                    width: 1,
                    cells: self.0,
                    longest_bytes: 2,
                    example_records: vec![],
                    more_records: false,
                }],
            })
        }
    }

    #[test]
    fn owned_writer_forwards_every_hook_and_byte_counter() {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024).unwrap());
        let scope = provider.resources().scope().unwrap();
        for admitted in [true, false] {
            let mut writer = if admitted {
                FormatWriterHandle::try_new(Probe(0), scope.allocation()).unwrap()
            } else {
                FormatWriterHandle::from_legacy(Box::new(Probe(0)))
            };
            let record = Record::new(
                SharedStorage::from_arc(Arc::new(Schema::new(vec![]))),
                vec![],
            );
            let doc = DocumentContext::new(
                DocumentId::next(),
                Arc::from("input.csv"),
                EnvelopeRecord::empty(),
            );
            assert_eq!(writer.bytes_written(), Some(0));
            writer.write_record(&record).unwrap();
            assert_eq!(writer.bytes_written(), Some(1));
            writer.flush_bytes().unwrap();
            assert_eq!(writer.bytes_written(), Some(5));
            writer.begin_document(&doc).unwrap();
            assert_eq!(writer.bytes_written(), Some(13));
            writer.end_document(&doc).unwrap();
            assert_eq!(writer.bytes_written(), Some(29));
            writer.as_mut().flush().unwrap();
            assert_eq!(writer.bytes_written(), Some(31));
            assert_eq!(
                writer
                    .truncation_summary()
                    .map(|summary| summary.total_cells()),
                Some(31)
            );
            drop(writer);
            assert_eq!(provider.used(), 0);
        }
    }
}
