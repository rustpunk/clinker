//! Output file splitting — format-agnostic `SplittingWriter` with key-group-preserving rotation.
//!
//! Delegates format-specific concerns (headers, preambles, footers) entirely
//! to the writer factory. On rotation: flush + drop current writer, create
//! new file via file factory, create new format writer via writer factory.
//!
//! Architecture: Beam FileIO.Sink pattern — the splitter is a pure rotation
//! orchestrator, the factory owns format lifecycle.

use std::alloc::Layout;
use std::io::{self, Write};

use clinker_record::owned_storage::{
    AllocationLease, AllocationScope, ResourceError, SharedStorage,
};
use clinker_record::{DocumentContext, GroupByKey, Record, Schema, Value, value_to_group_key};

use crate::counting::{CountingWriter, SharedByteCounter};
use crate::error::FormatError;
use crate::traits::{FormatWriter, FormatWriterHandle};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// SplitPolicy + OversizeGroupPolicy
// ---------------------------------------------------------------------------

/// Runtime split configuration for `SplittingWriter`.
///
/// Converted from `SplitConfig` (serde) in the executor at construction time.
/// Follows the `CsvOutputOptions` → `CsvWriterConfig` pattern.
#[derive(Debug, Clone)]
pub struct SplitPolicy {
    /// Soft record count limit per file.
    pub max_records: Option<u64>,
    /// Soft byte size limit per file.
    pub max_bytes: Option<u64>,
    /// Optional field name — never split mid-group.
    pub group_key: Option<String>,
    /// Behavior when a single key group exceeds file limits.
    pub oversize_group: OversizeGroupPolicy,
}

/// Policy for when a single key group exceeds the file size/record limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OversizeGroupPolicy {
    /// Log a warning and allow the oversized file.
    #[default]
    Warn,
    /// Return a `FormatError` — pipeline stops.
    Error,
    /// Silently allow the oversized file.
    Allow,
}

// ---------------------------------------------------------------------------
// Factories
// ---------------------------------------------------------------------------

/// Factory that creates a new raw I/O sink for each split file.
/// `seq` is the 1-based file sequence number.
pub type FileFactory = Box<dyn Fn(u32) -> io::Result<Box<dyn Write + Send>> + Send>;

type FactoryFn = dyn Fn(
        CountingWriter<Box<dyn Write + Send>>,
        SharedStorage<Schema>,
    ) -> Result<FormatWriterHandle, FormatError>
    + Send;

/// Retains a writer-construction closure and its backing allocation admission.
///
/// Called on each rotation, including the first file. Captured format state
/// (such as a CSV header shared between files) retains its separate owners.
/// The admitted closure remains charged until its Box backing is deallocated;
/// this handle stays inline and never exposes the Box or its lease.
///
/// The owned callable cannot be detached from its charge:
///
/// ```compile_fail
/// use clinker_format::splitting::WriterFactory;
/// fn detach(factory: WriterFactory) {
///     let _callable = factory.callable;
/// }
/// ```
///
/// The backing lease cannot be independently released:
///
/// ```compile_fail
/// use clinker_format::splitting::WriterFactory;
/// fn release(factory: WriterFactory) {
///     drop(factory._allocation);
/// }
/// ```
pub struct WriterFactory {
    // Field order is load-bearing: dropping the Box must finish deallocation
    // before the external lease releases its exact concrete closure charge.
    callable: Box<FactoryFn>,
    _allocation: Option<AllocationLease>,
}

impl WriterFactory {
    /// Reserve the actual concrete closure layout before fallible allocation.
    ///
    /// This admits only the closure backing. Captured allocations need their
    /// own owners; it does not infer their sizes from the closure layout.
    /// Allocation failure drops the intact closure while its backing grant is
    /// still live.
    ///
    /// # Errors
    /// Returns the authority's refusal or the backing allocation failure.
    pub fn try_new<F>(factory: F, scope: &AllocationScope) -> Result<Self, ResourceError>
    where
        F: Fn(
                CountingWriter<Box<dyn Write + Send>>,
                SharedStorage<Schema>,
            ) -> Result<FormatWriterHandle, FormatError>
            + Send
            + 'static,
    {
        let allocation = scope.reserve(Layout::new::<F>())?;
        let callable = crate::reserved::try_box(factory).map_err(|(error, factory)| {
            drop(factory);
            error
        })?;
        Ok(Self {
            callable,
            _allocation: Some(allocation),
        })
    }

    /// Preserve an unchanged non-CSV implementation or test probe at migration.
    ///
    /// This explicitly leaves the closure backing ungoverned. CSV factories
    /// and newly changed production closure allocations must use [`Self::try_new`].
    pub fn from_legacy<F>(factory: F) -> Self
    where
        F: Fn(
                CountingWriter<Box<dyn Write + Send>>,
                SharedStorage<Schema>,
            ) -> Result<FormatWriterHandle, FormatError>
            + Send
            + 'static,
    {
        Self {
            callable: Box::new(factory),
            _allocation: None,
        }
    }

    /// Construct one writer while retaining this factory's closure ownership.
    ///
    /// The callback determines whether construction performs I/O. The returned
    /// writer keeps its own backing admission independently of the factory.
    pub fn create(
        &self,
        counting: CountingWriter<Box<dyn Write + Send>>,
        schema: SharedStorage<Schema>,
    ) -> Result<FormatWriterHandle, FormatError> {
        (self.callable)(counting, schema)
    }
}

// ---------------------------------------------------------------------------
// SplittingWriter
// ---------------------------------------------------------------------------

/// File-splitting wrapper — format-agnostic.
///
/// Delegates format-specific concerns (headers, preambles, footers) entirely
/// to the writer factory. On rotation: flush + drop current writer, create
/// new file via file factory, create new format writer via writer factory.
///
/// Rotation trigger: `(limit_reached AND key_changed)` when `group_key` is
/// set, or `limit_reached` for mechanical splitting without `group_key`.
/// Greedy split at first key boundary after threshold is optimal (ICDT 2009).
pub struct SplittingWriter {
    file_factory: FileFactory,
    writer_factory: WriterFactory,
    schema: SharedStorage<Schema>,
    policy: SplitPolicy,
    current_writer: Option<FormatWriterHandle>,
    /// Shared byte counter — owned by SplittingWriter, passed to each
    /// `CountingWriter` on rotation. Reset to zero on file open.
    byte_counter: SharedByteCounter,
    /// Records written to the *current* file (reset on rotation).
    records_in_file: u64,
    current_key: Option<Vec<GroupByKey>>,
    file_seq: u32,
    /// Whether an oversize warning has been emitted for the current group.
    oversize_warned: bool,
    /// Cumulative bytes written across all rotated files (excludes current file).
    total_bytes: u64,
}

impl SplittingWriter {
    pub fn new(
        file_factory: FileFactory,
        writer_factory: WriterFactory,
        schema: SharedStorage<Schema>,
        policy: SplitPolicy,
    ) -> Self {
        Self {
            file_factory,
            writer_factory,
            schema,
            policy,
            current_writer: None,
            byte_counter: SharedByteCounter::new(),
            records_in_file: 0,
            current_key: None,
            file_seq: 0,
            oversize_warned: false,
            total_bytes: 0,
        }
    }

    /// Open a new split file via the factories.
    fn open_new_file(&mut self) -> Result<(), FormatError> {
        self.file_seq += 1;
        let raw = (self.file_factory)(self.file_seq).map_err(FormatError::Io)?;
        self.total_bytes += self.byte_counter.bytes_written();
        self.byte_counter.reset();
        let counting = CountingWriter::new(raw, self.byte_counter.clone());
        let writer = self.writer_factory.create(counting, self.schema.clone())?;
        self.current_writer = Some(writer);
        self.records_in_file = 0;
        self.oversize_warned = false;
        Ok(())
    }

    /// Flush and close the current writer, preparing for rotation.
    fn rotate_file(&mut self) -> Result<(), FormatError> {
        if let Some(ref mut writer) = self.current_writer {
            writer.flush()?;
        }
        self.current_writer = None;
        self.open_new_file()?;
        Ok(())
    }

    /// Extract group key from a record, if `group_key` is configured.
    fn extract_group_key(&self, record: &Record) -> Result<Option<Vec<GroupByKey>>, FormatError> {
        let field_name = match &self.policy.group_key {
            Some(name) => name,
            None => return Ok(None),
        };

        let val = record.get(field_name).unwrap_or(&Value::Null);
        match value_to_group_key(val, field_name, 0) {
            Ok(Some(key)) => Ok(Some(vec![key])),
            // Null key treated as its own distinct group (test_split_null_key_treated_as_group)
            Ok(None) => Ok(Some(vec![GroupByKey::Null])),
            Err(e) => Err(FormatError::InvalidRecord {
                row: 0,
                message: format!("group key extraction failed: {e:?}"),
            }),
        }
    }

    /// Check if either record count or byte limit is reached for the current file.
    fn is_limit_reached(&self) -> bool {
        if let Some(max) = self.policy.max_records
            && self.records_in_file >= max
        {
            return true;
        }
        if let Some(max) = self.policy.max_bytes
            && self.current_writer.is_some()
            && self.byte_counter.bytes_written() >= max
        {
            return true;
        }
        false
    }

    /// Handle oversize group based on policy.
    fn handle_oversize_group(&mut self) -> Result<(), FormatError> {
        if self.oversize_warned {
            return Ok(()); // already handled for this group
        }
        self.oversize_warned = true;

        match self.policy.oversize_group {
            OversizeGroupPolicy::Allow => Ok(()),
            OversizeGroupPolicy::Warn => {
                tracing::warn!(
                    group_key = ?self.current_key,
                    records = self.records_in_file,
                    "oversize group: single key group exceeds split limit"
                );
                Ok(())
            }
            OversizeGroupPolicy::Error => Err(FormatError::InvalidRecord {
                row: 0,
                message: format!(
                    "oversize group: single key group exceeds split limit ({} records)",
                    self.records_in_file,
                ),
            }),
        }
    }
}

impl FormatWriter for SplittingWriter {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Lazy-open first file if no current writer
        if self.current_writer.is_none() {
            self.open_new_file()?;
        }

        // Extract group key if configured
        let new_key = self.extract_group_key(record)?;

        // Check rotation: limit reached AND key changed (or no group_key → exact limit)
        let limit_reached = self.is_limit_reached();
        let key_changed = match (&self.current_key, &new_key) {
            (Some(current), Some(new_k)) => current != new_k,
            _ => false,
        };

        let should_rotate = if self.policy.group_key.is_some() {
            limit_reached && key_changed
        } else {
            limit_reached // Mechanical split at exact limit
        };

        if should_rotate {
            self.rotate_file()?;
        }

        // Check for oversize group (limit reached but can't rotate — same key)
        if limit_reached && !should_rotate && self.policy.group_key.is_some() {
            self.handle_oversize_group()?;
        }

        // Update current key
        if let Some(key) = new_key {
            self.current_key = Some(key);
        }

        // Write record
        self.current_writer.as_mut().unwrap().write_record(record)?;
        self.records_in_file += 1;

        // Push the format writer's internally buffered bytes through to the
        // CountingWriter so the byte-limit check sees the true on-disk size.
        // Uses the non-finalizing drain rather than `flush`: a finalizing flush
        // would close the JSON array / XML root / envelope trailer here, and the
        // next record would then land after the closed document, corrupting it.
        if self.policy.max_bytes.is_some() {
            self.current_writer.as_mut().unwrap().flush_bytes()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if let Some(ref mut writer) = self.current_writer {
            writer.flush()?;
        }
        Ok(())
    }

    /// Drain the active inner writer's buffered bytes without finalizing its
    /// document, keeping the non-finalizing contract intact when a split sink
    /// is itself driven by byte accounting. With no active writer there is
    /// nothing to drain.
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        if let Some(ref mut writer) = self.current_writer {
            writer.flush_bytes()?;
        }
        Ok(())
    }

    fn bytes_written(&self) -> Option<u64> {
        Some(self.total_bytes + self.byte_counter.bytes_written())
    }

    /// Forward per-document opening framing to the active inner writer so it
    /// reaches the real format writer rather than dying at this splitting
    /// shim. Opens the first file if none is active yet, so a `begin_document`
    /// preceding the document's first record still lands on a real writer.
    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        if self.current_writer.is_none() {
            self.open_new_file()?;
        }
        self.current_writer
            .as_mut()
            .expect("file opened above")
            .begin_document(doc)
    }

    /// Forward per-document closing framing to the active inner writer, if one
    /// is open. With no active writer (no record ever arrived) there is no
    /// framing to close.
    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        if let Some(writer) = self.current_writer.as_mut() {
            writer.end_document(doc)?;
        }
        Ok(())
    }
}
