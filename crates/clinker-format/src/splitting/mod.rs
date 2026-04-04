//! Output file splitting — `SplittingWriter` with key-group-preserving rotation.
//!
//! Wraps a `FormatWriter` inside the writer thread. Uses factory pattern
//! to create new writer instances on rotation. Captures header from first
//! file and reuses for all splits.

use std::io::{self, Write};
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, Value, value_to_group_key};

use crate::csv::writer::{CsvWriter, CsvWriterConfig};
use crate::error::FormatError;
use crate::traits::FormatWriter;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// CountingWriter
// ---------------------------------------------------------------------------

/// Counts bytes written through the inner writer.
///
/// Wraps `BufWriter` — counts format-output bytes (pre-buffer I/O).
/// Flush mandatory on rotation (SO #23452660).
pub struct CountingWriter<W: Write> {
    inner: W,
    bytes_written: u64,
}

impl<W: Write> CountingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

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
    /// CSV: repeat header row in each split file.
    pub repeat_header: bool,
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
// SplittingWriter
// ---------------------------------------------------------------------------

/// File-splitting wrapper around `CsvWriter`.
///
/// Lives inside the writer thread (7/7 systems surveyed place rotation
/// inside the writer). Uses factory pattern to create new writer instances
/// on rotation. Captures header from first file and reuses for all splits.
///
/// Rotation trigger: `(limit_reached AND key_changed)` when `group_key` is
/// set, or `limit_reached` for mechanical splitting without `group_key`.
/// Greedy split at first key boundary after threshold is optimal (ICDT 2009).
pub struct SplittingWriter<F>
where
    F: Fn(u32) -> io::Result<Box<dyn Write + Send>> + Send,
{
    factory: F,
    schema: Arc<Schema>,
    writer_config: CsvWriterConfig,
    policy: SplitPolicy,
    current_writer: Option<CsvWriter<CountingWriter<Box<dyn Write + Send>>>>,
    /// Header captured from first file, reused for all splits.
    captured_header: Option<Vec<Box<str>>>,
    /// Records written to the *current* file (reset on rotation).
    records_in_file: u64,
    current_key: Option<Vec<GroupByKey>>,
    file_seq: u32,
    /// Whether an oversize warning has been emitted for the current group.
    oversize_warned: bool,
}

impl<F> SplittingWriter<F>
where
    F: Fn(u32) -> io::Result<Box<dyn Write + Send>> + Send,
{
    pub fn new(
        factory: F,
        schema: Arc<Schema>,
        writer_config: CsvWriterConfig,
        policy: SplitPolicy,
    ) -> Self {
        Self {
            factory,
            schema,
            writer_config,
            policy,
            current_writer: None,
            captured_header: None,
            records_in_file: 0,
            current_key: None,
            file_seq: 0,
            oversize_warned: false,
        }
    }

    /// Open a new split file via the factory.
    fn open_new_file(&mut self) -> Result<(), FormatError> {
        self.file_seq += 1;
        let raw = (self.factory)(self.file_seq).map_err(FormatError::Io)?;
        let counting = CountingWriter::new(raw);
        let mut csv = CsvWriter::new(
            counting,
            Arc::clone(&self.schema),
            self.writer_config.clone(),
        );

        // On rotation (not first file), replay captured header
        if let Some(ref header) = self.captured_header
            && self.policy.repeat_header
        {
            csv.write_preset_header(header)?;
        }

        self.current_writer = Some(csv);
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
        match value_to_group_key(val, field_name, None, 0) {
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
            && let Some(ref writer) = self.current_writer
            && writer.get_ref().bytes_written() >= max
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

    /// Capture header from the first record (schema columns + overflow).
    fn capture_header(&mut self, record: &Record) {
        let mut header: Vec<Box<str>> = self.schema.columns().to_vec();
        if let Some(overflow) = record.overflow_fields() {
            header.extend(overflow.map(|(k, _)| Box::from(k)));
        }
        self.captured_header = Some(header);
    }

    /// Number of split files written so far (including the current one).
    pub fn file_count(&self) -> u32 {
        self.file_seq
    }
}

impl<F> FormatWriter for SplittingWriter<F>
where
    F: Fn(u32) -> io::Result<Box<dyn Write + Send>> + Send,
{
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Lazy-open first file if no current writer
        if self.current_writer.is_none() {
            self.open_new_file()?;
        }

        // Capture header from first record (for reuse on rotation)
        if self.captured_header.is_none() {
            self.capture_header(record);
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

        // Flush csv internal buffer to push bytes through to CountingWriter.
        // Required for accurate byte-limit checking (csv::Writer buffers 8KB).
        if self.policy.max_bytes.is_some() {
            self.current_writer.as_mut().unwrap().flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        if let Some(ref mut writer) = self.current_writer {
            writer.flush()?;
        }
        Ok(())
    }
}
