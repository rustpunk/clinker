//! Byte-counting I/O wrapper and shared counter.
//!
//! `SharedByteCounter` is shared between a `CountingWriter` (write side) and
//! a `CountedFormatWriter` or `SplittingWriter` (read side). `CountingWriter`
//! increments the counter on every `write()` call.
//!
//! Uses `Arc<AtomicU64>` for `Send + Sync` — required because `SplittingWriter`
//! and its contents must be `Send`.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clinker_record::Record;

use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Byte counter shared between a `CountingWriter` (write side) and
/// a `CountedFormatWriter` or `SplittingWriter` (read side).
///
/// Clone is cheap (`Arc` bump). The counter is monotonic within a single
/// writer lifetime; call `reset()` on rotation to zero out for the new file.
#[derive(Clone, Debug, Default)]
pub struct SharedByteCounter(Arc<AtomicU64>);

impl SharedByteCounter {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Total bytes written through the associated `CountingWriter`.
    pub fn bytes_written(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Reset counter to zero (e.g. on file rotation).
    pub fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }

    /// Increment by `n` bytes. Called internally by `CountingWriter::write`.
    fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
}

/// Writer wrapper that counts bytes flowing through to the inner writer.
///
/// Increments a `SharedByteCounter` on every `write()` call. The counter
/// can be read by the owner (e.g., `SplittingWriter`) without reaching
/// through the format writer's type hierarchy.
///
/// Wraps `BufWriter` — counts format-output bytes (pre-buffer I/O).
/// Flush mandatory on rotation (SO #23452660).
pub struct CountingWriter<W: Write> {
    inner: W,
    counter: SharedByteCounter,
}

impl<W: Write> CountingWriter<W> {
    pub fn new(inner: W, counter: SharedByteCounter) -> Self {
        Self { inner, counter }
    }

    /// Access the shared byte counter.
    pub fn counter(&self) -> &SharedByteCounter {
        &self.counter
    }

    /// Total bytes written through this writer (delegates to shared counter).
    pub fn bytes_written(&self) -> u64 {
        self.counter.bytes_written()
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.counter.add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Format writer wrapper that exposes byte count from a `SharedByteCounter`.
///
/// Wraps any `Box<dyn FormatWriter>` produced by a writer factory that
/// used a `CountingWriter` internally. The `SharedByteCounter` is the
/// same instance passed to the `CountingWriter`, so `bytes_written()`
/// reflects actual bytes flushed to the I/O layer.
///
/// Used for non-split writers; split writers query their own counter
/// inside `SplittingWriter` directly.
pub struct CountedFormatWriter {
    inner: Box<dyn FormatWriter>,
    counter: SharedByteCounter,
}

impl CountedFormatWriter {
    pub fn new(inner: Box<dyn FormatWriter>, counter: SharedByteCounter) -> Self {
        Self { inner, counter }
    }
}

impl FormatWriter for CountedFormatWriter {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.inner.write_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()
    }

    fn bytes_written(&self) -> Option<u64> {
        Some(self.counter.bytes_written())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_byte_counter_new_starts_at_zero() {
        let counter = SharedByteCounter::new();
        assert_eq!(counter.bytes_written(), 0);
    }

    #[test]
    fn test_shared_byte_counter_reset() {
        let counter = SharedByteCounter::new();
        counter.add(100);
        assert_eq!(counter.bytes_written(), 100);
        counter.reset();
        assert_eq!(counter.bytes_written(), 0);
    }

    #[test]
    fn test_shared_byte_counter_clone_shares_state() {
        let counter = SharedByteCounter::new();
        let clone = counter.clone();
        counter.add(42);
        assert_eq!(clone.bytes_written(), 42);
    }

    #[test]
    fn test_counting_writer_accuracy() {
        let counter = SharedByteCounter::new();
        let mut buf = Vec::new();
        let mut cw = CountingWriter::new(&mut buf, counter.clone());
        cw.write_all(b"hello").unwrap();
        cw.write_all(b" world").unwrap();
        assert_eq!(cw.bytes_written(), 11);
        assert_eq!(counter.bytes_written(), 11);
        assert_eq!(buf.len(), 11);
    }

    #[test]
    fn test_counting_writer_flush() {
        let counter = SharedByteCounter::new();
        let mut buf = Vec::new();
        let mut cw = CountingWriter::new(&mut buf, counter.clone());
        cw.write_all(b"data before flush").unwrap();
        cw.flush().unwrap();
        assert_eq!(counter.bytes_written(), 17);
    }

    #[test]
    fn test_counted_format_writer_bytes_written() {
        use clinker_record::{Schema, Value};
        use std::sync::Arc;

        use crate::csv::writer::{CsvWriter, CsvWriterConfig};

        let counter = SharedByteCounter::new();
        let buf: Vec<u8> = Vec::new();
        let counting = CountingWriter::new(buf, counter.clone());
        let schema = Arc::new(Schema::new(vec!["x".into()]));
        let csv = CsvWriter::new(counting, Arc::clone(&schema), CsvWriterConfig::default());
        let mut counted = CountedFormatWriter::new(Box::new(csv), counter.clone());

        // FormatWriter::bytes_written should return Some via the shared counter
        assert_eq!(counted.bytes_written(), Some(0));

        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(42)]);
        counted.write_record(&record).unwrap();
        counted.flush().unwrap();

        let bytes = counted.bytes_written().unwrap();
        assert!(bytes > 0, "should have written some bytes");
    }
}
