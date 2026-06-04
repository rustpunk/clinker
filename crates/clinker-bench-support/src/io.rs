//! I/O utilities for capturing pipeline output in tests and benchmarks.
//!
//! Pipelines write output through a `Box<dyn Write + Send>` registered
//! against the output node name. Tests and benches inject a
//! [`SharedBuffer`] so the post-run byte stream is inspectable from the
//! test thread without touching the filesystem.
//!
//! The reader-side helpers ([`slow_reader`], [`fast_reader`]) build the
//! `Box<dyn Read + Send>` payload a `FileSlot` expects. They return the
//! reader rather than a fully-built `FileSlot` because constructing a
//! slot requires `clinker_exec::source::multi_file::FileSlot`, and
//! `clinker-bench-support` sits beneath `clinker-exec` in the workspace
//! dep graph — a `clinker-exec` import here would form a cycle.

use std::io::{self, Cursor, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Thread-safe, cloneable in-memory buffer for capturing output.
///
/// Two clones of the same `SharedBuffer` share the underlying `Vec<u8>`,
/// so a writer thread and the test assertion side see the same data.
/// `Send + Sync` via `Arc<Mutex<Vec<u8>>>` — safe to pass into any
/// pipeline executor entry that takes `Box<dyn Write + Send>`.
#[derive(Clone, Default)]
pub struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Create a new empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of the buffer contents as bytes.
    pub fn contents(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }

    /// Return the buffer contents as a UTF-8 string.
    ///
    /// Panics if the captured bytes are not valid UTF-8 — callers that
    /// expect binary output should use [`Self::contents`] instead.
    pub fn as_string(&self) -> String {
        String::from_utf8(self.contents()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// `std::io::Read` adapter that paces row delivery by sleeping `delay`
/// after each newline-terminated chunk.
///
/// Integration tests need to drive the executor against a Source whose
/// per-row arrival is observably slow (live-channel back-pressure,
/// fused-Transform streaming, streaming-Output drain races). A real
/// filesystem or network reader would introduce flakiness; this adapter
/// produces deterministic, configurable per-row latency at the byte
/// stream layer where the CSV format reader actually sits.
///
/// The blocking `std::thread::sleep` is intentional — each Source reader
/// owns a dedicated OS thread, so a blocking sleep paces that source
/// without stalling any other stage. The header row is exempt from the
/// sleep (the CSV reader consumes it during schema inference, before any
/// record-level pacing matters).
pub struct DelayedRowReader {
    bytes: Vec<u8>,
    pos: usize,
    delay: Duration,
    rows_read: usize,
}

impl DelayedRowReader {
    /// Build a reader that emits `csv`'s bytes and sleeps `delay`
    /// between rows.
    pub fn new(csv: &str, delay: Duration) -> Self {
        Self {
            bytes: csv.as_bytes().to_vec(),
            pos: 0,
            delay,
            rows_read: 0,
        }
    }
}

impl Read for DelayedRowReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.bytes.len() {
            return Ok(0);
        }
        // Row-bounded chunking keeps each sleep aligned with a record
        // boundary even when the format reader's internal buffer would
        // otherwise pull multiple rows in one call.
        let remaining = &self.bytes[self.pos..];
        let chunk_end = remaining
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(remaining.len());
        let n = chunk_end.min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        if n > 0 && remaining[..n].ends_with(b"\n") {
            self.rows_read += 1;
            if self.rows_read >= 1 && self.pos < self.bytes.len() {
                std::thread::sleep(self.delay);
            }
        }
        Ok(n)
    }
}

/// Box up a [`DelayedRowReader`] as the `Box<dyn Read + Send>` payload
/// a `FileSlot` expects. Callers pair the returned reader with their
/// own synthetic path because the `FileSlot` type lives in
/// `clinker-exec`, which this crate cannot depend on.
pub fn slow_reader(csv: &str, delay: Duration) -> Box<dyn Read + Send> {
    Box::new(DelayedRowReader::new(csv, delay))
}

/// Box up an in-memory CSV payload as a non-pacing `Box<dyn Read + Send>`
/// suitable for a `FileSlot`. Used as the fast-side counterpart to
/// [`slow_reader`] when only one source needs pacing.
pub fn fast_reader(csv: &str) -> Box<dyn Read + Send> {
    Box::new(Cursor::new(csv.as_bytes().to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_buffer_write_and_read() {
        let mut buf = SharedBuffer::new();
        buf.write_all(b"hello world").unwrap();
        assert_eq!(buf.contents(), b"hello world");
    }

    #[test]
    fn test_shared_buffer_as_string() {
        let mut buf = SharedBuffer::new();
        buf.write_all("café ☕".as_bytes()).unwrap();
        assert_eq!(buf.as_string(), "café ☕");
    }

    #[test]
    fn test_shared_buffer_clone_shares_data() {
        let mut buf = SharedBuffer::new();
        let clone = buf.clone();
        buf.write_all(b"written via original").unwrap();
        assert_eq!(
            clone.as_string(),
            "written via original",
            "clone should see data written through original"
        );
    }

    #[test]
    fn test_fast_reader_delivers_full_payload() {
        let mut reader = fast_reader("id,tag\n1,a\n2,b\n");
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "id,tag\n1,a\n2,b\n");
    }

    #[test]
    fn test_slow_reader_paces_between_rows() {
        // Two body rows after the header → at least one inter-row sleep
        // fires. A 20 ms delay is large enough to dominate scheduler
        // noise but small enough to keep this test cheap.
        let csv = "id,tag\n1,a\n2,b\n";
        let delay = Duration::from_millis(20);
        let mut reader = slow_reader(csv, delay);
        let start = std::time::Instant::now();
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        let elapsed = start.elapsed();
        assert_eq!(out, csv, "payload must arrive intact");
        assert!(
            elapsed >= delay,
            "slow_reader should sleep at least once between rows; \
             got elapsed={elapsed:?}, delay={delay:?}",
        );
    }
}
