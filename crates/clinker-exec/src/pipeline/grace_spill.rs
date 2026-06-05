//! Partition-scoped spill files for grace hash join.
//!
//! Format per file:
//!
//! ```text
//! [body: LZ4 frame containing
//!        a stream of records, each prefixed by a 4-byte LE length and
//!        encoded as a postcard-serialized `Record` (RecordPayload shape)]
//! [footer: magic u32 LE | version u16 LE | hash_bits u8 |
//!          partition_id u16 LE | record_count u64 LE]
//! ```
//!
//! The footer-trailing layout sidesteps the LZ4 frame finalization
//! problem: an LZ4 frame cannot be reopened to overwrite a prefix
//! without rewriting it, and seeking inside a partially-flushed frame
//! corrupts the encoder state. By writing the LZ4 frame first and then
//! appending a fixed-size raw footer, we get an exact `record_count`
//! at finish-time without juggling encoder buffers.
//!
//! Reader path validates the footer magic and version, then opens an
//! LZ4 decode stream over the body and yields records via postcard.
//! A 64MB per-record byte budget bounds reader allocations against
//! malformed input.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use clinker_plan::SpillError;
use clinker_plan::error::PipelineError;
use clinker_record::{Record, RecordPayload, Schema};

/// Failure raised while writing a grace-hash partition spill.
///
/// Splits a directory-level fault from a byte-level one so the executor
/// can route them differently. A `DirUnavailable` means the spill root
/// itself went bad mid-run (removed, unmounted, remounted read-only) and
/// renders with the dedicated directory diagnostic shared with the
/// node-buffer, sort, and aggregate spill paths; `Io` is a genuine
/// byte-stream fault (`ENOSPC`, a short write, an encoder finalize error)
/// or an internal state-machine violation.
#[derive(Debug)]
pub(crate) enum GraceSpillError {
    /// The spill root directory became unusable while creating a
    /// partition file. Carries the classified [`SpillError::DirUnavailable`].
    DirUnavailable(SpillError),
    /// A byte-stream fault during write/finalize, or an internal
    /// state-machine violation surfaced as an `io::Error`.
    Io(std::io::Error),
}

impl From<std::io::Error> for GraceSpillError {
    fn from(e: std::io::Error) -> Self {
        GraceSpillError::Io(e)
    }
}

/// Map a [`GraceSpillError`] to a [`PipelineError`] for the combine node
/// `node`, tagging the failing stage with `detail` (e.g. "build write",
/// "probe writer").
///
/// A mid-run directory fault becomes `PipelineError::Spill`, so it renders
/// with the same `DirUnavailable` diagnostic the node-buffer, sort, and
/// aggregate spill paths emit; a genuine byte fault or state-machine
/// violation becomes `PipelineError::Internal`.
pub(crate) fn grace_spill_error(e: GraceSpillError, node: &str, detail: &str) -> PipelineError {
    match e {
        GraceSpillError::DirUnavailable(spill) => PipelineError::Spill(spill),
        GraceSpillError::Io(io) => PipelineError::Internal {
            op: "combine",
            node: node.to_string(),
            detail: format!("grace hash {detail}: {io}"),
        },
    }
}

/// File-format magic bytes, written little-endian. Not a printable ASCII
/// constant on purpose — readers do an exact `u32::from_le_bytes` match,
/// no string handling.
pub(crate) const GRACE_SPILL_MAGIC: u32 = 0x434C_4B47;

/// Format revision. Bump when the on-disk layout changes incompatibly.
pub(crate) const GRACE_SPILL_VERSION: u16 = 1;

/// Maximum single-record postcard byte length the reader will allocate
/// for. Larger length prefixes are treated as corruption and surfaced as
/// `io::Error` rather than driving a runaway allocation.
pub(crate) const GRACE_SPILL_MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Footer byte size: 4 (magic) + 2 (version) + 1 (hash_bits) +
/// 2 (partition_id) + 8 (record_count) = 17 bytes.
const FOOTER_SIZE: usize = 4 + 2 + 1 + 2 + 8;

/// Reference-counted path to a finalized spill file.
pub(crate) type SpillFilePath = Arc<Path>;

/// Footer payload describing one partition spill file.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SpillHeader {
    pub magic: u32,
    pub version: u16,
    pub hash_bits: u8,
    pub partition_id: u16,
    pub record_count: u64,
}

/// Streaming writer for one partition's worth of records.
///
/// Owns a temp file handle plus an LZ4 frame encoder over a buffered
/// writer. `write_record` postcard-encodes the record and emits a
/// length-prefixed body chunk. `finish` finalizes the LZ4 frame, appends
/// the raw footer, and returns the file path; the caller takes ownership
/// of cleanup via the surrounding `tempfile::TempDir`.
pub(crate) struct GraceSpillWriter {
    encoder: FrameEncoder<BufWriter<File>>,
    path: PathBuf,
    hash_bits: u8,
    partition_id: u16,
    record_count: u64,
}

impl GraceSpillWriter {
    /// Create a new writer in `dir`. The file name encodes
    /// `partition_id` plus a per-process monotonic sequence so a
    /// single partition that fans out across multiple spill flushes
    /// doesn't overwrite earlier files.
    pub(crate) fn new(
        dir: &Path,
        hash_bits: u8,
        partition_id: u16,
    ) -> Result<Self, GraceSpillError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(
            "grace-p{partition_id:05}-b{hash_bits:02}-s{seq:08}.spill"
        ));
        // A create failure in a spill dir the run validated at startup means
        // the directory went bad mid-run. Classify it through the shared
        // `from_spill_dir_io` helper so the grace path surfaces the same
        // `DirUnavailable` diagnostic the node-buffer, sort, and aggregate
        // spill paths do, rather than rendering as a generic internal error.
        // `from_spill_dir_io` only lifts directory-kind faults (NotFound,
        // PermissionDenied, ReadOnlyFilesystem); a genuine byte fault on
        // create (e.g. ENOSPC) stays `SpillError::Io` and is re-narrowed to
        // `GraceSpillError::Io` so only true directory faults carry the
        // `DirUnavailable` diagnostic.
        let file =
            File::create(&path).map_err(|e| match SpillError::from_spill_dir_io(dir, e) {
                dir_err @ SpillError::DirUnavailable { .. } => {
                    GraceSpillError::DirUnavailable(dir_err)
                }
                SpillError::Io(io) => GraceSpillError::Io(io),
                // `from_spill_dir_io` returns only `DirUnavailable` or `Io`; any
                // other variant would be an unreachable classifier regression.
                other => GraceSpillError::Io(std::io::Error::other(other.to_string())),
            })?;
        let buf = BufWriter::new(file);
        let encoder = FrameEncoder::new(buf);
        Ok(Self {
            encoder,
            path,
            hash_bits,
            partition_id,
            record_count: 0,
        })
    }

    /// Append one record to the partition body.
    pub(crate) fn write_record(&mut self, record: &Record) -> Result<(), GraceSpillError> {
        let rv = record.record_var_pairs();
        let payload = RecordPayload {
            values: record.values().to_vec(),
            record_vars: if rv.is_empty() { None } else { Some(rv) },
        };
        let bytes =
            postcard::to_stdvec(&payload).map_err(|e| std::io::Error::other(e.to_string()))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| std::io::Error::other("grace spill: record exceeds u32 byte length"))?;
        self.encoder.write_all(&len.to_le_bytes())?;
        self.encoder.write_all(&bytes)?;
        self.record_count += 1;
        Ok(())
    }

    /// Finalize the LZ4 frame, append the footer, return the file
    /// path along with the byte length of the finished file.
    ///
    /// The byte length feeds [`crate::pipeline::memory::MemoryArbitrator::record_spill_bytes`]
    /// so the disk-quota poll can tally cumulative on-disk usage
    /// without re-stat'ing each finalized file.
    pub(crate) fn finish(self) -> Result<(SpillFilePath, u64), GraceSpillError> {
        let Self {
            encoder,
            path,
            hash_bits,
            partition_id,
            record_count,
        } = self;
        let buf = encoder
            .finish()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let mut file = buf
            .into_inner()
            .map_err(|e| std::io::Error::other(e.into_error().to_string()))?;
        // Seek to current end and append footer; LZ4 frame is closed.
        let body_end = file.seek(SeekFrom::End(0))?;
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..4].copy_from_slice(&GRACE_SPILL_MAGIC.to_le_bytes());
        footer[4..6].copy_from_slice(&GRACE_SPILL_VERSION.to_le_bytes());
        footer[6] = hash_bits;
        footer[7..9].copy_from_slice(&partition_id.to_le_bytes());
        footer[9..17].copy_from_slice(&record_count.to_le_bytes());
        file.write_all(&footer)?;
        file.flush()?;
        let total_bytes = body_end + FOOTER_SIZE as u64;
        Ok((Arc::from(path.as_path()), total_bytes))
    }
}

/// Streaming reader for a finalized spill file.
///
/// Validates the footer magic + version on `open`. Yields records via
/// `Iterator` until either the LZ4 stream returns EOF mid-frame or the
/// recorded `record_count` is exhausted.
pub(crate) struct GraceSpillReader {
    decoder: FrameDecoder<BoundedRead>,
    schema: Arc<Schema>,
    header: SpillHeader,
    records_read: u64,
    len_buf: [u8; 4],
}

impl GraceSpillReader {
    /// Open a spill file and validate its footer. The schema is supplied
    /// by the caller and reattached to each record on read; the file does
    /// not embed schema.
    pub(crate) fn open(path: &Path, schema: Arc<Schema>) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let total_len = file.metadata()?.len();
        if total_len < FOOTER_SIZE as u64 {
            return Err(std::io::Error::other(format!(
                "grace spill {} too short ({total_len} bytes < {FOOTER_SIZE} footer bytes)",
                path.display()
            )));
        }
        let body_len = total_len - FOOTER_SIZE as u64;
        // Read footer.
        file.seek(SeekFrom::Start(body_len))?;
        let mut footer = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer)?;
        let header = SpillHeader {
            magic: u32::from_le_bytes(footer[0..4].try_into().unwrap()),
            version: u16::from_le_bytes(footer[4..6].try_into().unwrap()),
            hash_bits: footer[6],
            partition_id: u16::from_le_bytes(footer[7..9].try_into().unwrap()),
            record_count: u64::from_le_bytes(footer[9..17].try_into().unwrap()),
        };
        if header.magic != GRACE_SPILL_MAGIC {
            return Err(std::io::Error::other(format!(
                "grace spill {}: bad magic {:#010x} (expected {:#010x})",
                path.display(),
                header.magic,
                GRACE_SPILL_MAGIC
            )));
        }
        if header.version != GRACE_SPILL_VERSION {
            return Err(std::io::Error::other(format!(
                "grace spill {}: unsupported version {} (expected {})",
                path.display(),
                header.version,
                GRACE_SPILL_VERSION
            )));
        }
        // Rewind for body read; bound the read at body_len so the LZ4
        // decoder never advances past the body into the footer.
        file.seek(SeekFrom::Start(0))?;
        let bounded = BoundedRead::new(file, body_len);
        let decoder = FrameDecoder::new(bounded);
        Ok(Self {
            decoder,
            schema,
            header,
            records_read: 0,
            len_buf: [0u8; 4],
        })
    }

    pub(crate) fn header(&self) -> &SpillHeader {
        &self.header
    }
}

impl Iterator for GraceSpillReader {
    type Item = std::io::Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records_read >= self.header.record_count {
            return None;
        }
        match self.decoder.read_exact(&mut self.len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Stream ended before the recorded record_count was
                // reached — surface as an error rather than silently
                // truncating, so callers don't lose rows on a corrupted
                // partition file.
                return Some(Err(std::io::Error::other(format!(
                    "grace spill: unexpected EOF after {} of {} records",
                    self.records_read, self.header.record_count
                ))));
            }
            Err(e) => return Some(Err(e)),
        }
        let len = u32::from_le_bytes(self.len_buf) as usize;
        if len > GRACE_SPILL_MAX_RECORD_BYTES {
            return Some(Err(std::io::Error::other(format!(
                "grace spill: record length {len} exceeds {GRACE_SPILL_MAX_RECORD_BYTES} byte cap"
            ))));
        }
        let mut buf = vec![0u8; len];
        if let Err(e) = self.decoder.read_exact(&mut buf) {
            return Some(Err(e));
        }
        let payload: RecordPayload = match postcard::from_bytes(&buf) {
            Ok(p) => p,
            Err(e) => return Some(Err(std::io::Error::other(e.to_string()))),
        };
        self.records_read += 1;
        Some(Ok(payload.into_record(Arc::clone(&self.schema))))
    }
}

/// `Read` adapter that surfaces EOF after `limit` bytes. Wraps the
/// spill file so the LZ4 decoder cannot stray into the trailing footer.
struct BoundedRead {
    inner: File,
    remaining: u64,
}

impl BoundedRead {
    fn new(inner: File, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}

impl Read for BoundedRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let cap = self.remaining.min(buf.len() as u64) as usize;
        let n = self.inner.read(&mut buf[..cap])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;
    use tempfile::TempDir;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["id".into(), "v".into()]))
    }

    fn record(s: &Arc<Schema>, id: i64, v: &str) -> Record {
        Record::new(
            Arc::clone(s),
            vec![Value::Integer(id), Value::String(v.into())],
        )
    }

    #[test]
    fn writer_reader_roundtrip() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        let mut w = GraceSpillWriter::new(dir.path(), 4, 7).unwrap();
        for i in 0..50 {
            w.write_record(&record(&s, i, &format!("row-{i}"))).unwrap();
        }
        let (path, bytes) = w.finish().unwrap();
        let on_disk = std::fs::metadata(&*path).unwrap().len();
        assert_eq!(bytes, on_disk, "reported byte count must match file size");

        let reader = GraceSpillReader::open(&path, Arc::clone(&s)).unwrap();
        assert_eq!(reader.header().record_count, 50);
        assert_eq!(reader.header().hash_bits, 4);
        assert_eq!(reader.header().partition_id, 7);
        let recs: Vec<Record> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 50);
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.get("id"), Some(&Value::Integer(i as i64)));
            assert_eq!(r.get("v"), Some(&Value::String(format!("row-{i}").into())));
        }
    }

    #[test]
    fn empty_partition_iterates_cleanly() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        let w = GraceSpillWriter::new(dir.path(), 4, 0).unwrap();
        let (path, _bytes) = w.finish().unwrap();
        let reader = GraceSpillReader::open(&path, Arc::clone(&s)).unwrap();
        assert_eq!(reader.header().record_count, 0);
        let recs: Vec<_> = reader.collect();
        assert!(recs.is_empty());
    }

    #[test]
    fn truncated_body_returns_error() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        let mut w = GraceSpillWriter::new(dir.path(), 4, 1).unwrap();
        for i in 0..10 {
            w.write_record(&record(&s, i, "x")).unwrap();
        }
        let (path, _bytes) = w.finish().unwrap();

        // Corrupt: chop off the last 32 bytes BEFORE the footer (i.e. body bytes).
        let total = std::fs::metadata(&*path).unwrap().len();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&*path)
            .unwrap();
        f.set_len(total - FOOTER_SIZE as u64 - 32).unwrap();
        // Re-append a synthetic footer claiming 10 records but with truncated body.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&*path)
            .unwrap();
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..4].copy_from_slice(&GRACE_SPILL_MAGIC.to_le_bytes());
        footer[4..6].copy_from_slice(&GRACE_SPILL_VERSION.to_le_bytes());
        footer[6] = 4;
        footer[7..9].copy_from_slice(&1u16.to_le_bytes());
        footer[9..17].copy_from_slice(&10u64.to_le_bytes());
        f.write_all(&footer).unwrap();
        drop(f);

        let reader = GraceSpillReader::open(&path, s).unwrap();
        // At least one read must fail; the iterator must not panic.
        let mut saw_err = false;
        for item in reader {
            if item.is_err() {
                saw_err = true;
                break;
            }
        }
        assert!(saw_err, "truncated spill must surface read error");
    }

    #[test]
    fn bad_magic_rejected() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        let mut w = GraceSpillWriter::new(dir.path(), 4, 0).unwrap();
        w.write_record(&record(&s, 1, "a")).unwrap();
        let (path, _bytes) = w.finish().unwrap();

        // Overwrite magic bytes with garbage.
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&*path)
            .unwrap();
        let footer_offset = std::fs::metadata(&*path).unwrap().len() - FOOTER_SIZE as u64;
        f.seek(SeekFrom::Start(footer_offset)).unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        drop(f);

        assert!(GraceSpillReader::open(&path, s).is_err());
    }

    #[test]
    fn spill_dir_removed_mid_run_yields_distinct_diagnostic() {
        // Mirror of the node-buffer/sort spill regression: a grace-hash
        // partition spill into a dir that vanished after startup
        // validation must surface the distinct `DirUnavailable`
        // classification, not a bare `io::Error` that the executor would
        // fold into a generic internal error. This is the grace-hash half
        // of the documented "Aggregate, sort, and grace-hash Combine all
        // surface the directory-level cause" guarantee.
        let scratch = TempDir::new().unwrap();
        let spill_dir = scratch.path().join("grace-spill-root");
        std::fs::create_dir(&spill_dir).unwrap();
        std::fs::remove_dir(&spill_dir).unwrap();

        let err = match GraceSpillWriter::new(&spill_dir, 4, 0) {
            Ok(_) => panic!("GraceSpillWriter::new should fail when the dir is gone"),
            Err(e) => e,
        };
        assert!(
            matches!(
                &err,
                GraceSpillError::DirUnavailable(SpillError::DirUnavailable { .. })
            ),
            "expected DirUnavailable, got {err:?}"
        );

        // The shared boundary helper must route a directory fault to the
        // spill arm so it renders with the directory-level message, not a
        // generic `Internal`.
        let pipeline_err = grace_spill_error(err, "join1", "build write");
        assert!(
            matches!(
                pipeline_err,
                PipelineError::Spill(SpillError::DirUnavailable { .. })
            ),
            "expected PipelineError::Spill(DirUnavailable), got {pipeline_err:?}"
        );
        assert!(
            pipeline_err
                .to_string()
                .contains("became unavailable mid-run"),
            "{pipeline_err}"
        );
    }
}
