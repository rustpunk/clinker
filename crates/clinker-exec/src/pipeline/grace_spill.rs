//! Partition-scoped spill files for grace hash join.
//!
//! Format per file:
//!
//! ```text
//! [tag:  format byte — 0x00 uncompressed, 0x01 LZ4 frame]
//! [body: a stream of records, each prefixed by a 4-byte LE length and
//!        encoded as a postcard-serialized `Record` (RecordPayload shape);
//!        raw when the tag is 0x00, inside an LZ4 frame when 0x01]
//! [footer: magic u32 LE | version u16 LE | hash_bits u8 |
//!          partition_id u16 LE | record_count u64 LE]
//! ```
//!
//! The leading format tag records the workspace `[storage.spill] compress`
//! decision (see [`clinker_plan::config::CompressMode`]) so the reader
//! dispatches on the on-disk format without the writer's choice having to
//! travel out of band — the same posture the inter-stage
//! [`SpillWriter`](crate::pipeline::spill::SpillWriter) uses. `off` skips
//! the LZ4 frame so small partition spills avoid LZ4's per-frame fixed cost.
//!
//! The footer-trailing layout sidesteps the LZ4 frame finalization
//! problem: an LZ4 frame cannot be reopened to overwrite a prefix
//! without rewriting it, and seeking inside a partially-flushed frame
//! corrupts the encoder state. By writing the body first and then
//! appending a fixed-size raw footer, we get an exact `record_count`
//! at finish-time without juggling encoder buffers.
//!
//! Reader path validates the footer magic and version, then opens a
//! decode stream over the body (matching the format tag) and yields
//! records via postcard. A 64MB per-record byte budget bounds reader
//! allocations against malformed input.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

use clinker_plan::SpillError;
use clinker_plan::error::PipelineError;
use clinker_record::{Record, RecordPayload, Schema};

/// On-disk format tag for an uncompressed grace spill body: the postcard
/// record stream is written raw, with no LZ4 frame.
const FORMAT_TAG_UNCOMPRESSED: u8 = 0x00;
/// On-disk format tag for an LZ4-frame-compressed grace spill body.
const FORMAT_TAG_LZ4: u8 = 0x01;
/// Byte length of the leading format tag.
const TAG_SIZE: u64 = 1;

/// Record-stream sink for a grace spill body: either a raw buffered file
/// writer (uncompressed) or an LZ4 frame encoder wrapping one. Mirrors the
/// inter-stage spill path's `SpillSink`; the variant is chosen at
/// construction from the resolved compression decision and recorded in the
/// file's leading format tag.
enum GraceSpillSink {
    /// Uncompressed: postcard records written straight to the buffered file.
    Uncompressed(BufWriter<File>),
    /// LZ4-frame-compressed: the historical default for large partition spills.
    Lz4(FrameEncoder<BufWriter<File>>),
}

impl Write for GraceSpillSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            GraceSpillSink::Uncompressed(w) => w.write(buf),
            GraceSpillSink::Lz4(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            GraceSpillSink::Uncompressed(w) => w.flush(),
            GraceSpillSink::Lz4(e) => e.flush(),
        }
    }
}

impl GraceSpillSink {
    /// Finalize the body stream and recover the underlying file. For LZ4
    /// this flushes the frame's buffered tail (its `Into<io::Error>`
    /// conversion preserves the inner `io::Error` and its `StorageFull`
    /// kind for the classifier); for the uncompressed sink it flushes the
    /// `BufWriter`.
    fn into_file(self) -> std::io::Result<File> {
        let buf_writer = match self {
            GraceSpillSink::Uncompressed(w) => w,
            GraceSpillSink::Lz4(e) => e.finish().map_err(std::io::Error::from)?,
        };
        buf_writer.into_inner().map_err(|e| e.into_error())
    }
}

/// Record-stream source for a grace spill body, mirroring [`GraceSpillSink`]:
/// either a raw bounded file (uncompressed) or an LZ4 decoder over one. The
/// variant is selected from the file's leading format tag at
/// [`GraceSpillReader::open`].
enum GraceSpillSource {
    /// Uncompressed: postcard records read straight from the bounded body.
    Uncompressed(BoundedRead),
    /// LZ4-frame-compressed.
    Lz4(FrameDecoder<BoundedRead>),
}

impl Read for GraceSpillSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            GraceSpillSource::Uncompressed(r) => r.read(buf),
            GraceSpillSource::Lz4(r) => r.read(buf),
        }
    }
}

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
    /// The spill volume ran out of space (`ENOSPC`) while creating, writing,
    /// or finalizing a partition file. Carries the classified
    /// [`SpillError::DiskFull`] so the combine path surfaces the same E321
    /// disk-full diagnostic the node-buffer and sort spill paths do.
    DiskFull(SpillError),
    /// A byte-stream fault during write/finalize, or an internal
    /// state-machine violation surfaced as an `io::Error`.
    Io(std::io::Error),
}

impl From<std::io::Error> for GraceSpillError {
    fn from(e: std::io::Error) -> Self {
        GraceSpillError::Io(e)
    }
}

impl GraceSpillWriter {
    /// Classify a write/finalize `io::Error` against the partition file's
    /// parent directory: an `ENOSPC` becomes [`GraceSpillError::DiskFull`]
    /// and a directory-level fault becomes [`GraceSpillError::DirUnavailable`],
    /// matching the create-time classification so a disk filling mid-write
    /// renders as E321 rather than an opaque internal error.
    fn classify_io(&self, e: std::io::Error) -> GraceSpillError {
        let dir = self.path.parent().unwrap_or(&self.path);
        match SpillError::from_spill_dir_io(dir, e) {
            dir_err @ SpillError::DirUnavailable { .. } => GraceSpillError::DirUnavailable(dir_err),
            disk_err @ SpillError::DiskFull { .. } => GraceSpillError::DiskFull(disk_err),
            SpillError::Io(io) => GraceSpillError::Io(io),
            other => GraceSpillError::Io(std::io::Error::other(other.to_string())),
        }
    }
}

/// Map a [`GraceSpillError`] to a [`PipelineError`] for the combine node
/// `node`, tagging the failing stage with `detail` (e.g. "build write",
/// "probe writer").
///
/// A mid-run directory fault or a full-volume fault becomes
/// `PipelineError::Spill`, so it renders with the same `DirUnavailable` /
/// `DiskFull` diagnostic the node-buffer, sort, and aggregate spill paths
/// emit; a genuine byte fault or state-machine violation becomes
/// `PipelineError::Internal`.
pub(crate) fn grace_spill_error(e: GraceSpillError, node: &str, detail: &str) -> PipelineError {
    match e {
        GraceSpillError::DirUnavailable(spill) | GraceSpillError::DiskFull(spill) => {
            PipelineError::Spill(spill)
        }
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
/// Owns a body sink — a raw buffered file writer or an LZ4 frame encoder
/// over one, chosen from the resolved compression decision and recorded in
/// the leading format tag. `write_record` postcard-encodes the record and
/// emits a length-prefixed body chunk. `finish` finalizes the body, appends
/// the raw footer, and returns the file path; the caller takes ownership of
/// cleanup via the surrounding `tempfile::TempDir`.
pub(crate) struct GraceSpillWriter {
    sink: GraceSpillSink,
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
    ///
    /// `compress` selects the body encoding: `true` wraps the postcard
    /// record stream in an LZ4 frame, `false` writes it raw. The caller
    /// resolves it from the workspace `[storage.spill] compress` knob (see
    /// [`clinker_plan::config::CompressMode`]); the choice is recorded in
    /// the file's leading format tag so the reader dispatches without
    /// out-of-band state.
    pub(crate) fn new(
        dir: &Path,
        hash_bits: u8,
        partition_id: u16,
        compress: bool,
    ) -> Result<Self, GraceSpillError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(
            "grace-p{partition_id:05}-b{hash_bits:02}-s{seq:08}.spill"
        ));
        // A create failure in a spill dir the run validated at startup means
        // the directory went bad mid-run (removed/unmounted/read-only) or the
        // volume filled. Classify it through the shared `from_spill_dir_io`
        // helper so the grace path surfaces the same `DirUnavailable` /
        // `DiskFull` diagnostics the node-buffer, sort, and aggregate spill
        // paths do, rather than rendering as a generic internal error. A
        // genuine byte fault on create stays `SpillError::Io` and re-narrows
        // to `GraceSpillError::Io`.
        let classify_create = |e: std::io::Error| match SpillError::from_spill_dir_io(dir, e) {
            dir_err @ SpillError::DirUnavailable { .. } => GraceSpillError::DirUnavailable(dir_err),
            disk_err @ SpillError::DiskFull { .. } => GraceSpillError::DiskFull(disk_err),
            SpillError::Io(io) => GraceSpillError::Io(io),
            // `from_spill_dir_io` returns only `DirUnavailable`, `DiskFull`,
            // or `Io`; any other variant would be a classifier regression.
            other => GraceSpillError::Io(std::io::Error::other(other.to_string())),
        };
        // Grace spill files hold verbatim record bytes (potentially PII /
        // credentials) and must be owner-only on a shared spill volume — the
        // same 0o600 posture the inter-stage and aggregate spill paths get for
        // free from `tempfile::NamedTempFile`. This path names its files
        // deterministically (partition + sequence) rather than using a temp
        // name, so it opens with an explicit mode instead.
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&path).map_err(classify_create)?;
        // The format tag is the very first on-disk byte and sits ahead of the
        // (optionally framed) body so the reader can dispatch before opening a
        // decoder.
        let tag = if compress {
            FORMAT_TAG_LZ4
        } else {
            FORMAT_TAG_UNCOMPRESSED
        };
        file.write_all(&[tag]).map_err(classify_create)?;
        let buf = BufWriter::new(file);
        let sink = if compress {
            GraceSpillSink::Lz4(FrameEncoder::new(buf))
        } else {
            GraceSpillSink::Uncompressed(buf)
        };
        Ok(Self {
            sink,
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
        // Classify a write fault against the partition directory so an
        // `ENOSPC` mid-stream surfaces as `DiskFull` (E321), not internal Io.
        if let Err(e) = self.sink.write_all(&len.to_le_bytes()) {
            return Err(self.classify_io(e));
        }
        if let Err(e) = self.sink.write_all(&bytes) {
            return Err(self.classify_io(e));
        }
        self.record_count += 1;
        Ok(())
    }

    /// Finalize the body (LZ4 frame when compressed, a flush when raw),
    /// append the footer, and return the file path along with the byte
    /// length of the finished file.
    ///
    /// The byte length feeds [`crate::pipeline::memory::MemoryArbitrator::record_spill_bytes`]
    /// so the disk-quota poll can tally cumulative on-disk usage
    /// without re-stat'ing each finalized file.
    pub(crate) fn finish(self) -> Result<(SpillFilePath, u64), GraceSpillError> {
        // Classify every finalize-time io fault against the partition's
        // directory before `self` is consumed, so an `ENOSPC` flushing the
        // last block or footer surfaces as `DiskFull` (E321) — the disk
        // filling — rather than an opaque internal byte-stream error. The lz4
        // frame error preserves its inner `io::Error` (and its `StorageFull`
        // kind) through `Into<io::Error>`.
        let dir = self.path.parent().unwrap_or(&self.path).to_path_buf();
        let classify = |e: std::io::Error| -> GraceSpillError {
            match SpillError::from_spill_dir_io(&dir, e) {
                dir_err @ SpillError::DirUnavailable { .. } => {
                    GraceSpillError::DirUnavailable(dir_err)
                }
                disk_err @ SpillError::DiskFull { .. } => GraceSpillError::DiskFull(disk_err),
                SpillError::Io(io) => GraceSpillError::Io(io),
                other => GraceSpillError::Io(std::io::Error::other(other.to_string())),
            }
        };
        let Self {
            sink,
            path,
            hash_bits,
            partition_id,
            record_count,
        } = self;
        let mut file = sink.into_file().map_err(&classify)?;
        // Seek to current end and append footer; the body (raw or LZ4 frame)
        // is closed, and the leading format tag is already on disk.
        let body_end = file.seek(SeekFrom::End(0)).map_err(&classify)?;
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..4].copy_from_slice(&GRACE_SPILL_MAGIC.to_le_bytes());
        footer[4..6].copy_from_slice(&GRACE_SPILL_VERSION.to_le_bytes());
        footer[6] = hash_bits;
        footer[7..9].copy_from_slice(&partition_id.to_le_bytes());
        footer[9..17].copy_from_slice(&record_count.to_le_bytes());
        file.write_all(&footer).map_err(&classify)?;
        file.flush().map_err(&classify)?;
        let total_bytes = body_end + FOOTER_SIZE as u64;
        Ok((Arc::from(path.as_path()), total_bytes))
    }
}

/// Streaming reader for a finalized spill file.
///
/// Validates the footer magic + version on `open`, then dispatches the body
/// source on the leading format tag (raw vs LZ4). Yields records via
/// `Iterator` until either the body stream returns EOF or the recorded
/// `record_count` is exhausted.
pub(crate) struct GraceSpillReader {
    source: GraceSpillSource,
    schema: Arc<Schema>,
    header: SpillHeader,
    records_read: u64,
    len_buf: [u8; 4],
}

impl GraceSpillReader {
    /// Open a spill file, validate its footer, and select the body source
    /// from the leading format tag. The schema is supplied by the caller and
    /// reattached to each record on read; the file does not embed schema.
    pub(crate) fn open(path: &Path, schema: Arc<Schema>) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let total_len = file.metadata()?.len();
        // The on-disk layout is `tag (1) ++ body ++ footer`; a file shorter
        // than tag + footer cannot hold even an empty body.
        let min_len = TAG_SIZE + FOOTER_SIZE as u64;
        if total_len < min_len {
            return Err(std::io::Error::other(format!(
                "grace spill {} too short ({total_len} bytes < {min_len} tag+footer bytes)",
                path.display()
            )));
        }
        // Footer sits at the file tail; the body spans `[TAG_SIZE, footer)`.
        let footer_start = total_len - FOOTER_SIZE as u64;
        let body_len = footer_start - TAG_SIZE;
        // Read footer.
        file.seek(SeekFrom::Start(footer_start))?;
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
        // Read the format tag at offset 0, then position the body read at
        // offset TAG_SIZE bounded by `body_len` so the decoder never advances
        // past the body into the footer.
        file.seek(SeekFrom::Start(0))?;
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag)?;
        file.seek(SeekFrom::Start(TAG_SIZE))?;
        let bounded = BoundedRead::new(file, body_len);
        let source = match tag[0] {
            FORMAT_TAG_LZ4 => GraceSpillSource::Lz4(FrameDecoder::new(bounded)),
            FORMAT_TAG_UNCOMPRESSED => GraceSpillSource::Uncompressed(bounded),
            other => {
                return Err(std::io::Error::other(format!(
                    "grace spill {}: unknown format tag {other:#04x}; file is corrupt",
                    path.display()
                )));
            }
        };
        Ok(Self {
            source,
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
        match self.source.read_exact(&mut self.len_buf) {
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
        if let Err(e) = self.source.read_exact(&mut buf) {
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
/// spill file's body region so neither the raw record reader nor the LZ4
/// decoder strays into the trailing footer.
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

    /// LZ4 frame magic number, little-endian `0x184D2204` — the four bytes a
    /// compressed grace body opens with right after the 1-byte format tag.
    const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

    #[test]
    fn writer_reader_roundtrip() {
        // Round-trip through both the compressed and the uncompressed body
        // format; the knob changes only on-disk bytes, never the records.
        for compress in [true, false] {
            let dir = TempDir::new().unwrap();
            let s = schema();
            let mut w = GraceSpillWriter::new(dir.path(), 4, 7, compress).unwrap();
            for i in 0..50 {
                w.write_record(&record(&s, i, &format!("row-{i}"))).unwrap();
            }
            let (path, bytes) = w.finish().unwrap();
            let on_disk = std::fs::metadata(&*path).unwrap().len();
            assert_eq!(
                bytes, on_disk,
                "reported byte count must match file size (compress={compress})"
            );

            let reader = GraceSpillReader::open(&path, Arc::clone(&s)).unwrap();
            assert_eq!(reader.header().record_count, 50);
            assert_eq!(reader.header().hash_bits, 4);
            assert_eq!(reader.header().partition_id, 7);
            let recs: Vec<Record> = reader.map(|r| r.unwrap()).collect();
            assert_eq!(recs.len(), 50, "compress={compress}");
            for (i, r) in recs.iter().enumerate() {
                assert_eq!(r.get("id"), Some(&Value::Integer(i as i64)));
                assert_eq!(r.get("v"), Some(&Value::String(format!("row-{i}").into())));
            }
        }
    }

    // Grace spill files hold verbatim record bytes and must be owner-only on a
    // shared spill volume. Unlike the inter-stage and aggregate spill paths
    // (which get 0o600 for free from `tempfile::NamedTempFile`), this path names
    // its files deterministically and opens them itself, so the explicit mode is
    // a separate guarantee worth pinning.
    #[cfg(unix)]
    #[test]
    fn grace_spill_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let s = schema();
        let mut w = GraceSpillWriter::new(dir.path(), 4, 0, false).unwrap();
        w.write_record(&record(&s, 1, "row")).unwrap();
        let (path, _) = w.finish().unwrap();
        let mode = std::fs::metadata(&*path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "grace spill file must be owner-only");
    }

    // The leading format tag must record the writer's compression choice and
    // the body must honor it: an `off` grace spill is byte-raw (no LZ4 frame
    // magic after the tag) and an `on` one is LZ4-framed. This is the grace
    // half of the `[storage.spill] compress` knob guarantee — without it the
    // `--explain` per-operator compress line is a falsehood for grace-hash
    // Combine and SortMerge Phase B.
    #[test]
    fn format_tag_and_body_honor_compress_knob() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        for (compress, expected_tag) in [(true, FORMAT_TAG_LZ4), (false, FORMAT_TAG_UNCOMPRESSED)] {
            let mut w = GraceSpillWriter::new(dir.path(), 4, 3, compress).unwrap();
            for i in 0..8 {
                w.write_record(&record(&s, i, "x")).unwrap();
            }
            let (path, _bytes) = w.finish().unwrap();
            let raw = std::fs::read(&*path).unwrap();
            assert_eq!(
                raw[0], expected_tag,
                "compress={compress} must write tag {expected_tag:#04x}"
            );
            // The four bytes after the tag are the LZ4 frame magic only when
            // compressed; an `off` body opens straight into the first
            // length-prefixed postcard record, never the frame magic.
            let body_head = &raw[1..5];
            if compress {
                assert_eq!(
                    body_head, &LZ4_FRAME_MAGIC,
                    "on-mode body must be LZ4-framed"
                );
            } else {
                assert_ne!(
                    body_head, &LZ4_FRAME_MAGIC,
                    "off-mode body must be byte-raw, with no LZ4 frame magic"
                );
            }
        }
    }

    #[test]
    fn empty_partition_iterates_cleanly() {
        for compress in [true, false] {
            let dir = TempDir::new().unwrap();
            let s = schema();
            let w = GraceSpillWriter::new(dir.path(), 4, 0, compress).unwrap();
            let (path, _bytes) = w.finish().unwrap();
            let reader = GraceSpillReader::open(&path, Arc::clone(&s)).unwrap();
            assert_eq!(reader.header().record_count, 0);
            let recs: Vec<_> = reader.collect();
            assert!(recs.is_empty(), "compress={compress}");
        }
    }

    #[test]
    fn truncated_body_returns_error() {
        let dir = TempDir::new().unwrap();
        let s = schema();
        let mut w = GraceSpillWriter::new(dir.path(), 4, 1, true).unwrap();
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
        let mut w = GraceSpillWriter::new(dir.path(), 4, 0, true).unwrap();
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

        let err = match GraceSpillWriter::new(&spill_dir, 4, 0, true) {
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
