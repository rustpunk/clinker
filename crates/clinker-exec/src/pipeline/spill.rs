//! Spill-to-disk infrastructure: postcard record streams, optionally
//! LZ4-compressed, with a JSON schema header.
//!
//! `SpillWriter<P>` serializes (record, payload) pairs to a temp file;
//! `SpillReader<P>` reads them back. Parameterized over per-record payload
//! type `P` so DAG-walk sites can carry sidecar data (row number, metadata
//! maps) through sort permutation. Source/output sort callsites use
//! `P = ()` (zero spill bytes for the unit type).
//!
//! Generalization rationale: every production row-oriented ETL system
//! (Miller, Beam Sorter, Flink ExternalSorter, Differential Dataflow
//! `Batcher`, `extsort` crate) carries payload inside the spill envelope.
//! Position-indexed parallel arrays are contraindicated — see the Vector
//! RFC and the KAFKA-9408 postmortem.
//!
//! File format:
//!   Byte 0: format tag — `0x00` uncompressed, `0x01` LZ4 frame.
//!   Then a record stream (raw when uncompressed, inside an LZ4 frame when
//!   compressed):
//!     Line 0: JSON array of column names (schema header)
//!     Line 1..N: a sequence of length-prefixed frames. Each frame is a
//!                4-byte little-endian u32 byte length, then a 1-byte frame
//!                discriminator, then the frame body:
//!                  `0x00` record pair — postcard `(RecordPayload, P)`.
//!                  `0x01` context intern — postcard `DocumentContext`.
//!
//! Document envelope context is interned per file, not inlined per record.
//! The first time a record carrying a non-synthetic [`DocumentId`] is
//! written, the writer emits that document's context frame **before** the
//! record's pair frame; every later record of the same document carries
//! only its `doc_id` (a `u64`) in the `RecordPayload`. On read the context
//! frames build a `HashMap<DocumentId, Arc<DocumentContext>>` incrementally
//! and each record clones the shared `Arc` keyed by its `doc_id`. This is
//! bounded-memory: `O(distinct documents in file)` context frames, never
//! `O(records)`, and exactly one `Arc<DocumentContext>` per document on
//! reload. [`DocumentId::SYNTHETIC`] is never interned — a synthetic
//! `doc_id` resolves on read to the process-wide
//! [`synthetic_document_context`] singleton; a non-synthetic `doc_id`
//! absent from the table is a corrupt-file decode error, not a silent
//! synthetic fallback.
//!
//! The leading tag lets the reader dispatch on the on-disk format without
//! the writer's compression choice having to travel out-of-band. The choice
//! is the workspace `[storage.spill] compress` knob: `off` skips the LZ4
//! frame so small spills avoid LZ4's per-frame fixed cost, `on`/`auto` keep
//! it. See [`clinker_plan::config::CompressMode`].
//!
//! The schema header stays as JSON because it is a tiny human-readable
//! manifest written once per file. Record bodies use postcard for compact
//! binary encoding of `Value`s and the per-record metadata map.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{Serialize, de::DeserializeOwned};
use tempfile::NamedTempFile;

use clinker_record::{
    DocumentContext, DocumentId, Record, RecordPayload, Schema, synthetic_document_context,
};

use clinker_plan::SpillError;

/// On-disk format tag for an uncompressed spill file: the postcard record
/// stream is written raw, with no LZ4 frame.
const FORMAT_TAG_UNCOMPRESSED: u8 = 0x00;
/// On-disk format tag for an LZ4-frame-compressed spill file.
const FORMAT_TAG_LZ4: u8 = 0x01;

/// Frame discriminator for a record pair: the body is a postcard
/// `(RecordPayload, P)` tuple.
const FRAME_RECORD_PAIR: u8 = 0x00;
/// Frame discriminator for a document-context intern: the body is a postcard
/// [`DocumentContext`], emitted on the first sight of a non-synthetic
/// [`DocumentId`] in the stream.
const FRAME_CONTEXT_INTERN: u8 = 0x01;

/// Record-stream sink for a spill file: either a raw buffered writer
/// (uncompressed) or an LZ4 frame encoder wrapping one. Both expose
/// `io::Write` for the schema header and per-record bodies; the variant is
/// chosen at construction from the resolved compression decision and recorded
/// in the file's leading format tag.
enum SpillSink {
    /// Uncompressed: postcard records written straight to the buffered file.
    Uncompressed(BufWriter<NamedTempFile>),
    /// LZ4-frame-compressed: the historical default for large spills.
    Lz4(FrameEncoder<BufWriter<NamedTempFile>>),
}

impl Write for SpillSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            SpillSink::Uncompressed(w) => w.write(buf),
            SpillSink::Lz4(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SpillSink::Uncompressed(w) => w.flush(),
            SpillSink::Lz4(e) => e.flush(),
        }
    }
}

impl SpillSink {
    /// Finalize the stream and recover the underlying temp file. For LZ4 this
    /// flushes the frame's buffered tail; for the uncompressed sink it flushes
    /// the `BufWriter`. The spill directory is threaded in so an `ENOSPC` on
    /// the final flush is classified as `DiskFull` (E321) rather than a
    /// generic `Io`.
    fn into_temp_file(self, spill_dir: &Path) -> Result<NamedTempFile, SpillError> {
        let buf_writer = match self {
            SpillSink::Uncompressed(w) => w,
            // The lz4 frame error preserves the underlying `io::Error` (and
            // its `StorageFull` kind) through its `Into<io::Error>`
            // conversion, so the classifier still sees the real cause rather
            // than a flattened "other".
            SpillSink::Lz4(e) => e
                .finish()
                .map_err(|e| SpillError::from_spill_dir_io(spill_dir, std::io::Error::from(e)))?,
        };
        buf_writer
            .into_inner()
            .map_err(|e| SpillError::from_spill_dir_io(spill_dir, e.into_error()))
    }
}

/// Writes (record, payload) pairs to a spill file, optionally LZ4-compressed.
///
/// A 1-byte format tag is written first, then the schema as a JSON header
/// line, then a sequence of length-prefixed, discriminator-tagged frames:
/// each record becomes a record-pair frame holding a postcard
/// `(RecordPayload, P)` tuple, and the first record of each non-synthetic
/// document is preceded by a one-time context-intern frame holding that
/// document's [`DocumentContext`]. The `compress` flag at construction
/// selects the LZ4 or raw record stream and is recorded in the tag so
/// [`SpillReader`] dispatches without out-of-band state.
///
/// Memory model: streaming — records are encoded and flushed one at a time.
/// The only retained state is `seen_docs`, an `O(distinct documents)` set
/// that bounds context interning to one frame per document.
pub struct SpillWriter<P> {
    sink: SpillSink,
    schema: Arc<Schema>,
    /// Documents whose context frame has already been emitted, so each
    /// non-synthetic [`DocumentId`] is interned exactly once per file. Holds
    /// at most one entry per distinct document in the stream
    /// (`O(distinct documents)`), never one per record.
    seen_docs: HashSet<DocumentId>,
    /// Directory the spill file lives in, retained so a mid-stream write or
    /// finalize fault can be classified against it: an `ENOSPC` becomes
    /// `SpillError::DiskFull` and a directory-level fault becomes
    /// `SpillError::DirUnavailable`, rather than both collapsing into the
    /// generic `Io` variant. Holds the OS temp dir when no explicit spill
    /// dir was configured, so the diagnostic still names a real path.
    spill_dir: PathBuf,
    _payload: PhantomData<P>,
}

impl<P: Serialize> SpillWriter<P> {
    /// Create a new `SpillWriter`. Writes the format tag and schema header
    /// immediately. Files are created in `spill_dir` or the system temp
    /// directory. `compress` selects LZ4 framing (`true`) or a raw postcard
    /// stream (`false`); the choice is recorded in the file's leading tag.
    pub fn new(
        schema: Arc<Schema>,
        spill_dir: Option<&Path>,
        compress: bool,
    ) -> Result<Self, SpillError> {
        // A create failure in a spill dir the run validated at startup means
        // the directory went bad mid-run (unmounted, removed, remounted
        // read-only) or the volume is full. `from_spill_dir_io` lifts those
        // into the distinct `DirUnavailable` / `DiskFull` diagnostics rather
        // than a generic spill I/O error.
        let resolved_dir = spill_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(std::env::temp_dir);
        let temp_file = if let Some(dir) = spill_dir {
            // Test-only seam: when a test has armed the mid-run spill-root fault
            // for this root, this removes the live spill directory right before
            // the open below, so the open fails with `NotFound` and classifies
            // as `DirUnavailable`. Compiled out of release builds; an unarmed
            // seam is a no-op.
            #[cfg(test)]
            crate::executor::spill_purge::maybe_invalidate_spill_root_for_test(dir);
            NamedTempFile::new_in(dir).map_err(|e| SpillError::from_spill_dir_io(dir, e))?
        } else {
            NamedTempFile::new().map_err(|e| SpillError::from_spill_dir_io(&resolved_dir, e))?
        };

        let mut buf_writer = BufWriter::new(temp_file);

        // The format tag is the very first on-disk byte and sits outside the
        // LZ4 frame so the reader can dispatch before opening a decoder.
        let tag = if compress {
            FORMAT_TAG_LZ4
        } else {
            FORMAT_TAG_UNCOMPRESSED
        };
        buf_writer
            .write_all(&[tag])
            .map_err(|e| SpillError::from_spill_dir_io(&resolved_dir, e))?;

        let mut sink = if compress {
            SpillSink::Lz4(FrameEncoder::new(buf_writer))
        } else {
            SpillSink::Uncompressed(buf_writer)
        };

        // Write schema as first line: JSON array of column names.
        // Kept as JSON so the header is human-inspectable without a postcard decoder.
        let columns: Vec<&str> = schema.columns().iter().map(|c| &**c).collect();
        let schema_json = serde_json::to_string(&columns)?;
        sink.write_all(schema_json.as_bytes())
            .map_err(|e| SpillError::from_spill_dir_io(&resolved_dir, e))?;
        sink.write_all(b"\n")
            .map_err(|e| SpillError::from_spill_dir_io(&resolved_dir, e))?;

        Ok(SpillWriter {
            sink,
            schema,
            seen_docs: HashSet::new(),
            spill_dir: resolved_dir,
            _payload: PhantomData,
        })
    }

    /// Write one length-prefixed, discriminator-tagged frame: a 4-byte
    /// little-endian length covering the 1-byte `discriminator` plus
    /// `body`, then the discriminator, then the body. Classifies a write
    /// fault against the spill directory so an `ENOSPC` mid-stream surfaces
    /// as `DiskFull` (E321), not a generic `Io`.
    fn write_frame(&mut self, discriminator: u8, body: &[u8]) -> Result<(), SpillError> {
        let len = (body.len() + 1) as u32;
        self.sink
            .write_all(&len.to_le_bytes())
            .map_err(|e| SpillError::from_spill_dir_io(&self.spill_dir, e))?;
        self.sink
            .write_all(&[discriminator])
            .map_err(|e| SpillError::from_spill_dir_io(&self.spill_dir, e))?;
        self.sink
            .write_all(body)
            .map_err(|e| SpillError::from_spill_dir_io(&self.spill_dir, e))?;
        Ok(())
    }

    /// Serialize one (record, payload) pair.
    ///
    /// On the first sight of a record's non-synthetic [`DocumentId`], a
    /// one-time context-intern frame carrying its [`DocumentContext`] is
    /// emitted *before* the record-pair frame, so the reader's interning
    /// table is populated before any record references it. The record's
    /// `RecordPayload` then carries only the `doc_id`, never the full
    /// context. The synthetic document is never interned — it resolves on
    /// read to the shared synthetic singleton.
    pub fn write_pair(&mut self, record: &Record, payload: &P) -> Result<(), SpillError> {
        let doc_ctx = record.doc_ctx();
        let doc_id = doc_ctx.id();
        if doc_id != DocumentId::SYNTHETIC && self.seen_docs.insert(doc_id) {
            let ctx_bytes = postcard::to_stdvec(doc_ctx.as_ref())?;
            self.write_frame(FRAME_CONTEXT_INTERN, &ctx_bytes)?;
        }

        let rec_payload = RecordPayload::from_record(record);
        let bytes = postcard::to_stdvec(&(&rec_payload, payload))?;
        self.write_frame(FRAME_RECORD_PAIR, &bytes)
    }

    /// Convenience: write a record with the unit payload `()`.
    pub fn write_record(&mut self, record: &Record) -> Result<(), SpillError>
    where
        P: Default,
    {
        self.write_pair(record, &P::default())
    }

    /// Flush, finalize the record stream (LZ4 frame if compressed), and return
    /// a handle to the completed spill file.
    pub fn finish(self) -> Result<SpillFile<P>, SpillError> {
        let temp_file = self.sink.into_temp_file(&self.spill_dir)?;
        let path = temp_file.into_temp_path();
        Ok(SpillFile {
            path,
            schema: self.schema,
            _payload: PhantomData,
        })
    }
}

/// Handle to a completed spill file. Auto-deletes on drop via TempPath.
pub struct SpillFile<P> {
    path: tempfile::TempPath,
    schema: Arc<Schema>,
    _payload: PhantomData<P>,
}

impl<P: DeserializeOwned> SpillFile<P> {
    /// Open a reader over this spill file.
    ///
    /// Reads the leading format tag and selects the matching record-stream
    /// source: an LZ4 decoder for `0x01`, the raw file for `0x00`. An unknown
    /// tag is a corrupt-file error.
    pub fn reader(&self) -> Result<SpillReader<P>, SpillError> {
        let mut file = std::fs::File::open(&self.path)?;

        // The format tag is a single byte ahead of the (optionally framed)
        // record stream, written by `SpillWriter::new`.
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag)?;
        let source = match tag[0] {
            FORMAT_TAG_LZ4 => SpillSource::Lz4(BufReader::new(FrameDecoder::new(file))),
            FORMAT_TAG_UNCOMPRESSED => SpillSource::Uncompressed(BufReader::new(file)),
            other => {
                return Err(SpillError::InvalidSchema(format!(
                    "unknown spill format tag {other:#04x}; file is corrupt or not a spill file"
                )));
            }
        };
        let mut reader = SpillReader {
            source,
            schema: Arc::clone(&self.schema),
            doc_table: HashMap::new(),
            len_buf: [0u8; 4],
            _payload: PhantomData,
        };

        // Read and verify schema line
        let mut schema_line = String::new();
        reader.source.read_line(&mut schema_line)?;
        let columns: Vec<String> = serde_json::from_str(schema_line.trim())?;

        let expected: Vec<&str> = self.schema.columns().iter().map(|c| &**c).collect();
        let actual: Vec<&str> = columns.iter().map(|c| c.as_str()).collect();
        if expected != actual {
            return Err(SpillError::InvalidSchema(format!(
                "schema mismatch: expected {:?}, got {:?}",
                expected, actual
            )));
        }

        Ok(reader)
    }
}

impl<P> SpillFile<P> {
    /// Schema stored in this spill file.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Path to the spill file on disk (for diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Record-stream source for a spill file, mirroring [`SpillSink`]: either a
/// raw buffered file (uncompressed) or an LZ4 decoder over one. The variant is
/// selected from the file's leading format tag at [`SpillFile::reader`].
enum SpillSource {
    /// Uncompressed: postcard records read straight from the buffered file.
    Uncompressed(BufReader<std::fs::File>),
    /// LZ4-frame-compressed.
    Lz4(BufReader<FrameDecoder<std::fs::File>>),
}

impl Read for SpillSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            SpillSource::Uncompressed(r) => r.read(buf),
            SpillSource::Lz4(r) => r.read(buf),
        }
    }
}

impl BufRead for SpillSource {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            SpillSource::Uncompressed(r) => r.fill_buf(),
            SpillSource::Lz4(r) => r.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            SpillSource::Uncompressed(r) => r.consume(amt),
            SpillSource::Lz4(r) => r.consume(amt),
        }
    }
}

/// Reads (record, payload) pairs from a spill file, dispatching on the file's
/// format tag for the compressed or uncompressed record stream.
///
/// Memory model: streaming — one frame is decoded per `next` call. The only
/// retained state is `doc_table`, the per-file `DocumentId → Arc` interning
/// table built incrementally from context frames; it holds one `Arc` per
/// distinct document (`O(distinct documents)`), and every record of a
/// document clones that shared `Arc` rather than decoding the context again.
pub struct SpillReader<P> {
    source: SpillSource,
    schema: Arc<Schema>,
    /// Per-file envelope-context interning table. A context-intern frame
    /// decodes its [`DocumentContext`] into exactly one `Arc` here; every
    /// later record carrying that `DocumentId` clones the shared `Arc`.
    doc_table: HashMap<DocumentId, Arc<DocumentContext>>,
    len_buf: [u8; 4],
    _payload: PhantomData<P>,
}

impl<P: DeserializeOwned> Iterator for SpillReader<P> {
    type Item = Result<(Record, P), SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Drive past any context-intern frames — they populate the doc
        // table but yield no record — until a record-pair frame produces a
        // `(Record, P)`, or the stream ends.
        loop {
            // Read 4-byte length prefix.
            match self.source.read_exact(&mut self.len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                Err(e) => return Some(Err(SpillError::Io(e))),
            }
            let len = u32::from_le_bytes(self.len_buf) as usize;
            if len == 0 {
                // A frame is at minimum its 1-byte discriminator; a zero
                // length is structurally impossible from `write_frame`.
                return Some(Err(SpillError::InvalidSchema(
                    "spill frame length 0 (missing discriminator); file is corrupt".to_string(),
                )));
            }
            let mut buf = vec![0u8; len];
            if let Err(e) = self.source.read_exact(&mut buf) {
                return Some(Err(SpillError::Io(e)));
            }
            let (discriminator, body) = (buf[0], &buf[1..]);
            match discriminator {
                FRAME_CONTEXT_INTERN => {
                    if let Err(e) = self.intern_context(body) {
                        return Some(Err(e));
                    }
                    // Loop to the next frame.
                }
                FRAME_RECORD_PAIR => return Some(self.parse_pair(body)),
                other => {
                    return Some(Err(SpillError::InvalidSchema(format!(
                        "unknown spill frame discriminator {other:#04x}; file is corrupt"
                    ))));
                }
            }
        }
    }
}

impl<P: DeserializeOwned> SpillReader<P> {
    /// Decode a context-intern frame into one shared `Arc<DocumentContext>`
    /// and insert it into the per-file table keyed by the document's id.
    fn intern_context(&mut self, body: &[u8]) -> Result<(), SpillError> {
        let ctx: DocumentContext = postcard::from_bytes(body)?;
        self.doc_table.insert(ctx.id(), Arc::new(ctx));
        Ok(())
    }

    /// Decode a record-pair frame and reattach the document's shared
    /// context. A synthetic `doc_id` resolves to the process-wide synthetic
    /// singleton; a non-synthetic `doc_id` absent from the interning table
    /// is a corrupt-file error (a record frame must always be preceded by
    /// its document's context frame), never a silent synthetic fallback.
    fn parse_pair(&self, buf: &[u8]) -> Result<(Record, P), SpillError> {
        let (rec_payload, payload): (RecordPayload, P) = postcard::from_bytes(buf)?;
        let doc_ctx = if rec_payload.doc_id == DocumentId::SYNTHETIC {
            synthetic_document_context()
        } else {
            match self.doc_table.get(&rec_payload.doc_id) {
                Some(ctx) => Arc::clone(ctx),
                None => {
                    return Err(SpillError::InvalidSchema(format!(
                        "spill record references document {:?} with no interned context frame; \
                         file is corrupt",
                        rec_payload.doc_id
                    )));
                }
            }
        };
        let record = rec_payload.into_record(Arc::clone(&self.schema), doc_ctx);
        Ok((record, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;
    use indexmap::IndexMap;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            "name".into(),
            "amount".into(),
            "active".into(),
        ]))
    }

    /// Build a `DocumentContext` with the given id, file, and a single
    /// `Head` section carrying `(batch_id, total)` — a typical envelope
    /// shape with a nested `Value::Map` payload exercising the recursive
    /// `Value` path through the context frame.
    fn make_doc_ctx(id_seed: i64, file: &str) -> Arc<DocumentContext> {
        let mut head = IndexMap::new();
        head.insert(
            Box::from("batch_id"),
            Value::String(format!("RUN-{id_seed:03}").into()),
        );
        head.insert("total".into(), Value::Integer(id_seed * 10));
        let mut sections = IndexMap::new();
        sections.insert(Box::from("Head"), Value::Map(Box::new(head)));
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(file),
            sections,
        ))
    }

    fn make_record(schema: &Arc<Schema>, name: &str, amount: i64, active: bool) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![
                Value::String(name.into()),
                Value::Integer(amount),
                Value::Bool(active),
            ],
        )
    }

    // Round-trip every Value variant through both the compressed and the
    // uncompressed spill format. Both must reconstruct identical records: the
    // compression knob changes only the on-disk encoding, never the data.
    #[test]
    fn test_spill_roundtrip_all_value_types() {
        for compress in [true, false] {
            let schema = Arc::new(Schema::new(vec![
                "null_col".into(),
                "bool_col".into(),
                "int_col".into(),
                "float_col".into(),
                "str_col".into(),
                "date_col".into(),
                "dt_col".into(),
                "arr_col".into(),
            ]));

            let mut writer: SpillWriter<()> =
                SpillWriter::new(Arc::clone(&schema), None, compress).unwrap();

            for i in 0..100 {
                let record = Record::new(
                    Arc::clone(&schema),
                    vec![
                        Value::Null,
                        Value::Bool(i % 2 == 0),
                        Value::Integer(i as i64 * 100),
                        Value::Float(i as f64 * 1.5),
                        Value::String(format!("row_{i}").into()),
                        Value::Date(
                            chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                                + chrono::Duration::days(i as i64),
                        ),
                        Value::DateTime(
                            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                                .unwrap()
                                .and_hms_opt(12, 0, 0)
                                .unwrap()
                                + chrono::Duration::seconds(i as i64),
                        ),
                        Value::Array(vec![Value::Integer(i as i64), Value::String("x".into())]),
                    ],
                );
                writer.write_record(&record).unwrap();
            }

            let spill_file = writer.finish().unwrap();
            let reader = spill_file.reader().unwrap();
            let records: Vec<Record> = reader.map(|r| r.unwrap().0).collect();

            assert_eq!(records.len(), 100, "compress={compress}");

            // Check first record field-by-field
            assert_eq!(records[0].get("null_col"), Some(&Value::Null));
            assert_eq!(records[0].get("bool_col"), Some(&Value::Bool(true)));
            assert_eq!(records[0].get("int_col"), Some(&Value::Integer(0)));
            assert_eq!(records[0].get("float_col"), Some(&Value::Float(0.0)));
            assert_eq!(
                records[0].get("str_col"),
                Some(&Value::String("row_0".into()))
            );

            // Check last record
            assert_eq!(records[99].get("int_col"), Some(&Value::Integer(9900)));
            assert_eq!(records[99].get("bool_col"), Some(&Value::Bool(false)));
        }
    }

    #[test]
    fn test_spill_schema_preserved() {
        // Non-alphabetical column order
        let schema = Arc::new(Schema::new(vec![
            "z_col".into(),
            "a_col".into(),
            "m_col".into(),
        ]));

        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        writer.write_record(&record).unwrap();

        let spill_file = writer.finish().unwrap();
        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap().0).collect();

        assert_eq!(records.len(), 1);
        let read_schema = records[0].schema();
        assert_eq!(
            read_schema
                .columns()
                .iter()
                .map(|c| &**c)
                .collect::<Vec<_>>(),
            vec!["z_col", "a_col", "m_col"],
        );
    }

    #[test]
    fn test_spill_lz4_compression_ratio() {
        let schema = test_schema();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();

        // Write 1000 records; postcard binary is significantly more compact
        // than NDJSON. Assert the spill file is under 50% of the equivalent
        // raw NDJSON to confirm postcard + LZ4 are both contributing.
        let mut raw_ndjson_size = 0usize;
        for i in 0..1000 {
            let record = make_record(&schema, &format!("person_{i}"), i as i64 * 100, i % 2 == 0);
            raw_ndjson_size += format!(
                r#"{{"name":"person_{}","amount":{},"active":{}}}"#,
                i,
                i * 100,
                i % 2 == 0
            )
            .len()
                + 1;
            writer.write_record(&record).unwrap();
        }

        let spill_file = writer.finish().unwrap();
        let spill_size = std::fs::metadata(spill_file.path()).unwrap().len() as usize;

        // Postcard + LZ4 should be well under 50% of raw NDJSON
        assert!(
            spill_size < (raw_ndjson_size / 2),
            "Spill file ({spill_size} bytes) should be < 50% of raw NDJSON ({raw_ndjson_size} bytes)"
        );
    }

    // The leading format tag must record the writer's compression choice so
    // the reader dispatches on it: `0x01` for LZ4, `0x00` for raw.
    #[test]
    fn format_tag_records_compression_choice() {
        let schema = test_schema();
        for (compress, expected_tag) in [(true, 0x01u8), (false, 0x00u8)] {
            let mut writer: SpillWriter<()> =
                SpillWriter::new(Arc::clone(&schema), None, compress).unwrap();
            writer
                .write_record(&make_record(&schema, "Alice", 100, true))
                .unwrap();
            let spill_file = writer.finish().unwrap();
            let bytes = std::fs::read(spill_file.path()).unwrap();
            assert_eq!(
                bytes[0], expected_tag,
                "compress={compress} must write tag {expected_tag:#04x}"
            );
        }
    }

    // `compress = off` must produce a spill file with no LZ4 frame: after the
    // 1-byte tag, the body begins with the plain JSON schema header (`[`),
    // never the LZ4 frame magic `0x04 0x22 0x4D 0x18`.
    #[test]
    fn uncompressed_spill_has_no_lz4_frame_header() {
        let schema = test_schema();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, false).unwrap();
        writer
            .write_record(&make_record(&schema, "Alice", 100, true))
            .unwrap();
        let spill_file = writer.finish().unwrap();
        let bytes = std::fs::read(spill_file.path()).unwrap();

        assert_eq!(bytes[0], 0x00, "uncompressed tag");
        // LZ4 frame magic number, little-endian 0x184D2204.
        const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];
        assert_ne!(
            &bytes[1..5],
            &LZ4_FRAME_MAGIC,
            "uncompressed body must not start with an LZ4 frame header"
        );
        // The raw body is the JSON schema header, which opens with `[`.
        assert_eq!(
            bytes[1], b'[',
            "uncompressed body must begin with the JSON schema header"
        );
    }

    // Payload round-trips through the uncompressed format identically to the
    // compressed one — the knob changes only on-disk bytes.
    #[test]
    fn uncompressed_spill_roundtrips_payload() {
        let schema = test_schema();
        let mut writer: SpillWriter<u64> =
            SpillWriter::new(Arc::clone(&schema), None, false).unwrap();
        writer
            .write_pair(&make_record(&schema, "Alice", 100, true), &7)
            .unwrap();
        writer
            .write_pair(&make_record(&schema, "Bob", 200, false), &9)
            .unwrap();
        let spill_file = writer.finish().unwrap();
        let pairs: Vec<(Record, u64)> = spill_file.reader().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(pairs[0].1, 7);
        assert_eq!(pairs[1].0.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(pairs[1].1, 9);
    }

    #[test]
    fn test_spill_tempfile_cleanup() {
        let schema = test_schema();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();
        writer
            .write_record(&make_record(&schema, "Alice", 100, true))
            .unwrap();

        let spill_file = writer.finish().unwrap();
        let path = spill_file.path().to_path_buf();
        assert!(path.exists(), "Spill file should exist before drop");

        drop(spill_file);
        assert!(!path.exists(), "Spill file should be deleted after drop");
    }

    #[test]
    fn test_spill_empty_chunk() {
        let schema = test_schema();
        let writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None, true).unwrap();
        // Finish immediately — no records written
        let spill_file = writer.finish().unwrap();

        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap().0).collect();
        assert!(records.is_empty());
    }

    #[test]
    fn test_spill_rehydrate_uses_widened_schema() {
        // Widened schema includes every field the upstream operator emits.
        // Spill rehydrate reads values positionally out of the schema —
        // there is no off-schema side channel to round-trip.
        let schema = Arc::new(Schema::new(vec![
            "name".into(),
            "amount".into(),
            "active".into(),
            "extra_field".into(),
            "score".into(),
        ]));
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();

        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String("Alice".into()),
                Value::Integer(100),
                Value::Bool(true),
                Value::String("bonus".into()),
                Value::Integer(42),
            ],
        );
        writer.write_record(&record).unwrap();

        let spill_file = writer.finish().unwrap();
        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap().0).collect();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(
            records[0].get("extra_field"),
            Some(&Value::String("bonus".into()))
        );
        assert_eq!(records[0].get("score"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_spill_dir_override() {
        let custom_dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), Some(custom_dir.path()), true).unwrap();
        writer
            .write_record(&make_record(&schema, "Alice", 100, true))
            .unwrap();

        let spill_file = writer.finish().unwrap();
        assert!(
            spill_file.path().starts_with(custom_dir.path()),
            "Spill file {:?} should be in custom dir {:?}",
            spill_file.path(),
            custom_dir.path()
        );
    }

    #[test]
    fn spill_dir_removed_mid_run_yields_distinct_diagnostic() {
        // Simulate the spill dir vanishing after startup validation: create a
        // dir, hand its path to the writer, then delete it before the first
        // spill. The writer must surface `DirUnavailable`, not a generic
        // `Io`, so the operator sees a directory-level cause rather than an
        // opaque byte-stream failure.
        let scratch = tempfile::tempdir().unwrap();
        let spill_dir = scratch.path().join("spill-root");
        std::fs::create_dir(&spill_dir).unwrap();
        let schema = test_schema();
        std::fs::remove_dir(&spill_dir).unwrap();

        let err = match SpillWriter::<()>::new(Arc::clone(&schema), Some(&spill_dir), true) {
            Ok(_) => panic!("spill writer creation should fail when the dir is gone"),
            Err(e) => e,
        };
        assert!(
            matches!(err, SpillError::DirUnavailable { .. }),
            "expected DirUnavailable, got {err:?}"
        );
        // The rendered message names the directory and is distinct from the
        // generic spill I/O wording.
        let rendered = err.to_string();
        assert!(
            rendered.contains("became unavailable mid-run"),
            "{rendered}"
        );
    }

    // A document's envelope context (id, sections including a nested
    // Value::Map, and a non-empty source_file) must survive the spill
    // round-trip identically under both the compressed and uncompressed
    // body format.
    #[test]
    fn spill_roundtrip_preserves_doc_ctx() {
        for compress in [true, false] {
            let schema = test_schema();
            let doc = make_doc_ctx(1, "payments/run-001.xml");
            let mut writer: SpillWriter<()> =
                SpillWriter::new(Arc::clone(&schema), None, compress).unwrap();
            let mut rec = make_record(&schema, "Alice", 100, true);
            rec.set_doc_ctx(Arc::clone(&doc));
            writer.write_record(&rec).unwrap();

            let spill_file = writer.finish().unwrap();
            let records: Vec<Record> = spill_file.reader().unwrap().map(|r| r.unwrap().0).collect();
            assert_eq!(records.len(), 1, "compress={compress}");
            let ctx = records[0].doc_ctx();
            assert_eq!(ctx.id(), doc.id(), "compress={compress}");
            assert_eq!(
                ctx.source_file().as_ref(),
                "payments/run-001.xml",
                "source_file must survive (compress={compress})"
            );
            assert_eq!(
                ctx.get_section_field("Head", "batch_id"),
                Some(Value::String("RUN-001".into())),
                "section field must survive (compress={compress})"
            );
            assert_eq!(
                ctx.get_section_field("Head", "total"),
                Some(Value::Integer(10)),
                "compress={compress}"
            );
        }
    }

    // Every record of one document re-hydrates the SAME shared
    // `Arc<DocumentContext>` — one allocation per document on reload, not
    // per record. `Arc::ptr_eq` across all three records of the document
    // proves the interning table hands back a single shared Arc.
    #[test]
    fn spill_shares_one_arc_per_document() {
        let schema = test_schema();
        let doc = make_doc_ctx(2, "claims/batch.xml");
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();
        for i in 0..3 {
            let mut rec = make_record(&schema, &format!("row{i}"), i, true);
            rec.set_doc_ctx(Arc::clone(&doc));
            writer.write_record(&rec).unwrap();
        }
        let spill_file = writer.finish().unwrap();
        let records: Vec<Record> = spill_file.reader().unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(records.len(), 3);
        let first = records[0].doc_ctx();
        for rec in &records[1..] {
            assert!(
                Arc::ptr_eq(first, rec.doc_ctx()),
                "all records of one document must share one re-hydrated Arc",
            );
        }
    }

    // A synthetic-context record interns no context frame and re-hydrates
    // to the process-wide synthetic singleton (ptr_eq to the live
    // singleton), so in-pipeline-synthesized records cost zero context
    // frames on spill.
    #[test]
    fn spill_synthetic_doc_ctx_roundtrips_as_synthetic() {
        let schema = test_schema();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, true).unwrap();
        // make_record leaves the default synthetic context attached.
        writer
            .write_record(&make_record(&schema, "syn", 1, false))
            .unwrap();
        let spill_file = writer.finish().unwrap();

        // No context-intern frame is emitted for the synthetic document:
        // exactly one frame (the record pair) lands in the body.
        assert_eq!(
            count_body_frames(&spill_file),
            1,
            "synthetic document must intern no context frame",
        );

        let records: Vec<Record> = spill_file.reader().unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(records.len(), 1);
        assert!(
            Arc::ptr_eq(records[0].doc_ctx(), &synthetic_document_context()),
            "synthetic doc_id must re-hydrate to the shared singleton",
        );
        assert_eq!(records[0].doc_ctx().id(), DocumentId::SYNTHETIC);
    }

    // The interning table is `O(distinct documents)`, not `O(records)`: N
    // records spread over K distinct documents emit exactly K context
    // frames (plus N record frames), regardless of how many records each
    // document carries.
    #[test]
    fn spill_interning_table_is_o_distinct_docs() {
        let schema = test_schema();
        const DOCS: usize = 3;
        const RECS_PER_DOC: usize = 20;
        let docs: Vec<Arc<DocumentContext>> = (0..DOCS)
            .map(|d| make_doc_ctx(d as i64 + 1, &format!("doc-{d}.xml")))
            .collect();
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, false).unwrap();
        for doc in &docs {
            for i in 0..RECS_PER_DOC {
                let mut rec = make_record(&schema, "r", i as i64, true);
                rec.set_doc_ctx(Arc::clone(doc));
                writer.write_record(&rec).unwrap();
            }
        }
        let spill_file = writer.finish().unwrap();

        let (ctx_frames, rec_frames) = count_frames_by_kind(&spill_file);
        assert_eq!(ctx_frames, DOCS, "exactly one context frame per document");
        assert_eq!(
            rec_frames,
            DOCS * RECS_PER_DOC,
            "one record frame per record",
        );

        // Each document's records share that document's single Arc, and the
        // three documents' Arcs are mutually distinct.
        let records: Vec<Record> = spill_file.reader().unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(records.len(), DOCS * RECS_PER_DOC);
        let doc_a = records[0].doc_ctx();
        let doc_b = records[RECS_PER_DOC].doc_ctx();
        assert!(
            !Arc::ptr_eq(doc_a, doc_b),
            "distinct docs get distinct Arcs"
        );
        assert!(Arc::ptr_eq(doc_a, records[RECS_PER_DOC - 1].doc_ctx()));
    }

    // A non-synthetic record whose context frame never landed is a
    // corrupt-file decode error, never a silent synthetic fallback. Build a
    // valid spill, then strip its leading context-intern frame so the
    // record references an absent document.
    #[test]
    fn spill_missing_context_frame_is_decode_error() {
        let schema = test_schema();
        let doc = make_doc_ctx(9, "lone.xml");
        // Uncompressed so the body frames are byte-addressable for surgery.
        let mut writer: SpillWriter<()> =
            SpillWriter::new(Arc::clone(&schema), None, false).unwrap();
        let mut rec = make_record(&schema, "z", 1, true);
        rec.set_doc_ctx(Arc::clone(&doc));
        writer.write_record(&rec).unwrap();
        let spill_file = writer.finish().unwrap();

        // The on-disk layout is: [tag][schema line\n][ctx frame][record frame].
        // Drop the context frame, keeping tag + schema + record frame, so the
        // record's doc_id has no interned context.
        let raw = std::fs::read(spill_file.path()).unwrap();
        let nl = raw.iter().position(|&b| b == b'\n').unwrap();
        let header = &raw[..=nl];
        let mut rest = &raw[nl + 1..];
        // First frame is the context intern: skip its 4-byte length + body.
        let ctx_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        assert_eq!(rest[4], FRAME_CONTEXT_INTERN, "first body frame is context");
        rest = &rest[4 + ctx_len..];
        let mut surgical = Vec::with_capacity(header.len() + rest.len());
        surgical.extend_from_slice(header);
        surgical.extend_from_slice(rest);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &surgical).unwrap();
        // Re-open through a hand-built SpillFile pointing at the surgical file.
        let file = std::fs::File::open(tmp.path()).unwrap();
        drop(file);
        // Decode via a fresh reader over the surgical bytes.
        let err = decode_surgical(tmp.path(), &schema)
            .expect_err("a record with no interned context must be a decode error");
        match err {
            SpillError::InvalidSchema(msg) => assert!(
                msg.contains("no interned context frame"),
                "expected corrupt-context message, got: {msg}"
            ),
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    /// Count every length-prefixed frame in a finished spill file's body,
    /// transparently handling the LZ4 / raw body. Used by the interning
    /// tests to assert the frame budget without depending on reader logic.
    fn count_body_frames(spill_file: &SpillFile<()>) -> usize {
        let (ctx, rec) = count_frames_by_kind(spill_file);
        ctx + rec
    }

    /// Decode a spill file's body frames and tally `(context_frames,
    /// record_frames)`. Reads the leading format tag and schema line the
    /// same way `SpillFile::reader` does, then walks the frame stream.
    fn count_frames_by_kind(spill_file: &SpillFile<()>) -> (usize, usize) {
        let mut file = std::fs::File::open(spill_file.path()).unwrap();
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag).unwrap();
        let mut source: Box<dyn BufRead> = match tag[0] {
            FORMAT_TAG_LZ4 => Box::new(BufReader::new(FrameDecoder::new(file))),
            FORMAT_TAG_UNCOMPRESSED => Box::new(BufReader::new(file)),
            other => panic!("unknown tag {other:#04x}"),
        };
        let mut schema_line = String::new();
        source.read_line(&mut schema_line).unwrap();
        let (mut ctx, mut rec) = (0usize, 0usize);
        let mut len_buf = [0u8; 4];
        loop {
            match source.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("read error: {e}"),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            source.read_exact(&mut buf).unwrap();
            match buf[0] {
                FRAME_CONTEXT_INTERN => ctx += 1,
                FRAME_RECORD_PAIR => rec += 1,
                other => panic!("unknown frame discriminator {other:#04x}"),
            }
        }
        (ctx, rec)
    }

    /// Open a raw spill file at `path` (uncompressed) and decode it through
    /// the production `SpillReader` frame loop, returning the first decode
    /// error encountered. Mirrors `SpillFile::reader` without needing a
    /// `SpillFile` handle (the surgical file isn't one).
    fn decode_surgical(path: &Path, schema: &Arc<Schema>) -> Result<Vec<Record>, SpillError> {
        let mut file = std::fs::File::open(path)?;
        let mut tag = [0u8; 1];
        file.read_exact(&mut tag)?;
        let source = match tag[0] {
            FORMAT_TAG_UNCOMPRESSED => SpillSource::Uncompressed(BufReader::new(file)),
            FORMAT_TAG_LZ4 => SpillSource::Lz4(BufReader::new(FrameDecoder::new(file))),
            other => {
                return Err(SpillError::InvalidSchema(format!(
                    "unknown tag {other:#04x}"
                )));
            }
        };
        let mut reader: SpillReader<()> = SpillReader {
            source,
            schema: Arc::clone(schema),
            doc_table: HashMap::new(),
            len_buf: [0u8; 4],
            _payload: PhantomData,
        };
        let mut schema_line = String::new();
        reader.source.read_line(&mut schema_line)?;
        reader.map(|r| r.map(|(rec, ())| rec)).collect()
    }
}
