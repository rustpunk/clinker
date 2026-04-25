//! Spill-to-disk infrastructure: LZ4-compressed record streams with a JSON schema header.
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
//!   Line 0: JSON array of column names (schema header)
//!   Line 1..N: postcard-encoded `(RecordPayload, P)` pairs, length-prefixed
//!              as a 4-byte little-endian u32 before each record.
//! Entire stream is LZ4 frame-compressed.
//!
//! The schema header stays as JSON because it is a tiny human-readable
//! manifest written once per file. Record bodies use postcard for compact
//! binary encoding of `Value`s and the per-record metadata map.

use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{Serialize, de::DeserializeOwned};
use tempfile::NamedTempFile;

use clinker_record::{Record, RecordPayload, Schema};

/// Error type for spill operations.
#[derive(Debug)]
pub enum SpillError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Postcard(postcard::Error),
    InvalidSchema(String),
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Io(e) => write!(f, "spill I/O error: {e}"),
            SpillError::Json(e) => write!(f, "spill JSON header error: {e}"),
            SpillError::Postcard(e) => write!(f, "spill postcard error: {e}"),
            SpillError::InvalidSchema(msg) => write!(f, "spill schema error: {msg}"),
        }
    }
}

impl std::error::Error for SpillError {}

impl From<std::io::Error> for SpillError {
    fn from(e: std::io::Error) -> Self {
        SpillError::Io(e)
    }
}

impl From<serde_json::Error> for SpillError {
    fn from(e: serde_json::Error) -> Self {
        SpillError::Json(e)
    }
}

impl From<postcard::Error> for SpillError {
    fn from(e: postcard::Error) -> Self {
        SpillError::Postcard(e)
    }
}

impl From<lz4_flex::frame::Error> for SpillError {
    fn from(e: lz4_flex::frame::Error) -> Self {
        SpillError::Io(std::io::Error::other(e.to_string()))
    }
}

/// Writes (record, payload) pairs to an LZ4-compressed spill file.
///
/// The schema is written once as a JSON header line. Each subsequent
/// record is written as a 4-byte little-endian length prefix followed
/// by a postcard-encoded `(RecordPayload, P)` tuple.
pub struct SpillWriter<P> {
    encoder: FrameEncoder<BufWriter<NamedTempFile>>,
    schema: Arc<Schema>,
    _payload: PhantomData<P>,
}

impl<P: Serialize> SpillWriter<P> {
    /// Create a new SpillWriter. Writes schema header immediately.
    /// Files are created in `spill_dir` or the system temp directory.
    pub fn new(schema: Arc<Schema>, spill_dir: Option<&Path>) -> Result<Self, SpillError> {
        let temp_file = if let Some(dir) = spill_dir {
            NamedTempFile::new_in(dir)?
        } else {
            NamedTempFile::new()?
        };

        let buf_writer = BufWriter::new(temp_file);
        let mut encoder = FrameEncoder::new(buf_writer);

        // Write schema as first line: JSON array of column names.
        // Kept as JSON so the header is human-inspectable without a postcard decoder.
        let columns: Vec<&str> = schema.columns().iter().map(|c| &**c).collect();
        let schema_json = serde_json::to_string(&columns)?;
        encoder.write_all(schema_json.as_bytes())?;
        encoder.write_all(b"\n")?;

        Ok(SpillWriter {
            encoder,
            schema,
            _payload: PhantomData,
        })
    }

    /// Serialize one (record, payload) pair.
    ///
    /// Each pair is written as a 4-byte little-endian length prefix
    /// followed by a postcard-encoded `(RecordPayload, P)` tuple.
    pub fn write_pair(&mut self, record: &Record, payload: &P) -> Result<(), SpillError> {
        let meta = record.metadata_pairs();
        let rec_payload = RecordPayload {
            values: record.values().to_vec(),
            metadata: if meta.is_empty() { None } else { Some(meta) },
        };
        let bytes = postcard::to_stdvec(&(&rec_payload, payload))?;
        let len = bytes.len() as u32;
        self.encoder.write_all(&len.to_le_bytes())?;
        self.encoder.write_all(&bytes)?;
        Ok(())
    }

    /// Convenience: write a record with the unit payload `()`.
    pub fn write_record(&mut self, record: &Record) -> Result<(), SpillError>
    where
        P: Default,
    {
        self.write_pair(record, &P::default())
    }

    /// Flush, finalize LZ4 frame, return handle to the completed spill file.
    pub fn finish(self) -> Result<SpillFile<P>, SpillError> {
        let buf_writer = self.encoder.finish()?;
        let temp_file = buf_writer
            .into_inner()
            .map_err(|e| SpillError::Io(e.into_error()))?;
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
    pub fn reader(&self) -> Result<SpillReader<P>, SpillError> {
        let file = std::fs::File::open(&self.path)?;
        let decoder = FrameDecoder::new(file);
        let mut buf_reader = BufReader::new(decoder);

        // Read and verify schema line
        let mut schema_line = String::new();
        buf_reader.read_line(&mut schema_line)?;
        let columns: Vec<String> = serde_json::from_str(schema_line.trim())?;

        let expected: Vec<&str> = self.schema.columns().iter().map(|c| &**c).collect();
        let actual: Vec<&str> = columns.iter().map(|c| c.as_str()).collect();
        if expected != actual {
            return Err(SpillError::InvalidSchema(format!(
                "schema mismatch: expected {:?}, got {:?}",
                expected, actual
            )));
        }

        Ok(SpillReader {
            reader: buf_reader,
            schema: Arc::clone(&self.schema),
            len_buf: [0u8; 4],
            _payload: PhantomData,
        })
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

/// Reads (record, payload) pairs from an LZ4-compressed spill file.
pub struct SpillReader<P> {
    reader: BufReader<FrameDecoder<std::fs::File>>,
    schema: Arc<Schema>,
    len_buf: [u8; 4],
    _payload: PhantomData<P>,
}

impl<P: DeserializeOwned> Iterator for SpillReader<P> {
    type Item = Result<(Record, P), SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read 4-byte length prefix
        match self.reader.read_exact(&mut self.len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(SpillError::Io(e))),
        }
        let len = u32::from_le_bytes(self.len_buf) as usize;
        let mut buf = vec![0u8; len];
        if let Err(e) = self.reader.read_exact(&mut buf) {
            return Some(Err(SpillError::Io(e)));
        }
        Some(self.parse_pair(&buf))
    }
}

impl<P: DeserializeOwned> SpillReader<P> {
    fn parse_pair(&self, buf: &[u8]) -> Result<(Record, P), SpillError> {
        let (rec_payload, payload): (RecordPayload, P) = postcard::from_bytes(buf)?;
        let record = rec_payload.into_record(Arc::clone(&self.schema));
        Ok((record, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            "name".into(),
            "amount".into(),
            "active".into(),
        ]))
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

    #[test]
    fn test_spill_roundtrip_all_value_types() {
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

        let mut writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();

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

        assert_eq!(records.len(), 100);

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

    #[test]
    fn test_spill_schema_preserved() {
        // Non-alphabetical column order
        let schema = Arc::new(Schema::new(vec![
            "z_col".into(),
            "a_col".into(),
            "m_col".into(),
        ]));

        let mut writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();
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
        let mut writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();

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

    #[test]
    fn test_spill_tempfile_cleanup() {
        let schema = test_schema();
        let mut writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();
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
        let writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();
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
        let mut writer: SpillWriter<()> = SpillWriter::new(Arc::clone(&schema), None).unwrap();

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
            SpillWriter::new(Arc::clone(&schema), Some(custom_dir.path())).unwrap();
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
}
