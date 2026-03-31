//! Spill-to-disk infrastructure: LZ4-compressed NDJSON with schema header.
//!
//! `SpillWriter` serializes records to a temp file; `SpillReader` reads them back.
//! These are reusable IO primitives — the actual spill triggers (sort buffer
//! overflow, blocking stage pressure) are implemented in Phase 8.
//!
//! File format:
//!   Line 0: JSON array of column names (schema)
//!   Line 1..N: JSON objects with column names as keys (records)
//! Entire stream is LZ4 frame-compressed.

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use tempfile::NamedTempFile;

use clinker_record::{Record, Schema, Value};

/// Error type for spill operations.
#[derive(Debug)]
pub enum SpillError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidSchema(String),
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Io(e) => write!(f, "spill I/O error: {e}"),
            SpillError::Json(e) => write!(f, "spill JSON error: {e}"),
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

impl From<lz4_flex::frame::Error> for SpillError {
    fn from(e: lz4_flex::frame::Error) -> Self {
        SpillError::Io(std::io::Error::other(e.to_string()))
    }
}

/// Writes records to an LZ4-compressed NDJSON spill file.
///
/// Records are manually serialized to JSON objects using schema columns +
/// values + overflow fields. No `Serialize` derive on `Record` required.
pub struct SpillWriter {
    encoder: FrameEncoder<BufWriter<NamedTempFile>>,
    schema: Arc<Schema>,
}

impl SpillWriter {
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

        // Write schema as first line: JSON array of column names
        let columns: Vec<&str> = schema.columns().iter().map(|c| &**c).collect();
        let schema_json = serde_json::to_string(&columns)?;
        encoder.write_all(schema_json.as_bytes())?;
        encoder.write_all(b"\n")?;

        Ok(SpillWriter { encoder, schema })
    }

    /// Serialize one record as an NDJSON line.
    /// Manually builds a JSON object from schema columns + values + overflow.
    pub fn write_record(&mut self, record: &Record) -> Result<(), SpillError> {
        use serde_json::{Map, Value as JsonValue};

        let mut obj = Map::with_capacity(record.total_field_count());

        // Schema fields
        for (i, col) in self.schema.columns().iter().enumerate() {
            let val = record.values().get(i).unwrap_or(&Value::Null);
            obj.insert(col.to_string(), value_to_json(val));
        }

        // Overflow fields (CXL-emitted, not in schema)
        if let Some(overflow) = record.overflow_fields() {
            for (key, val) in overflow {
                obj.insert(key.to_string(), value_to_json(val));
            }
        }

        let line = serde_json::to_string(&JsonValue::Object(obj))?;
        self.encoder.write_all(line.as_bytes())?;
        self.encoder.write_all(b"\n")?;

        Ok(())
    }

    /// Flush, finalize LZ4 frame, return handle to the completed spill file.
    pub fn finish(self) -> Result<SpillFile, SpillError> {
        let buf_writer = self.encoder.finish()?;
        let temp_file = buf_writer
            .into_inner()
            .map_err(|e| SpillError::Io(e.into_error()))?;
        let path = temp_file.into_temp_path();
        Ok(SpillFile {
            path,
            schema: self.schema,
        })
    }
}

/// Handle to a completed spill file. Auto-deletes on drop via TempPath.
pub struct SpillFile {
    path: tempfile::TempPath,
    schema: Arc<Schema>,
}

impl SpillFile {
    /// Open a reader over this spill file.
    pub fn reader(&self) -> Result<SpillReader, SpillError> {
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
            lines: buf_reader,
            schema: Arc::clone(&self.schema),
            line_buf: String::new(),
        })
    }

    /// Schema stored in this spill file.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Path to the spill file on disk (for diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Reads records from an LZ4-compressed NDJSON spill file.
pub struct SpillReader {
    lines: BufReader<FrameDecoder<std::fs::File>>,
    schema: Arc<Schema>,
    line_buf: String,
}

impl Iterator for SpillReader {
    type Item = Result<Record, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.line_buf.clear();
        match self.lines.read_line(&mut self.line_buf) {
            Ok(0) => None, // EOF
            Ok(_) => {
                let line = self.line_buf.trim();
                if line.is_empty() {
                    return None;
                }
                Some(self.parse_record(line))
            }
            Err(e) => Some(Err(SpillError::Io(e))),
        }
    }
}

impl SpillReader {
    fn parse_record(&self, line: &str) -> Result<Record, SpillError> {
        let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)?;

        // Extract schema fields in order
        let mut values = Vec::with_capacity(self.schema.column_count());
        for col in self.schema.columns() {
            let val = obj.get(&**col).map(json_to_value).unwrap_or(Value::Null);
            values.push(val);
        }

        let mut record = Record::new(Arc::clone(&self.schema), values);

        // Extract overflow fields (keys not in schema)
        for (key, json_val) in &obj {
            if self.schema.index(key).is_none() {
                record.set_overflow(key.clone().into_boxed_str(), json_to_value(json_val));
            }
        }

        Ok(record)
    }
}

/// Convert a clinker Value to serde_json::Value for serialization.
fn value_to_json(val: &Value) -> serde_json::Value {
    // Value already implements Serialize, so we can use serde_json::to_value
    serde_json::to_value(val).unwrap_or(serde_json::Value::Null)
}

/// Convert a serde_json::Value back to a clinker Value.
fn json_to_value(json: &serde_json::Value) -> Value {
    // Value implements Deserialize with custom logic
    serde_json::from_value(json.clone()).unwrap_or(Value::Null)
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

        let mut writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();

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
        let records: Vec<Record> = reader.map(|r| r.unwrap()).collect();

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

        let mut writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        writer.write_record(&record).unwrap();

        let spill_file = writer.finish().unwrap();
        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap()).collect();

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
        let mut writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();

        // Write 1000 records of repetitive tabular data
        let mut raw_ndjson_size = 0usize;
        for i in 0..1000 {
            let record = make_record(&schema, &format!("person_{i}"), i as i64 * 100, i % 2 == 0);
            // Estimate raw NDJSON size
            raw_ndjson_size += format!(
                r#"{{"name":"person_{}","amount":{},"active":{}}}"#,
                i,
                i * 100,
                i % 2 == 0
            )
            .len()
                + 1; // newline
            writer.write_record(&record).unwrap();
        }

        let spill_file = writer.finish().unwrap();
        let spill_size = std::fs::metadata(spill_file.path()).unwrap().len() as usize;

        assert!(
            spill_size < (raw_ndjson_size * 80 / 100),
            "Spill file ({spill_size} bytes) should be < 80% of raw NDJSON ({raw_ndjson_size} bytes)"
        );
    }

    #[test]
    fn test_spill_tempfile_cleanup() {
        let schema = test_schema();
        let mut writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();
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
        let writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();
        // Finish immediately — no records written
        let spill_file = writer.finish().unwrap();

        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap()).collect();
        assert!(records.is_empty());
    }

    #[test]
    fn test_spill_overflow_fields_preserved() {
        let schema = test_schema();
        let mut writer = SpillWriter::new(Arc::clone(&schema), None).unwrap();

        let mut record = make_record(&schema, "Alice", 100, true);
        record.set_overflow("extra_field".into(), Value::String("bonus".into()));
        record.set_overflow("score".into(), Value::Integer(42));
        writer.write_record(&record).unwrap();

        let spill_file = writer.finish().unwrap();
        let reader = spill_file.reader().unwrap();
        let records: Vec<Record> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 1);
        // Schema fields
        assert_eq!(records[0].get("name"), Some(&Value::String("Alice".into())));
        // Overflow fields
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
        let mut writer = SpillWriter::new(Arc::clone(&schema), Some(custom_dir.path())).unwrap();
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
