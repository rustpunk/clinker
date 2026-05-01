use std::io::Write;
use std::sync::{Arc, Mutex};

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the CSV writer.
#[derive(Clone)]
pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub include_header: bool,
    /// Whether engine-stamped schema columns (today: `$ck.<field>`
    /// correlation snapshots) are emitted into the CSV. Defaults to
    /// `false` — engine-internal namespaces are stripped from the
    /// default output unless the Output node opts in via
    /// `include_correlation_keys: true`.
    pub include_engine_stamped: bool,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            include_header: true,
            include_engine_stamped: false,
        }
    }
}

/// Streaming CSV writer wrapping `csv::Writer`.
///
/// Writes schema fields in schema order, then overflow fields in
/// sorted key order (deterministic output). Header row is written
/// on first `write_record()` call if `include_header` is true.
pub struct CsvWriter<W: Write> {
    inner: csv::Writer<W>,
    schema: Arc<Schema>,
    config: CsvWriterConfig,
    header_written: bool,
}

impl<W: Write> CsvWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: CsvWriterConfig) -> Self {
        let inner = csv::WriterBuilder::new()
            .delimiter(config.delimiter)
            .from_writer(writer);
        Self {
            inner,
            schema,
            config,
            header_written: false,
        }
    }

    /// Write a pre-captured header row, bypassing first-record discovery.
    ///
    /// Used by `SplittingWriter` on rotation to ensure all split files
    /// have identical headers (captured from the first file).
    pub fn write_preset_header(&mut self, header: &[Box<str>]) -> Result<(), FormatError> {
        let refs: Vec<&str> = header.iter().map(|h| h.as_ref()).collect();
        self.inner.write_record(&refs)?;
        self.header_written = true;
        Ok(())
    }

    /// Borrow the underlying writer (e.g. for byte-count inspection).
    pub fn get_ref(&self) -> &W {
        self.inner.get_ref()
    }
}

impl<W: Write + Send> FormatWriter for CsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Header is built from the writer's pinned schema. Engine-stamped
        // columns (today: `$ck.<field>`) are stripped unless the Output
        // node opts in. Framing metadata stays in `$meta.*` and only
        // surfaces via `iter_meta` when `include_metadata` is set on the
        // Output node.
        if self.config.include_header && !self.header_written {
            let header: Vec<&str> =
                filtered_header_columns(&self.schema, self.config.include_engine_stamped);
            self.inner.write_record(&header)?;
            self.header_written = true;
        }

        let fields: Vec<String> = if self.config.include_engine_stamped {
            record
                .iter_all_fields()
                .map(|(_, v)| value_to_csv_cell(v))
                .collect()
        } else {
            record
                .iter_user_fields()
                .map(|(_, v)| value_to_csv_cell(v))
                .collect()
        };

        self.inner.write_record(&fields)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()?;
        Ok(())
    }
}

/// CSV writer wrapper that captures the header (schema columns only)
/// from the first record into shared state for replay on split rotation.
///
/// Only used by the CSV writer factory when splitting is enabled.
/// Subsequent split files receive the captured header via `write_preset_header()`.
/// Non-CSV formats do not need this — their factories are stateless.
pub struct HeaderCapturingCsvWriter<W: Write> {
    inner: CsvWriter<W>,
    schema: Arc<Schema>,
    shared_header: Arc<Mutex<Option<Vec<Box<str>>>>>,
    captured: bool,
}

impl<W: Write> HeaderCapturingCsvWriter<W> {
    pub fn new(
        inner: CsvWriter<W>,
        schema: Arc<Schema>,
        shared_header: Arc<Mutex<Option<Vec<Box<str>>>>>,
    ) -> Self {
        Self {
            inner,
            schema,
            shared_header,
            captured: false,
        }
    }
}

impl<W: Write + Send> FormatWriter for HeaderCapturingCsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        if !self.captured {
            // Capture header so split rotations replay the same column
            // set; engine-stamped columns are filtered identically to
            // the inner CSV writer.
            let include = self.inner.config.include_engine_stamped;
            let header: Vec<Box<str>> = self
                .schema
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    include
                        || self
                            .schema
                            .field_metadata(*i)
                            .is_none_or(|m| !m.is_engine_stamped())
                })
                .map(|(_, name)| name.clone())
                .collect();
            *self.shared_header.lock().unwrap() = Some(header);
            self.captured = true;
        }
        self.inner.write_record(record)
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()
    }
}

/// Build the CSV header name list from `schema`, optionally including
/// engine-stamped columns.
fn filtered_header_columns(schema: &Arc<Schema>, include_engine_stamped: bool) -> Vec<&str> {
    schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            include_engine_stamped
                || schema
                    .field_metadata(*i)
                    .is_none_or(|m| !m.is_engine_stamped())
        })
        .map(|(_, c)| c.as_ref())
        .collect()
}

/// Serialize a Value to a CSV cell string.
fn value_to_csv_cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "true" } else { "false" }.into(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
        Value::Map(m) => serde_json::to_string(m.as_ref()).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::reader::{CsvReader, CsvReaderConfig};
    use crate::traits::FormatReader;

    fn make_schema(cols: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()))
    }

    fn make_record(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), values)
    }

    fn write_to_string(
        schema: &Arc<Schema>,
        config: CsvWriterConfig,
        records: &[Record],
    ) -> String {
        let mut buf = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buf, Arc::clone(schema), config);
            for r in records {
                writer.write_record(r).unwrap();
            }
            writer.flush().unwrap();
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_csv_writer_basic_output() {
        let schema = make_schema(&["name", "age"]);
        let records = vec![
            make_record(
                &schema,
                vec![Value::String("Alice".into()), Value::String("30".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Bob".into()), Value::String("25".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Charlie".into()), Value::String("35".into())],
            ),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "name,age\nAlice,30\nBob,25\nCharlie,35\n");
    }

    #[test]
    fn test_csv_writer_with_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: true,
                ..Default::default()
            },
            &records,
        );
        assert!(output.starts_with("x,y\n"));
    }

    #[test]
    fn test_csv_writer_no_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: false,
                ..Default::default()
            },
            &records,
        );
        assert_eq!(output, "1,2\n");
    }

    #[test]
    fn test_csv_writer_null_as_empty() {
        let schema = make_schema(&["a", "b", "c"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("x".into()),
                Value::Null,
                Value::String("z".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // Null becomes empty string between delimiters
        assert_eq!(output, "a,b,c\nx,,z\n");
    }

    fn make_schema_with_engine_stamp(user_col: &str, stamp_col: &str) -> Arc<Schema> {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        SchemaBuilder::new()
            .with_field(user_col)
            .with_field_meta(stamp_col, FieldMetadata::source_correlation(user_col))
            .build()
    }

    #[test]
    fn test_csv_writer_strips_engine_stamped_by_default() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n7\n");
        assert!(!output.contains("$ck.id"));
    }

    #[test]
    fn test_csv_writer_includes_engine_stamped_on_opt_in() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let config = CsvWriterConfig {
            include_engine_stamped: true,
            ..Default::default()
        };
        let output = write_to_string(&schema, config, &[record]);
        assert_eq!(output, "id,$ck.id\n7,7\n");
    }

    #[test]
    fn test_csv_writer_emits_schema_fields_only() {
        // Writer contract after the overflow rip: metadata stays in
        // `$meta.*` and is stripped from the default output.
        let schema = make_schema(&["id"]);
        let mut record = make_record(&schema, vec![Value::Integer(1)]);
        record.set_meta("zulu", Value::String("z".into())).unwrap();
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n1\n");
    }

    #[test]
    fn test_csv_writer_widened_schema_emit_order() {
        // Widened schema controls output order; the fixture now declares
        // every emitted column up front, so record.set always lands at a
        // known slot and iter_all_fields walks them in schema order.
        let schema = make_schema(&["id", "zulu", "alpha", "mike"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::String("z".into()),
                Value::String("a".into()),
                Value::String("m".into()),
            ],
        );
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id,zulu,alpha,mike\n1,z,a,m\n");
    }

    #[test]
    fn test_csv_writer_quoting_special_chars() {
        let schema = make_schema(&["name", "bio"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Likes commas, and\nnewlines".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // csv crate should quote the field containing comma and newline
        assert!(output.contains("\"Likes commas, and\nnewlines\""));
    }

    #[test]
    fn test_csv_roundtrip_lossless() {
        let input = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";

        // Read
        let mut reader = CsvReader::from_reader(input.as_bytes(), CsvReaderConfig::default());
        let schema = reader.schema().unwrap();
        let mut records = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            records.push(r);
        }
        assert_eq!(records.len(), 3);

        // Write
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);

        // Read again
        let mut reader2 = CsvReader::from_reader(output.as_bytes(), CsvReaderConfig::default());
        let schema2 = reader2.schema().unwrap();
        let mut records2 = Vec::new();
        while let Some(r) = reader2.next_record().unwrap() {
            records2.push(r);
        }

        // Schemas match
        assert_eq!(schema.columns(), schema2.columns());

        // Records match field by field
        assert_eq!(records.len(), records2.len());
        for (r1, r2) in records.iter().zip(records2.iter()) {
            for col in schema.columns() {
                assert_eq!(r1.get(col), r2.get(col), "mismatch on column {col}");
            }
        }
    }
}
