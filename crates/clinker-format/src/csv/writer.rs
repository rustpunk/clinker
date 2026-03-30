use std::io::Write;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the CSV writer.
pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub include_header: bool,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            include_header: true,
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
}

impl<W: Write + Send> FormatWriter for CsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Collect overflow field names sorted alphabetically
        let mut overflow_keys: Vec<&str> = record
            .overflow_fields()
            .map(|iter| iter.map(|(k, _)| k).collect())
            .unwrap_or_default();
        overflow_keys.sort_unstable();

        // Write header on first record
        if self.config.include_header && !self.header_written {
            let mut header: Vec<&str> = self
                .schema
                .columns()
                .iter()
                .map(|c| c.as_ref())
                .collect();
            header.extend(overflow_keys.iter().copied());
            self.inner.write_record(&header)?;
            self.header_written = true;
        }

        // Write schema fields in order
        let mut fields: Vec<String> = self
            .schema
            .columns()
            .iter()
            .map(|col| {
                record
                    .get(col)
                    .map(value_to_csv_cell)
                    .unwrap_or_default()
            })
            .collect();

        // Append overflow fields in sorted key order
        for key in &overflow_keys {
            let cell = record
                .get(key)
                .map(value_to_csv_cell)
                .unwrap_or_default();
            fields.push(cell);
        }

        self.inner.write_record(&fields)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.inner.flush()?;
        Ok(())
    }
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

    fn write_to_string(schema: &Arc<Schema>, config: CsvWriterConfig, records: &[Record]) -> String {
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
            make_record(&schema, vec![Value::String("Alice".into()), Value::String("30".into())]),
            make_record(&schema, vec![Value::String("Bob".into()), Value::String("25".into())]),
            make_record(&schema, vec![Value::String("Charlie".into()), Value::String("35".into())]),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "name,age\nAlice,30\nBob,25\nCharlie,35\n");
    }

    #[test]
    fn test_csv_writer_with_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(&schema, vec![Value::Integer(1), Value::Integer(2)])];
        let output = write_to_string(
            &schema,
            CsvWriterConfig { include_header: true, ..Default::default() },
            &records,
        );
        assert!(output.starts_with("x,y\n"));
    }

    #[test]
    fn test_csv_writer_no_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(&schema, vec![Value::Integer(1), Value::Integer(2)])];
        let output = write_to_string(
            &schema,
            CsvWriterConfig { include_header: false, ..Default::default() },
            &records,
        );
        assert_eq!(output, "1,2\n");
    }

    #[test]
    fn test_csv_writer_null_as_empty() {
        let schema = make_schema(&["a", "b", "c"]);
        let records = vec![make_record(
            &schema,
            vec![Value::String("x".into()), Value::Null, Value::String("z".into())],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // Null becomes empty string between delimiters
        assert_eq!(output, "a,b,c\nx,,z\n");
    }

    #[test]
    fn test_csv_writer_overflow_fields_sorted() {
        let schema = make_schema(&["id"]);
        let mut record = make_record(&schema, vec![Value::Integer(1)]);
        record.set_overflow("zulu".into(), Value::String("z".into()));
        record.set_overflow("alpha".into(), Value::String("a".into()));
        record.set_overflow("mike".into(), Value::String("m".into()));

        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        // Schema field first, then overflow in alphabetical order
        assert_eq!(output, "id,alpha,mike,zulu\n1,a,m,z\n");
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
