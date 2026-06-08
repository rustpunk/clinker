use std::io::Read;
use std::sync::Arc;

use clinker_record::{Record, Schema, SchemaBuilder, Value};

use crate::bom::SkipBom;
use crate::error::FormatError;
use crate::traits::FormatReader;

/// Configuration for the CSV reader.
pub struct CsvReaderConfig {
    pub delimiter: u8,
    pub quote_char: u8,
    pub has_header: bool,
}

impl Default for CsvReaderConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote_char: b'"',
            has_header: true,
        }
    }
}

/// Streaming CSV reader wrapping `csv::Reader`.
///
/// All fields are read as `Value::String` — type coercion is CXL's
/// responsibility. Schema is inferred from the header row (or generated
/// synthetically as `col_0`, `col_1`, ...).
///
/// A leading UTF-8 BOM (prepended by Excel "CSV UTF-8" and PowerShell
/// `Out-File -Encoding utf8`) is stripped before parsing via [`SkipBom`],
/// so the first header cell — or first data field under
/// `has_header: false` — never carries the `U+FEFF` marker.
pub struct CsvReader<R: Read> {
    inner: csv::Reader<SkipBom<R>>,
    schema: Option<Arc<Schema>>,
    config: CsvReaderConfig,
    row_count: u64,
    record_buf: csv::StringRecord,
}

impl<R: Read> CsvReader<R> {
    pub fn from_reader(reader: R, config: CsvReaderConfig) -> Self {
        let inner = csv::ReaderBuilder::new()
            .delimiter(config.delimiter)
            .quote(config.quote_char)
            .has_headers(config.has_header)
            .from_reader(SkipBom::new(reader));
        Self {
            inner,
            schema: None,
            config,
            row_count: 0,
            record_buf: csv::StringRecord::new(),
        }
    }

    fn ensure_schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref schema) = self.schema {
            return Ok(Arc::clone(schema));
        }

        let schema = if self.config.has_header {
            let headers = self.inner.headers()?;
            if headers.is_empty() {
                return Err(FormatError::SchemaInference("header row is empty".into()));
            }
            headers
                .iter()
                .map(Box::<str>::from)
                .collect::<SchemaBuilder>()
                .build()
        } else if !self.inner.read_record(&mut self.record_buf)? {
            // Peek at the first record to determine column count. No
            // record at all ⇒ empty file ⇒ empty schema.
            SchemaBuilder::new().build()
        } else {
            let count = self.record_buf.len();
            (0..count)
                .map(|i| format!("col_{i}"))
                .collect::<SchemaBuilder>()
                .build()
        };

        self.schema = Some(Arc::clone(&schema));
        Ok(schema)
    }
}

impl<R: Read + Send> FormatReader for CsvReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        self.ensure_schema()
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        let schema = self.ensure_schema()?;

        // If we peeked a record during no-header schema inference, consume it first
        if !self.config.has_header && self.row_count == 0 && !self.record_buf.is_empty() {
            self.row_count += 1;
            let values: Vec<Value> = self
                .record_buf
                .iter()
                .map(|f| Value::String(f.into()))
                .collect();
            return Ok(Some(Record::new(schema, values)));
        }

        if !self.inner.read_record(&mut self.record_buf)? {
            return Ok(None);
        }

        self.row_count += 1;
        let values: Vec<Value> = self
            .record_buf
            .iter()
            .map(|f| Value::String(f.into()))
            .collect();
        Ok(Some(Record::new(schema, values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::FieldResolver;

    #[test]
    fn test_csv_reader_basic_three_rows() {
        let csv = "name,age\nAlice,30\nBob,25\nCharlie,35";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let mut count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            count += 1;
            // Verify fields are accessible
            assert!(record.get("name").is_some());
            assert!(record.get("age").is_some());
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_csv_reader_schema_from_header() {
        let csv = "first_name,last_name,email\nA,B,C";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let schema = reader.schema().unwrap();
        assert_eq!(
            schema.columns().iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["first_name", "last_name", "email"]
        );
    }

    #[test]
    fn test_csv_reader_no_header_synthetic_names() {
        let csv = "Alice,30\nBob,25";
        let config = CsvReaderConfig {
            has_header: false,
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        let schema = reader.schema().unwrap();
        assert_eq!(
            schema.columns().iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["col_0", "col_1"]
        );
        // Verify both records are still readable
        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.get("col_0"), Some(&Value::String("Alice".into())));
        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r2.get("col_0"), Some(&Value::String("Bob".into())));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_csv_reader_tab_delimiter() {
        let csv = "name\tage\nAlice\t30";
        let config = CsvReaderConfig {
            delimiter: b'\t',
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        let _schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(record.get("age"), Some(&Value::String("30".into())));
    }

    #[test]
    fn test_csv_reader_quoted_fields() {
        let csv = "name,bio\nAlice,\"Likes commas, and\nnewlines\"";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("bio"),
            Some(&Value::String("Likes commas, and\nnewlines".into()))
        );
    }

    #[test]
    fn test_csv_reader_empty_file() {
        let csv = "";
        let config = CsvReaderConfig {
            has_header: false,
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        let _schema = reader.schema().unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_format_reader_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<CsvReader<std::io::Cursor<Vec<u8>>>>();
    }

    /// Prefixes `body` with a UTF-8 BOM, mimicking an Excel "CSV UTF-8"
    /// or PowerShell `Out-File -Encoding utf8` export.
    fn with_bom(body: &str) -> Vec<u8> {
        let mut bytes = crate::bom::UTF8_BOM.to_vec();
        bytes.extend_from_slice(body.as_bytes());
        bytes
    }

    #[test]
    fn test_csv_reader_strips_leading_bom_header() {
        // The first header cell must resolve as `id`, not `\u{feff}id`,
        // so downstream field references parse identically to BOM-less
        // input.
        let body = "id,name\n1,Alice\n2,Bob";
        let bytes = with_bom(body);
        let mut bom = CsvReader::from_reader(&bytes[..], CsvReaderConfig::default());
        let schema = bom.schema().unwrap();
        assert_eq!(
            schema.columns().iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["id", "name"]
        );
        let r1 = bom.next_record().unwrap().unwrap();
        assert_eq!(r1.get("id"), Some(&Value::String("1".into())));
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_csv_reader_bom_matches_bomless() {
        // Full parse equivalence: a BOM-prefixed file and its BOM-less
        // twin yield identical schema and records.
        let body = "id,name\n1,Alice\n2,Bob\n3,Carol";

        let mut plain = CsvReader::from_reader(body.as_bytes(), CsvReaderConfig::default());
        let plain_schema: Vec<String> = plain
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|c| c.to_string())
            .collect();
        let mut plain_rows = Vec::new();
        while let Some(r) = plain.next_record().unwrap() {
            plain_rows.push((r.get("id").cloned(), r.get("name").cloned()));
        }

        let bytes = with_bom(body);
        let mut bom = CsvReader::from_reader(&bytes[..], CsvReaderConfig::default());
        let bom_schema: Vec<String> = bom
            .schema()
            .unwrap()
            .columns()
            .iter()
            .map(|c| c.to_string())
            .collect();
        let mut bom_rows = Vec::new();
        while let Some(r) = bom.next_record().unwrap() {
            bom_rows.push((r.get("id").cloned(), r.get("name").cloned()));
        }

        assert_eq!(plain_schema, bom_schema);
        assert_eq!(plain_rows, bom_rows);
        assert_eq!(plain_rows.len(), 3);
    }

    #[test]
    fn test_csv_reader_strips_leading_bom_no_header() {
        // Under `has_header: false` the BOM would otherwise corrupt the
        // first data field of the first record, not a header cell.
        let body = "Alice,30\nBob,25";
        let config = CsvReaderConfig {
            has_header: false,
            ..Default::default()
        };
        let bytes = with_bom(body);
        let mut bom = CsvReader::from_reader(&bytes[..], config);
        let _schema = bom.schema().unwrap();
        let r1 = bom.next_record().unwrap().unwrap();
        assert_eq!(r1.get("col_0"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("col_1"), Some(&Value::String("30".into())));
        let r2 = bom.next_record().unwrap().unwrap();
        assert_eq!(r2.get("col_0"), Some(&Value::String("Bob".into())));
        assert!(bom.next_record().unwrap().is_none());
    }

    // FieldResolver integration — verify Record from CSV implements it
    #[test]
    fn test_csv_record_field_resolver() {
        let csv = "name,age\nAda,30";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();

        // Use FieldResolver trait methods
        assert_eq!(record.resolve("name"), Some(&Value::String("Ada".into())));
        assert_eq!(record.resolve("age"), Some(&Value::String("30".into())));
        assert_eq!(record.resolve("missing"), None);
    }
}
