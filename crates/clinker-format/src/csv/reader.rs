use std::io::Read;
use std::sync::Arc;

use clinker_record::{Record, Schema, SchemaBuilder, Value};

use crate::bom::SkipBom;
use crate::charset::Charset;
use crate::error::FormatError;
use crate::multi_value::{SplitValues, split_text_value_escaped};
use crate::traits::FormatReader;

/// Configuration for the CSV reader.
pub struct CsvReaderConfig {
    pub delimiter: u8,
    pub quote_char: u8,
    pub has_header: bool,
    /// Character set the field bytes are decoded through. Defaults to UTF-8;
    /// set from the source's declared `encoding` so a non-UTF-8 export (e.g.
    /// `iso-8859-1`) decodes high bytes correctly instead of failing UTF-8
    /// validation or silently corrupting them.
    pub charset: Charset,
    /// In-cell parse declarations: a column's cell text is split on its
    /// delimiter into the several values a `multiple: true` column holds.
    /// Empty by default; populated from the source's `split_values`.
    pub split_values: Vec<SplitValues>,
}

impl Default for CsvReaderConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote_char: b'"',
            has_header: true,
            charset: Charset::default(),
            split_values: Vec::new(),
        }
    }
}

/// Streaming CSV reader wrapping `csv::Reader`.
///
/// All fields are read as `Value::String` — type coercion is CXL's
/// responsibility. Schema is inferred from the header row (or generated
/// synthetically as `col_0`, `col_1`, ...).
///
/// Rows are read as raw bytes (`csv::ByteRecord`) and each field — including
/// header cells — is decoded through the configured [`Charset`]. UTF-8 (the
/// default) validates strictly; a declared `iso-8859-1` source decodes high
/// bytes faithfully. This per-field decode replaces the csv crate's implicit
/// UTF-8-only string path so the source's declared `encoding` is honored.
///
/// A leading UTF-8 BOM (prepended by Excel "CSV UTF-8" and PowerShell
/// `Out-File -Encoding utf8`) is stripped before parsing via [`SkipBom`],
/// so the first header cell — or first data field under
/// `has_header: false` — never carries the `U+FEFF` marker.
pub struct CsvReader<R: Read> {
    inner: csv::Reader<SkipBom<R>>,
    schema: Option<Arc<Schema>>,
    config: CsvReaderConfig,
    /// Per-column split declaration, index-aligned to the schema columns:
    /// `Some(entry)` for a column a `split_values` entry covers, `None`
    /// otherwise. Built once alongside the schema so per-record decoding is a
    /// positional lookup rather than a name scan. Empty until the schema is
    /// resolved and when no `split_values` are declared. The whole entry is
    /// stored (not just the delimiter) so a column can carry an `escape:` or
    /// `json:` recovery mode as well.
    split_specs: Vec<Option<SplitValues>>,
    row_count: u64,
    record_buf: csv::ByteRecord,
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
            split_specs: Vec::new(),
            row_count: 0,
            record_buf: csv::ByteRecord::new(),
        }
    }

    fn ensure_schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref schema) = self.schema {
            return Ok(Arc::clone(schema));
        }

        let schema = if self.config.has_header {
            let charset = self.config.charset;
            let headers = self.inner.byte_headers()?;
            if headers.is_empty() {
                return Err(FormatError::SchemaInference("header row is empty".into()));
            }
            // Decode each header cell through the configured charset so a
            // non-UTF-8 column name resolves identically to its body fields.
            let columns: Vec<Box<str>> = headers
                .iter()
                .map(|f| charset.decode(f.to_vec()).map(Box::<str>::from))
                .collect::<Result<_, _>>()?;
            columns.into_iter().collect::<SchemaBuilder>().build()
        } else if !self.inner.read_byte_record(&mut self.record_buf)? {
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

        // Build the index-aligned split map: for each column a `split_values`
        // entry names, record its whole declaration at that column's position.
        // The entry's `field` is the document field name, which for CSV is the
        // column (header) name. E358 has already checked, at plan time, that
        // every entry names a real `multiple: true` column; an entry that
        // matches no column here is simply inert.
        if !self.config.split_values.is_empty() {
            self.split_specs = schema
                .columns()
                .iter()
                .map(|name| {
                    self.config
                        .split_values
                        .iter()
                        .find(|e| e.field.as_str() == name.as_ref())
                        .cloned()
                })
                .collect();
        }

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
        let charset = self.config.charset;

        // If we peeked a record during no-header schema inference, consume it first
        if !self.config.has_header && self.row_count == 0 && !self.record_buf.is_empty() {
            self.row_count += 1;
            let values = decode_record(&self.record_buf, charset, &self.split_specs)?;
            return Ok(Some(Record::new(schema, values)));
        }

        if !self.inner.read_byte_record(&mut self.record_buf)? {
            return Ok(None);
        }

        self.row_count += 1;
        let values = decode_record(&self.record_buf, charset, &self.split_specs)?;
        Ok(Some(Record::new(schema, values)))
    }
}

/// Decode every field of a raw [`csv::ByteRecord`] through `charset` into a
/// column vector. A field that is not valid in the configured charset (only
/// possible under [`Charset::Utf8`]) fails the record loudly rather than
/// substituting replacement bytes.
///
/// A column carrying a `Some(entry)` in `split_specs` — a `multiple: true`
/// column a `split_values` entry covers — materializes as a `Value::Array`:
///
/// - the default delimited decode splits the cell on the entry's delimiter;
/// - a non-empty `escape` un-escapes as it splits, recovering a sink's
///   `on_conflict: escape` output exactly;
/// - `json: true` parses the whole cell as an embedded JSON array, recovering a
///   sink's `on_conflict: encode_json` output.
///
/// An empty cell yields an empty array (zero values) under every mode, not a
/// one-element array holding an empty string: a blank CSV cell means the column
/// has no values, whereas an author who wrote a delimiter between two empties
/// meant those empties. Every other column stays a scalar `Value::String`;
/// element typing is CXL's responsibility downstream.
fn decode_record(
    rec: &csv::ByteRecord,
    charset: Charset,
    split_specs: &[Option<SplitValues>],
) -> Result<Vec<Value>, FormatError> {
    rec.iter()
        .enumerate()
        .map(|(i, f)| {
            let s = charset.decode(f.to_vec())?;
            let Some(entry) = split_specs.get(i).and_then(Option::as_ref) else {
                return Ok(Value::String(s.into()));
            };
            if s.is_empty() {
                return Ok(Value::Array(Vec::new()));
            }
            if entry.json {
                let parsed: serde_json::Value = serde_json::from_str(&s).map_err(|e| {
                    FormatError::Json(format!(
                        "split_values `json: true` on field '{}': cell is not valid JSON: {e}",
                        entry.field
                    ))
                })?;
                // A `multiple:` column must hold an array; a non-array JSON cell
                // (a bare scalar or object) would bind a scalar/map at an array
                // column, so reject it loudly rather than deliver the wrong shape.
                let serde_json::Value::Array(items) = &parsed else {
                    return Err(FormatError::Json(format!(
                        "split_values `json: true` on field '{}': cell is JSON but not an array \
                         (a `multiple:` column holds an array)",
                        entry.field
                    )));
                };
                // `json_to_value` binds a JSON integer via `as_i64()` and falls
                // back to `as_f64()`, so an integer that fits `u64` but not `i64`
                // — which serde preserved exactly — would be silently coerced to a
                // lossy float. Reject it loudly instead. (A value clinker itself
                // wrote via `encode_json` is always an `i64`, so this only guards
                // an externally-authored cell; an integer beyond `u64` is already
                // a float at parse time, a serde_json limitation.)
                for item in items {
                    if let serde_json::Value::Number(n) = item
                        && n.is_u64()
                        && !n.is_i64()
                    {
                        return Err(FormatError::Json(format!(
                            "split_values `json: true` on field '{}': integer {n} exceeds the \
                             supported range and would lose precision as a float",
                            entry.field
                        )));
                    }
                }
                return Ok(crate::json::reader::json_to_value(&parsed));
            }
            Ok(split_text_value_escaped(
                &Value::String(s.into()),
                &entry.delimiter,
                &entry.escape,
            ))
        })
        .collect()
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

    /// A config whose `tags` column is split on the given delimiter.
    fn split_config(field: &str, delimiter: &str) -> CsvReaderConfig {
        CsvReaderConfig {
            split_values: vec![SplitValues {
                field: field.to_string(),
                delimiter: delimiter.to_string(),
                escape: String::new(),
                json: false,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn split_values_parses_cell_into_array() {
        let csv = "order_id,tags\n1,a;b;c";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("order_id"), Some(&Value::String("1".into())));
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_empty_cell_yields_empty_array() {
        let csv = "order_id,tags\n1,";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("tags"), Some(&Value::Array(Vec::new())));
    }

    #[test]
    fn split_values_single_value_yields_one_element_array() {
        let csv = "order_id,tags\n1,solo";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![Value::String("solo".into())]))
        );
    }

    #[test]
    fn split_values_preserves_interior_empty_parts() {
        // Only a *fully* empty cell collapses to `[]`; a delimiter written
        // between two empties is the author declaring those empties.
        let csv = "order_id,tags\n1,a;;c";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_only_touches_covered_columns() {
        // `notes` is not declared for splitting, so it stays a scalar even
        // though it contains the delimiter.
        let csv = "order_id,tags,notes\n1,a;b,x;y";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ]))
        );
        assert_eq!(record.get("notes"), Some(&Value::String("x;y".into())));
    }

    #[test]
    fn split_values_splits_a_quoted_cell_after_csv_unquoting() {
        // A quoted cell containing the CSV field delimiter is unquoted by the
        // csv parser first; the intra-cell split then runs on the recovered
        // text, so the field delimiter inside the quotes is not a boundary.
        let csv = "order_id,tags\n1,\"a,b;c\"";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), split_config("tags", ";"));
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a,b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_escape_recovers_escaped_delimiter() {
        // A cell written under `on_conflict: escape`: `a\;b` is one value
        // "a;b", `c` is another. The escape-aware split recovers both.
        let csv = "order_id,tags\n1,a\\;b;c";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a;b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_escape_recovers_escaped_escape_char() {
        // An escaped escape `\\` is one literal backslash, not a boundary.
        let csv = "order_id,tags\n1,a\\\\b;c";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a\\b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_json_decodes_embedded_array() {
        // A cell written under `on_conflict: encode_json` is a JSON array,
        // recovered whole regardless of delimiters/quotes inside the values.
        let csv = "order_id,tags\n1,\"[\"\"a;b\"\",\"\"c\"\"]\"";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a;b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn split_values_json_empty_cell_yields_empty_array() {
        let csv = "order_id,tags\n1,";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("tags"), Some(&Value::Array(Vec::new())));
    }

    #[test]
    fn split_values_json_non_array_cell_errors() {
        // A `multiple:` column must hold an array; a JSON scalar/object cell is
        // rejected loudly rather than binding a non-array Value at the column.
        let csv = "order_id,tags\n1,\"{\"\"a\"\":1}\"";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let err = reader.next_record().unwrap_err();
        assert!(
            matches!(&err, FormatError::Json(m) if m.contains("not an array")),
            "expected a not-an-array error, got {err:?}"
        );
    }

    #[test]
    fn split_values_json_integer_out_of_i64_range_errors() {
        // An integer that fits u64 but not i64 would be silently coerced to a
        // lossy float by json_to_value; the CSV json decode rejects it loudly.
        let csv = "order_id,ids\n1,[12345678901234567890]";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "ids".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let err = reader.next_record().unwrap_err();
        assert!(
            matches!(&err, FormatError::Json(m) if m.contains("exceeds the supported range")),
            "expected an out-of-range integer error, got {err:?}"
        );
    }

    #[test]
    fn split_values_json_in_range_integers_decode() {
        // An i64-range integer array decodes exactly (no spurious rejection).
        let csv = "order_id,ids\n1,\"[1,2,9007199254740993]\"";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "ids".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("ids"),
            Some(&Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(9007199254740993),
            ]))
        );
    }

    #[test]
    fn split_values_json_invalid_cell_errors() {
        let csv = "order_id,tags\n1,not-json";
        let config = CsvReaderConfig {
            split_values: vec![SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(csv.as_bytes(), config);
        reader.schema().unwrap();
        let err = reader.next_record().unwrap_err();
        assert!(
            matches!(&err, FormatError::Json(m) if m.contains("not valid JSON")),
            "expected a JSON decode error, got {err:?}"
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

    /// Build CSV bytes whose body field carries the Latin-1 high byte for `é`.
    /// `name\nCaf\xE9\n` is not valid UTF-8, so it exercises the non-UTF-8
    /// decode path rather than incidental ASCII.
    fn latin1_bytes() -> Vec<u8> {
        let mut bytes = b"name\nCaf".to_vec();
        bytes.push(0xE9); // 'é' in ISO-8859-1
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn test_csv_reader_latin1_decodes_high_bytes() {
        // Under the declared Latin-1 charset the high byte decodes to `é`.
        let config = CsvReaderConfig {
            charset: Charset::Latin1,
            ..Default::default()
        };
        let bytes = latin1_bytes();
        let mut reader = CsvReader::from_reader(&bytes[..], config);
        let _schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("name"), Some(&Value::String("Café".into())));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_csv_reader_latin1_decodes_high_byte_in_header() {
        // A high byte in the header row decodes through the same charset, so
        // the column name resolves to its decoded form.
        let mut bytes = b"caf".to_vec();
        bytes.push(0xE9); // header `café`
        bytes.extend_from_slice(b"\n1\n");
        let config = CsvReaderConfig {
            charset: Charset::Latin1,
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(&bytes[..], config);
        let schema = reader.schema().unwrap();
        assert_eq!(
            schema.columns().iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["café"]
        );
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("café"), Some(&Value::String("1".into())));
    }

    #[test]
    fn test_csv_reader_utf8_default_rejects_high_byte() {
        // The same Latin-1 bytes under the default UTF-8 charset fail loudly
        // rather than substituting replacement characters.
        let bytes = latin1_bytes();
        let mut reader = CsvReader::from_reader(&bytes[..], CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let err = reader.next_record().unwrap_err();
        assert!(
            matches!(&err, FormatError::Charset(m) if m.contains("not valid UTF-8")),
            "expected a UTF-8 decode error naming the encoding, got {err:?}"
        );
    }

    #[test]
    fn test_csv_reader_utf8_default_decodes_multibyte() {
        // Valid multi-byte UTF-8 is unchanged under the default charset.
        let csv = "name\nCafé\nAño";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Café".into())));
        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r2.get("name"), Some(&Value::String("Año".into())));
    }
}
