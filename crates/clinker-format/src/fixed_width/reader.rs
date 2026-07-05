//! Fixed-width record reader.
//!
//! The schema is constructed positionally from the user-declared
//! [`Column`] list (`width` / `start..end` byte ranges). Bytes
//! outside the declared ranges are structurally invisible to the
//! reader — there is no "extra column" concept the way CSV's
//! header row or JSON's per-record key set has one.
//!
//! Consequence for `on_unmapped: auto_widen` (the engine-wide
//! default): the `$widened` sidecar absorber slot exists on the
//! source's plan-time schema (the engine adds it uniformly), but
//! the fixed-width reader cannot ever populate it — the slot's
//! payload stays `Value::Null` for every record. The executor's
//! `wrap_with_schema_coercion` emits a one-time `tracing::info`
//! per fixed-width source whose policy is `auto_widen`, naming
//! the source so the user can either accept the empty sidecar or
//! switch to `on_unmapped: drop` / `reject` for explicit scalar
//! semantics.

use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::bom::SkipBom;
use crate::fixed_width::field::{self, ResolvedField};
use crate::schema::Column;
use clinker_record::schema_def::LineSeparator;
use clinker_record::{Record, Schema, SchemaBuilder};

use crate::error::FormatError;
use crate::traits::FormatReader;

/// Configuration for the fixed-width reader.
pub struct FixedWidthReaderConfig {
    pub line_separator: LineSeparator,
}

impl Default for FixedWidthReaderConfig {
    fn default() -> Self {
        Self {
            line_separator: LineSeparator::Lf,
        }
    }
}

/// Schema-driven fixed-width record reader.
/// Byte-offset extraction with per-field UTF-8 validation and type coercion.
/// Schema injected at construction (constructor injection pattern).
pub struct FixedWidthReader<R: Read> {
    // Source wrapped in `SkipBom` so a leading UTF-8 BOM (Windows utf8
    // export) is dropped before byte-offset extraction, which would
    // otherwise shift every field of the first record.
    reader: BufReader<SkipBom<R>>,
    fields: Vec<ResolvedField>,
    schema: Arc<Schema>,
    config: FixedWidthReaderConfig,
    record_length: usize,
    line_buf: Vec<u8>,
    row_number: u64,
}

impl<R: Read> FixedWidthReader<R> {
    pub fn new(
        reader: R,
        fields: Vec<Column>,
        config: FixedWidthReaderConfig,
    ) -> Result<Self, FormatError> {
        let resolved: Vec<ResolvedField> = fields
            .iter()
            .map(ResolvedField::from_column)
            .collect::<Result<_, _>>()?;

        let schema = fields
            .iter()
            .map(|f| Box::<str>::from(f.name.as_str()))
            .collect::<SchemaBuilder>()
            .build();
        let record_length = resolved.iter().map(ResolvedField::end).max().unwrap_or(0);

        Ok(Self {
            reader: BufReader::new(SkipBom::new(reader)),
            fields: resolved,
            schema,
            config,
            record_length,
            line_buf: Vec::with_capacity(256),
            row_number: 0,
        })
    }
}

impl<R: Read + Send> FormatReader for FixedWidthReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if !field::read_physical_line(
            &mut self.reader,
            &self.config.line_separator,
            self.record_length,
            self.row_number,
            &mut self.line_buf,
        )? {
            return Ok(None);
        }
        self.row_number += 1;

        let mut values = Vec::with_capacity(self.fields.len());
        for f in &self.fields {
            values.push(field::extract_value(f, &self.line_buf, self.row_number)?);
        }

        Ok(Some(Record::new(Arc::clone(&self.schema), values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use clinker_record::Value;
    use clinker_record::schema_def::Justify;
    use cxl::typecheck::Type;

    fn field(name: &str) -> Column {
        Column::bare(name, Type::String)
    }

    #[test]
    fn test_fixedwidth_read_basic() {
        // "00042Alice               20240115"
        //  ^^^^^ id (0..5, int)
        //       ^^^^^^^^^^^^^^^^^^^^ name (5..25, string)
        //                           ^^^^^^^^ date (25..33, string)
        let data = b"00042Alice               20240115\n";

        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(5);
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.ty = Type::String;
                f.start = Some(5);
                f.width = Some(20);
                f
            },
            {
                let mut f = field("date");
                f.ty = Type::String;
                f.start = Some(25);
                f.width = Some(8);
                f
            },
        ];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();

        let schema = reader.schema().unwrap();
        assert_eq!(schema.columns().len(), 3);

        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::Integer(42)));
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get("date"), Some(&Value::String("20240115".into())));

        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_fixedwidth_strips_leading_bom() {
        // A leading UTF-8 BOM (Windows utf8 export) must be dropped before
        // byte-offset extraction, or it shifts every field of the first row.
        let mut data = crate::bom::UTF8_BOM.to_vec();
        data.extend_from_slice(b"00042Alice               20240115\n");

        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(5);
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.ty = Type::String;
                f.start = Some(5);
                f.width = Some(20);
                f
            },
            {
                let mut f = field("date");
                f.ty = Type::String;
                f.start = Some(25);
                f.width = Some(8);
                f
            },
        ];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::Integer(42)));
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get("date"), Some(&Value::String("20240115".into())));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_fixedwidth_read_start_end() {
        // Same as basic but using end instead of width
        let data = b"00042Alice               20240115\n";

        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.end = Some(5); // end - start = 5 = same as width: 5
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.ty = Type::String;
                f.start = Some(5);
                f.end = Some(25); // end - start = 20
                f
            },
            {
                let mut f = field("date");
                f.ty = Type::String;
                f.start = Some(25);
                f.end = Some(33); // end - start = 8
                f
            },
        ];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("id"), Some(&Value::Integer(42)));
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get("date"), Some(&Value::String("20240115".into())));
    }

    #[test]
    fn test_fixedwidth_read_trim_trailing() {
        let data = b"Smith   \n";

        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(8);
            // trim defaults to true
            f
        }];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Smith".into())));
    }

    #[test]
    fn test_fixedwidth_read_no_trim() {
        let data = b"Smith   \n";

        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(8);
            f.trim = Some(false);
            f
        }];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("Smith   ".into())));
    }

    #[test]
    fn test_fixedwidth_read_right_justified_numeric() {
        let data = b"  042\n";

        let fields = vec![{
            let mut f = field("amount");
            f.ty = Type::Int;
            f.start = Some(0);
            f.width = Some(5);
            f.justify = Some(Justify::Right);
            f.pad = Some(" ".into());
            f
        }];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("amount"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_fixedwidth_read_zero_padded_numeric() {
        let data = b"00042\n";

        let fields = vec![{
            let mut f = field("amount");
            f.ty = Type::Int;
            f.start = Some(0);
            f.width = Some(5);
            f.justify = Some(Justify::Right);
            f.pad = Some("0".into());
            f
        }];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("amount"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_fixedwidth_read_date_format() {
        let data = b"20240115\n";

        let fields = vec![{
            let mut f = field("dt");
            f.ty = Type::Date;
            f.start = Some(0);
            f.width = Some(8);
            f.format = Some("%Y%m%d".into());
            f
        }];

        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(
            rec.get("dt"),
            Some(&Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()))
        );
    }

    #[test]
    fn test_fixedwidth_read_crlf_separator() {
        let data = b"Alice\r\nBob  \r\n";

        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(5);
            f
        }];

        let config = FixedWidthReaderConfig {
            line_separator: LineSeparator::CrLf,
        };
        let mut reader = FixedWidthReader::new(&data[..], fields, config).unwrap();

        let rec1 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec1.get("name"), Some(&Value::String("Alice".into())));

        let rec2 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("name"), Some(&Value::String("Bob".into())));

        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_fixedwidth_read_no_separator() {
        // Two 10-byte records concatenated with no delimiter
        let data = b"00042Alice00099Bob  ";

        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(5);
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.ty = Type::String;
                f.start = Some(5);
                f.width = Some(5);
                f
            },
        ];

        let config = FixedWidthReaderConfig {
            line_separator: LineSeparator::None,
        };
        let mut reader = FixedWidthReader::new(&data[..], fields, config).unwrap();

        let rec1 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec1.get("id"), Some(&Value::Integer(42)));
        assert_eq!(rec1.get("name"), Some(&Value::String("Alice".into())));

        let rec2 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("id"), Some(&Value::Integer(99)));
        assert_eq!(rec2.get("name"), Some(&Value::String("Bob".into())));

        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_fixedwidth_lf_missing_newline_is_bounded_record_error() {
        // A line-separated (LF) source whose line never terminates must fail with
        // a typed, row-tagged record error rather than buffering the whole input
        // into one record — the bounded-memory guarantee on malformed input.
        let data = vec![b'x'; 10_000];
        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(8);
            f
        }];
        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let err = reader.next_record().unwrap_err();
        match err {
            FormatError::InvalidRecord { row, message } => {
                assert_eq!(row, 1);
                assert!(message.contains("overruns"), "message: {message}");
            }
            other => panic!("expected InvalidRecord, got {other:?}"),
        }
    }

    #[test]
    fn test_schema_zero_width_field_rejected() {
        let fields = vec![{
            let mut f = field("bad");
            f.start = Some(0);
            f.width = Some(0);
            f
        }];

        let result = FixedWidthReader::new(&b""[..], fields, FixedWidthReaderConfig::default());
        let msg = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for zero-width field"),
        };
        assert!(msg.contains("width"), "error should mention width: {msg}");
    }
}
