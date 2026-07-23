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
use crate::multi_value::SplitValues;
use crate::schema::Column;
use clinker_record::schema_def::LineSeparator;
use clinker_record::{Record, Schema, SchemaBuilder};

use crate::error::FormatError;
use crate::traits::FormatReader;

/// Configuration for the fixed-width reader.
pub struct FixedWidthReaderConfig {
    pub line_separator: LineSeparator,
    /// In-cell parse declarations: a field's text is split on its delimiter
    /// into the several values a `multiple: true` column holds. Empty by
    /// default; populated from the source's `split_values`.
    pub split_values: Vec<SplitValues>,
}

impl Default for FixedWidthReaderConfig {
    fn default() -> Self {
        Self {
            line_separator: LineSeparator::Lf,
            split_values: Vec::new(),
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
    /// Per-field split delimiter, index-aligned to `fields`: `Some(delim)` for
    /// a field a `split_values` entry covers, `None` otherwise. Built once at
    /// construction so per-record extraction is a positional lookup.
    split_delims: Vec<Option<String>>,
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

        // Map each field to its split delimiter, if any. The entry's `field` is
        // the document field name, which for fixed-width is the column name.
        // E358 has already verified at plan time that every entry names a real
        // `multiple: true` column; an unmatched entry is inert.
        let split_delims: Vec<Option<String>> = if config.split_values.is_empty() {
            Vec::new()
        } else {
            resolved
                .iter()
                .map(|f| {
                    config
                        .split_values
                        .iter()
                        .find(|e| e.field == f.name)
                        .map(|e| e.delimiter.clone())
                })
                .collect()
        };

        Ok(Self {
            reader: BufReader::new(SkipBom::new(reader)),
            fields: resolved,
            split_delims,
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
        for (i, f) in self.fields.iter().enumerate() {
            let value = match self.split_delims.get(i).and_then(Option::as_deref) {
                Some(delim) => {
                    field::extract_split_value(f, &self.line_buf, self.row_number, delim)?
                }
                None => field::extract_value(f, &self.line_buf, self.row_number)?,
            };
            values.push(value);
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

    fn split_config(field: &str, delimiter: &str) -> FixedWidthReaderConfig {
        FixedWidthReaderConfig {
            split_values: vec![SplitValues {
                field: field.to_string(),
                delimiter: delimiter.to_string(),
                escape: String::new(),
                json: false,
            }],
            ..Default::default()
        }
    }

    /// A `tags` string field over a fixed byte range parses into a string array.
    #[test]
    fn split_values_parses_fixed_field_into_string_array() {
        // "01" id (0..2 int), "a;b;c     " tags (2..12 string, space-padded)
        let data = b"01a;b;c     \n";
        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(2);
                f
            },
            {
                let mut f = field("tags");
                f.ty = Type::String;
                f.start = Some(2);
                f.width = Some(10);
                f
            },
        ];
        let mut reader =
            FixedWidthReader::new(&data[..], fields, split_config("tags", ";")).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("id"), Some(&Value::Integer(1)));
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    /// Fixed-width types each element itself (it is the sole coercion pass), so
    /// an `int` multiple field materializes as an array of integers.
    #[test]
    fn split_values_types_each_element_for_numeric_field() {
        // "1;2;3     " codes (0..10 int, space-padded)
        let data = b"1;2;3     \n";
        let fields = vec![{
            let mut f = field("codes");
            f.ty = Type::Int;
            f.start = Some(0);
            f.width = Some(10);
            f
        }];
        let mut reader =
            FixedWidthReader::new(&data[..], fields, split_config("codes", ";")).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            record.get("codes"),
            Some(&Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]))
        );
    }

    /// A blank field (all padding) yields an empty array, not one empty value.
    #[test]
    fn split_values_blank_fixed_field_yields_empty_array() {
        // id "01", tags all spaces over 2..12
        let data = b"01          \n";
        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(2);
                f
            },
            {
                let mut f = field("tags");
                f.ty = Type::String;
                f.start = Some(2);
                f.width = Some(10);
                f
            },
        ];
        let mut reader =
            FixedWidthReader::new(&data[..], fields, split_config("tags", ";")).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.get("tags"), Some(&Value::Array(Vec::new())));
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
            ..Default::default()
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
            ..Default::default()
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
    fn test_fixedwidth_lf_missing_newline_reads_prefix_and_stays_bounded() {
        // A line-separated (LF) source whose line never terminates must not buffer
        // the whole input into one record. The declared-width prefix reads and the
        // remaining bytes are discarded to end of input, so a malformed file stays
        // within the bounded-memory guarantee rather than growing a single record.
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
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.get("name"), Some(&Value::String("xxxxxxxx".into())));
        // The single unterminated physical line was fully consumed.
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_fixedwidth_lf_wide_lines_read_prefix_and_resync() {
        // Physical lines wider than the mapped schema (trailing filler the schema
        // does not declare) read their declared-width prefix and resync to the
        // next record at the newline — a common fixed-LRECL extract shape. The
        // 8-byte name prefix is followed by filler bytes the schema never maps.
        let data = format!("{:<8}ignored-filler\n{:<8}more-filler\n", "Alice", "Bob").into_bytes();
        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(8);
            f
        }];
        let mut reader =
            FixedWidthReader::new(&data[..], fields, FixedWidthReaderConfig::default()).unwrap();
        let rec1 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec1.get("name"), Some(&Value::String("Alice".into())));
        let rec2 = reader.next_record().unwrap().unwrap();
        assert_eq!(rec2.get("name"), Some(&Value::String("Bob".into())));
        assert!(reader.next_record().unwrap().is_none());
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

    /// A declared range whose end exceeds `usize::MAX` cannot exist; the reader
    /// rejects it at construction so `field.end()` stays a non-wrapping add on
    /// the per-record read path, matching the write path's guard.
    #[test]
    fn test_fixedwidth_read_range_end_overflow_rejected() {
        let fields = vec![{
            let mut f = field("bad");
            f.start = Some(usize::MAX);
            f.width = Some(2);
            f
        }];

        let result = FixedWidthReader::new(&b""[..], fields, FixedWidthReaderConfig::default());
        let msg = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for overflowing range"),
        };
        assert!(msg.contains("overflows"), "error should say so: {msg}");
    }
}
