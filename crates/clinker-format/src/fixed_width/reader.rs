use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use chrono::NaiveDate;
use clinker_record::schema_def::{FieldDef, FieldType, Justify, LineSeparator};
use clinker_record::{Record, Schema, SchemaBuilder, Value};

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
    reader: BufReader<R>,
    fields: Vec<ResolvedField>,
    schema: Arc<Schema>,
    config: FixedWidthReaderConfig,
    record_length: usize,
    line_buf: Vec<u8>,
    row_number: u64,
}

/// Pre-resolved field with computed width (from width or end-start).
struct ResolvedField {
    name: String,
    start: usize,
    width: usize,
    field_type: Option<FieldType>,
    format: Option<String>,
    justify: Option<Justify>,
    pad: String,
    trim: bool,
}

impl ResolvedField {
    fn from_field_def(f: &FieldDef) -> Result<Self, FormatError> {
        let start = f.start.ok_or_else(|| FormatError::InvalidRecord {
            row: 0,
            message: format!("field '{}': fixed-width field must have 'start'", f.name),
        })?;

        let width = if let Some(w) = f.width {
            w
        } else if let Some(end) = f.end {
            if end < start {
                return Err(FormatError::InvalidRecord {
                    row: 0,
                    message: format!(
                        "field '{}': 'end' ({}) must be >= 'start' ({})",
                        f.name, end, start
                    ),
                });
            }
            end - start
        } else {
            return Err(FormatError::InvalidRecord {
                row: 0,
                message: format!(
                    "field '{}': fixed-width field must have 'width' or 'end'",
                    f.name
                ),
            });
        };

        if width == 0 {
            return Err(FormatError::InvalidRecord {
                row: 0,
                message: format!("field '{}': width must be > 0", f.name),
            });
        }

        Ok(ResolvedField {
            name: f.name.clone(),
            start,
            width,
            field_type: f.field_type.clone(),
            format: f.format.clone(),
            justify: f.justify.clone(),
            pad: f.pad.clone().unwrap_or_else(|| " ".into()),
            trim: f.trim.unwrap_or(true),
        })
    }
}

impl<R: Read> FixedWidthReader<R> {
    pub fn new(
        reader: R,
        fields: Vec<FieldDef>,
        config: FixedWidthReaderConfig,
    ) -> Result<Self, FormatError> {
        let resolved: Vec<ResolvedField> = fields
            .iter()
            .map(ResolvedField::from_field_def)
            .collect::<Result<_, _>>()?;

        let schema = fields
            .iter()
            .map(|f| Box::<str>::from(f.name.as_str()))
            .collect::<SchemaBuilder>()
            .build();
        let record_length = resolved
            .iter()
            .map(|f| f.start + f.width)
            .max()
            .unwrap_or(0);

        Ok(Self {
            reader: BufReader::new(reader),
            fields: resolved,
            schema,
            config,
            record_length,
            line_buf: Vec::with_capacity(256),
            row_number: 0,
        })
    }

    fn read_line(&mut self) -> Result<bool, FormatError> {
        self.line_buf.clear();
        match self.config.line_separator {
            LineSeparator::Lf => {
                let n = self.reader.read_until(b'\n', &mut self.line_buf)?;
                if n == 0 {
                    return Ok(false);
                }
                // Strip trailing \n
                if self.line_buf.last() == Some(&b'\n') {
                    self.line_buf.pop();
                }
            }
            LineSeparator::CrLf => {
                let n = self.reader.read_until(b'\n', &mut self.line_buf)?;
                if n == 0 {
                    return Ok(false);
                }
                // Strip trailing \r\n
                if self.line_buf.ends_with(b"\r\n") {
                    self.line_buf.truncate(self.line_buf.len() - 2);
                } else if self.line_buf.last() == Some(&b'\n') {
                    self.line_buf.pop();
                }
            }
            LineSeparator::None => {
                self.line_buf.resize(self.record_length, 0);
                let mut total = 0;
                while total < self.record_length {
                    let n = self.reader.read(&mut self.line_buf[total..])?;
                    if n == 0 {
                        if total == 0 {
                            return Ok(false);
                        }
                        return Err(FormatError::InvalidRecord {
                            row: self.row_number + 1,
                            message: format!(
                                "incomplete record: expected {} bytes, got {}",
                                self.record_length, total
                            ),
                        });
                    }
                    total += n;
                }
            }
        }
        Ok(true)
    }

    fn extract_field(&self, field: &ResolvedField) -> Result<Value, FormatError> {
        let end = field.start + field.width;
        if end > self.line_buf.len() {
            // Line shorter than field extent — pad conceptually with spaces
            let available = if field.start < self.line_buf.len() {
                &self.line_buf[field.start..]
            } else {
                b"" as &[u8]
            };
            let s = std::str::from_utf8(available).map_err(|e| FormatError::InvalidRecord {
                row: self.row_number,
                message: format!("field '{}': invalid UTF-8: {e}", field.name),
            })?;
            return self.parse_field_value(field, s);
        }

        let raw = &self.line_buf[field.start..end];
        let s = std::str::from_utf8(raw).map_err(|e| FormatError::InvalidRecord {
            row: self.row_number,
            message: format!("field '{}': invalid UTF-8: {e}", field.name),
        })?;

        self.parse_field_value(field, s)
    }

    fn parse_field_value(&self, field: &ResolvedField, raw: &str) -> Result<Value, FormatError> {
        if !field.trim {
            // No trimming — return raw value as-is
            if raw.is_empty() {
                return Ok(Value::Null);
            }
            return match &field.field_type {
                Some(FieldType::String) | None => Ok(Value::String(raw.into())),
                _ => {
                    // Even for no-trim, typed fields need parsing from the raw string
                    self.parse_typed_value(field, raw)
                }
            };
        }

        // Strip padding based on justification
        let stripped = match field.justify {
            Some(Justify::Right) => {
                // Right-justified: strip leading pad chars
                let pad_char = field.pad.chars().next().unwrap_or(' ');
                raw.trim_start_matches(pad_char)
            }
            Some(Justify::Left) | None => {
                // Left-justified (default): strip trailing pad chars
                let pad_char = field.pad.chars().next().unwrap_or(' ');
                raw.trim_end_matches(pad_char)
            }
        };

        let value_str = stripped.trim();

        if value_str.is_empty() {
            return Ok(Value::Null);
        }

        self.parse_typed_value(field, value_str)
    }

    fn parse_typed_value(
        &self,
        field: &ResolvedField,
        value_str: &str,
    ) -> Result<Value, FormatError> {
        match &field.field_type {
            Some(FieldType::Integer) => {
                let v: i64 = value_str.parse().map_err(|e| FormatError::InvalidRecord {
                    row: self.row_number,
                    message: format!(
                        "field '{}': cannot parse '{}' as integer: {e}",
                        field.name, value_str
                    ),
                })?;
                Ok(Value::Integer(v))
            }
            Some(FieldType::Float) => {
                let v: f64 = value_str.parse().map_err(|e| FormatError::InvalidRecord {
                    row: self.row_number,
                    message: format!(
                        "field '{}': cannot parse '{}' as float: {e}",
                        field.name, value_str
                    ),
                })?;
                Ok(Value::Float(v))
            }
            Some(FieldType::Date) => {
                let fmt = field.format.as_deref().unwrap_or("%Y-%m-%d");
                let d = NaiveDate::parse_from_str(value_str, fmt).map_err(|e| {
                    FormatError::InvalidRecord {
                        row: self.row_number,
                        message: format!(
                            "field '{}': cannot parse '{}' as date with format '{}': {e}",
                            field.name, value_str, fmt
                        ),
                    }
                })?;
                Ok(Value::Date(d))
            }
            Some(FieldType::Boolean) => {
                let b = match value_str.to_lowercase().as_str() {
                    "true" | "1" | "yes" | "y" => true,
                    "false" | "0" | "no" | "n" => false,
                    _ => {
                        return Err(FormatError::InvalidRecord {
                            row: self.row_number,
                            message: format!(
                                "field '{}': cannot parse '{}' as boolean",
                                field.name, value_str
                            ),
                        });
                    }
                };
                Ok(Value::Bool(b))
            }
            Some(FieldType::String) | None => Ok(Value::String(value_str.into())),
            _ => Ok(Value::String(value_str.into())),
        }
    }
}

impl<R: Read + Send> FormatReader for FixedWidthReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if !self.read_line()? {
            return Ok(None);
        }
        self.row_number += 1;

        let mut values = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            values.push(self.extract_field(field)?);
        }

        Ok(Some(Record::new(Arc::clone(&self.schema), values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::schema_def::FieldDef;

    fn field(name: &str) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: None,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
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
                f.field_type = Some(FieldType::Integer);
                f.start = Some(0);
                f.width = Some(5);
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.field_type = Some(FieldType::String);
                f.start = Some(5);
                f.width = Some(20);
                f
            },
            {
                let mut f = field("date");
                f.field_type = Some(FieldType::String);
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
    fn test_fixedwidth_read_start_end() {
        // Same as basic but using end instead of width
        let data = b"00042Alice               20240115\n";

        let fields = vec![
            {
                let mut f = field("id");
                f.field_type = Some(FieldType::Integer);
                f.start = Some(0);
                f.end = Some(5); // end - start = 5 = same as width: 5
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.field_type = Some(FieldType::String);
                f.start = Some(5);
                f.end = Some(25); // end - start = 20
                f
            },
            {
                let mut f = field("date");
                f.field_type = Some(FieldType::String);
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
            f.field_type = Some(FieldType::String);
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
            f.field_type = Some(FieldType::String);
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
            f.field_type = Some(FieldType::Integer);
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
            f.field_type = Some(FieldType::Integer);
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
            f.field_type = Some(FieldType::Date);
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
            f.field_type = Some(FieldType::String);
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
                f.field_type = Some(FieldType::Integer);
                f.start = Some(0);
                f.width = Some(5);
                f.justify = Some(Justify::Right);
                f.pad = Some("0".into());
                f
            },
            {
                let mut f = field("name");
                f.field_type = Some(FieldType::String);
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
