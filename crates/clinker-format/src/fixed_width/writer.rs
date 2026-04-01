use std::io::Write;

use clinker_record::schema_def::{FieldDef, FieldType, Justify, LineSeparator, TruncationPolicy};
use clinker_record::{Record, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the fixed-width writer.
pub struct FixedWidthWriterConfig {
    pub line_separator: LineSeparator,
}

impl Default for FixedWidthWriterConfig {
    fn default() -> Self {
        Self {
            line_separator: LineSeparator::Lf,
        }
    }
}

/// Pre-resolved field for writing.
struct WriteField {
    name: String,
    width: usize,
    #[allow(dead_code)]
    field_type: Option<FieldType>,
    justify: Justify,
    pad_char: char,
    truncation: TruncationPolicy,
}

/// Schema-driven fixed-width record writer.
/// Type-aware truncation: numeric -> Error, string -> Warn (configurable per field).
pub struct FixedWidthWriter<W: Write> {
    writer: W,
    fields: Vec<WriteField>,
    config: FixedWidthWriterConfig,
    truncation_warnings: Vec<String>,
}

impl<W: Write> FixedWidthWriter<W> {
    pub fn new(
        writer: W,
        fields: Vec<FieldDef>,
        config: FixedWidthWriterConfig,
    ) -> Result<Self, FormatError> {
        let resolved: Vec<WriteField> = fields
            .iter()
            .map(|f| {
                let width = f
                    .width
                    .or_else(|| f.end.and_then(|e| e.checked_sub(f.start.unwrap_or(0))))
                    .ok_or_else(|| FormatError::InvalidRecord {
                        row: 0,
                        message: format!(
                            "field '{}': fixed-width field must have 'width' or 'end'",
                            f.name
                        ),
                    })?;

                let is_numeric = matches!(
                    f.field_type,
                    Some(FieldType::Integer) | Some(FieldType::Float) | Some(FieldType::Decimal)
                );

                let justify = f.justify.clone().unwrap_or(if is_numeric {
                    Justify::Right
                } else {
                    Justify::Left
                });

                let pad_char = f
                    .pad
                    .as_deref()
                    .and_then(|s| s.chars().next())
                    .unwrap_or(' ');

                let truncation = f.truncation.clone().unwrap_or(if is_numeric {
                    TruncationPolicy::Error
                } else {
                    TruncationPolicy::Warn
                });

                Ok(WriteField {
                    name: f.name.clone(),
                    width,
                    field_type: f.field_type.clone(),
                    justify,
                    pad_char,
                    truncation,
                })
            })
            .collect::<Result<_, FormatError>>()?;

        Ok(Self {
            writer,
            fields: resolved,
            config,
            truncation_warnings: Vec::new(),
        })
    }

    /// Get any truncation warnings emitted during writing.
    pub fn truncation_warnings(&self) -> &[String] {
        &self.truncation_warnings
    }

    fn format_value(&self, _field: &WriteField, value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::String(s) => s.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => d.format("%Y%m%d").to_string(),
            Value::DateTime(dt) => dt.format("%Y%m%d%H%M%S").to_string(),
            Value::Array(_) | Value::Map(_) => String::new(),
        }
    }

    fn pad_and_justify(&self, field: &WriteField, value: &str) -> String {
        if value.len() >= field.width {
            return value[..field.width].to_string();
        }

        let padding = field.width - value.len();
        match field.justify {
            Justify::Left => {
                let mut s = value.to_string();
                for _ in 0..padding {
                    s.push(field.pad_char);
                }
                s
            }
            Justify::Right => {
                let mut s = String::with_capacity(field.width);
                for _ in 0..padding {
                    s.push(field.pad_char);
                }
                s.push_str(value);
                s
            }
        }
    }
}

impl<W: Write + Send> FormatWriter for FixedWidthWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        for field in &self.fields {
            let value = record.get(&field.name).cloned().unwrap_or(Value::Null);

            let formatted = self.format_value(field, &value);

            // Check truncation
            if formatted.len() > field.width {
                match field.truncation {
                    TruncationPolicy::Error => {
                        return Err(FormatError::InvalidRecord {
                            row: 0,
                            message: format!(
                                "field '{}': value '{}' ({} chars) exceeds width {} — truncation policy is 'error'",
                                field.name,
                                formatted,
                                formatted.len(),
                                field.width
                            ),
                        });
                    }
                    TruncationPolicy::Warn => {
                        self.truncation_warnings.push(format!(
                            "field '{}': value '{}' truncated to {} chars",
                            field.name, formatted, field.width
                        ));
                        let padded = self.pad_and_justify(field, &formatted);
                        self.writer.write_all(padded.as_bytes())?;
                        continue;
                    }
                    TruncationPolicy::Silent => {
                        let padded = self.pad_and_justify(field, &formatted);
                        self.writer.write_all(padded.as_bytes())?;
                        continue;
                    }
                }
            }

            let padded = self.pad_and_justify(field, &formatted);
            self.writer.write_all(padded.as_bytes())?;
        }

        // Write line separator
        match self.config.line_separator {
            LineSeparator::Lf => self.writer.write_all(b"\n")?,
            LineSeparator::CrLf => self.writer.write_all(b"\r\n")?,
            LineSeparator::None => {} // no separator
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::schema_def::FieldDef;
    use clinker_record::{Record, Schema, Value};
    use std::sync::Arc;

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

    fn make_record(cols: &[&str], vals: Vec<Value>) -> Record {
        let schema = Arc::new(Schema::new(cols.iter().map(|c| (*c).into()).collect()));
        Record::new(schema, vals)
    }

    #[test]
    fn test_fixedwidth_write_basic() {
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
                f.width = Some(10);
                f
            },
            {
                let mut f = field("amount");
                f.field_type = Some(FieldType::Float);
                f.start = Some(15);
                f.width = Some(8);
                f.justify = Some(Justify::Right);
                f
            },
        ];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(
                &["id", "name", "amount"],
                vec![
                    Value::Integer(42),
                    Value::String("Alice".into()),
                    Value::Float(99.5),
                ],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        // id(5) + name(10) + amount(8) = 23 chars + \n
        assert_eq!(output, "00042Alice         99.5\n");
    }

    #[test]
    fn test_fixedwidth_write_left_justify() {
        let fields = vec![{
            let mut f = field("name");
            f.field_type = Some(FieldType::String);
            f.start = Some(0);
            f.width = Some(10);
            f.justify = Some(Justify::Left);
            f
        }];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("Alice".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "Alice     \n");
    }

    #[test]
    fn test_fixedwidth_write_right_justify() {
        let fields = vec![{
            let mut f = field("amount");
            f.field_type = Some(FieldType::Integer);
            f.start = Some(0);
            f.width = Some(8);
            f.justify = Some(Justify::Right);
            f
        }];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["amount"], vec![Value::Integer(42)]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "      42\n");
    }

    #[test]
    fn test_fixedwidth_write_truncate_warning() {
        let fields = vec![{
            let mut f = field("name");
            f.field_type = Some(FieldType::String);
            f.start = Some(0);
            f.width = Some(5);
            f.truncation = Some(TruncationPolicy::Warn);
            f
        }];

        let mut buf = Vec::new();
        let warning_count;
        let warning_msg;
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("LongName".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_count = writer.truncation_warnings().len();
            warning_msg = writer.truncation_warnings()[0].clone();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "LongN\n"); // truncated to 5 chars
        assert_eq!(warning_count, 1);
        assert!(warning_msg.contains("truncated"));
    }

    #[test]
    fn test_fixedwidth_write_truncate_numeric_error() {
        let fields = vec![{
            let mut f = field("amount");
            f.field_type = Some(FieldType::Integer);
            f.start = Some(0);
            f.width = Some(3);
            // Default truncation for numeric is Error
            f
        }];

        let mut buf = Vec::new();
        let mut writer =
            FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();

        let rec = make_record(&["amount"], vec![Value::Integer(12345)]);
        let err = writer.write_record(&rec);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("truncation"),
            "error should mention truncation: {msg}"
        );
    }

    #[test]
    fn test_fixedwidth_roundtrip() {
        use crate::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
        use crate::traits::FormatReader;

        let write_fields = vec![
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
                f.width = Some(10);
                f.justify = Some(Justify::Left);
                f
            },
        ];
        let read_fields = write_fields.clone();

        // Write
        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, write_fields, FixedWidthWriterConfig::default())
                    .unwrap();
            let rec = make_record(
                &["id", "name"],
                vec![Value::Integer(42), Value::String("Alice".into())],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        // Read back
        let mut reader = FixedWidthReader::new(
            buf.as_slice(),
            read_fields,
            FixedWidthReaderConfig::default(),
        )
        .unwrap();

        let roundtrip = reader.next_record().unwrap().unwrap();
        assert_eq!(roundtrip.get("id"), Some(&Value::Integer(42)));
        assert_eq!(roundtrip.get("name"), Some(&Value::String("Alice".into())));
    }
}
