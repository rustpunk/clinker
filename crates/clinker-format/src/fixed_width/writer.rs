use std::io::Write;

use clinker_record::schema_def::{FieldDef, FieldType, Justify, LineSeparator, TruncationPolicy};
use clinker_record::{DocumentContext, Record, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::traits::FormatWriter;

/// Configuration for the fixed-width writer.
#[derive(Clone)]
pub struct FixedWidthWriterConfig {
    pub line_separator: LineSeparator,
    /// Per-document envelope reconstruction. `None` (the default) renders no
    /// framing. `Some` is set by the executor under `reconstruct_envelope:
    /// true`. A computed footer record count is rejected at plan time (E346)
    /// for fixed-width, so the spec the executor passes here never carries
    /// `footer_record_count_field`.
    pub envelope: Option<OutputEnvelopeSpec>,
}

impl Default for FixedWidthWriterConfig {
    fn default() -> Self {
        Self {
            line_separator: LineSeparator::Lf,
            envelope: None,
        }
    }
}

/// Pre-resolved field for writing.
struct WriteField {
    name: String,
    width: usize,
    justify: Justify,
    pad_char: char,
    truncation: TruncationPolicy,
}

/// Schema-driven fixed-width record writer.
/// Type-aware truncation: numeric -> Error, string -> Warn (configurable per field).
///
/// Under `reconstruct_envelope`, `begin_document` emits the header section's
/// field values as one leading line and `end_document` the footer's as one
/// trailing line, each joined positionally in declared field order with the
/// configured line separator. The body streams between them, so framing stays
/// O(1-record).
pub struct FixedWidthWriter<W: Write> {
    writer: W,
    fields: Vec<WriteField>,
    config: FixedWidthWriterConfig,
    truncation_warnings: Vec<String>,
    /// Per-document envelope framer, present only when `config.envelope` is.
    framer: Option<EnvelopeFramer>,
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
                    Some(FieldType::Integer) | Some(FieldType::Float)
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
                    justify,
                    pad_char,
                    truncation,
                })
            })
            .collect::<Result<_, FormatError>>()?;

        let framer = config
            .envelope
            .clone()
            .and_then(OutputEnvelopeSpec::into_framer);
        Ok(Self {
            writer,
            fields: resolved,
            config,
            truncation_warnings: Vec::new(),
            framer,
        })
    }

    /// Emit one envelope section as a single fixed-width line: the section's
    /// field values (in declared order) concatenated, then the configured line
    /// separator. Envelope sections carry no width schema, so values are
    /// written unpadded — a header/trailer LINE round-trips, but not a
    /// column-positioned one (that would need a width declaration the envelope
    /// config does not carry). Called only for a section the document actually
    /// carries (a missing section emits no line). A computed footer count is
    /// rejected at plan time for fixed-width (E346).
    fn write_section_line(
        &mut self,
        fields: &indexmap::IndexMap<Box<str>, Value>,
    ) -> Result<(), FormatError> {
        let mut line = String::new();
        for value in fields.values() {
            line.push_str(&value_to_envelope_cell(value));
        }
        self.writer.write_all(line.as_bytes())?;
        match self.config.line_separator {
            LineSeparator::Lf => self.writer.write_all(b"\n")?,
            LineSeparator::CrLf => self.writer.write_all(b"\r\n")?,
            LineSeparator::None => {}
        }
        Ok(())
    }

    /// Get any truncation warnings emitted during writing.
    pub fn truncation_warnings(&self) -> &[String] {
        &self.truncation_warnings
    }

    /// Convert a `Value` to its fixed-width string form.
    ///
    /// `Value::Map` has no canonical scalar serialization for a
    /// fixed-width column (the prior behavior of returning an empty
    /// string silently dropped the payload, hiding routing bugs —
    /// e.g. a `$widened` sidecar reaching the writer without
    /// `include_unmapped: true` expansion), so this returns
    /// `FormatError::UnserializableMapValue` carrying the offending
    /// column. The fixed-width writer is the single point of truth
    /// for map rejection on this path; there is no upstream pre-walk.
    ///
    /// `Value::Array` retains the prior empty-cell shape because
    /// fixed-width's strict positional layout makes JSON-in-cell
    /// unworkable. Users who need an array's contents in a
    /// fixed-width field must coerce to a scalar in CXL before
    /// the emit.
    fn format_value(&self, field: &WriteField, value: &Value) -> Result<String, FormatError> {
        Ok(match value {
            Value::Null => String::new(),
            Value::String(s) => s.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => d.format("%Y%m%d").to_string(),
            Value::DateTime(dt) => dt.format("%Y%m%d%H%M%S").to_string(),
            Value::Array(_) => String::new(),
            Value::Map(_) => {
                return Err(FormatError::UnserializableMapValue {
                    format: "fixed-width",
                    column: field.name.clone(),
                });
            }
        })
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

            let formatted = self.format_value(field, &value)?;

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

        if let Some(framer) = self.framer.as_mut() {
            framer.count_record();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }

    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_mut() else {
            return Ok(());
        };
        framer.begin();
        // Clone the header fields out so the immutable framer borrow ends
        // before the `&mut self` write below; `None` (document lacks the
        // configured section) emits no header line.
        let header = framer.header_fields(doc).cloned();
        if let Some(fields) = header.as_ref() {
            self.write_section_line(fields)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        let footer = framer.footer_fields(doc).cloned();
        if let Some(fields) = footer.as_ref() {
            self.write_section_line(fields)?;
        }
        Ok(())
    }
}

/// Stringify an envelope section value for a fixed-width header/trailer line.
/// Envelope sections carry no width schema, so values are written as their
/// natural string form (no padding); `Null` is the empty string.
fn value_to_envelope_cell(value: &Value) -> String {
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

    /// Fixed-width writer rejects `Value::Map` payloads with
    /// `FormatError::UnserializableMapValue`. The previous behavior
    /// silently emitted an empty fixed-width field for any map
    /// in `format_value`; the explicit precheck in `write_record`
    /// surfaces the misroute (typically a `$widened` sidecar
    /// reaching the writer without `include_unmapped: true`
    /// expansion).
    #[test]
    fn test_fixed_width_writer_rejects_map_value() {
        let schema = Arc::new(Schema::new(vec!["id".into(), "payload".into()]));
        let mut sidecar: indexmap::IndexMap<Box<str>, Value> = indexmap::IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(7), Value::Map(Box::new(sidecar))],
        );
        let mut id_field = field("id");
        id_field.width = Some(5);
        let mut payload_field = field("payload");
        payload_field.width = Some(10);
        let fields = vec![id_field, payload_field];
        let mut buf = Vec::new();
        let mut writer =
            FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableMapValue { format, column } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column, "payload");
            }
            other => panic!("expected UnserializableMapValue, got {other:?}"),
        }
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    #[test]
    fn fixed_width_envelope_emits_header_and_footer_lines() {
        // A header line and a footer line bracket the body, each joining the
        // section's field values (unpadded — envelope sections carry no width
        // schema). A computed footer count is rejected at plan time for
        // fixed-width (E346), so the spec here carries none.
        let mut amount = field("amount");
        amount.field_type = Some(FieldType::Integer);
        amount.width = Some(5);
        amount.justify = Some(Justify::Right);
        amount.pad = Some("0".into());
        let config = FixedWidthWriterConfig {
            line_separator: LineSeparator::Lf,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: None,
            }),
        };
        let doc = doc_with_sections(&[
            ("Head", &[("tag", Value::String("HDR".into()))]),
            ("Foot", &[("tag", Value::String("TRL".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let mut w = FixedWidthWriter::new(&mut buf, vec![amount], config).unwrap();
            w.begin_document(&doc).unwrap();
            w.write_record(&make_record(&["amount"], vec![Value::Integer(7)]))
                .unwrap();
            w.end_document(&doc).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out, "HDR\n00007\nTRL\n", "got: {out}");
    }
}
