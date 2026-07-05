use std::io::Write;

use clinker_record::schema_def::{Justify, LineSeparator, TruncationPolicy};
use clinker_record::{DocumentContext, Record, Value};
use cxl::typecheck::Type;

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::fixed_width::field;
use crate::schema::Column;
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
    /// 0-based byte offset of the field's first cell byte within the record,
    /// resolved with the same semantics the reader slices by.
    start: usize,
    width: usize,
    justify: Justify,
    pad_char: char,
    truncation: TruncationPolicy,
}

/// Schema-driven fixed-width record writer.
/// Type-aware truncation: numeric -> Error, string -> Warn (configurable per field).
///
/// Every field is emitted at its declared byte range (`start` plus
/// `width`/`end`, resolved with the reader's semantics), independent of
/// declaration order; gaps between declared ranges are space-filled so a
/// written record reads back under the same schema. Overlapping ranges are
/// rejected at construction. A column omitting `start` continues at the
/// previous column's end (sequential layout).
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
        fields: Vec<Column>,
        config: FixedWidthWriterConfig,
    ) -> Result<Self, FormatError> {
        // Byte positions resolve exactly as the reader's (`start` plus
        // `width`/`end`), so what this writer emits at a range is what the
        // reader slices back out. A column omitting `start` continues at the
        // previous column's end, keeping a width-only schema sequential.
        let mut resolved: Vec<WriteField> = Vec::with_capacity(fields.len());
        let mut next_start = 0usize;
        for f in &fields {
            let start = f.start.unwrap_or(next_start);
            let width = field::resolve_width(f, start)?;
            next_start = start
                .checked_add(width)
                .ok_or_else(|| field::invalid_field(&f.name, "'start' + width overflows"))?;

            let is_numeric = matches!(
                f.ty.unwrap_nullable(),
                Type::Int | Type::Float | Type::Decimal | Type::Numeric
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

            resolved.push(WriteField {
                name: f.name.clone(),
                start,
                width,
                justify,
                pad_char,
                truncation,
            });
        }

        // Emit in byte order regardless of declaration order. Overlapping
        // ranges have no consistent byte layout — later bytes would clobber
        // earlier ones — so they are a construction defect, not a per-record
        // surprise.
        resolved.sort_by_key(|f| f.start);
        for pair in resolved.windows(2) {
            let (prev, next) = (&pair[0], &pair[1]);
            if next.start < prev.start + prev.width {
                return Err(field::invalid_field(
                    &next.name,
                    &format!(
                        "range {}..{} overlaps field '{}' ({}..{})",
                        next.start,
                        next.start + next.width,
                        prev.name,
                        prev.start,
                        prev.start + prev.width
                    ),
                ));
            }
        }

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
            // The decimal already carries its column scale (coercion rounds to
            // it), and Display renders that scale, e.g. `2.50`.
            Value::Decimal(d) => d.to_string(),
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
        // Fields are sorted by `start` and non-overlapping (enforced at
        // construction), so a cursor walk left-to-right space-fills any byte
        // range the schema leaves undeclared and lands every cell at the
        // position the reader slices.
        let mut cursor = 0usize;
        for field in &self.fields {
            write_gap(&mut self.writer, field.start - cursor)?;

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
                    }
                    TruncationPolicy::Silent => {}
                }
            }

            let padded = self.pad_and_justify(field, &formatted);
            self.writer.write_all(padded.as_bytes())?;
            cursor = field.start + field.width;
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

/// Space-fill the `len`-byte gap between the write cursor and the next
/// field's start, in bounded chunks so an arbitrarily wide gap stays O(1)
/// memory with no per-record allocation.
fn write_gap<W: Write>(writer: &mut W, mut len: usize) -> Result<(), FormatError> {
    const FILL: [u8; 64] = [b' '; 64];
    while len > 0 {
        let n = len.min(FILL.len());
        writer.write_all(&FILL[..n])?;
        len -= n;
    }
    Ok(())
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
        Value::Decimal(d) => d.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Date(d) => d.format("%Y%m%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y%m%d%H%M%S").to_string(),
        Value::Array(_) | Value::Map(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Record, Schema, Value};
    use std::sync::Arc;

    fn field(name: &str) -> Column {
        Column::bare(name, Type::String)
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
                f.width = Some(10);
                f
            },
            {
                let mut f = field("amount");
                f.ty = Type::Float;
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
            f.ty = Type::String;
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
            f.ty = Type::Int;
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
            f.ty = Type::String;
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
            f.ty = Type::Int;
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

    /// A schema whose declared ranges leave a gap emits the gap as spaces so
    /// each field lands at the byte position the reader slices — the
    /// round-trip the sequential emitter used to break by writing the fields
    /// adjacent. `b` declares `end` (not `width`) to pin end-resolution to
    /// the same byte range on both sides.
    #[test]
    fn test_fixedwidth_write_gapped_starts_roundtrip() {
        use crate::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
        use crate::traits::FormatReader;

        let fields = vec![
            {
                let mut f = field("a");
                f.ty = Type::String;
                f.start = Some(0);
                f.width = Some(2);
                f
            },
            {
                let mut f = field("b");
                f.ty = Type::String;
                f.start = Some(5);
                f.end = Some(7);
                f
            },
        ];
        let read_fields = fields.clone();

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(
                &["a", "b"],
                vec![Value::String("AB".into()), Value::String("CD".into())],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf.clone()).unwrap();
        assert_eq!(output, "AB   CD\n", "bytes 2..5 must be space-filled");

        let mut reader = FixedWidthReader::new(
            buf.as_slice(),
            read_fields,
            FixedWidthReaderConfig::default(),
        )
        .unwrap();
        let roundtrip = reader.next_record().unwrap().unwrap();
        assert_eq!(roundtrip.get("a"), Some(&Value::String("AB".into())));
        assert_eq!(roundtrip.get("b"), Some(&Value::String("CD".into())));
    }

    /// Fields declared out of byte order are emitted at their declared
    /// positions, not in declaration order.
    #[test]
    fn test_fixedwidth_write_out_of_order_starts_roundtrip() {
        use crate::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
        use crate::traits::FormatReader;

        let fields = vec![
            {
                let mut f = field("b");
                f.ty = Type::String;
                f.start = Some(5);
                f.width = Some(5);
                f
            },
            {
                let mut f = field("a");
                f.ty = Type::String;
                f.start = Some(0);
                f.width = Some(5);
                f
            },
        ];
        let read_fields = fields.clone();

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(
                &["a", "b"],
                vec![Value::String("Alice".into()), Value::String("Bob".into())],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf.clone()).unwrap();
        assert_eq!(output, "AliceBob  \n", "a occupies 0..5, b occupies 5..10");

        let mut reader = FixedWidthReader::new(
            buf.as_slice(),
            read_fields,
            FixedWidthReaderConfig::default(),
        )
        .unwrap();
        let roundtrip = reader.next_record().unwrap().unwrap();
        assert_eq!(roundtrip.get("a"), Some(&Value::String("Alice".into())));
        assert_eq!(roundtrip.get("b"), Some(&Value::String("Bob".into())));
    }

    /// A gap wider than the fill chunk is still fully space-filled (exercises
    /// the chunked gap writer across more than one chunk).
    #[test]
    fn test_fixedwidth_write_wide_gap_fully_space_filled() {
        let fields = vec![
            {
                let mut f = field("a");
                f.ty = Type::String;
                f.start = Some(0);
                f.width = Some(2);
                f
            },
            {
                let mut f = field("b");
                f.ty = Type::String;
                f.start = Some(100);
                f.width = Some(2);
                f
            },
        ];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(
                &["a", "b"],
                vec![Value::String("XX".into()), Value::String("YY".into())],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output.len(), 103, "2 + 98-space gap + 2 + newline");
        assert_eq!(&output[..2], "XX");
        assert!(
            output[2..100].bytes().all(|b| b == b' '),
            "bytes 2..100 must all be spaces"
        );
        assert_eq!(&output[100..102], "YY");
    }

    /// Overlapping declared ranges have no consistent byte layout and are a
    /// typed construction error naming both fields.
    #[test]
    fn test_fixedwidth_write_overlapping_fields_rejected() {
        let fields = vec![
            {
                let mut f = field("a");
                f.ty = Type::String;
                f.start = Some(0);
                f.width = Some(5);
                f
            },
            {
                let mut f = field("b");
                f.ty = Type::String;
                f.start = Some(3);
                f.width = Some(5);
                f
            },
        ];

        let mut buf = Vec::new();
        let err = FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default())
            .err()
            .expect("overlapping ranges must be rejected at construction");
        match err {
            FormatError::InvalidRecord { row, message } => {
                assert_eq!(row, 0, "construction defect reports row 0");
                assert!(
                    message.contains("'b'") && message.contains("'a'"),
                    "message should name both fields: {message}"
                );
                assert!(
                    message.contains("3..8") && message.contains("0..5"),
                    "message should carry both ranges: {message}"
                );
            }
            other => panic!("expected InvalidRecord, got {other:?}"),
        }
    }

    /// The writer enforces the reader's `width`/`end` mutual exclusivity, so
    /// a schema that would be rejected on read is rejected on write too.
    #[test]
    fn test_fixedwidth_write_width_and_end_together_rejected() {
        let fields = vec![{
            let mut f = field("a");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(5);
            f.end = Some(5);
            f
        }];

        let mut buf = Vec::new();
        let err = FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default())
            .err()
            .expect("width+end together must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("mutually exclusive"),
            "error should state the exclusivity: {msg}"
        );
    }

    /// Columns that omit `start` keep the sequential layout: each continues
    /// at the previous column's end.
    #[test]
    fn test_fixedwidth_write_startless_schema_stays_sequential() {
        let fields = vec![
            {
                let mut f = field("a");
                f.ty = Type::String;
                f.width = Some(3);
                f
            },
            {
                let mut f = field("b");
                f.ty = Type::Int;
                f.width = Some(4);
                f
            },
        ];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(
                &["a", "b"],
                vec![Value::String("x".into()), Value::Integer(42)],
            );
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "x    42\n", "a at 0..3, b at 3..7, no gap");
    }

    /// A declared range whose end exceeds `usize::MAX` cannot exist; it is a
    /// typed construction error rather than an arithmetic wrap.
    #[test]
    fn test_fixedwidth_write_range_end_overflow_rejected() {
        let fields = vec![{
            let mut f = field("a");
            f.ty = Type::String;
            f.start = Some(usize::MAX);
            f.width = Some(2);
            f
        }];

        let mut buf = Vec::new();
        let err = FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default())
            .err()
            .expect("overflowing range must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("overflows"), "error should say so: {msg}");
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    #[test]
    fn fixed_width_envelope_emits_header_and_footer_lines() {
        // A header line and a footer line bracket the body, each joining the
        // section's field values (unpadded — envelope sections carry no width
        // schema). A computed footer count is rejected at plan time for
        // fixed-width (E346), so the spec here carries none.
        let mut amount = field("amount");
        amount.ty = Type::Int;
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
