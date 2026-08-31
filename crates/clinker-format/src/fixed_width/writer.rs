use std::io::Write;

use clinker_record::schema_def::{Justify, LineSeparator, TruncationPolicy};
use clinker_record::{DocumentContext, Record, Value};
use cxl::typecheck::Type;

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::fixed_width::field::{self, ResolvedRepeatingGroup};
use crate::schema::{Column, FixedWidthFill, FixedWidthOverflow, FixedWidthTruncateKeep};
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
#[derive(Clone)]
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

struct WriteGroup {
    resolved: ResolvedRepeatingGroup,
    fields: Vec<WriteField>,
}

enum WriteLayout {
    Scalar(WriteField),
    Group(WriteGroup),
}

impl WriteLayout {
    fn name(&self) -> &str {
        match self {
            Self::Scalar(field) => &field.name,
            Self::Group(group) => &group.resolved.name,
        }
    }

    fn start(&self) -> usize {
        match self {
            Self::Scalar(field) => field.start,
            Self::Group(group) => group.resolved.start,
        }
    }

    fn end(&self) -> usize {
        match self {
            Self::Scalar(field) => field.start + field.width,
            Self::Group(group) => group.resolved.end(),
        }
    }

    fn is_group(&self) -> bool {
        matches!(self, Self::Group(_))
    }
}

fn write_field(column: &Column, start: usize, width: usize) -> Result<WriteField, FormatError> {
    let is_numeric = matches!(
        column.ty.unwrap_nullable(),
        Type::Int | Type::Float | Type::Decimal | Type::Numeric
    );
    let justify = column.justify.clone().unwrap_or(if is_numeric {
        Justify::Right
    } else {
        Justify::Left
    });
    field::validate_pad(&column.name, column.pad.as_deref())?;
    let pad_char = column
        .pad
        .as_deref()
        .and_then(|pad| pad.chars().next())
        .unwrap_or(' ');
    let truncation = column.truncation.clone().unwrap_or(if is_numeric {
        TruncationPolicy::Error
    } else {
        TruncationPolicy::Warn
    });
    Ok(WriteField {
        name: column.name.clone(),
        start,
        width,
        justify,
        pad_char,
        truncation,
    })
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
    layouts: Vec<WriteLayout>,
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
        let mut layouts: Vec<WriteLayout> = Vec::with_capacity(fields.len());
        let mut next_start = 0usize;
        for column in &fields {
            let start = column.start.unwrap_or(next_start);
            let layout = if column.fields.is_some()
                || column.occurs.is_some()
                || column.count_field.is_some()
            {
                let resolved = ResolvedRepeatingGroup::from_column_at(column, start)?;
                let children = column.fields.as_deref().unwrap_or(&[]);
                let write_fields = resolved
                    .fields
                    .iter()
                    .map(|resolved_child| {
                        let child = children
                            .iter()
                            .find(|child| child.name == resolved_child.name)
                            .expect("resolved child came from this declaration");
                        write_field(child, resolved_child.start, resolved_child.width)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                WriteLayout::Group(WriteGroup {
                    resolved,
                    fields: write_fields,
                })
            } else {
                let width = field::resolve_width(column, start)?;
                start.checked_add(width).ok_or_else(|| {
                    field::invalid_field(&column.name, "'start' + width overflows")
                })?;
                WriteLayout::Scalar(write_field(column, start, width)?)
            };
            next_start = layout.end();
            layouts.push(layout);
        }

        // Emit in byte order regardless of declaration order. Overlapping
        // ranges have no consistent byte layout — later bytes would clobber
        // earlier ones — so they are a construction defect, not a per-record
        // surprise.
        layouts.sort_by_key(WriteLayout::start);
        for pair in layouts.windows(2) {
            let (prev, next) = (&pair[0], &pair[1]);
            if next.start() < prev.end() {
                let error = if next.is_group() || prev.is_group() {
                    field::invalid_group(
                        if next.is_group() {
                            next.name()
                        } else {
                            prev.name()
                        },
                        &format!(
                            "range {}..{} overlaps field '{}' ({}..{}); give the group, count, payload, and adjacent fields disjoint maximum ranges",
                            next.start(),
                            next.end(),
                            prev.name(),
                            prev.start(),
                            prev.end()
                        ),
                    )
                } else {
                    field::invalid_field(
                        next.name(),
                        &format!(
                            "range {}..{} overlaps field '{}' ({}..{})",
                            next.start(),
                            next.end(),
                            prev.name(),
                            prev.start(),
                            prev.end()
                        ),
                    )
                };
                return Err(error);
            }
        }

        let framer = config
            .envelope
            .clone()
            .and_then(OutputEnvelopeSpec::into_framer);
        Ok(Self {
            writer,
            layouts,
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
        writer: &mut W,
        config: &FixedWidthWriterConfig,
        fields: &indexmap::IndexMap<Box<str>, Value>,
    ) -> Result<(), FormatError> {
        let mut line = String::new();
        for value in fields.values() {
            line.push_str(&value_to_envelope_cell(value));
        }
        writer.write_all(line.as_bytes())?;
        match config.line_separator {
            LineSeparator::Lf => writer.write_all(b"\n")?,
            LineSeparator::CrLf => writer.write_all(b"\r\n")?,
            LineSeparator::None => {}
        }
        Ok(())
    }

    /// Get any truncation warnings emitted during writing.
    pub fn truncation_warnings(&self) -> &[String] {
        &self.truncation_warnings
    }

    /// Encode and validate one complete record before the destination sees any
    /// bytes. Capacity is bounded by the maximum resolved record layout plus
    /// its fixed line separator.
    fn encode_record(&mut self, record: &Record) -> Result<Vec<u8>, FormatError> {
        for (name, _) in record.iter_user_fields() {
            if !self.layouts.iter().any(|layout| layout.name() == name) {
                return Err(FormatError::SchemaDrift {
                    format: "fixed-width",
                    column: name.to_string(),
                });
            }
        }

        let separator_width = match self.config.line_separator {
            LineSeparator::Lf => 1,
            LineSeparator::CrLf => 2,
            LineSeparator::None => 0,
        };
        let max_record_width = self.layouts.iter().map(WriteLayout::end).max().unwrap_or(0);
        let capacity = max_record_width
            .checked_add(separator_width)
            .ok_or_else(|| FormatError::InvalidRecord {
                row: 0,
                message: "fixed-width record length overflows after adding its line separator"
                    .to_string(),
            })?;
        let mut encoded = Vec::with_capacity(capacity);
        let null = Value::Null;
        let mut shifted_left = 0usize;
        let layouts = &self.layouts;
        let warnings = &mut self.truncation_warnings;

        for layout in layouts {
            let start = layout.start().checked_sub(shifted_left).ok_or_else(|| {
                FormatError::InvalidRecord {
                    row: 0,
                    message: format!(
                        "field '{}': prior shifted groups move this field before byte zero",
                        layout.name()
                    ),
                }
            })?;
            encoded.resize(start, b' ');
            match layout {
                WriteLayout::Scalar(field) => {
                    let value = record.get(&field.name).unwrap_or(&null);
                    encode_scalar_cell(&mut encoded, field, value, warnings, None)?;
                }
                WriteLayout::Group(group) => {
                    let value = record.get(&group.resolved.name).unwrap_or(&null);
                    let width = encode_group(&mut encoded, group, value, warnings)?;
                    if matches!(group.resolved.occurs.fill, FixedWidthFill::Shift) {
                        shifted_left += group.resolved.max_width() - width;
                    }
                }
            }
        }

        match self.config.line_separator {
            LineSeparator::Lf => encoded.push(b'\n'),
            LineSeparator::CrLf => encoded.extend_from_slice(b"\r\n"),
            LineSeparator::None => {}
        }
        Ok(encoded)
    }
}

fn encode_group(
    encoded: &mut Vec<u8>,
    group: &WriteGroup,
    value: &Value,
    warnings: &mut Vec<String>,
) -> Result<usize, FormatError> {
    let supplied = match value {
        Value::Null => &[][..],
        Value::Array(values) => values.as_slice(),
        _ => {
            return Err(FormatError::InvalidRecord {
                row: 0,
                message: format!(
                    "group '{}': expected an array of records; provide `[]` for zero occurrences",
                    group.resolved.name
                ),
            });
        }
    };
    if supplied.len() < group.resolved.occurs.min {
        return Err(FormatError::InvalidRecord {
            row: 0,
            message: format!(
                "group '{}': declared minimum is {}, but the record contains {} occurrence(s)",
                group.resolved.name,
                group.resolved.occurs.min,
                supplied.len()
            ),
        });
    }

    let selected = if supplied.len() <= group.resolved.occurs.max {
        supplied
    } else {
        match group.resolved.occurs.on_overflow {
            FixedWidthOverflow::Error => {
                return Err(FormatError::InvalidRecord {
                    row: 0,
                    message: format!(
                        "group '{}': declared maximum is {}, but the record contains {} occurrence(s); reduce the array or select `on_overflow: truncate` with `keep: first|last`",
                        group.resolved.name,
                        group.resolved.occurs.max,
                        supplied.len()
                    ),
                });
            }
            FixedWidthOverflow::Truncate => match group.resolved.occurs.keep {
                Some(FixedWidthTruncateKeep::First) => &supplied[..group.resolved.occurs.max],
                Some(FixedWidthTruncateKeep::Last) => {
                    &supplied[supplied.len() - group.resolved.occurs.max..]
                }
                None => unreachable!("layout validation requires a retained end"),
            },
        }
    };

    if let Some(count_field) = &group.resolved.count_field {
        let count = format!("{:0width$}", selected.len(), width = count_field.width);
        encoded.extend_from_slice(count.as_bytes());
    }

    let slots = match group.resolved.occurs.fill {
        FixedWidthFill::Pad => group.resolved.occurs.max,
        FixedWidthFill::Shift => selected.len(),
    };
    for index in 0..slots {
        let values = match selected.get(index) {
            Some(Value::Map(values)) => Some(values.as_ref()),
            Some(_) => {
                return Err(FormatError::InvalidRecord {
                    row: 0,
                    message: format!(
                        "group '{}': occurrence {} is not a record; provide a map with the declared child fields",
                        group.resolved.name,
                        index + 1
                    ),
                });
            }
            None => None,
        };
        let occurrence_start = encoded.len();
        for child in &group.fields {
            encoded.resize(occurrence_start + child.start, b' ');
            let value = values
                .and_then(|map| map.get(child.name.as_str()))
                .unwrap_or(&Value::Null);
            encode_scalar_cell(encoded, child, value, warnings, Some(&group.resolved.name))?;
        }
        encoded.resize(occurrence_start + group.resolved.occurrence_width(), b' ');
    }
    Ok(group.resolved.encoded_width(selected.len()))
}

fn encode_scalar_cell(
    encoded: &mut Vec<u8>,
    field: &WriteField,
    value: &Value,
    warnings: &mut Vec<String>,
    group_name: Option<&str>,
) -> Result<(), FormatError> {
    if group_name.is_some() && matches!(value, Value::Array(_) | Value::Map(_)) {
        return Err(FormatError::InvalidRecord {
            row: 0,
            message: format!(
                "group '{}': child '{}' must be scalar; flatten the occurrence record to the declared child fields",
                group_name.unwrap_or_default(),
                field.name
            ),
        });
    }
    let formatted = format_scalar_value(field, value)?;
    if formatted.len() > field.width {
        match field.truncation {
            TruncationPolicy::Error => {
                let message = match group_name {
                    Some(group_name) => format!(
                        "group '{group_name}': child '{}' is {} bytes, exceeding its declared width {}; shorten the child value or change its width/truncation policy",
                        field.name,
                        formatted.len(),
                        field.width
                    ),
                    None => format!(
                        "field '{}': value '{}' ({} bytes) exceeds width {} — truncation policy is 'error'",
                        field.name,
                        formatted,
                        formatted.len(),
                        field.width
                    ),
                };
                return Err(FormatError::InvalidRecord { row: 0, message });
            }
            TruncationPolicy::Warn => {
                warnings.push(match group_name {
                    Some(group_name) => format!(
                        "group '{group_name}': child '{}' truncated from {} to {} bytes",
                        field.name,
                        formatted.len(),
                        field.width
                    ),
                    None => format!(
                        "field '{}': value '{}' truncated to {} bytes",
                        field.name, formatted, field.width
                    ),
                });
            }
            TruncationPolicy::Silent => {}
        }
    }
    encoded.extend_from_slice(pad_and_justify(field, &formatted).as_bytes());
    Ok(())
}

fn format_scalar_value(field: &WriteField, value: &Value) -> Result<String, FormatError> {
    Ok(match value {
        Value::Null => String::new(),
        Value::String(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Date(value) => value.format("%Y%m%d").to_string(),
        Value::DateTime(value) => value.format("%Y%m%d%H%M%S").to_string(),
        Value::Array(_) => {
            return Err(FormatError::UnserializableArrayValue {
                format: "fixed-width",
                column: field.name.clone(),
            });
        }
        Value::Map(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "fixed-width",
                column: field.name.clone(),
            });
        }
    })
}

fn pad_and_justify(field: &WriteField, value: &str) -> String {
    let mut cut = value.len().min(field.width);
    while !value.is_char_boundary(cut) {
        cut -= 1;
    }
    let kept = &value[..cut];
    let padding = field.width - cut;
    let mut output = String::with_capacity(field.width);
    match field.justify {
        Justify::Left => {
            output.push_str(kept);
            output.extend(std::iter::repeat_n(field.pad_char, padding));
        }
        Justify::Right => {
            output.extend(std::iter::repeat_n(field.pad_char, padding));
            output.push_str(kept);
        }
    }
    output
}

impl<W: Write + Send> FormatWriter for FixedWidthWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        let encoded = self.encode_record(record)?;
        self.writer.write_all(&encoded)?;

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
        // Render the header directly off the framer's borrow into the
        // DocumentContext: `write_section_line` takes the disjoint `writer`
        // field, so it runs while the framer borrow is live. `None` (document
        // lacks the configured section) emits no header line.
        if let Some(fields) = framer.header_fields(doc) {
            Self::write_section_line(&mut self.writer, &self.config, fields)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        if let Some(fields) = framer.footer_fields(doc) {
            Self::write_section_line(&mut self.writer, &self.config, fields)?;
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

    /// Multiple records over a multi-field schema emit byte-exact output,
    /// including a record whose value for a field is missing (borrowed Null
    /// cell) — the value path the clone elimination touches.
    #[test]
    fn test_fixedwidth_write_multi_record_output_identity() {
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
        ];

        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            writer
                .write_record(&make_record(
                    &["id", "name"],
                    vec![Value::Integer(1), Value::String("Alice".into())],
                ))
                .unwrap();
            writer
                .write_record(&make_record(
                    &["id", "name"],
                    vec![Value::Integer(22), Value::String("Bob".into())],
                ))
                .unwrap();
            // Record missing `name`: its cell is a borrowed Null (empty, padded).
            writer
                .write_record(&make_record(&["id"], vec![Value::Integer(333)]))
                .unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(
            output,
            "00001Alice     \n00022Bob       \n00333          \n"
        );
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

    /// A non-ASCII value whose UTF-8 encoding overruns the field's byte width
    /// is cut at a character boundary — never mid-codepoint — so the emitted
    /// cell stays valid UTF-8 of exactly `width` bytes instead of panicking on
    /// a non-boundary byte slice. `"café"` is 5 bytes (`é` is 2), so a width-4
    /// field keeps `"caf"` and pads the remaining byte.
    #[test]
    fn test_fixedwidth_write_truncate_non_ascii_warn_byte_safe() {
        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(4);
            f.truncation = Some(TruncationPolicy::Warn);
            f
        }];

        let mut buf = Vec::new();
        let warning_msg;
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("café".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_msg = writer.truncation_warnings()[0].clone();
        }

        // Valid UTF-8 of exactly the byte width — the partial `é` is dropped,
        // not split, and the freed byte is space-padded.
        let output = String::from_utf8(buf).expect("output must be valid UTF-8");
        assert_eq!(output, "caf \n");
        // Diagnostics report byte counts, not char counts.
        assert!(
            warning_msg.contains("bytes"),
            "warning should say bytes: {warning_msg}"
        );
        assert!(
            !warning_msg.contains("chars"),
            "warning must not say chars: {warning_msg}"
        );
    }

    /// A multi-byte character that does not fit in the byte width at all yields
    /// an all-pad cell of exactly `width` bytes rather than panicking. Silent
    /// truncation emits no warning but still produces a byte-exact cell.
    #[test]
    fn test_fixedwidth_write_truncate_non_ascii_silent_byte_safe() {
        let fields = vec![{
            let mut f = field("flag");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(1);
            f.truncation = Some(TruncationPolicy::Silent);
            f
        }];

        let mut buf = Vec::new();
        let warning_count;
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            // `é` is 2 bytes; width 1 cannot hold it, so the cell is one pad byte.
            let rec = make_record(&["flag"], vec![Value::String("é".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_count = writer.truncation_warnings().len();
        }

        let output = String::from_utf8(buf).expect("output must be valid UTF-8");
        assert_eq!(output, " \n");
        assert_eq!(warning_count, 0, "silent truncation emits no warning");
    }

    /// A multi-byte `pad` character cannot fill an exact byte width — each push
    /// would add more than one byte — so it is rejected at construction with a
    /// typed field error naming the constraint.
    #[test]
    fn test_fixedwidth_write_multibyte_pad_rejected() {
        let fields = vec![{
            let mut f = field("name");
            f.ty = Type::String;
            f.start = Some(0);
            f.width = Some(5);
            // U+00B7 MIDDLE DOT is 2 UTF-8 bytes.
            f.pad = Some("·".into());
            f
        }];

        let mut buf = Vec::new();
        let err = FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default())
            .err()
            .expect("multi-byte pad must be rejected at construction");
        match err {
            FormatError::InvalidRecord { row, message } => {
                assert_eq!(row, 0, "construction defect reports row 0");
                assert!(
                    message.contains("single-byte"),
                    "message should state the single-byte constraint: {message}"
                );
                assert!(
                    message.contains("'name'"),
                    "message should name the field: {message}"
                );
            }
            other => panic!("expected InvalidRecord, got {other:?}"),
        }
    }

    /// A multi-CHARACTER pad (all-ASCII, e.g. `"0 "`) is also rejected at
    /// construction: the pad contract is single-byte end-to-end (#806), so the
    /// writer no longer silently honors only its first character.
    #[test]
    fn test_fixedwidth_write_multichar_pad_rejected() {
        let fields = vec![{
            let mut f = field("id");
            f.ty = Type::Int;
            f.start = Some(0);
            f.width = Some(5);
            f.justify = Some(Justify::Right);
            // Two ASCII bytes: previously accepted, honoring only '0'.
            f.pad = Some("0 ".into());
            f
        }];

        let mut buf = Vec::new();
        let err = FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default())
            .err()
            .expect("multi-character pad must be rejected at construction");
        match err {
            FormatError::InvalidRecord { row, message } => {
                assert_eq!(row, 0);
                assert!(
                    message.contains("single-byte"),
                    "message should state the single-byte constraint: {message}"
                );
                assert!(
                    message.contains("'id'"),
                    "message should name the field: {message}"
                );
            }
            other => panic!("expected InvalidRecord, got {other:?}"),
        }
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

    /// Fixed-width writer rejects `Value::Array` payloads with
    /// `FormatError::UnserializableArrayValue`, parallel to the map
    /// rejection. The prior behavior emitted an empty positional cell for
    /// any array, silently dropping the payload and hiding a misroute (e.g.
    /// a `match: collect` combine output sent to a fixed-width output).
    #[test]
    fn test_fixed_width_writer_rejects_array_value() {
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::Integer(7),
                Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            ],
        );
        let mut id_field = field("id");
        id_field.width = Some(5);
        let mut tags_field = field("tags");
        tags_field.width = Some(10);
        let fields = vec![id_field, tags_field];
        let mut buf = Vec::new();
        let mut writer =
            FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableArrayValue { format, column } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column, "tags");
            }
            other => panic!("expected UnserializableArrayValue, got {other:?}"),
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

    /// A record carrying a user column the fixed-width layout does not declare
    /// — the shape `auto_widen` produces when a later record surfaces a column
    /// the first lacked — is a loud SchemaDrift, not a silently-narrower line
    /// (issue #805). Checked before any byte is emitted, so the drifting
    /// record leaves no partial line behind.
    #[test]
    fn test_fixedwidth_write_undeclared_column_is_schema_drift() {
        let fields = vec![{
            let mut f = field("id");
            f.ty = Type::Int;
            f.start = Some(0);
            f.width = Some(5);
            f
        }];
        let mut buf = Vec::new();
        let mut writer =
            FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        // Record carries `region` beyond the declared `id`.
        let record = make_record(
            &["id", "region"],
            vec![Value::Integer(7), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::SchemaDrift { format, column } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column, "region");
            }
            other => panic!("expected SchemaDrift, got {other:?}"),
        }
        drop(writer);
        assert!(
            buf.is_empty(),
            "a drifting record must not leave a partial line behind"
        );
    }

    /// A record whose columns are all declared, or a subset of the declared
    /// layout, writes normally — the drift guard is not a false positive,
    /// including on a record missing a declared field (a legitimate absent,
    /// pad-filled cell).
    #[test]
    fn test_fixedwidth_write_declared_subset_is_not_drift() {
        let fields = vec![
            {
                let mut f = field("id");
                f.ty = Type::Int;
                f.start = Some(0);
                f.width = Some(3);
                f
            },
            {
                let mut f = field("name");
                f.ty = Type::String;
                f.start = Some(3);
                f.width = Some(5);
                f
            },
        ];
        let mut buf = Vec::new();
        {
            let mut writer =
                FixedWidthWriter::new(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            // Record declares only `id` — `name` is a legitimate absent cell.
            writer
                .write_record(&make_record(&["id"], vec![Value::Integer(7)]))
                .unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "  7     \n");
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

    #[test]
    fn fixed_width_envelope_two_documents_each_reframed() {
        // Two documents in one stream each get their own header/footer line
        // rendered from their own `$doc` sections. Exercises the per-document
        // framing across `begin_document` / `end_document` more than once — the
        // section maps are rendered in place off the framer's borrow.
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
        let doc1 = doc_with_sections(&[
            ("Head", &[("tag", Value::String("H1".into()))]),
            ("Foot", &[("tag", Value::String("T1".into()))]),
        ]);
        let doc2 = doc_with_sections(&[
            ("Head", &[("tag", Value::String("H2".into()))]),
            ("Foot", &[("tag", Value::String("T2".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let mut w = FixedWidthWriter::new(&mut buf, vec![amount], config).unwrap();
            w.begin_document(&doc1).unwrap();
            w.write_record(&make_record(&["amount"], vec![Value::Integer(7)]))
                .unwrap();
            w.end_document(&doc1).unwrap();
            w.begin_document(&doc2).unwrap();
            w.write_record(&make_record(&["amount"], vec![Value::Integer(8)]))
                .unwrap();
            w.end_document(&doc2).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out, "H1\n00007\nT1\nH2\n00008\nT2\n", "got: {out}");
    }
}
