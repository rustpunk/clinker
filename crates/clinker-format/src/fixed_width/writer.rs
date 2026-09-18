use clinker_record::owned_storage::OwnedKey;
use std::io::Write;

use clinker_record::schema_def::{Justify, LineSeparator, TruncationPolicy};
use clinker_record::{DocumentContext, Record, Value};
use cxl::typecheck::Type;

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::error::{OutputEncodingKind, OutputFieldName};
use crate::fixed_width::field::{self, ResolvedRepeatingGroup};
use crate::preparation::{
    FormatEncoder, OutputOperation, PreparedWriter, WriterResources, WriterScope,
};
use crate::reserved::{ReservedText, ReservedVec};
use crate::schema::{Column, FixedWidthFill, FixedWidthOverflow, FixedWidthTruncateKeep};
use crate::traits::FormatWriter;
use clinker_record::owned_storage::SharedStorage;

fn output_error(name: &str, field: usize, offset: usize, kind: OutputEncodingKind) -> FormatError {
    FormatError::OutputEncoding {
        format: "fixed-width",
        field: field + 1,
        offset,
        kind,
        field_name: OutputFieldName::new(name),
        element: None,
    }
}
fn retained_text(value: &str, scope: &WriterScope) -> Result<ReservedText, FormatError> {
    let mut text = ReservedText::new(scope.allocation().clone());
    text.push_str(value)?;
    Ok(text)
}
struct PreparedField {
    name: ReservedText,
    start: usize,
    width: usize,
    justify: Justify,
    pad: u8,
    truncation: TruncationPolicy,
    trim: bool,
    read_right: bool,
    read_pad: Option<char>,
}
impl PreparedField {
    fn new(column: &Column, start: usize, scope: &WriterScope) -> Result<Self, FormatError> {
        let width = field::scalar_width(column, start)
            .map_err(|e| output_error(e.name, 0, start, OutputEncodingKind::FixedWidthLayout))?;
        let numeric = matches!(
            column.ty.unwrap_nullable(),
            Type::Int | Type::Float | Type::Decimal | Type::Numeric
        );
        Ok(Self {
            name: retained_text(&column.name, scope)?,
            start,
            width,
            justify: column.justify.clone().unwrap_or(if numeric {
                Justify::Right
            } else {
                Justify::Left
            }),
            pad: column
                .pad
                .as_deref()
                .and_then(|s| s.bytes().next())
                .unwrap_or(b' '),
            truncation: column.truncation.clone().unwrap_or(if numeric {
                TruncationPolicy::Error
            } else {
                TruncationPolicy::Warn
            }),
            trim: column.trim.unwrap_or(true),
            read_right: matches!(column.justify, Some(Justify::Right)),
            read_pad: column.pad.as_deref().unwrap_or(" ").chars().next(),
        })
    }
}
struct PreparedGroup {
    name: ReservedText,
    start: usize,
    width: usize,
    occurrence_width: usize,
    count_width: usize,
    occurs: crate::schema::FixedWidthOccurs,
    fields: ReservedVec<PreparedField>,
}
enum PreparedLayout {
    Scalar(PreparedField),
    Group(PreparedGroup),
}
impl PreparedLayout {
    fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name.as_str(),
            Self::Group(g) => g.name.as_str(),
        }
    }
    fn start(&self) -> usize {
        match self {
            Self::Scalar(f) => f.start,
            Self::Group(g) => g.start,
        }
    }
    fn width(&self) -> usize {
        match self {
            Self::Scalar(f) => f.width,
            Self::Group(g) => g.width,
        }
    }
}
struct PreparedConfig {
    layouts: ReservedVec<PreparedLayout>,
    separator: LineSeparator,
    envelope: Option<crate::envelope_writer::PreparedEnvelope>,
}
/// Immutable admitted physical layout shared by every destination in a factory.
/// Column/type trees remain with the caller; only derived rendering policy and
/// admitted names are retained, with no recursive type or value copies.
#[derive(Clone)]
pub struct FixedWidthEncoderConfig(SharedStorage<PreparedConfig>);
impl FixedWidthEncoderConfig {
    /// Validate borrowed declarations, then admit each retained layout/name.
    pub fn new(
        fields: &[Column],
        config: &FixedWidthWriterConfig,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let envelope = config.envelope.as_ref();
        Self::from_names(
            fields,
            config.line_separator.clone(),
            envelope.and_then(|e| e.header_from_doc.as_deref()),
            envelope.and_then(|e| e.footer_from_doc.as_deref()),
            envelope.and_then(|e| e.footer_record_count_field.as_deref()),
            resources,
        )
    }
    /// Borrow compiled envelope names without an intermediate owned spec.
    pub fn from_names(
        fields: &[Column],
        separator: LineSeparator,
        header: Option<&str>,
        footer: Option<&str>,
        count: Option<&str>,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        scope.check_cancelled()?;
        field::check_write_layout(fields)
            .map_err(|e| output_error(e.name, 0, 0, OutputEncodingKind::FixedWidthLayout))?;
        if let Some(count) = count {
            return Err(output_error(
                count,
                0,
                0,
                OutputEncodingKind::FixedWidthEnvelope,
            ));
        }
        let mut layouts = ReservedVec::new(scope.allocation().clone());
        layouts.reserve_exact(fields.len())?;
        let mut next_start = 0;
        for column in fields {
            scope.check_cancelled()?;
            let start = column.start.unwrap_or(next_start);
            let layout = if field::is_group(column) {
                let dimensions = field::group_dimensions(column, start).map_err(|e| {
                    output_error(e.name, 0, start, OutputEncodingKind::FixedWidthLayout)
                })?;
                let children = column.fields.as_deref().unwrap_or_default();
                let mut fields = ReservedVec::new(scope.allocation().clone());
                fields.reserve_exact(children.len())?;
                let mut next_child = 0;
                for child in children {
                    scope.check_cancelled()?;
                    let child =
                        PreparedField::new(child, child.start.unwrap_or(next_child), &scope)?;
                    next_child = child.start + child.width;
                    fields.push(child)?;
                }
                fields.as_mut_slice().sort_unstable_by_key(|f| f.start);
                PreparedLayout::Group(PreparedGroup {
                    name: retained_text(&column.name, &scope)?,
                    start,
                    width: dimensions.max_width,
                    occurrence_width: dimensions.occurrence_width,
                    count_width: column.count_field.as_ref().map_or(0, |c| c.width),
                    occurs: column.occurs.clone().ok_or_else(|| {
                        output_error(&column.name, 0, start, OutputEncodingKind::FixedWidthLayout)
                    })?,
                    fields,
                })
            } else {
                PreparedLayout::Scalar(PreparedField::new(column, start, &scope)?)
            };
            next_start = layout.start() + layout.width();
            layouts.push(layout)?;
        }
        layouts
            .as_mut_slice()
            .sort_unstable_by_key(PreparedLayout::start);
        Ok(Self(SharedStorage::try_new(
            PreparedConfig {
                layouts,
                separator,
                envelope: crate::envelope_writer::PreparedEnvelope::from_names(
                    header, footer, None, &scope,
                )?,
            },
            scope.allocation(),
        )?))
    }
}
#[derive(Clone, Copy, Default)]
struct FixedWidthState {
    records: u64,
    document_open: bool,
}
/// Streams complete records/sections into an admitted stage. Only delivered
/// operations change counters or warning history; no record values are retained.
pub struct FixedWidthEncoder {
    config: FixedWidthEncoderConfig,
    state: FixedWidthState,
    warnings: ReservedVec<ReservedText>,
}
/// Owns prepared warning replacement until successful delivery or cancellation.
pub struct FixedWidthPending {
    state: FixedWidthState,
    warnings: Option<ReservedVec<ReservedText>>,
}
impl FixedWidthEncoder {
    /// Admit layout/configuration before retaining it; borrows caller columns.
    pub fn new(
        fields: &[Column],
        config: &FixedWidthWriterConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        Self::from_config(
            FixedWidthEncoderConfig::new(fields, config, &resources)?,
            resources,
        )
    }
    /// Share admitted immutable policy; each writer owns its warning history.
    pub fn from_config(
        config: FixedWidthEncoderConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        Ok(Self {
            config,
            state: FixedWidthState::default(),
            warnings: ReservedVec::new(resources.scope()?.allocation().clone()),
        })
    }
    /// Admit the concrete wrapper through its actual backing deallocation.
    pub fn into_boxed_writer<W: Write + Send + 'static>(
        self,
        destination: W,
        resources: WriterResources,
    ) -> Result<crate::traits::FormatWriterHandle, FormatError> {
        let scope = resources.scope()?;
        let writer = PreparedWriter::new(destination, self, resources)?;
        Ok(crate::traits::FormatWriterHandle::try_new(
            writer,
            scope.allocation(),
        )?)
    }
    /// Complete committed warnings; pending failures never alter this history.
    pub fn truncation_warnings(&self) -> &[ReservedText] {
        self.warnings.as_slice()
    }
    /// Successfully delivered body records since the last delivered begin.
    pub fn record_count(&self) -> u64 {
        self.state.records
    }
    /// Whether the last delivered document transition opened a document.
    pub fn document_open(&self) -> bool {
        self.state.document_open
    }
}
impl<W: Write + Send> FormatWriter for PreparedWriter<W, FixedWidthEncoder> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::Record(record))
    }
    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::BeginDocument(doc))
    }
    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::EndDocument(doc))
    }
    fn flush(&mut self) -> Result<(), FormatError> {
        PreparedWriter::flush(self)
    }
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        PreparedWriter::flush_bytes(self)
    }
}

// f64 display fits 327 bytes, decimal fits 32 and calendar types fit 32.
// Strings bypass this fixed scratch and retain their original record owner.
fn physical_scalar<T>(
    value: &Value,
    name: &str,
    index: usize,
    envelope: bool,
    action: impl FnOnce(&str) -> Result<T, FormatError>,
) -> Result<T, FormatError> {
    use std::fmt::Write as _;
    struct Scratch {
        bytes: [u8; 1024],
        len: usize,
    }
    impl std::fmt::Write for Scratch {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let end = self
                .len
                .checked_add(s.len())
                .filter(|end| *end <= self.bytes.len())
                .ok_or(std::fmt::Error)?;
            self.bytes[self.len..end].copy_from_slice(s.as_bytes());
            self.len = end;
            Ok(())
        }
    }
    let mut text = Scratch {
        bytes: [0; 1024],
        len: 0,
    };
    let result = match value {
        Value::Null => return action(""),
        Value::String(s) => return action(s.as_str()),
        Value::Bool(v) => return action(if *v { "true" } else { "false" }),
        Value::Integer(v) => write!(text, "{v}"),
        Value::Float(v) => write!(text, "{v}"),
        Value::Decimal(v) => write!(text, "{v}"),
        Value::Date(v) => v.format("%Y%m%d").write_to(&mut text),
        Value::DateTime(v) => v.format("%Y%m%d%H%M%S").write_to(&mut text),
        Value::Array(_) | Value::Map(_) => {
            return Err(output_error(
                name,
                index,
                0,
                if envelope {
                    OutputEncodingKind::FixedWidthEnvelope
                } else {
                    OutputEncodingKind::FixedWidthScalar
                },
            ));
        }
    };
    result.map_err(|_| output_error(name, index, 0, OutputEncodingKind::FixedWidthScalar))?;
    let text = std::str::from_utf8(&text.bytes[..text.len])
        .map_err(|_| output_error(name, index, 0, OutputEncodingKind::FixedWidthScalar))?;
    action(text)
}
fn padding(
    stage: &mut dyn Write,
    byte: u8,
    mut count: usize,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    let bytes = [byte; 1024];
    while count != 0 {
        scope.check_cancelled()?;
        let n = count.min(bytes.len());
        stage.write_all(&bytes[..n])?;
        count -= n;
    }
    Ok(())
}
fn separator(stage: &mut dyn Write, separator: &LineSeparator) -> Result<(), FormatError> {
    stage.write_all(match separator {
        LineSeparator::Lf => b"\n",
        LineSeparator::CrLf => b"\r\n",
        LineSeparator::None => b"",
    })?;
    Ok(())
}
fn kept<'a>(text: &'a str, field: &PreparedField) -> &'a str {
    let mut end = text.len().min(field.width);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
fn append_warning(
    warnings: &mut ReservedVec<ReservedText>,
    field: &PreparedField,
    text: &str,
    group: Option<&str>,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    use std::fmt::Write as _;
    struct Message {
        text: ReservedText,
        error: Option<crate::preparation::ResourceError>,
    }
    impl std::fmt::Write for Message {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.text.push_str(text).map_err(|error| {
                self.error = Some(error);
                std::fmt::Error
            })
        }
    }
    let mut message = Message {
        text: ReservedText::new(scope.allocation().clone()),
        error: None,
    };
    let result = if let Some(group) = group {
        write!(
            message,
            "group '{group}': child '{}' truncated from {} to {} bytes",
            field.name.as_str(),
            text.len(),
            field.width
        )
    } else {
        write!(
            message,
            "field '{}': value '{}' truncated to {} bytes",
            field.name.as_str(),
            text,
            field.width
        )
    };
    if result.is_err() {
        return Err(message
            .error
            .unwrap_or_else(|| {
                crate::preparation::ResourceError::new(
                    crate::preparation::ResourceErrorKind::Layout,
                    0,
                    0,
                )
            })
            .into());
    }
    warnings.push(message.text)?;
    Ok(())
}
fn prepared_cell(
    stage: &mut dyn Write,
    field: &PreparedField,
    value: &Value,
    index: usize,
    group: Option<&str>,
    warnings: &mut ReservedVec<ReservedText>,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    physical_scalar(value, field.name.as_str(), index, false, |text| {
        if text.len() > field.width {
            match field.truncation {
                TruncationPolicy::Error => {
                    return Err(output_error(
                        field.name.as_str(),
                        index,
                        field.width,
                        OutputEncodingKind::FixedWidthTruncation,
                    ));
                }
                TruncationPolicy::Warn => append_warning(warnings, field, text, group, scope)?,
                TruncationPolicy::Silent => {}
            }
        }
        let text = kept(text, field);
        if matches!(field.justify, Justify::Right) {
            padding(stage, field.pad, field.width - text.len(), scope)?;
        }
        stage.write_all(text.as_bytes())?;
        if matches!(field.justify, Justify::Left) {
            padding(stage, field.pad, field.width - text.len(), scope)?;
        }
        Ok(())
    })
}
fn blank_cell(
    field: &PreparedField,
    value: &Value,
    index: usize,
    scope: &WriterScope,
) -> Result<bool, FormatError> {
    if !field.trim || matches!(value, Value::Array(_) | Value::Map(_)) {
        return Ok(false);
    }
    physical_scalar(value, field.name.as_str(), index, false, |text| {
        let text = kept(text, field);
        let pads = field.width - text.len();
        let left = if matches!(field.justify, Justify::Right) {
            pads
        } else {
            0
        };
        let right = pads - left;
        let pad = char::from(field.pad);
        let (leading, trailing) = if field.read_right {
            (left, right)
        } else {
            (right, left)
        };
        // Virtual padding is handled once regardless of its declared width.
        // Only actual text is scanned, in the reader trimming direction.
        let mut stripping = leading == 0 || Some(pad) == field.read_pad;
        if !stripping && !pad.is_whitespace() {
            return Ok(false);
        }
        let mut check = |offset: usize, c: char| -> Result<bool, FormatError> {
            if offset % 1024 == 0 {
                scope.check_cancelled()?;
            }
            if stripping && Some(c) == field.read_pad {
                return Ok(true);
            }
            stripping = false;
            Ok(c.is_whitespace())
        };
        if field.read_right {
            for (offset, c) in text.chars().enumerate() {
                if !check(offset, c)? {
                    return Ok(false);
                }
            }
        } else {
            for (offset, c) in text.chars().rev().enumerate() {
                if !check(offset, c)? {
                    return Ok(false);
                }
            }
        }
        Ok(trailing == 0 || (stripping && Some(pad) == field.read_pad) || pad.is_whitespace())
    })
}
fn prepared_group(
    stage: &mut dyn Write,
    group: &PreparedGroup,
    value: &Value,
    index: usize,
    warnings: &mut ReservedVec<ReservedText>,
    scope: &WriterScope,
) -> Result<usize, FormatError> {
    let failure = |kind| output_error(group.name.as_str(), index, group.start, kind);
    let supplied = match value {
        Value::Null => &[][..],
        Value::Array(values) => values.as_slice(),
        _ => return Err(failure(OutputEncodingKind::FixedWidthOccurrence)),
    };
    if supplied.len() < group.occurs.min {
        return Err(failure(OutputEncodingKind::FixedWidthCardinality));
    }
    let selected = if supplied.len() <= group.occurs.max {
        supplied
    } else {
        match (group.occurs.on_overflow, group.occurs.keep) {
            (FixedWidthOverflow::Truncate, Some(FixedWidthTruncateKeep::First)) => {
                &supplied[..group.occurs.max]
            }
            (FixedWidthOverflow::Truncate, Some(FixedWidthTruncateKeep::Last)) => {
                &supplied[supplied.len() - group.occurs.max..]
            }
            _ => return Err(failure(OutputEncodingKind::FixedWidthCardinality)),
        }
    };
    if group.count_width != 0 {
        // usize has at most 20 decimal digits; no count-width-sized scratch.
        let count = i64::try_from(selected.len())
            .map_err(|_| failure(OutputEncodingKind::FixedWidthCardinality))?;
        physical_scalar(
            &Value::Integer(count),
            group.name.as_str(),
            index,
            false,
            |text| {
                padding(stage, b'0', group.count_width - text.len(), scope)?;
                stage.write_all(text.as_bytes())?;
                Ok(())
            },
        )?;
    }
    let slots = match group.occurs.fill {
        FixedWidthFill::Pad => group.occurs.max,
        FixedWidthFill::Shift => selected.len(),
    };
    for occurrence in 0..slots {
        scope.check_cancelled()?;
        let values = match selected.get(occurrence) {
            Some(Value::Map(values)) => Some(values.as_map()),
            Some(_) => return Err(failure(OutputEncodingKind::FixedWidthOccurrence)),
            None => None,
        };
        if let Some(values) = values
            && group.count_width == 0
            && matches!(group.occurs.fill, FixedWidthFill::Pad)
        {
            let mut blank = true;
            for field in group.fields.as_slice() {
                scope.check_cancelled()?;
                if !blank_cell(
                    field,
                    values.get(field.name.as_str()).unwrap_or(&Value::Null),
                    index,
                    scope,
                )? {
                    blank = false;
                    break;
                }
            }
            if blank {
                return Err(failure(OutputEncodingKind::FixedWidthBlankOccurrence));
            }
        }
        let mut at = 0;
        for field in group.fields.as_slice() {
            scope.check_cancelled()?;
            padding(stage, b' ', field.start - at, scope)?;
            let value = values
                .and_then(|values| values.get(field.name.as_str()))
                .unwrap_or(&Value::Null);
            prepared_cell(
                stage,
                field,
                value,
                index,
                Some(group.name.as_str()),
                warnings,
                scope,
            )?;
            at = field.start + field.width;
        }
        padding(stage, b' ', group.occurrence_width - at, scope)?;
    }
    Ok(group.count_width + slots * group.occurrence_width)
}
impl FormatEncoder for FixedWidthEncoder {
    type Pending = FixedWidthPending;
    fn prepare(
        &self,
        operation: OutputOperation<'_>,
        stage: &mut dyn Write,
        scope: &WriterScope,
    ) -> Result<Self::Pending, FormatError> {
        scope.check_cancelled()?;
        let mut state = self.state;
        let mut warnings = None;
        match operation {
            OutputOperation::Record(record) => {
                for (index, (name, _)) in record.iter_user_fields().enumerate() {
                    scope.check_cancelled()?;
                    if !self
                        .config
                        .0
                        .layouts
                        .as_slice()
                        .iter()
                        .any(|layout| layout.name() == name)
                    {
                        return Err(output_error(
                            name,
                            index,
                            0,
                            OutputEncodingKind::SchemaDrift,
                        ));
                    }
                }
                let mut pending = ReservedVec::new(scope.allocation().clone());
                let mut at = 0;
                let mut shifted = 0;
                for (index, layout) in self.config.0.layouts.as_slice().iter().enumerate() {
                    scope.check_cancelled()?;
                    let start = layout.start() - shifted;
                    padding(stage, b' ', start - at, scope)?;
                    let value = record.get(layout.name()).unwrap_or(&Value::Null);
                    let width = match layout {
                        PreparedLayout::Scalar(field) => {
                            prepared_cell(stage, field, value, index, None, &mut pending, scope)?;
                            field.width
                        }
                        PreparedLayout::Group(group) => {
                            prepared_group(stage, group, value, index, &mut pending, scope)?
                        }
                    };
                    shifted += layout.width() - width;
                    at = start + width;
                }
                separator(stage, &self.config.0.separator)?;
                if !pending.is_empty() {
                    let mut replacement = ReservedVec::new(scope.allocation().clone());
                    for text in self.warnings.as_slice().iter().chain(pending.as_slice()) {
                        replacement.push(retained_text(text.as_str(), scope)?)?;
                    }
                    warnings = Some(replacement);
                }
                state.records = state.records.saturating_add(1);
            }
            OutputOperation::BeginDocument(doc) | OutputOperation::EndDocument(doc) => {
                let begin = matches!(operation, OutputOperation::BeginDocument(_));
                if let Some(envelope) = &self.config.0.envelope {
                    let fields = if begin {
                        envelope.header_fields(doc)
                    } else {
                        envelope.footer_fields(doc)
                    };
                    if let Some(fields) = fields {
                        for (index, (name, value)) in fields.iter().enumerate() {
                            scope.check_cancelled()?;
                            physical_scalar(value, name, index, true, |text| {
                                stage.write_all(text.as_bytes())?;
                                Ok(())
                            })?;
                        }
                        separator(stage, &self.config.0.separator)?;
                    }
                    state.document_open = begin;
                    if begin {
                        state.records = 0;
                    }
                }
            }
            OutputOperation::Finalize => {}
        }
        Ok(FixedWidthPending { state, warnings })
    }
    fn commit(&mut self, pending: Self::Pending) {
        self.state = pending.state;
        if let Some(warnings) = pending.warnings {
            self.warnings = warnings;
        }
    }
}

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
        field::validate_write_layout(&fields)?;
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
        fields: &indexmap::IndexMap<OwnedKey, Value>,
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
            Some(Value::Map(values)) => Some(values.as_map()),
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
        let renders_as_unused_padding = match values {
            Some(values) => group_occurrence_is_blank(group, values)?,
            None => false,
        };
        if group.resolved.count_field.is_none()
            && matches!(group.resolved.occurs.fill, FixedWidthFill::Pad)
            && renders_as_unused_padding
        {
            return Err(FormatError::InvalidRecord {
                row: 0,
                message: format!(
                    "group '{}': occurrence {} renders exactly like an unused padded slot; add a `count_field` or provide at least one non-padding child value",
                    group.resolved.name,
                    index + 1
                ),
            });
        }
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

fn group_occurrence_is_blank(
    group: &WriteGroup,
    values: &indexmap::IndexMap<OwnedKey, Value>,
) -> Result<bool, FormatError> {
    for (field, resolved) in group.fields.iter().zip(&group.resolved.fields) {
        let value = values.get(field.name.as_str()).unwrap_or(&Value::Null);
        if matches!(value, Value::Array(_) | Value::Map(_)) {
            return Ok(false);
        }
        let formatted = format_scalar_value(field, value)?;
        let padded = pad_and_justify(field, &formatted);
        if !field::strip_padding(&padded, resolved).is_empty() {
            return Ok(false);
        }
    }
    Ok(true)
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
    use clinker_record::owned_storage::{OwnedMap, OwnedValues, SharedStorage};
    use clinker_record::{Record, Schema, Value};
    use std::sync::Arc;

    fn field(name: &str) -> Column {
        Column::bare(name, Type::String)
    }

    fn make_record(cols: &[&str], vals: Vec<Value>) -> Record {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(
            cols.iter().map(|c| (*c).into()).collect(),
        )));
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
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "payload".into()])));
        let mut sidecar: indexmap::IndexMap<OwnedKey, Value> = indexmap::IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        let record = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::Map(OwnedMap::from_map(sidecar))],
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
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::Integer(7),
                Value::Array(OwnedValues::from_vec(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                ])),
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
