use std::io::Write;

use clinker_record::schema_def::{Justify, LineSeparator, TruncationPolicy};
use clinker_record::{DocumentContext, Record, Value};
use cxl::typecheck::Type;

use crate::envelope_writer::OutputEnvelopeSpec;
use crate::error::FormatError;
use crate::error::{OutputEncodingKind, OutputFieldName};
use crate::fixed_width::field;
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

// Slots are initialized before delivery, including spare capacity. This lets
// commit move owners into existing slots without a fallible push or allocation.
// Field order is load-bearing: all String backings drop before their leases.
struct WarningHistory {
    strings: ReservedVec<String>,
    leases: ReservedVec<Option<crate::preparation::AllocationLease>>,
    len: usize,
}
impl WarningHistory {
    fn empty(scope: &WriterScope) -> Self {
        Self {
            strings: ReservedVec::new(scope.allocation().clone()),
            leases: ReservedVec::new(scope.allocation().clone()),
            len: 0,
        }
    }
    fn with_capacity(capacity: usize, scope: &WriterScope) -> Result<Self, FormatError> {
        let mut history = Self::empty(scope);
        history.strings.reserve_exact(capacity)?;
        history.leases.reserve_exact(capacity)?;
        for _ in 0..capacity {
            scope.check_cancelled()?;
            history.strings.push(String::new())?;
            history.leases.push(None)?;
        }
        Ok(history)
    }
    fn slots(
        &mut self,
    ) -> impl Iterator<
        Item = (
            &mut String,
            &mut Option<crate::preparation::AllocationLease>,
        ),
    > {
        self.strings
            .as_mut_slice()
            .iter_mut()
            .zip(self.leases.as_mut_slice())
    }
    // The caller establishes enough initialized vacant slots in prepare.
    // Source slots become empty while the grants follow their String owners.
    fn append(&mut self, source: &mut Self) {
        let old_len = self.len;
        let source_len = source.len;
        for ((text, lease), (from_text, from_lease)) in self
            .slots()
            .skip(old_len)
            .zip(source.slots().take(source_len))
        {
            std::mem::swap(text, from_text);
            std::mem::swap(lease, from_lease);
        }
        self.len += source_len;
        source.len = 0;
    }
}
struct PendingWarnings {
    messages: WarningHistory,
    replacement: Option<WarningHistory>,
}
impl PendingWarnings {
    fn new(
        current: &WarningHistory,
        mut pending: ReservedVec<ReservedText>,
        scope: &WriterScope,
    ) -> Result<Option<Self>, FormatError> {
        if pending.is_empty() {
            return Ok(None);
        }
        let count = pending.len();
        let needed = current.len.checked_add(count).ok_or_else(|| {
            crate::preparation::ResourceError::new(
                crate::preparation::ResourceErrorKind::Layout,
                count,
                0,
            )
        })?;
        let replacement = if needed > current.strings.len() {
            let preferred = current
                .strings
                .len()
                .checked_mul(2)
                .unwrap_or(needed)
                .max(needed);
            let replacement = match WarningHistory::with_capacity(preferred, scope) {
                Err(FormatError::Resource(error))
                    if preferred != needed
                        && error.kind == crate::preparation::ResourceErrorKind::Budget =>
                {
                    WarningHistory::with_capacity(needed, scope)?
                }
                result => result?,
            };
            Some(replacement)
        } else {
            None
        };
        let mut messages = WarningHistory::with_capacity(count, scope)?;
        for ((text, lease), pending) in messages.slots().zip(pending.as_mut_slice()) {
            scope.check_cancelled()?;
            let owner = std::mem::replace(pending, ReservedText::new(scope.allocation().clone()));
            (*text, *lease) = owner.into_string_parts();
        }
        messages.len = count;
        Ok(Some(Self {
            messages,
            replacement,
        }))
    }
    fn commit(mut self, history: &mut WarningHistory) {
        if let Some(mut replacement) = self.replacement {
            replacement.append(history);
            *history = replacement;
        }
        history.append(&mut self.messages);
    }
}
/// Streams complete records/sections into an admitted stage. Only delivered
/// operations change counters or warning history; no record values are retained.
pub struct FixedWidthEncoder {
    config: FixedWidthEncoderConfig,
    state: FixedWidthState,
    warnings: WarningHistory,
}
/// Owns prepared warning replacement until successful delivery or cancellation.
pub struct FixedWidthPending {
    state: FixedWidthState,
    warnings: Option<PendingWarnings>,
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
            warnings: WarningHistory::empty(&resources.scope()?),
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
    pub fn truncation_warnings(&self) -> &[String] {
        &self.warnings.strings.as_slice()[..self.warnings.len]
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
            if offset.is_multiple_of(1024) {
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
                warnings = PendingWarnings::new(&self.warnings, pending, scope)?;
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
            warnings.commit(&mut self.warnings);
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues, SharedStorage};
    use clinker_record::{Record, Schema, Value};
    use std::sync::Arc;

    fn finite_writer<W: Write + Send>(
        destination: W,
        fields: Vec<Column>,
        config: FixedWidthWriterConfig,
    ) -> Result<PreparedWriter<W, FixedWidthEncoder>, FormatError> {
        let provider = crate::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
        );
        let encoder = FixedWidthEncoder::new(&fields, &config, provider.resources())?;
        Ok(PreparedWriter::new(
            destination,
            encoder,
            provider.resources(),
        )?)
    }

    fn layout_error(fields: Vec<Column>) -> FormatError {
        let error =
            field::validate_write_layout(&fields).expect_err("pure validation rejects layout");
        let result = finite_writer(Vec::new(), fields, FixedWidthWriterConfig::default());
        assert!(matches!(
            result,
            Err(FormatError::OutputEncoding {
                format: "fixed-width",
                kind: OutputEncodingKind::FixedWidthLayout,
                ..
            })
        ));
        error
    }

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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("LongName".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_count = writer.encoder().truncation_warnings().len();
            warning_msg = writer.encoder().truncation_warnings()[0].clone();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("café".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_msg = writer.encoder().truncation_warnings()[0].clone();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            // `é` is 2 bytes; width 1 cannot hold it, so the cell is one pad byte.
            let rec = make_record(&["flag"], vec![Value::String("é".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            warning_count = writer.encoder().truncation_warnings().len();
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

        let err = layout_error(fields);
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

        let err = layout_error(fields);
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
            finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();

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
                finite_writer(&mut buf, write_fields, FixedWidthWriterConfig::default()).unwrap();
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
    /// the bounded fixed-width scalar error. The previous behavior
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
            finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field: 2,
                offset: 0,
                kind: OutputEncodingKind::FixedWidthScalar,
                field_name: column,
                element: None,
            } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column.to_string(), "payload");
            }
            other => panic!("expected bounded Map rejection, got {other:?}"),
        }
    }

    /// Fixed-width writer rejects `Value::Array` payloads with
    /// the bounded fixed-width scalar error, parallel to the map
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
            finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field: 2,
                offset: 0,
                kind: OutputEncodingKind::FixedWidthScalar,
                field_name: column,
                element: None,
            } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column.to_string(), "tags");
            }
            other => panic!("expected bounded Array rejection, got {other:?}"),
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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

        let err = layout_error(fields);
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

        let err = layout_error(fields);
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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

        let err = layout_error(fields);
        let msg = err.to_string();
        assert!(msg.contains("overflows"), "error should say so: {msg}");
    }

    /// A record carrying a user column the fixed-width layout does not declare
    /// — the shape `auto_widen` produces when a later record surfaces a column
    /// the first lacked — is a loud schema-drift error, not a silently-narrower line
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
            finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
        // Record carries `region` beyond the declared `id`.
        let record = make_record(
            &["id", "region"],
            vec![Value::Integer(7), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field: 2,
                offset: 0,
                kind: OutputEncodingKind::SchemaDrift,
                field_name: column,
                element: None,
            } => {
                assert_eq!(format, "fixed-width");
                assert_eq!(column.to_string(), "region");
            }
            other => panic!("expected bounded SchemaDrift rejection, got {other:?}"),
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
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
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
            let mut w = finite_writer(&mut buf, vec![amount], config).unwrap();
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
            let mut w = finite_writer(&mut buf, vec![amount], config).unwrap();
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
