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
use crate::truncation::{ColumnTruncation, TRUNCATION_EXAMPLE_LIMIT, TruncationSummary};
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
/// Declared 0-based position of the top-level column `name` borrows from: the
/// column itself or one of its repeating-group children. Layout validation
/// reports names borrowed from `columns`, so identity is by address.
fn declared_position(columns: &[Column], name: &str) -> usize {
    let is = |column: &Column| std::ptr::eq(column.name.as_str(), name);
    columns
        .iter()
        .position(|column| {
            is(column) || column.fields.as_deref().unwrap_or_default().iter().any(is)
        })
        .unwrap_or(0)
}
/// Diagnostic identity of one cell. `field` in [`FormatError::OutputEncoding`]
/// is the 1-based position of the column among the record's user fields (the
/// numbering every writer shares and the DLQ indexes by), so a record cell is
/// resolved against its record, and only when an error is actually built.
#[derive(Clone, Copy)]
struct CellAt<'a> {
    /// Name excerpt carried in the diagnostic.
    name: &'a str,
    record: Option<&'a Record>,
    /// Record column the position is resolved from; for a repeating-group
    /// child this is the group.
    column: &'a str,
    /// Declared 0-based position: used when no record carries `column`.
    declared: usize,
    /// 1-based occurrence of a repeating-group child.
    element: Option<std::num::NonZeroUsize>,
}
impl<'a> CellAt<'a> {
    fn fixed(name: &'a str, declared: usize) -> Self {
        Self {
            name,
            record: None,
            column: name,
            declared,
            element: None,
        }
    }
    fn record(record: &'a Record, column: &'a str, declared: usize) -> Self {
        Self {
            record: Some(record),
            ..Self::fixed(column, declared)
        }
    }
    fn child(self, name: &'a str, occurrence: usize) -> Self {
        Self {
            name,
            element: std::num::NonZeroUsize::new(occurrence + 1),
            ..self
        }
    }
    fn error(self, offset: usize, kind: OutputEncodingKind) -> FormatError {
        let field = self
            .record
            .and_then(|record| {
                record
                    .iter_user_fields()
                    .position(|(name, _)| name == self.column)
            })
            .unwrap_or(self.declared);
        FormatError::OutputEncoding {
            format: "fixed-width",
            field: field + 1,
            offset,
            kind,
            field_name: OutputFieldName::new(self.name),
            element: self.element,
        }
    }
}
fn retained_text(value: &str, scope: &WriterScope) -> Result<ReservedText, FormatError> {
    let mut text = ReservedText::new(scope.allocation().clone());
    text.push_str(value)?;
    Ok(text)
}
struct PreparedField {
    name: ReservedText,
    /// Declared position of the top-level column (the group, for a child).
    declared: usize,
    start: usize,
    width: usize,
    justify: Justify,
    pad: u8,
    truncation: TruncationPolicy,
    /// Index of this field's truncation tally; `Some` exactly for `warn`.
    tally: Option<usize>,
    trim: bool,
    read_right: bool,
    read_pad: Option<char>,
}
impl PreparedField {
    /// `slots` counts the tallies assigned so far and advances for a `warn`
    /// field, so every tally index is dense and fixed at construction.
    fn new(
        column: &Column,
        start: usize,
        declared: usize,
        slots: &mut usize,
        scope: &WriterScope,
    ) -> Result<Self, FormatError> {
        let width = field::scalar_width(column, start).map_err(|e| {
            output_error(
                e.name,
                declared,
                start,
                OutputEncodingKind::FixedWidthLayout,
            )
        })?;
        let numeric = matches!(
            column.ty.unwrap_nullable(),
            Type::Int | Type::Float | Type::Decimal | Type::Numeric
        );
        let truncation = column.truncation.clone().unwrap_or(if numeric {
            TruncationPolicy::Error
        } else {
            TruncationPolicy::Warn
        });
        let tally = matches!(truncation, TruncationPolicy::Warn).then(|| {
            *slots += 1;
            *slots - 1
        });
        Ok(Self {
            name: retained_text(&column.name, scope)?,
            declared,
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
            truncation,
            tally,
            trim: column.trim.unwrap_or(true),
            read_right: matches!(column.justify, Some(Justify::Right)),
            read_pad: column.pad.as_deref().unwrap_or(" ").chars().next(),
        })
    }
}
struct PreparedGroup {
    name: ReservedText,
    declared: usize,
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
    fn declared(&self) -> usize {
        match self {
            Self::Scalar(f) => f.declared,
            Self::Group(g) => g.declared,
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
    /// Number of `warn` fields, top-level and group children alike.
    tallies: usize,
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
        field::check_write_layout(fields).map_err(|e| {
            output_error(
                e.name,
                declared_position(fields, e.name),
                0,
                OutputEncodingKind::FixedWidthLayout,
            )
        })?;
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
        let mut tallies = 0;
        for (declared, column) in fields.iter().enumerate() {
            scope.check_cancelled()?;
            let start = column.start.unwrap_or(next_start);
            let layout = if field::is_group(column) {
                let dimensions = field::group_dimensions(column, start).map_err(|e| {
                    output_error(
                        e.name,
                        declared,
                        start,
                        OutputEncodingKind::FixedWidthLayout,
                    )
                })?;
                let children = column.fields.as_deref().unwrap_or_default();
                let mut fields = ReservedVec::new(scope.allocation().clone());
                fields.reserve_exact(children.len())?;
                let mut next_child = 0;
                for child in children {
                    scope.check_cancelled()?;
                    let child = PreparedField::new(
                        child,
                        child.start.unwrap_or(next_child),
                        declared,
                        &mut tallies,
                        &scope,
                    )?;
                    next_child = child.start + child.width;
                    fields.push(child)?;
                }
                fields.as_mut_slice().sort_unstable_by_key(|f| f.start);
                PreparedLayout::Group(PreparedGroup {
                    name: retained_text(&column.name, &scope)?,
                    declared,
                    start,
                    width: dimensions.max_width,
                    occurrence_width: dimensions.occurrence_width,
                    count_width: column.count_field.as_ref().map_or(0, |c| c.width),
                    occurs: column.occurs.clone().ok_or_else(|| {
                        output_error(
                            &column.name,
                            declared,
                            start,
                            OutputEncodingKind::FixedWidthLayout,
                        )
                    })?,
                    fields,
                })
            } else {
                PreparedLayout::Scalar(PreparedField::new(
                    column,
                    start,
                    declared,
                    &mut tallies,
                    &scope,
                )?)
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
                tallies,
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

/// One `warn` field's account over every delivered record.
#[derive(Clone, Copy, Default)]
struct Tally {
    cells: u64,
    longest: usize,
    /// Delivered record numbers (1-based, across documents) of the first
    /// records that truncated here; `listed` of them are set.
    examples: [u64; TRUNCATION_EXAMPLE_LIMIT],
    listed: usize,
    more: bool,
}
/// The record being prepared: what it truncated, per tally, before delivery
/// decides whether it counts.
#[derive(Clone, Copy, Default)]
struct Hit {
    cells: u64,
    longest: usize,
}
/// Per-record staging, reused for every record. `dirty` marks hits left by a
/// record that failed after truncating, so the next prepare clears them.
struct Scratch {
    hits: ReservedVec<Hit>,
    dirty: bool,
}
impl Scratch {
    fn hit(&mut self, tally: usize, original: usize) {
        if let Some(hit) = self.hits.as_mut_slice().get_mut(tally) {
            hit.cells = hit.cells.saturating_add(1);
            hit.longest = hit.longest.max(original);
            self.dirty = true;
        }
    }
    fn clear(&mut self) {
        if self.dirty {
            self.hits.as_mut_slice().fill(Hit::default());
            self.dirty = false;
        }
    }
}
fn filled<T: Copy + Default>(
    len: usize,
    scope: &WriterScope,
) -> Result<ReservedVec<T>, FormatError> {
    let mut values = ReservedVec::new(scope.allocation().clone());
    values.reserve_exact(len)?;
    for _ in 0..len {
        values.push(T::default())?;
    }
    Ok(values)
}
/// Streams complete records/sections into an admitted stage. Only delivered
/// operations change counters or truncation tallies; no record values are
/// retained. Every tally and the per-record staging are sized by the layout
/// when the encoder is built, so recording a truncation never allocates and a
/// `truncation: warn` field never fails a record.
pub struct FixedWidthEncoder {
    config: FixedWidthEncoderConfig,
    state: FixedWidthState,
    /// Body records delivered across every document.
    delivered: u64,
    tallies: ReservedVec<Tally>,
    // `prepare` takes `&self`; the staging it writes is folded in `commit`.
    scratch: std::cell::RefCell<Scratch>,
}
/// Prepared state, applied only once delivery succeeds.
pub struct FixedWidthPending {
    state: FixedWidthState,
    record: bool,
    truncated: bool,
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
    /// Share admitted immutable policy; each writer owns its truncation tallies.
    pub fn from_config(
        config: FixedWidthEncoderConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        let slots = config.0.tallies;
        Ok(Self {
            state: FixedWidthState::default(),
            delivered: 0,
            tallies: filled(slots, &scope)?,
            scratch: std::cell::RefCell::new(Scratch {
                hits: filled(slots, &scope)?,
                dirty: false,
            }),
            config,
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
    /// Values delivered records had cut under `truncation: warn`, in layout
    /// order; a record that failed or was never delivered is not counted.
    /// `None` when nothing was truncated.
    pub fn truncation_summary(&self) -> Option<TruncationSummary> {
        let tallies = self.tallies.as_slice();
        let mut columns = Vec::new();
        let mut account = |field: &PreparedField, group: Option<&str>| {
            let Some(tally) = field.tally.and_then(|index| tallies.get(index)) else {
                return;
            };
            if tally.cells == 0 {
                return;
            }
            columns.push(ColumnTruncation {
                column: match group {
                    Some(group) => format!("{group}.{}", field.name.as_str()),
                    None => field.name.as_str().to_string(),
                },
                width: field.width,
                cells: tally.cells,
                longest_bytes: tally.longest,
                example_records: tally.examples[..tally.listed].to_vec(),
                more_records: tally.more,
            });
        };
        for layout in self.config.0.layouts.as_slice() {
            match layout {
                PreparedLayout::Scalar(field) => account(field, None),
                PreparedLayout::Group(group) => {
                    for field in group.fields.as_slice() {
                        account(field, Some(group.name.as_str()));
                    }
                }
            }
        }
        (!columns.is_empty()).then_some(TruncationSummary { columns })
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
    fn truncation_summary(&self) -> Option<TruncationSummary> {
        self.encoder().truncation_summary()
    }
}

// f64 display fits 327 bytes, decimal fits 32 and calendar types fit 32.
// Strings bypass this fixed scratch and retain their original record owner.
fn physical_scalar<T>(
    value: &Value,
    at: CellAt<'_>,
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
            return Err(at.error(
                0,
                if envelope {
                    OutputEncodingKind::FixedWidthEnvelope
                } else {
                    OutputEncodingKind::FixedWidthScalar
                },
            ));
        }
    };
    result.map_err(|_| at.error(0, OutputEncodingKind::FixedWidthScalar))?;
    let text = std::str::from_utf8(&text.bytes[..text.len])
        .map_err(|_| at.error(0, OutputEncodingKind::FixedWidthScalar))?;
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
fn prepared_cell(
    stage: &mut dyn Write,
    field: &PreparedField,
    value: &Value,
    at: CellAt<'_>,
    scratch: &mut Scratch,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    physical_scalar(value, at, false, |text| {
        if text.len() > field.width {
            match (field.truncation.clone(), field.tally) {
                (TruncationPolicy::Error, _) => {
                    return Err(at.error(field.width, OutputEncodingKind::FixedWidthTruncation));
                }
                (TruncationPolicy::Warn, Some(tally)) => scratch.hit(tally, text.len()),
                (TruncationPolicy::Warn, None) | (TruncationPolicy::Silent, _) => {}
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
    at: CellAt<'_>,
    scope: &WriterScope,
) -> Result<bool, FormatError> {
    if !field.trim || matches!(value, Value::Array(_) | Value::Map(_)) {
        return Ok(false);
    }
    physical_scalar(value, at, false, |text| {
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
    cell: CellAt<'_>,
    scratch: &mut Scratch,
    scope: &WriterScope,
) -> Result<usize, FormatError> {
    let failure = |kind| cell.error(group.start, kind);
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
        physical_scalar(&Value::Integer(count), cell, false, |text| {
            padding(stage, b'0', group.count_width - text.len(), scope)?;
            stage.write_all(text.as_bytes())?;
            Ok(())
        })?;
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
                    cell.child(field.name.as_str(), occurrence),
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
                cell.child(field.name.as_str(), occurrence),
                scratch,
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
        let mut record_op = false;
        let mut scratch = self.scratch.try_borrow_mut().map_err(|_| {
            FormatError::FixedWidth("truncation staging re-entered during prepare".to_string())
        })?;
        scratch.clear();
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
                let mut at = 0;
                let mut shifted = 0;
                for layout in self.config.0.layouts.as_slice() {
                    scope.check_cancelled()?;
                    let start = layout.start() - shifted;
                    padding(stage, b' ', start - at, scope)?;
                    let value = record.get(layout.name()).unwrap_or(&Value::Null);
                    let cell = CellAt::record(record, layout.name(), layout.declared());
                    let width = match layout {
                        PreparedLayout::Scalar(field) => {
                            prepared_cell(stage, field, value, cell, &mut scratch, scope)?;
                            field.width
                        }
                        PreparedLayout::Group(group) => {
                            prepared_group(stage, group, value, cell, &mut scratch, scope)?
                        }
                    };
                    shifted += layout.width() - width;
                    at = start + width;
                }
                separator(stage, &self.config.0.separator)?;
                state.records = state.records.saturating_add(1);
                record_op = true;
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
                            physical_scalar(value, CellAt::fixed(name, index), true, |text| {
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
        Ok(FixedWidthPending {
            state,
            record: record_op,
            truncated: scratch.dirty,
        })
    }
    fn commit(&mut self, pending: Self::Pending) {
        self.state = pending.state;
        if !pending.record {
            return;
        }
        self.delivered = self.delivered.saturating_add(1);
        if !pending.truncated {
            return;
        }
        let scratch = self.scratch.get_mut();
        for (tally, hit) in self
            .tallies
            .as_mut_slice()
            .iter_mut()
            .zip(scratch.hits.as_slice())
        {
            if hit.cells == 0 {
                continue;
            }
            tally.cells = tally.cells.saturating_add(hit.cells);
            tally.longest = tally.longest.max(hit.longest);
            match tally.examples.get_mut(tally.listed) {
                Some(slot) => {
                    *slot = self.delivered;
                    tally.listed += 1;
                }
                None => tally.more = true,
            }
        }
        scratch.clear();
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
        let summary;
        {
            let mut writer =
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("LongName".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            summary = writer.truncation_summary();
        }

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "LongN\n"); // truncated to 5 chars
        assert_eq!(
            summary.unwrap().columns,
            vec![ColumnTruncation {
                column: "name".into(),
                width: 5,
                cells: 1,
                longest_bytes: 8,
                example_records: vec![1],
                more_records: false,
            }]
        );
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
        let summary;
        {
            let mut writer =
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            let rec = make_record(&["name"], vec![Value::String("café".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            summary = writer.truncation_summary().unwrap();
        }

        // Valid UTF-8 of exactly the byte width — the partial `é` is dropped,
        // not split, and the freed byte is space-padded.
        let output = String::from_utf8(buf).expect("output must be valid UTF-8");
        assert_eq!(output, "caf \n");
        // The account measures bytes, not chars: `café` is 5 bytes, 4 chars.
        assert_eq!(summary.columns[0].longest_bytes, 5);
        assert_eq!(summary.columns[0].width, 4);
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
        let summary;
        {
            let mut writer =
                finite_writer(&mut buf, fields, FixedWidthWriterConfig::default()).unwrap();
            // `é` is 2 bytes; width 1 cannot hold it, so the cell is one pad byte.
            let rec = make_record(&["flag"], vec![Value::String("é".into())]);
            writer.write_record(&rec).unwrap();
            writer.flush().unwrap();
            summary = writer.truncation_summary();
        }

        let output = String::from_utf8(buf).expect("output must be valid UTF-8");
        assert_eq!(output, " \n");
        assert_eq!(summary, None, "silent truncation records nothing");
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
