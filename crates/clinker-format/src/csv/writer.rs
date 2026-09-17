use clinker_record::owned_storage::{OwnedKey, SharedStorage};
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Mutex;

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::charset::Charset;

use crate::envelope_writer::OutputEnvelopeSpec;
use crate::error::FormatError;
use crate::error::OutputEncodingKind;
use crate::multi_value::{JoinValues, OnConflict};
use crate::preparation::{FormatEncoder, OutputOperation, WriterResources, WriterScope};
use crate::reserved::{ReservedBuffer, ReservedText, ReservedVec};
use crate::schema::DEFAULT_VALUE_DELIMITER;
use crate::traits::{FormatWriter, FormatWriterHandle};

struct PreparedColumn {
    index: usize,
    multiple: bool,
    join: Option<usize>,
}

fn csv_charset_error(mut error: FormatError) -> FormatError {
    if let FormatError::OutputEncoding { format, .. } = &mut error {
        *format = "CSV";
    }
    error
}
struct PreparedJoin {
    field: ReservedText,
    delimiter: ReservedText,
    escape: ReservedText,
    policy: OnConflict,
}

/// Borrowed constructor input. Runtime factories can borrow their validated
/// policy directly rather than cloning a tree of config strings first.
pub struct CsvEncoderOptions<'a> {
    /// Closed byte repertoire for every header, scalar and composite cell.
    pub charset: Charset,
    pub delimiter: u8,
    pub include_header: bool,
    pub include_engine_stamped: bool,
    pub lossless: bool,
    pub join_values: &'a [JoinValues],
    pub declared_multiple: &'a BTreeSet<String>,
    pub envelope_header: Option<&'a str>,
    pub envelope_footer: Option<&'a str>,
    pub envelope_count: Option<&'a str>,
}
impl<'a> From<&'a CsvWriterConfig> for CsvEncoderOptions<'a> {
    fn from(config: &'a CsvWriterConfig) -> Self {
        Self {
            charset: Charset::Utf8,
            delimiter: config.delimiter,
            include_header: config.include_header,
            include_engine_stamped: config.include_engine_stamped,
            lossless: config.error_on_undeclared_columns,
            join_values: &config.join_values,
            declared_multiple: &config.declared_multiple,
            envelope_header: config
                .envelope
                .as_ref()
                .and_then(|e| e.header_from_doc.as_deref()),
            envelope_footer: config
                .envelope
                .as_ref()
                .and_then(|e| e.footer_from_doc.as_deref()),
            envelope_count: config
                .envelope
                .as_ref()
                .and_then(|e| e.footer_record_count_field.as_deref()),
        }
    }
}

/// Immutable admitted policy shared by every writer from one CSV factory.
#[derive(Clone)]
pub struct CsvEncoderConfig(SharedStorage<PreparedCsvConfig>);
struct PreparedCsvConfig {
    charset: Charset,
    delimiter: u8,
    include_header: bool,
    include_engine_stamped: bool,
    lossless: bool,
    joins: ReservedVec<PreparedJoin>,
    multiple: ReservedVec<ReservedText>,
    envelope: Option<crate::envelope_writer::PreparedEnvelope>,
}
impl CsvEncoderConfig {
    /// Admit policy text and inventories before copying; schema mappings belong
    /// to each encoder while this immutable configuration is shared.
    pub fn new(
        options: CsvEncoderOptions<'_>,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        let mut joins = ReservedVec::new(scope.allocation().clone());
        joins.reserve_exact(options.join_values.len())?;
        for join in options.join_values {
            joins.push(PreparedJoin {
                field: admitted_text(&scope, &join.field)?,
                delimiter: admitted_text(&scope, &join.delimiter)?,
                escape: admitted_text(&scope, &join.escape)?,
                policy: join.on_conflict,
            })?;
        }
        let mut multiple = ReservedVec::new(scope.allocation().clone());
        multiple.reserve_exact(options.declared_multiple.len())?;
        for name in options.declared_multiple {
            multiple.push(admitted_text(&scope, name)?)?;
        }
        let envelope = crate::envelope_writer::PreparedEnvelope::from_names(
            options.envelope_header,
            options.envelope_footer,
            options.envelope_count,
            &scope,
        )?;
        Ok(Self(SharedStorage::try_new(
            PreparedCsvConfig {
                charset: options.charset,
                delimiter: options.delimiter,
                include_header: options.include_header,
                include_engine_stamped: options.include_engine_stamped,
                lossless: options.lossless,
                joins,
                multiple,
                envelope,
            },
            scope.allocation(),
        )?))
    }
}

/// Shared split header. The shared backing is admitted during construction.
/// Header names and slots are admitted while preparing the operation that emits
/// the header, and become visible to later writers only after successful delivery.
#[derive(Clone)]
pub struct CsvHeaderCapture(SharedStorage<CapturedHeader>);
struct CapturedHeader {
    text: Mutex<Option<ReservedVec<ReservedText>>>,
}
impl CsvHeaderCapture {
    pub fn new(resources: &WriterResources) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        let capture = SharedStorage::try_new(
            CapturedHeader {
                text: Mutex::new(None),
            },
            scope.allocation(),
        )?;
        // Some platforms allocate the native mutex on first lock. Establish
        // that fixed control-block storage before any captured operation.
        drop(capture.text.lock().unwrap_or_else(|e| e.into_inner()));
        Ok(Self(capture))
    }
}

/// CSV encoder for sealed operations. Borrows schema/input values and retains
/// only admitted column policy and optional preset header text. Each rendered
/// cell is counted, admitted, written, and released before the next cell.
pub struct CsvEncoder {
    config: CsvEncoderConfig,
    schema: SharedStorage<Schema>,
    columns: ReservedVec<PreparedColumn>,
    header_written: bool,
    preset: Option<ReservedVec<ReservedText>>,
    capture: Option<CsvHeaderCapture>,
    records: u64,
    scope: WriterScope,
}

/// Pending counters and admitted header capture move only after delivery;
/// preparation does not mutate the encoder.
pub struct CsvPending {
    header_written: bool,
    records: u64,
    captured: Option<ReservedVec<ReservedText>>,
}

fn csv_io_error(error: csv::Error) -> FormatError {
    match error.into_kind() {
        csv::ErrorKind::Io(error) => FormatError::Io(error),
        _ => crate::preparation::ResourceError::new(
            crate::preparation::ResourceErrorKind::Authority,
            0,
            0,
        )
        .into(),
    }
}

fn csv_error(field: usize, kind: OutputEncodingKind) -> FormatError {
    FormatError::OutputEncoding {
        format: "CSV",
        field: field + 1,
        offset: 0,
        kind,
        field_name: crate::error::OutputFieldName::new(""),
        element: None,
    }
}

fn admitted_text(scope: &WriterScope, text: &str) -> Result<ReservedText, FormatError> {
    let mut result = ReservedText::new(scope.allocation().clone());
    result.push_str(text)?;
    Ok(result)
}

impl CsvEncoder {
    /// Admit the concrete prepared writer before allocation. The returned
    /// handle keeps that charge until its backing has been deallocated; the
    /// caller continues to own the destination's separate allocation contract.
    pub fn into_boxed_writer<W: Write + Send + 'static>(
        self,
        destination: W,
        resources: WriterResources,
    ) -> Result<FormatWriterHandle, FormatError> {
        let scope = resources.scope()?;
        let writer = crate::preparation::PreparedWriter::new(destination, self, resources)?;
        Ok(FormatWriterHandle::try_new(writer, scope.allocation())?)
    }
    /// Config is borrowed only during construction; all retained variable data
    /// is copied after admission. The caller already owns the compiled schema.
    pub fn new(
        schema: SharedStorage<Schema>,
        config: &CsvWriterConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        Self::from_config(
            schema,
            CsvEncoderConfig::new(config.into(), &resources)?,
            resources,
        )
    }

    /// Build schema mappings under this writer's scope, sharing already-admitted
    /// immutable factory policy without copying its strings.
    pub fn from_config(
        schema: SharedStorage<Schema>,
        config: CsvEncoderConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        let mut columns = ReservedVec::new(scope.allocation().clone());
        let count = schema
            .columns()
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                config.0.include_engine_stamped
                    || schema
                        .field_metadata(*i)
                        .is_none_or(|m| !m.is_engine_stamped())
            })
            .count();
        columns.reserve_exact(count)?;
        for (index, name) in schema.columns().iter().enumerate() {
            if !config.0.include_engine_stamped
                && schema
                    .field_metadata(index)
                    .is_some_and(|m| m.is_engine_stamped())
            {
                continue;
            }
            let join = config
                .0
                .joins
                .as_slice()
                .iter()
                .position(|join| join.field.as_str() == name.as_ref());
            columns.push(PreparedColumn {
                index,
                multiple: config
                    .0
                    .multiple
                    .as_slice()
                    .iter()
                    .any(|field| field.as_str() == name.as_ref()),
                join,
            })?;
        }
        Ok(Self {
            config,
            schema,
            columns,
            header_written: false,
            preset: None,
            capture: None,
            records: 0,
            scope,
        })
    }

    /// Attach the same admitted capture to each split encoder before use.
    pub fn with_header_capture(mut self, capture: CsvHeaderCapture) -> Self {
        self.capture = Some(capture);
        self
    }

    /// Retain an admitted replay header; its bytes join the next record's
    /// preparation, so an invalid first body cannot publish a header prefix.
    pub fn set_preset_header(&mut self, header: &[Box<str>]) -> Result<(), FormatError> {
        let mut pending = ReservedVec::new(self.scope.allocation().clone());
        pending.reserve_exact(header.len())?;
        for text in header {
            pending.push(admitted_text(&self.scope, text)?)?;
        }
        self.preset = Some(pending);
        Ok(())
    }

    fn text_field(
        &self,
        writer: &mut csv::Writer<&mut dyn Write>,
        text: &str,
        column: usize,
        scope: &WriterScope,
    ) -> Result<(), FormatError> {
        if self.config.0.charset == Charset::Utf8 {
            writer.write_field(text.as_bytes()).map_err(csv_io_error)?;
            return Ok(());
        }
        // A cell is indivisible to the CSV quoting API. Count and validate
        // before admitting its exact encoded layout; no historical capacity
        // survives this field. Both passes observe cancellation in the sink.
        let mut count = CellCount {
            bytes: 0,
            failure: None,
            scope,
        };
        let counted = self
            .config
            .0
            .charset
            .encode_to(text, column + 1, &mut count);
        if let Some(error) = count.failure {
            return Err(error.into());
        }
        counted.map_err(csv_charset_error)?;
        let mut bytes = ReservedBuffer::new(scope.allocation().clone());
        bytes.reserve_exact(count.bytes)?;
        let mut sink = CellBuffer {
            bytes: &mut bytes,
            failure: None,
            scope,
        };
        let encoded = self.config.0.charset.encode_to(text, column + 1, &mut sink);
        if let Some(error) = sink.failure {
            return Err(error.into());
        }
        encoded.map_err(csv_charset_error)?;
        writer.write_field(bytes.as_slice()).map_err(csv_io_error)?;
        Ok(())
    }

    fn cell(
        &self,
        writer: &mut csv::Writer<&mut dyn Write>,
        value: &Value,
        column: usize,
        join: Option<&PreparedJoin>,
        multiple: bool,
        scope: &WriterScope,
    ) -> Result<(), FormatError> {
        if let Value::String(text) = value {
            return self.text_field(writer, text, column, scope);
        }
        if !matches!(value, Value::Array(_) | Value::Map(_)) {
            return scalar_text(value, column, |text| {
                self.text_field(writer, text, column, scope)
            });
        }
        if matches!(value, Value::Map(_)) {
            return Err(csv_error(column, OutputEncodingKind::Map));
        }
        if !multiple {
            return Err(csv_error(column, OutputEncodingKind::Array));
        }
        let _json_error = if join.is_some_and(|j| j.policy == OnConflict::EncodeJson) {
            validate_csv_json(value, column, scope, 0)?;
            // serde_json 1.0.149 ErrorImpl owns ErrorCode + two usize positions.
            // ErrorCode has unit variants, Box<str>, or io::Error. This layout
            // includes a usize discriminant AND both payloads, with their
            // alignment/padding, so it conservatively covers the sole box.
            // Validated finite values and decoded keys cannot take ValueSer's
            // custom-message path. Only allocation-free sink I/O errors remain;
            // render_csv_array drops the library error before this grant.
            Some(scope.reserve(std::alloc::Layout::new::<(
                usize,
                Box<str>,
                std::io::Error,
                usize,
                usize,
            )>())?)
        } else {
            None
        };
        let mut count = CellCount {
            bytes: 0,
            failure: None,
            scope,
        };
        let counted = render_csv_array(&mut count, value, column, join);
        if let Some(error) = count.failure {
            return Err(error.into());
        }
        counted?;
        let mut bytes = ReservedBuffer::new(scope.allocation().clone());
        bytes.reserve_exact(count.bytes)?;
        let mut sink = CellBuffer {
            bytes: &mut bytes,
            failure: None,
            scope,
        };
        let rendered = render_csv_array(&mut sink, value, column, join);
        if let Some(error) = sink.failure {
            return Err(error.into());
        }
        rendered?;
        // The shared renderer emits UTF-8. Keep that admitted buffer live
        // while the exact encoded cell is admitted, accounting their overlap.
        let text = std::str::from_utf8(bytes.as_slice())
            .map_err(|_| csv_error(column, OutputEncodingKind::Charset))?;
        self.text_field(writer, text, column, scope)
    }

    fn section(
        &self,
        writer: &mut csv::Writer<&mut dyn Write>,
        fields: &indexmap::IndexMap<OwnedKey, Value>,
        count: Option<i64>,
        scope: &WriterScope,
    ) -> Result<(), FormatError> {
        for (index, (name, value)) in fields.iter().enumerate() {
            self.cell(writer, value, index, None, false, scope)
                .map_err(|mut error| {
                    if let FormatError::OutputEncoding { field_name, .. } = &mut error {
                        *field_name = crate::error::OutputFieldName::new(name.as_str());
                    }
                    error
                })?;
        }
        if let Some(count) = count {
            self.cell(
                writer,
                &Value::Integer(count),
                fields.len(),
                None,
                false,
                scope,
            )?;
        }
        writer
            .write_record(std::iter::empty::<&[u8]>())
            .map_err(csv_io_error)?;
        Ok(())
    }
}

// The CSV library flushes buffered bytes from Drop. Once preparation has an
// outcome, that destructor must not perform new stage work or replace the
// original error with a later resource failure.
struct PreparationSink<'a> {
    stage: &'a mut dyn Write,
    active: &'a std::cell::Cell<bool>,
}
impl Write for PreparationSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.active.get() {
            self.stage.write(bytes)
        } else {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.active.get() {
            self.stage.flush()
        } else {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }
}

impl FormatEncoder for CsvEncoder {
    type Pending = CsvPending;
    fn prepare(
        &self,
        operation: OutputOperation<'_>,
        stage: &mut dyn Write,
        scope: &WriterScope,
    ) -> Result<CsvPending, FormatError> {
        // csv 1.4 owns exactly this fixed Vec<u8>; its core state is inline.
        // The grant outlives the library writer, including its drop-time flush.
        let _buffer = scope.reserve(std::alloc::Layout::array::<u8>(8192).map_err(|_| {
            crate::preparation::ResourceError::new(
                crate::preparation::ResourceErrorKind::Layout,
                8192,
                0,
            )
        })?)?;
        // csv 1.4 boxes ErrorKind on a failed write. Unwrap each error while
        // this exact allocation grant is live, including drop-time flushing.
        let _error = scope.reserve(std::alloc::Layout::new::<csv::ErrorKind>())?;
        let active = std::cell::Cell::new(true);
        let mut sink = PreparationSink {
            stage,
            active: &active,
        };
        let mut writer = csv::WriterBuilder::new()
            .delimiter(self.config.0.delimiter)
            .buffer_capacity(8192)
            .flexible(true)
            .from_writer(&mut sink as &mut dyn Write);
        let result = (|| {
            let mut pending = CsvPending {
                header_written: self.header_written,
                records: self.records,
                captured: None,
            };
            match operation {
                OutputOperation::Record(record) => {
                    let capture = self
                        .capture
                        .as_ref()
                        .map(|capture| capture.0.text.lock().unwrap_or_else(|e| e.into_inner()));
                    if self.config.0.lossless {
                        for (index, (name, _)) in record.iter_user_fields().enumerate() {
                            if !self.schema.contains(name) {
                                return Err(FormatError::OutputEncoding {
                                    format: "CSV",
                                    field: index + 1,
                                    offset: 0,
                                    kind: OutputEncodingKind::SchemaDrift,
                                    field_name: crate::error::OutputFieldName::new(name),
                                    element: None,
                                });
                            }
                        }
                    }
                    let replay = capture.as_ref().and_then(|header| header.as_ref());
                    if !self.header_written
                        && self.config.0.envelope.is_none()
                        && (self.config.0.include_header
                            || self.preset.is_some()
                            || replay.is_some())
                    {
                        // Capture exactly the names this operation emits. A preset
                        // replaces schema names; suppressed headers publish nothing.
                        // Keep all copies pending until the complete body delivers.
                        let mut captured = capture
                            .as_ref()
                            .is_some_and(|header| header.is_none())
                            .then(|| ReservedVec::new(scope.allocation().clone()));
                        if let Some(header) = &mut captured {
                            header.reserve_exact(self.columns.len())?;
                        }
                        if let Some(preset) = self.preset.as_ref().or(replay) {
                            if preset.len() != self.columns.len() {
                                return Err(csv_error(
                                    self.columns.len(),
                                    OutputEncodingKind::SchemaDrift,
                                ));
                            }
                            for (index, text) in preset.as_slice().iter().enumerate() {
                                if let Some(header) = &mut captured {
                                    header.push(admitted_text(scope, text.as_str())?)?;
                                }
                                self.text_field(&mut writer, text.as_str(), index, scope)?;
                            }
                        } else {
                            for column in self.columns.as_slice() {
                                if let Some(header) = &mut captured {
                                    header.push(admitted_text(
                                        scope,
                                        self.schema.columns()[column.index].as_ref(),
                                    )?)?;
                                }
                                self.text_field(
                                    &mut writer,
                                    self.schema.columns()[column.index].as_ref(),
                                    column.index,
                                    scope,
                                )?;
                            }
                        }
                        writer
                            .write_record(std::iter::empty::<&[u8]>())
                            .map_err(csv_io_error)?;
                        pending.header_written = true;
                        pending.captured = captured;
                    }
                    for column in self.columns.as_slice() {
                        scope.check_cancelled()?;
                        let value = record
                            .get(self.schema.columns()[column.index].as_ref())
                            .unwrap_or(&Value::Null);
                        self.cell(
                            &mut writer,
                            value,
                            column.index,
                            column
                                .join
                                .map(|index| &self.config.0.joins.as_slice()[index]),
                            column.multiple,
                            scope,
                        )
                        .map_err(|mut error| {
                            if let FormatError::OutputEncoding {
                                field, field_name, ..
                            } = &mut error
                            {
                                let name = self.schema.columns()[column.index].as_ref();
                                // Resolve against the caller's projected record,
                                // whose order can differ from this pinned schema.
                                *field = record
                                    .schema()
                                    .columns()
                                    .iter()
                                    .position(|column| column.as_ref() == name)
                                    .map_or(column.index + 1, |index| index + 1);
                                *field_name = crate::error::OutputFieldName::new(name);
                            }
                            error
                        })?;
                    }
                    writer
                        .write_record(std::iter::empty::<&[u8]>())
                        .map_err(csv_io_error)?;
                    pending.records = self.records.saturating_add(1);
                }
                OutputOperation::BeginDocument(doc) => {
                    if let Some(envelope) = &self.config.0.envelope {
                        if let Some(fields) = envelope.header_fields(doc) {
                            self.section(&mut writer, fields, None, scope)?;
                        }
                        pending.records = 0;
                    }
                }
                OutputOperation::EndDocument(doc) => {
                    if let Some(envelope) = &self.config.0.envelope
                        && let Some(fields) = envelope.footer_fields(doc)
                    {
                        self.section(
                            &mut writer,
                            fields,
                            envelope.has_count().then_some(self.records as i64),
                            scope,
                        )?;
                    }
                }
                OutputOperation::Finalize => {}
            }
            writer.flush()?;
            Ok(pending)
        })();
        active.set(false);
        drop(writer);
        result
    }
    fn commit(&mut self, pending: CsvPending) {
        self.header_written = pending.header_written;
        self.records = pending.records;
        if let (Some(capture), Some(header)) = (&self.capture, pending.captured) {
            let mut stored = capture.0.text.lock().unwrap_or_else(|e| e.into_inner());
            if stored.is_none() {
                *stored = Some(header);
            }
        }
    }
}

impl<W: Write + Send> FormatWriter for crate::preparation::PreparedWriter<W, CsvEncoder> {
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
        crate::preparation::PreparedWriter::flush(self)
    }
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        crate::preparation::PreparedWriter::flush_bytes(self)
    }
}

// Scalar text is bounded by the existing numeric/date types. Strings bypass it.
/// Borrow exact scalar CSV text for a caller-owned operation. Numeric workspace
/// is fixed on the stack; any allocation in `action` belongs to its caller.
pub fn scalar_text<T>(
    value: &Value,
    column: usize,
    action: impl FnOnce(&str) -> Result<T, FormatError>,
) -> Result<T, FormatError> {
    use std::fmt::Write as _;
    struct Text {
        bytes: [u8; 1024],
        len: usize,
    }
    impl std::fmt::Write for Text {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let end = self
                .len
                .checked_add(text.len())
                .filter(|end| *end <= self.bytes.len())
                .ok_or(std::fmt::Error)?;
            self.bytes[self.len..end].copy_from_slice(text.as_bytes());
            self.len = end;
            Ok(())
        }
    }
    let mut text = Text {
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
        Value::Date(v) => v.format("%Y-%m-%d").write_to(&mut text),
        Value::DateTime(v) => v.format("%Y-%m-%dT%H:%M:%S%.f").write_to(&mut text),
        Value::Array(_) => return Err(csv_error(column, OutputEncodingKind::Array)),
        Value::Map(_) => return Err(csv_error(column, OutputEncodingKind::Map)),
    };
    result.map_err(|_| {
        crate::preparation::ResourceError::new(
            crate::preparation::ResourceErrorKind::Layout,
            1024,
            0,
        )
    })?;
    action(
        std::str::from_utf8(&text.bytes[..text.len])
            .map_err(|_| csv_error(column, OutputEncodingKind::Charset))?,
    )
}

// Preserve typed resource evidence outside the library's admitted error box.
// Return immediately on the failed chunk so cancellation never drains a value.
struct CellCount<'a> {
    bytes: usize,
    failure: Option<crate::preparation::ResourceError>,
    scope: &'a WriterScope,
}
impl Write for CellCount<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = self.scope.check_cancelled().and_then(|()| {
            self.bytes.checked_add(bytes.len()).ok_or_else(|| {
                crate::preparation::ResourceError::new(
                    crate::preparation::ResourceErrorKind::Layout,
                    usize::MAX,
                    0,
                )
            })
        });
        self.bytes = result.map_err(|error| {
            self.failure = Some(error);
            std::io::Error::from(std::io::ErrorKind::Other)
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct CellBuffer<'a> {
    bytes: &'a mut ReservedBuffer,
    failure: Option<crate::preparation::ResourceError>,
    scope: &'a WriterScope,
}
impl Write for CellBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for chunk in bytes.chunks(8192) {
            self.scope
                .check_cancelled()
                .and_then(|()| self.bytes.extend_from_slice(chunk))
                .map_err(|error| {
                    self.failure = Some(error);
                    std::io::Error::from(std::io::ErrorKind::Other)
                })?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn render_csv_array(
    out: &mut dyn Write,
    value: &Value,
    column: usize,
    join: Option<&PreparedJoin>,
) -> Result<(), FormatError> {
    let Value::Array(items) = value else {
        return Err(csv_error(column, OutputEncodingKind::Array));
    };
    let policy = join.map_or(OnConflict::Error, |j| j.policy);
    if policy == OnConflict::EncodeJson {
        return serde_json::to_writer(out, &crate::json::writer::ValueSer(value))
            .map_err(|_| csv_error(column, OutputEncodingKind::Json));
    }
    let delimiter = join.map_or(DEFAULT_VALUE_DELIMITER, |j| j.delimiter.as_str());
    let escape = join.map_or("\\", |j| j.escape.as_str());
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            out.write_all(delimiter.as_bytes())?;
        }
        scalar_text(item, column, |text| {
            if policy == OnConflict::Error && text.contains(delimiter) {
                let mut error = csv_error(column, OutputEncodingKind::JoinCollision);
                if let FormatError::OutputEncoding {
                    element, offset, ..
                } = &mut error
                {
                    *element = std::num::NonZeroUsize::new(index + 1);
                    *offset = text.find(delimiter).unwrap_or(0);
                }
                return Err(error);
            }
            if policy == OnConflict::Escape
                && let (Some(esc), Some(delim)) = (escape.chars().next(), delimiter.chars().next())
            {
                for ch in text.chars() {
                    let mut bytes = [0; 4];
                    if ch == esc || ch == delim {
                        out.write_all(esc.encode_utf8(&mut bytes).as_bytes())?;
                    }
                    out.write_all(ch.encode_utf8(&mut bytes).as_bytes())?;
                }
                return Ok(());
            }
            out.write_all(text.as_bytes())?;
            Ok(())
        })?;
    }
    Ok(())
}

fn validate_csv_json(
    value: &Value,
    field: usize,
    scope: &WriterScope,
    depth: usize,
) -> Result<(), FormatError> {
    use clinker_record::nested_key::{MAX_NESTED_VALUE_DEPTH, NestedKey};
    scope.check_cancelled()?;
    match value {
        Value::Float(n) if !n.is_finite() => {
            return Err(csv_error(field, OutputEncodingKind::Json));
        }
        Value::Array(values) => {
            if depth >= MAX_NESTED_VALUE_DEPTH {
                return Err(csv_error(field, OutputEncodingKind::Json));
            }
            for value in values {
                validate_csv_json(value, field, scope, depth + 1)?;
            }
        }
        Value::Map(values) => {
            if depth >= MAX_NESTED_VALUE_DEPTH {
                return Err(csv_error(field, OutputEncodingKind::Json));
            }
            for (position, (key, value)) in values.iter().enumerate() {
                // Valid keys borrow. A malformed key's existing decoder owns a
                // copy only on error; admit that exact diagnostic allocation.
                let _diagnostic =
                    scope.reserve(std::alloc::Layout::array::<u8>(key.len()).map_err(|_| {
                        crate::preparation::ResourceError::new(
                            crate::preparation::ResourceErrorKind::Layout,
                            key.len(),
                            0,
                        )
                    })?)?;
                let decoded = NestedKey::decode(key)
                    .map_err(|_| csv_error(field, OutputEncodingKind::Json))?;
                for prior in values.keys().take(position) {
                    scope.check_cancelled()?;
                    let prior = NestedKey::decode(prior)
                        .map_err(|_| csv_error(field, OutputEncodingKind::Json))?;
                    if prior.text == decoded.text {
                        return Err(csv_error(field, OutputEncodingKind::Json));
                    }
                }
                drop(_diagnostic);
                validate_csv_json(value, field, scope, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Configuration for the CSV writer.
#[derive(Clone)]
pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub include_header: bool,
    /// Whether engine-stamped schema columns (today: `$ck.<field>`
    /// correlation snapshots) are emitted into the CSV. Defaults to
    /// `false` — engine-internal namespaces are stripped from the
    /// default output unless the Sink node opts in via
    /// `include_correlation_keys: true`.
    pub include_engine_stamped: bool,
    /// Per-document envelope reconstruction. `None` (the default) renders no
    /// framing and keeps the output byte-identical to the boundary-unaware
    /// path. `Some` is set by the executor only when the Output declares
    /// `reconstruct_envelope: true` with a non-empty envelope config.
    pub envelope: Option<OutputEnvelopeSpec>,
    /// Raise [`FormatError::SchemaDrift`] when a record carries a user column
    /// the pinned schema does not name, instead of silently skipping it.
    ///
    /// Set by the executor to the Output's `include_unmapped`: when the user
    /// asked to carry every column through (`include_unmapped: true`), a
    /// record column with no header slot is a data-loss drift that must fail
    /// loudly. The buffered Output arm pre-widens the header to the batch
    /// union, so this guard is a no-op there; it earns its keep on the
    /// bounded-memory paths that pin the header to the first record (the
    /// streaming fused arm and envelope framing), where a union is impossible.
    ///
    /// Left `false` (the default) the writer keeps the "output schema is the
    /// column contract — extra record fields are not written" behavior, which
    /// `include_unmapped: false` relies on to narrow output deliberately.
    pub error_on_undeclared_columns: bool,
    /// Per-column overrides for how a `multiple:` field is joined into one
    /// delimited cell. A `Value::Array` at any column joins with the default
    /// delimiter `;` and `on_conflict: error` unless an entry here names the
    /// column, mirroring the source-side `split_values`. Empty by default;
    /// populated from the output's `join_values`.
    pub join_values: Vec<JoinValues>,
    /// Exact output-facing column names whose schema declares
    /// `multiple: true`. Arrays are encoded only in these columns; an array
    /// reaching any other column is a routing/type-contract violation and is
    /// rejected before the row is written.
    pub declared_multiple: BTreeSet<String>,
}

impl Default for CsvWriterConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            include_header: true,
            include_engine_stamped: false,
            envelope: None,
            error_on_undeclared_columns: false,
            join_values: Vec::new(),
            declared_multiple: BTreeSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::reader::{CsvReader, CsvReaderConfig};
    use crate::traits::FormatReader;
    use clinker_record::owned_storage::{OwnedMap, OwnedValues};
    use std::sync::Arc;

    #[test]
    fn csv_json_count_cancellation_stops_serialization_early() {
        use crate::preparation::{
            AllocationAuthority, AllocationLease, OperationStage, OwnerId, ResourceAuthority,
            ResourceError, ResourceErrorKind,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CancelAfter(AtomicUsize);
        impl AllocationAuthority for CancelAfter {
            fn try_reserve(
                self: Arc<Self>,
                _: OwnerId,
                _: std::alloc::Layout,
            ) -> Result<AllocationLease, ResourceError> {
                Err(ResourceError::new(ResourceErrorKind::Budget, 1, 0))
            }
            fn release(&self, _: OwnerId, _: usize) {}
            fn check_cancelled(&self) -> Result<(), ResourceError> {
                if self.0.fetch_add(1, Ordering::Relaxed) >= 4 {
                    Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
                } else {
                    Ok(())
                }
            }
        }
        impl ResourceAuthority for CancelAfter {
            fn create_stage(
                self: Arc<Self>,
                _: WriterScope,
            ) -> Result<OperationStage, FormatError> {
                Err(ResourceError::new(ResourceErrorKind::Budget, 1, 0).into())
            }
        }
        let authority = Arc::new(CancelAfter(AtomicUsize::new(0)));
        let scope = WriterResources::new(authority.clone()).scope().unwrap();
        authority.0.store(0, Ordering::Relaxed);
        let value = Value::Array(OwnedValues::from_vec(vec![Value::Integer(7); 10_000]));
        let mut sink = CellCount {
            bytes: 0,
            failure: None,
            scope: &scope,
        };
        assert!(serde_json::to_writer(&mut sink, &crate::json::writer::ValueSer(&value)).is_err());
        assert_eq!(sink.failure.unwrap().kind, ResourceErrorKind::Cancelled);
        assert_eq!(
            authority.0.load(Ordering::Relaxed),
            5,
            "stop at the first failed chunk, without traversing the remaining array"
        );
        assert!(sink.bytes < 32);
    }

    fn make_schema(cols: &[&str]) -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(
            cols.iter().map(|c| (*c).into()).collect(),
        )))
    }

    fn make_record(schema: &SharedStorage<Schema>, values: Vec<Value>) -> Record {
        Record::new(schema.clone(), values)
    }

    fn prepared_writer<W: Write>(
        destination: W,
        schema: SharedStorage<Schema>,
        config: CsvWriterConfig,
    ) -> crate::preparation::PreparedWriter<W, CsvEncoder> {
        let provider = crate::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
        );
        let encoder = CsvEncoder::new(schema, &config, provider.resources()).unwrap();
        crate::preparation::PreparedWriter::new(destination, encoder, provider.resources()).unwrap()
    }

    fn write_to_string(
        schema: &SharedStorage<Schema>,
        config: CsvWriterConfig,
        records: &[Record],
    ) -> String {
        let mut buf = Vec::new();
        {
            let mut writer = prepared_writer(&mut buf, schema.clone(), config);
            for r in records {
                writer.write_record(r).unwrap();
            }
            writer.flush().unwrap();
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_csv_writer_basic_output() {
        let schema = make_schema(&["name", "age"]);
        let records = vec![
            make_record(
                &schema,
                vec![Value::String("Alice".into()), Value::String("30".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Bob".into()), Value::String("25".into())],
            ),
            make_record(
                &schema,
                vec![Value::String("Charlie".into()), Value::String("35".into())],
            ),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "name,age\nAlice,30\nBob,25\nCharlie,35\n");
    }

    #[test]
    fn test_csv_writer_with_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: true,
                ..Default::default()
            },
            &records,
        );
        assert!(output.starts_with("x,y\n"));
    }

    #[test]
    fn test_csv_writer_no_header() {
        let schema = make_schema(&["x", "y"]);
        let records = vec![make_record(
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        )];
        let output = write_to_string(
            &schema,
            CsvWriterConfig {
                include_header: false,
                ..Default::default()
            },
            &records,
        );
        assert_eq!(output, "1,2\n");
    }

    #[test]
    fn test_csv_writer_null_as_empty() {
        let schema = make_schema(&["a", "b", "c"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("x".into()),
                Value::Null,
                Value::String("z".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // Null becomes empty string between delimiters
        assert_eq!(output, "a,b,c\nx,,z\n");
    }

    /// Sub-second datetimes must carry their fractional part into CSV text. The
    /// prior whole-second `%Y-%m-%dT%H:%M:%S` rendering silently dropped
    /// millisecond/microsecond/nanosecond precision (#883). `%.f` emits nothing
    /// for a whole-second value (nanos=0 stays byte-identical) and otherwise
    /// trims to a 3/6/9-digit group, so each precision tier renders exactly.
    #[test]
    fn test_csv_writer_datetime_subsecond_preserved() {
        use chrono::NaiveDate;
        let schema = make_schema(&["ts"]);
        let base = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let cases = [
            (0, "2024-01-15T10:30:00"),
            (123_000_000, "2024-01-15T10:30:00.123"),
            (123_456_000, "2024-01-15T10:30:00.123456"),
            (123_456_789, "2024-01-15T10:30:00.123456789"),
            (1, "2024-01-15T10:30:00.000000001"),
        ];
        for (nanos, expected) in cases {
            let dt = base.and_hms_nano_opt(10, 30, 0, nanos).unwrap();
            let record = make_record(&schema, vec![Value::DateTime(dt)]);
            let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
            assert_eq!(output, format!("ts\n{expected}\n"), "nanos={nanos}");
        }
    }

    /// The emitted sub-second text is round-trippable: reading the CSV cell back
    /// out and parsing it with a fractional-second-aware pattern reconstructs
    /// the exact `NaiveDateTime`, nanoseconds intact.
    #[test]
    fn test_csv_writer_datetime_subsecond_roundtrips() {
        use chrono::{NaiveDate, NaiveDateTime};
        let schema = make_schema(&["ts"]);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_nano_opt(10, 30, 0, 123_456_789)
            .unwrap();
        let record = make_record(&schema, vec![Value::DateTime(dt)]);
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);

        // Read the single data cell back out through the CSV reader (which
        // yields the raw text as a string — no typed coercion on this path).
        let mut reader = CsvReader::from_reader(output.as_bytes(), CsvReaderConfig::default());
        let _ = reader.schema().unwrap();
        let row = reader.next_record().unwrap().unwrap();
        let cell = match row.get("ts").unwrap() {
            Value::String(s) => s.as_str().to_owned(),
            other => panic!("expected string cell, got {other:?}"),
        };

        let parsed = NaiveDateTime::parse_from_str(&cell, "%Y-%m-%dT%H:%M:%S%.f").unwrap();
        assert_eq!(parsed, dt);
    }

    fn make_schema_with_engine_stamp(user_col: &str, stamp_col: &str) -> SharedStorage<Schema> {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        SchemaBuilder::new()
            .with_field(user_col)
            .with_field_meta(stamp_col, FieldMetadata::source_correlation(user_col))
            .build()
    }

    #[test]
    fn test_csv_writer_strips_engine_stamped_by_default() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n7\n");
        assert!(!output.contains("$ck.id"));
    }

    #[test]
    fn test_csv_writer_includes_engine_stamped_on_opt_in() {
        let schema = make_schema_with_engine_stamp("id", "$ck.id");
        let record = make_record(&schema, vec![Value::Integer(7), Value::Integer(7)]);
        let config = CsvWriterConfig {
            include_engine_stamped: true,
            ..Default::default()
        };
        let output = write_to_string(&schema, config, &[record]);
        assert_eq!(output, "id,$ck.id\n7,7\n");
    }

    #[test]
    fn test_csv_writer_widened_schema_emit_order() {
        // Widened schema controls output order; the fixture declares
        // every emitted column up front, so record.set always lands at
        // a known slot and the writer walks them in schema order.
        let schema = make_schema(&["id", "zulu", "alpha", "mike"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::String("z".into()),
                Value::String("a".into()),
                Value::String("m".into()),
            ],
        );
        let output = write_to_string(&schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id,zulu,alpha,mike\n1,z,a,m\n");
    }

    /// Body cells follow the writer's pinned schema order, so a record
    /// whose own schema orders the same fields differently still lands
    /// each value under its header column.
    #[test]
    fn test_csv_writer_record_schema_order_differs_from_writer_schema() {
        let writer_schema = make_schema(&["a", "b"]);
        let record_schema = make_schema(&["b", "a"]);
        let record = make_record(
            &record_schema,
            vec![Value::String("B".into()), Value::String("A".into())],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }

    /// A writer-schema column absent from the record emits an empty
    /// cell, keeping later columns aligned with the header.
    #[test]
    fn test_csv_writer_missing_field_emits_empty_cell() {
        let writer_schema = make_schema(&["a", "b", "c"]);
        let record_schema = make_schema(&["c", "a"]);
        let record = make_record(
            &record_schema,
            vec![Value::String("C".into()), Value::String("A".into())],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b,c\nA,,C\n");
    }

    /// Engine-stamped stripping is keyed off the writer's pinned
    /// schema — the same schema the header filter consults — so a
    /// record schema lacking the stamp metadata cannot smuggle the
    /// column's value into the body row.
    #[test]
    fn test_csv_writer_engine_stamp_filter_uses_writer_schema() {
        let writer_schema = make_schema_with_engine_stamp("id", "$ck.id");
        // Same columns, but this record's schema carries no metadata.
        let record_schema = make_schema(&["id", "$ck.id"]);
        let record = make_record(&record_schema, vec![Value::Integer(7), Value::Integer(7)]);
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "id\n7\n");
    }

    /// Record fields the writer schema does not declare are not
    /// emitted — the pinned schema is the output contract.
    #[test]
    fn test_csv_writer_ignores_fields_outside_writer_schema() {
        let writer_schema = make_schema(&["a", "b"]);
        let record_schema = make_schema(&["a", "extra", "b"]);
        let record = make_record(
            &record_schema,
            vec![
                Value::String("A".into()),
                Value::String("X".into()),
                Value::String("B".into()),
            ],
        );
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }

    /// The reusable row buffer must be cleared per cell, so a short value
    /// following a long value in the same column carries no leftover bytes —
    /// the canonical buffer-reuse bug.
    #[test]
    fn test_csv_writer_row_buffer_reuse_no_stale_bytes() {
        let schema = make_schema(&["v", "w"]);
        let records = vec![
            make_record(
                &schema,
                vec![
                    Value::String("a-very-long-value-here".into()),
                    Value::String("xxxxxxxx".into()),
                ],
            ),
            make_record(
                &schema,
                vec![Value::String("hi".into()), Value::String("y".into())],
            ),
        ];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        assert_eq!(output, "v,w\na-very-long-value-here,xxxxxxxx\nhi,y\n");
    }

    #[test]
    fn test_csv_writer_quoting_special_chars() {
        let schema = make_schema(&["name", "bio"]);
        let records = vec![make_record(
            &schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Likes commas, and\nnewlines".into()),
            ],
        )];
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);
        // csv crate should quote the field containing comma and newline
        assert!(output.contains("\"Likes commas, and\nnewlines\""));
    }

    #[test]
    fn test_csv_roundtrip_lossless() {
        let input = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";

        // Read
        let mut reader = CsvReader::from_reader(input.as_bytes(), CsvReaderConfig::default());
        let schema = reader.schema().unwrap();
        let mut records = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            records.push(r);
        }
        assert_eq!(records.len(), 3);

        // Write
        let output = write_to_string(&schema, CsvWriterConfig::default(), &records);

        // Read again
        let mut reader2 = CsvReader::from_reader(output.as_bytes(), CsvReaderConfig::default());
        let schema2 = reader2.schema().unwrap();
        let mut records2 = Vec::new();
        while let Some(r) = reader2.next_record().unwrap() {
            records2.push(r);
        }

        // Schemas match
        assert_eq!(schema.columns(), schema2.columns());

        // Records match field by field
        assert_eq!(records.len(), records2.len());
        for (r1, r2) in records.iter().zip(records2.iter()) {
            for col in schema.columns() {
                assert_eq!(r1.get(col), r2.get(col), "mismatch on column {col}");
            }
        }
    }

    /// CSV writer rejects `Value::Map` payloads with
    /// `OutputEncodingKind::Map`. Preparing the record catches a misroute
    /// (e.g. a `$widened` sidecar reaching the writer without
    /// `include_unmapped: true` expansion) before destination delivery.
    #[test]
    fn test_csv_writer_rejects_map_value() {
        use indexmap::IndexMap;
        let schema = make_schema(&["id", "payload"]);
        let mut sidecar: IndexMap<OwnedKey, Value> = IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        sidecar.insert("b".into(), Value::String("two".into()));
        let record = make_record(
            &schema,
            vec![Value::Integer(7), Value::Map(OwnedMap::from_map(sidecar))],
        );
        let mut buf = Vec::new();
        let mut writer = prepared_writer(&mut buf, schema.clone(), CsvWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::Map,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "payload");
                assert_eq!(field, 2);
                assert_eq!(offset, 0);
                assert_eq!(element, None);
            }
            other => panic!("expected CSV map rejection, got {other:?}"),
        }
    }

    use crate::multi_value::{JoinValues, OnConflict};

    fn declared_config(fields: &[&str]) -> CsvWriterConfig {
        CsvWriterConfig {
            declared_multiple: fields.iter().map(|field| (*field).to_string()).collect(),
            ..Default::default()
        }
    }

    fn join_config(entries: Vec<JoinValues>) -> CsvWriterConfig {
        let declared_multiple = entries.iter().map(|entry| entry.field.clone()).collect();
        CsvWriterConfig {
            join_values: entries,
            declared_multiple,
            ..Default::default()
        }
    }

    /// With no `join_values` entry, a `Value::Array` of scalars joins into one
    /// cell with the default `;` delimiter — AC#1, the write-side inverse of the
    /// reader's default split, requiring no configuration.
    #[test]
    fn join_values_default_semicolon_no_config() {
        let schema = make_schema(&["id", "tags"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(7),
                Value::Array(OwnedValues::from_vec(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                    Value::String("c".into()),
                ])),
            ],
        );
        let output = write_to_string(&schema, declared_config(&["tags"]), &[record]);
        assert_eq!(output, "id,tags\n7,a;b;c\n");
    }

    /// An empty array emits an empty cell; a one-element array emits that value
    /// with no delimiter (AC#5). A second column keeps the empty cell
    /// unambiguous (a lone empty field on a one-column row is CSV-quoted `""`).
    #[test]
    fn join_values_empty_and_single() {
        let schema = make_schema(&["id", "tags"]);
        let empty = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::Array(OwnedValues::from_vec(Vec::new())),
            ],
        );
        let single = make_record(
            &schema,
            vec![
                Value::Integer(2),
                Value::Array(OwnedValues::from_vec(vec![Value::String("solo".into())])),
            ],
        );
        let output = write_to_string(&schema, declared_config(&["tags"]), &[empty, single]);
        assert_eq!(output, "id,tags\n1,\n2,solo\n");
    }

    /// A custom per-column delimiter is honored.
    #[test]
    fn join_values_custom_delimiter() {
        let schema = make_schema(&["codes"]);
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(vec![
                Value::String("x".into()),
                Value::String("y".into()),
            ]))],
        );
        let config = join_config(vec![JoinValues {
            field: "codes".into(),
            delimiter: "|".into(),
            on_conflict: OnConflict::Error,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);
        assert_eq!(output, "codes\nx|y\n");
    }

    /// The central AC#2 test at the writer boundary: a value that contains the
    /// delimiter under `on_conflict: error` refuses the record with a
    /// `OutputEncodingKind::JoinCollision` locating the field and offending value,
    /// and no corrupted body cell is emitted.
    #[test]
    fn join_values_on_conflict_error_names_field_and_value() {
        let schema = make_schema(&["tags"]);
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(vec![
                Value::String("a;b".into()),
                Value::String("c".into()),
            ]))],
        );
        let mut buf = Vec::new();
        let mut writer = prepared_writer(&mut buf, schema.clone(), declared_config(&["tags"]));
        let err = writer.write_record(&record).unwrap_err();
        assert!(err.is_join_collision());
        match err {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::JoinCollision,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "tags");
                assert_eq!(field, 1);
                assert_eq!(offset, 1);
                assert_eq!(element.map(std::num::NonZeroUsize::get), Some(1));
                let Value::Array(values) = &record.values()[field - 1] else {
                    panic!("collision must identify the original array field");
                };
                assert_eq!(
                    values[element.unwrap().get() - 1],
                    Value::String("a;b".into())
                );
            }
            other => panic!("expected CSV join collision, got {other:?}"),
        }
        drop(writer);
        assert!(
            !String::from_utf8_lossy(&buf).contains("a;b"),
            "no corrupted cell must be emitted for the failing record"
        );
    }

    /// `on_conflict: escape` escapes the delimiter (and the escape char itself),
    /// and the CSV reader's matching `split_values` `escape:` recovers the exact
    /// original values — a full round trip (AC#3).
    #[test]
    fn join_values_escape_round_trips() {
        let schema = make_schema(&["tags"]);
        let original = vec![
            Value::String("a;b".into()),
            Value::String(r"c\d".into()),
            Value::String("e".into()),
        ];
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(original.clone()))],
        );
        let config = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::Escape,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);

        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(output.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(
            back.get("tags"),
            Some(&Value::Array(OwnedValues::from_vec(original)))
        );
    }

    /// `on_conflict: encode_json` round-trips exactly, including values carrying
    /// the delimiter, quotes, and newlines (AC#4).
    #[test]
    fn join_values_encode_json_round_trips() {
        let schema = make_schema(&["payload"]);
        let original = vec![
            Value::String("a;b".into()),
            Value::String("c\"d".into()),
            Value::String("e\nf".into()),
        ];
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(original.clone()))],
        );
        let config = join_config(vec![JoinValues {
            field: "payload".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::EncodeJson,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let output = write_to_string(&schema, config, &[record]);

        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "payload".into(),
                delimiter: ";".into(),
                escape: String::new(),
                json: true,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(output.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(
            back.get("payload"),
            Some(&Value::Array(OwnedValues::from_vec(original)))
        );
    }

    /// AC#6 interaction: a value containing BOTH the intra-cell delimiter and the
    /// CSV field delimiter is handled correctly under each policy. Under
    /// `escape` the cell is CSV-quoted (it holds a comma) yet still recovers.
    #[test]
    fn join_values_interaction_with_csv_field_delimiter() {
        let schema = make_schema(&["tags"]);
        let original = vec![Value::String("a,b;c".into()), Value::String("d".into())];
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(original.clone()))],
        );

        // error: the ';' inside "a,b;c" collides.
        let mut buf = Vec::new();
        let mut w = prepared_writer(&mut buf, schema.clone(), declared_config(&["tags"]));
        assert!(w.write_record(&record).unwrap_err().is_join_collision());

        // escape: round-trips through the reader despite the CSV comma-quoting.
        let config = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::Escape,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let out = write_to_string(&schema, config, &[record]);
        let read_config = CsvReaderConfig {
            split_values: vec![crate::multi_value::SplitValues {
                field: "tags".into(),
                delimiter: ";".into(),
                escape: "\\".into(),
                json: false,
            }],
            ..Default::default()
        };
        let mut reader = CsvReader::from_reader(out.as_bytes(), read_config);
        reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(
            back.get("tags"),
            Some(&Value::Array(OwnedValues::from_vec(original)))
        );
    }

    /// A single empty-string value `[""]` is indistinguishable from an empty
    /// field under the delimited policies (both emit an empty cell, which reads
    /// back as zero values); `encode_json` preserves it. This pins the documented
    /// limitation so a future change to the empty-cell contract is deliberate.
    #[test]
    fn join_values_single_empty_string_collapses_under_delimited_but_not_json() {
        let schema = make_schema(&["id", "tags"]);
        let record = make_record(
            &schema,
            vec![
                Value::Integer(1),
                Value::Array(OwnedValues::from_vec(vec![Value::String("".into())])),
            ],
        );
        // Delimited (default error): empty cell, reads back as [] (0 values).
        let delimited = write_to_string(
            &schema,
            declared_config(&["tags"]),
            std::slice::from_ref(&record),
        );
        assert_eq!(delimited, "id,tags\n1,\n");

        // encode_json: [""], distinguishable from an empty field.
        let json_cfg = join_config(vec![JoinValues {
            field: "tags".into(),
            delimiter: ";".into(),
            on_conflict: OnConflict::EncodeJson,
            escape: "\\".into(),
            repeat_as: None,
            wrap_in: None,
        }]);
        let json = write_to_string(&schema, json_cfg, &[record]);
        assert_eq!(json, "id,tags\n1,\"[\"\"\"\"]\"\n");
    }

    /// An envelope `$doc` section value that is an array is rejected loudly (it
    /// is document metadata, not a `multiple:` data column) — it must not be
    /// silently joined the way a record cell is.
    #[test]
    fn envelope_section_cell_rejects_an_array() {
        let schema = make_schema(&["amount"]);
        let doc = doc_with_sections(&[(
            "Foot",
            &[(
                "checksum",
                Value::Array(OwnedValues::from_vec(vec![Value::String("a".into())])),
            )],
        )]);
        let config = CsvWriterConfig {
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: None,
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let mut writer = prepared_writer(Vec::new(), schema, config);
        let err = writer.end_document(&doc).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::Array,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "checksum");
                assert_eq!(field, 1);
                assert_eq!(offset, 0);
                assert_eq!(element, None);
            }
            other => panic!("expected CSV array rejection, got {other:?}"),
        }
        assert!(writer.destination().is_empty());
    }

    /// A `Value::Array` carrying a nested `Array`/`Map` element is still
    /// rejected — a flat cell cannot hold nested structure, so the misroute
    /// detection is not lost when scalar arrays start joining.
    #[test]
    fn join_values_rejects_nested_element() {
        let schema = make_schema(&["tags"]);
        let record = make_record(
            &schema,
            vec![Value::Array(OwnedValues::from_vec(vec![
                Value::String("a".into()),
                Value::Array(OwnedValues::from_vec(vec![Value::String("nested".into())])),
            ]))],
        );
        let mut buf = Vec::new();
        let mut writer = prepared_writer(&mut buf, schema.clone(), CsvWriterConfig::default());
        match writer.write_record(&record).unwrap_err() {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::Array,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "tags");
                assert_eq!(field, 1);
                assert_eq!(offset, 0);
                assert_eq!(element, None);
            }
            other => panic!("expected CSV array rejection for nested element, got {other:?}"),
        }
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    #[test]
    fn csv_envelope_frames_header_body_footer() {
        let schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            include_header: false,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("SUM".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let mut writer = prepared_writer(&mut buf, schema.clone(), config);
            writer.begin_document(&doc).unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(10)]))
                .unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(20)]))
                .unwrap();
            writer.end_document(&doc).unwrap();
            writer.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out, "A\n10\n20\nSUM,2\n", "got: {out}");
    }

    /// A prepared CSV writer with capture must preserve envelope framing:
    /// opening and closing rows surround the body, and the footer counts only
    /// successfully delivered records.
    #[test]
    fn header_capturing_csv_writer_forwards_document_framing() {
        let schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            include_header: false,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("SUM".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let provider = crate::preparation::MemoryOnlyResources::new(
                std::num::NonZeroUsize::new(256 * 1024).unwrap(),
            );
            let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
            let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources())
                .unwrap()
                .with_header_capture(capture);
            let mut writer =
                crate::preparation::PreparedWriter::new(&mut buf, encoder, provider.resources())
                    .unwrap();
            writer.begin_document(&doc).unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(10)]))
                .unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(20)]))
                .unwrap();
            writer.end_document(&doc).unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "A\n10\n20\nSUM,2\n");
    }

    #[test]
    fn header_capture_pending_overlap_and_poison_keep_one_published_header() {
        use crate::preparation::MemoryOnlyResources;
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(256 * 1024).unwrap());
        let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
        let schema = make_schema(&["name"]);
        let record = make_record(&schema, vec![Value::Integer(1)]);
        let mut first = CsvEncoder::new(
            schema.clone(),
            &CsvWriterConfig::default(),
            provider.resources(),
        )
        .unwrap()
        .with_header_capture(capture.clone());
        let mut second = CsvEncoder::new(schema, &CsvWriterConfig::default(), provider.resources())
            .unwrap()
            .with_header_capture(capture.clone());
        let scope = provider.resources().scope().unwrap();
        let baseline = provider.used();
        let pending_first = first
            .prepare(
                OutputOperation::Record(&record),
                &mut std::io::sink(),
                &scope,
            )
            .unwrap();
        let pending_charge = provider.used() - baseline;
        assert!(pending_charge > 0);
        let pending_second = second
            .prepare(
                OutputOperation::Record(&record),
                &mut std::io::sink(),
                &scope,
            )
            .unwrap();
        assert_eq!(provider.used(), baseline + 2 * pending_charge);
        assert!(capture.0.text.lock().unwrap().is_none());
        first.commit(pending_first);
        assert_eq!(
            provider.used(),
            baseline + 2 * pending_charge,
            "published and pending copies both remain owned"
        );
        // Poisoning must preserve the already-published admitted text.
        let poisoned = capture.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.0.text.lock().unwrap();
                panic!("poison capture mutex");
            })
            .join()
            .is_err()
        );
        second.commit(pending_second);
        assert_eq!(
            provider.used(),
            baseline + pending_charge,
            "losing pending copy releases after commit"
        );
        assert_eq!(
            capture
                .0
                .text
                .lock()
                .err()
                .unwrap()
                .into_inner()
                .as_ref()
                .unwrap()
                .as_slice()[0]
                .as_str(),
            "name"
        );
        let mut replay = CsvEncoder::new(
            make_schema(&["other"]),
            &CsvWriterConfig::default(),
            provider.resources(),
        )
        .unwrap()
        .with_header_capture(capture.clone());
        let mut bytes = Vec::new();
        let pending = replay
            .prepare(OutputOperation::Record(&record), &mut bytes, &scope)
            .unwrap();
        replay.commit(pending);
        assert_eq!(bytes, b"name\n\"\"\n");
        drop((first, second, replay, capture, scope));
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn header_capture_failed_delivery_and_replay_leave_committed_state_unchanged() {
        use crate::preparation::{MemoryOnlyResources, PreparedWriter, ResourceErrorKind};
        struct FailAfterPrefix {
            bytes: Vec<u8>,
        }
        impl Write for FailAfterPrefix {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.bytes.is_empty() {
                    self.bytes.extend_from_slice(&bytes[..2]);
                    Ok(2)
                } else {
                    Err(std::io::ErrorKind::BrokenPipe.into())
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                panic!("poisoned writer must not flush")
            }
        }
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(256 * 1024).unwrap());
        let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
        let schema = make_schema(&["name"]);
        let record = make_record(&schema, vec![Value::String("body".into())]);
        for replay in [false, true] {
            if replay {
                let encoder = CsvEncoder::new(
                    schema.clone(),
                    &CsvWriterConfig::default(),
                    provider.resources(),
                )
                .unwrap()
                .with_header_capture(capture.clone());
                let mut writer =
                    PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
                writer.write_record(&record).unwrap();
                assert_eq!(writer.destination(), b"name\nbody\n");
            }
            let encoder = CsvEncoder::new(
                schema.clone(),
                &CsvWriterConfig::default(),
                provider.resources(),
            )
            .unwrap()
            .with_header_capture(capture.clone());
            let mut writer = PreparedWriter::new(
                FailAfterPrefix { bytes: Vec::new() },
                encoder,
                provider.resources(),
            )
            .unwrap();
            let retained = provider.used();
            assert!(writer.write_record(&record).is_err());
            assert_eq!(writer.destination().bytes, b"na");
            assert!(!writer.encoder().header_written);
            assert_eq!(writer.encoder().records, 0);
            assert_eq!(capture.0.text.lock().unwrap().is_some(), replay);
            assert_eq!(provider.used(), retained);
            for error in [
                writer.write_record(&record).unwrap_err(),
                writer.flush_bytes().unwrap_err(),
                writer.flush().unwrap_err(),
            ] {
                assert!(
                    matches!(error, FormatError::Resource(error) if error.kind == ResourceErrorKind::DeliveryPoisoned)
                );
            }
            assert_eq!(writer.destination().bytes, b"na");
        }
        drop(capture);
        assert_eq!(provider.used(), 0);
    }

    #[test]
    fn header_capture_replay_filters_engine_stamps_with_explicit_opt_in() {
        for include in [false, true] {
            let provider = crate::preparation::MemoryOnlyResources::new(
                std::num::NonZeroUsize::new(256 * 1024).unwrap(),
            );
            let capture = CsvHeaderCapture::new(&provider.resources()).unwrap();
            let schema = make_schema_with_engine_stamp("id", "$ck.id");
            let config = CsvWriterConfig {
                include_engine_stamped: include,
                ..Default::default()
            };
            for _ in 0..2 {
                let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources())
                    .unwrap()
                    .with_header_capture(capture.clone());
                let mut writer = crate::preparation::PreparedWriter::new(
                    Vec::new(),
                    encoder,
                    provider.resources(),
                )
                .unwrap();
                writer
                    .write_record(&make_record(
                        &schema,
                        vec![Value::Integer(7), Value::Integer(9)],
                    ))
                    .unwrap();
                assert_eq!(
                    writer.destination(),
                    if include {
                        b"id,$ck.id\n7,9\n".as_slice()
                    } else {
                        b"id\n7\n".as_slice()
                    }
                );
            }
            drop(capture);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn csv_envelope_off_is_byte_identical() {
        // No envelope spec: begin/end_document are no-ops and the body is the
        // plain CSV the boundary-unaware path produces.
        let schema = make_schema(&["amount"]);
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        {
            let mut writer = prepared_writer(
                &mut buf,
                schema.clone(),
                CsvWriterConfig {
                    include_header: false,
                    ..Default::default()
                },
            );
            writer.begin_document(&doc).unwrap();
            writer
                .write_record(&make_record(&schema, vec![Value::Integer(10)]))
                .unwrap();
            writer.end_document(&doc).unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "10\n");
    }

    /// In lossless mode (`error_on_undeclared_columns: true`, mirroring
    /// `include_unmapped: true`) a record carrying a user column the pinned
    /// schema lacks raises SchemaDrift rather than silently writing a narrower
    /// row. This is the bounded-memory backstop for paths that pin the header
    /// to the first record and cannot pre-scan a union (issue #805).
    #[test]
    fn csv_undeclared_column_is_schema_drift_in_lossless_mode() {
        let writer_schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            error_on_undeclared_columns: true,
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut writer = prepared_writer(&mut buf, writer_schema.clone(), config);
        // Record carries a `region` column the pinned schema lacks.
        let drift_schema = make_schema(&["amount", "region"]);
        let record = make_record(
            &drift_schema,
            vec![Value::Integer(10), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::SchemaDrift,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "region");
                assert_eq!(field, 2);
                assert_eq!(offset, 0);
                assert_eq!(element, None);
            }
            other => panic!("expected CSV schema drift, got {other:?}"),
        }
    }

    /// The same guard fires under envelope framing (the header is suppressed
    /// and the body streams headerless, so a union is impossible there too).
    #[test]
    fn csv_envelope_schema_drift_is_loud() {
        let writer_schema = make_schema(&["amount"]);
        let config = CsvWriterConfig {
            include_header: false,
            error_on_undeclared_columns: true,
            envelope: Some(OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: None,
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        let mut writer = prepared_writer(&mut buf, writer_schema.clone(), config);
        writer.begin_document(&doc).unwrap();
        let drift_schema = make_schema(&["amount", "region"]);
        let record = make_record(
            &drift_schema,
            vec![Value::Integer(10), Value::String("US".into())],
        );
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::OutputEncoding {
                format,
                field,
                offset,
                kind: OutputEncodingKind::SchemaDrift,
                field_name,
                element,
            } => {
                assert_eq!(format, "CSV");
                assert_eq!(field_name.to_string(), "region");
                assert_eq!(field, 2);
                assert_eq!(offset, 0);
                assert_eq!(element, None);
            }
            other => panic!("expected CSV schema drift, got {other:?}"),
        }
    }

    /// A record whose columns are all within the pinned schema writes normally
    /// even in lossless mode — the drift guard is not a false positive on the
    /// no-drift case (including a record missing a declared column).
    #[test]
    fn csv_lossless_mode_no_drift_writes_normally() {
        let schema = make_schema(&["a", "b"]);
        let config = CsvWriterConfig {
            error_on_undeclared_columns: true,
            ..Default::default()
        };
        // Second record is missing `b` — a legitimate absent (empty) cell, not
        // drift.
        let records = vec![
            make_record(
                &schema,
                vec![Value::String("A".into()), Value::String("B".into())],
            ),
            make_record(&make_schema(&["a"]), vec![Value::String("A2".into())]),
        ];
        let output = write_to_string(&schema, config, &records);
        assert_eq!(output, "a,b\nA,B\nA2,\n");
    }

    /// With the guard off (the `include_unmapped: false` narrowing contract),
    /// a record field outside the pinned schema is silently not written — the
    /// output schema stays the deliberate column contract.
    #[test]
    fn csv_undeclared_column_dropped_when_guard_off() {
        let writer_schema = make_schema(&["a", "b"]);
        let record_schema = make_schema(&["a", "extra", "b"]);
        let record = make_record(
            &record_schema,
            vec![
                Value::String("A".into()),
                Value::String("X".into()),
                Value::String("B".into()),
            ],
        );
        // Default config: error_on_undeclared_columns is false.
        let output = write_to_string(&writer_schema, CsvWriterConfig::default(), &[record]);
        assert_eq!(output, "a,b\nA,B\n");
    }
}
