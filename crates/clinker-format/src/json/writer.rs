//! JSON writer supporting array mode, NDJSON mode, pretty-printing,
//! and null omission. Implements `FormatWriter`.
//!
//! Dotted column names expand back into nested objects on the way out —
//! `Address.City` and `Address.State` become one `"Address"` object with two
//! keys — so a document read from nested JSON writes back with its shape
//! intact. The names are decoded with the shared record-space grammar in
//! [`clinker_record::field_path`], the same grammar the XML writer expands
//! elements with, so one column set produces the same tree in both formats.
//!
//! Under `reconstruct_envelope` the whole-stream array framing is REPLACED by
//! per-document object framing: each enveloped document becomes one JSON
//! object `{ "<header>": {...}, "body": [ ... ], "<footer>": {...} }`, with the
//! header/footer keys named by the source's `$doc` sections and the footer
//! optionally carrying a streaming record count. Multiple documents in one
//! stream are each individually framed — wrapped in an outer array in `array`
//! mode, or one object per line in `ndjson` mode. The body array streams one
//! record at a time, so no document is ever buffered.

use clinker_record::owned_storage::{OwnedKey, SharedStorage};
use std::io::Write;

use clinker_record::field_path;
use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::OutputEnvelopeSpec;
use crate::error::{FormatError, OutputEncodingKind};
use crate::preparation::{
    FormatEncoder, OutputOperation, PreparedWriter, WriterResources, WriterScope,
};
use crate::reserved::{ReservedText, ReservedVec};
use crate::traits::FormatWriter;

fn json_output_error(field: usize, name: &str) -> FormatError {
    json_encoding_error(field, name, OutputEncodingKind::Json)
}

fn json_encoding_error(field: usize, name: &str, kind: OutputEncodingKind) -> FormatError {
    FormatError::OutputEncoding {
        format: "JSON",
        field: field + 1,
        offset: 0,
        kind,
        field_name: crate::error::OutputFieldName::new(name),
        element: None,
    }
}

struct PreparedJsonConfig {
    mode: JsonOutputMode,
    pretty: bool,
    preserve_nulls: bool,
    include_engine_stamped: bool,
    envelope: Option<crate::envelope_writer::PreparedEnvelope>,
}

/// Admitted immutable JSON policy shared across a factory's physical writers.
#[derive(Clone)]
pub struct JsonEncoderConfig(SharedStorage<PreparedJsonConfig>);
impl JsonEncoderConfig {
    /// Copies only envelope names, after admission; the caller owns its options.
    pub fn new(
        config: &JsonWriterConfig,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let envelope = config.envelope.as_ref();
        Self::from_names(
            config,
            envelope.and_then(|e| e.header_from_doc.as_deref()),
            envelope.and_then(|e| e.footer_from_doc.as_deref()),
            envelope.and_then(|e| e.footer_record_count_field.as_deref()),
            resources,
        )
    }
    /// Admits borrowed compiled envelope names before retaining any copies.
    pub fn from_names(
        config: &JsonWriterConfig,
        header: Option<&str>,
        footer: Option<&str>,
        count: Option<&str>,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        Ok(Self(SharedStorage::try_new(
            PreparedJsonConfig {
                mode: config.format,
                pretty: config.pretty,
                preserve_nulls: config.preserve_nulls,
                include_engine_stamped: config.include_engine_stamped,
                envelope: crate::envelope_writer::PreparedEnvelope::from_names(
                    header, footer, count, &scope,
                )?,
            },
            scope.allocation(),
        )?))
    }
}

struct JsonTreeNode {
    name: ReservedText,
    field: Option<usize>,
    children: ReservedVec<JsonTreeNode>,
}
struct JsonCache {
    schema: clinker_record::owned_storage::SharedStorageIdentity<Schema>,
    tree: ReservedVec<JsonTreeNode>,
}
#[derive(Clone, Copy, Default)]
struct JsonState {
    records: u64,
    any_document: bool,
    document_open: bool,
}

/// Prepares complete operations from borrowed values into the governed stage.
/// Only admitted schema paths and payload-free identity survive each operation.
/// Raw construction without a finite resource provider is unavailable.
///
/// ```compile_fail
/// use clinker_format::json::writer::JsonWriter;
/// ```
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::sync::Arc;
/// use clinker_format::{FormatWriter, json::writer::{JsonEncoder, JsonWriterConfig}};
/// use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
/// use clinker_record::{Record, Schema, Value, owned_storage::SharedStorage};
/// let resources = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
/// let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into()])));
/// let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
/// let encoder = JsonEncoder::new(schema, &JsonWriterConfig::default(), resources.resources())?;
/// let mut bytes = Vec::new();
/// let mut writer = PreparedWriter::new(&mut bytes, encoder, resources.resources())?;
/// writer.write_record(&record)?;
/// writer.flush()?;
/// drop(writer);
/// assert_eq!(bytes, b"[\n{\"id\":1}\n]\n");
/// assert_eq!(resources.used(), 0);
/// # Ok::<(), clinker_format::FormatError>(())
/// ```
pub struct JsonEncoder {
    config: JsonEncoderConfig,
    cache: Option<JsonCache>,
    state: JsonState,
}
/// A replacement cache overlaps the committed cache until delivery succeeds.
pub struct JsonPending {
    replacement: Option<JsonCache>,
    state: JsonState,
    finalized: bool,
}
impl JsonEncoder {
    /// Admits retained policy; schema values remain owned by the caller.
    pub fn new(
        schema: SharedStorage<Schema>,
        config: &JsonWriterConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        Self::from_config(schema, JsonEncoderConfig::new(config, &resources)?)
    }
    /// Shares admitted policy without retaining the caller's schema columns.
    pub fn from_config(
        _schema: SharedStorage<Schema>,
        config: JsonEncoderConfig,
    ) -> Result<Self, FormatError> {
        Ok(Self {
            config,
            cache: None,
            state: JsonState::default(),
        })
    }
    /// Admit the concrete wrapper until its actual backing is deallocated.
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
}
impl<W: Write + Send> FormatWriter for PreparedWriter<W, JsonEncoder> {
    fn write_record(&mut self, r: &Record) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::Record(r))
    }
    fn begin_document(&mut self, d: &DocumentContext) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::BeginDocument(d))
    }
    fn end_document(&mut self, d: &DocumentContext) -> Result<(), FormatError> {
        self.write_operation(OutputOperation::EndDocument(d))
    }
    fn flush(&mut self) -> Result<(), FormatError> {
        PreparedWriter::flush(self)
    }
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        PreparedWriter::flush_bytes(self)
    }
}

fn json_tree<'a>(
    fields: impl Iterator<Item = (usize, &'a str)>,
    scope: &WriterScope,
) -> Result<ReservedVec<JsonTreeNode>, FormatError> {
    let mut root: ReservedVec<JsonTreeNode> = ReservedVec::new(scope.allocation().clone());
    for (field, full_name) in fields {
        let mut body = &mut root;
        let mut segments = field_path::segments(full_name).peekable();
        while let Some(segment) = segments.next() {
            scope.check_cancelled()?;
            let segment = segment
                .map_err(|_| json_encoding_error(field, full_name, OutputEncodingKind::JsonPath))?;
            let mut name = ReservedText::new(scope.allocation().clone());
            segment.write_to(|chunk| name.push_str(chunk))?;
            let leaf = segments.peek().is_none();
            let position = body
                .as_slice()
                .iter()
                .position(|n| n.name.as_str() == name.as_str());
            let at = match position {
                Some(at) => {
                    if leaf || body.as_slice()[at].field.is_some() {
                        return Err(json_encoding_error(
                            field,
                            full_name,
                            OutputEncodingKind::JsonPath,
                        ));
                    }
                    at
                }
                None => {
                    let at = body.len();
                    body.push(JsonTreeNode {
                        name,
                        field: leaf.then_some(field),
                        children: ReservedVec::new(scope.allocation().clone()),
                    })?;
                    at
                }
            };
            body = &mut body.as_mut_slice()[at].children;
        }
    }
    Ok(root)
}

fn validate_json_value(
    value: &Value,
    field: usize,
    name: &str,
    scope: &WriterScope,
    depth: usize,
) -> Result<(), FormatError> {
    use clinker_record::nested_key::{MAX_NESTED_VALUE_DEPTH, NestedKey};
    scope.check_cancelled()?;
    match value {
        Value::Float(n) if !n.is_finite() => return Err(json_output_error(field, name)),
        Value::Array(values) => {
            if depth >= MAX_NESTED_VALUE_DEPTH {
                return Err(json_output_error(field, name));
            }
            for value in values {
                validate_json_value(value, field, name, scope, depth + 1)?;
            }
        }
        Value::Map(values) => {
            if depth >= MAX_NESTED_VALUE_DEPTH {
                return Err(json_output_error(field, name));
            }
            for (position, (key, value)) in values.iter().enumerate() {
                // The existing key decoder borrows valid keys; only its error
                // owns a copy. Hold the exact possible copy through conversion.
                let diagnostic = scope.reserve(
                    std::alloc::Layout::array::<u8>(key.len())
                        .map_err(|_| json_output_error(field, name))?,
                )?;
                let decoded = NestedKey::decode(key).map_err(|_| json_output_error(field, name))?;
                for prior in values.keys().take(position) {
                    scope.check_cancelled()?;
                    let prior =
                        NestedKey::decode(prior).map_err(|_| json_output_error(field, name))?;
                    if prior.text == decoded.text {
                        return Err(json_output_error(field, name));
                    }
                }
                drop(diagnostic);
                validate_json_value(value, field, name, scope, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// Both records and sections borrow their original values. The count is the
// sole new scalar and lives on the operation's stack.
enum JsonValues<'a> {
    Record(&'a [Value]),
    Section(&'a indexmap::IndexMap<OwnedKey, Value>, &'a Value),
}
impl JsonValues<'_> {
    fn get(&self, field: usize) -> &Value {
        match self {
            Self::Record(values) => &values[field],
            Self::Section(fields, count) => fields.get_index(field).map_or(count, |(_, v)| v),
        }
    }
}
struct PreparedBodySer<'a> {
    body: &'a [JsonTreeNode],
    values: &'a JsonValues<'a>,
    preserve_nulls: bool,
}
fn json_present(body: &[JsonTreeNode], values: &JsonValues<'_>) -> bool {
    body.iter().any(|n| {
        n.field.map_or_else(
            || json_present(n.children.as_slice(), values),
            |i| !values.get(i).is_null(),
        )
    })
}
impl serde::Serialize for PreparedBodySer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for node in self.body {
            if let Some(field) = node.field {
                let value = self.values.get(field);
                if self.preserve_nulls || !value.is_null() {
                    map.serialize_entry(node.name.as_str(), &ValueSer(value))?;
                }
            } else if self.preserve_nulls || json_present(node.children.as_slice(), self.values) {
                map.serialize_entry(
                    node.name.as_str(),
                    &Self {
                        body: node.children.as_slice(),
                        values: self.values,
                        preserve_nulls: self.preserve_nulls,
                    },
                )?;
            }
        }
        map.end()
    }
}
fn json_serialize(
    stage: &mut dyn Write,
    tree: &[JsonTreeNode],
    values: &JsonValues<'_>,
    preserve_nulls: bool,
    pretty: bool,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    use serde::Serialize;
    // Pinned serde_json ErrorImpl: ErrorCode (unit, Box<str>, or io::Error)
    // plus line/column. Prevalidation eliminates custom-message allocations.
    // The formatter owns only scalar state and borrowed indentation bytes.
    let _error = scope.reserve(std::alloc::Layout::new::<(
        usize,
        Box<str>,
        std::io::Error,
        usize,
        usize,
    )>())?;
    let value = PreparedBodySer {
        body: tree,
        values,
        preserve_nulls,
    };
    let result = if pretty {
        value.serialize(&mut serde_json::Serializer::pretty(stage))
    } else {
        value.serialize(&mut serde_json::Serializer::new(stage))
    };
    result.map_err(|error| {
        if error.is_io() {
            FormatError::Io(error.into())
        } else {
            json_output_error(0, "")
        }
    })
}
fn json_section(
    stage: &mut dyn Write,
    fields: &indexmap::IndexMap<OwnedKey, Value>,
    count: Option<(&str, i64)>,
    pretty: bool,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    let tree = json_tree(
        fields
            .keys()
            .enumerate()
            .map(|(i, name)| (i, name.as_ref()))
            .chain(count.map(|(name, _)| (fields.len(), name))),
        scope,
    )?;
    for (i, (name, value)) in fields.iter().enumerate() {
        validate_json_value(value, i, name, scope, 0)?;
    }
    let count = Value::Integer(count.map_or(0, |(_, count)| count));
    json_serialize(
        stage,
        tree.as_slice(),
        &JsonValues::Section(fields, &count),
        true,
        pretty,
        scope,
    )
}

impl FormatEncoder for JsonEncoder {
    type Pending = JsonPending;
    fn prepare(
        &self,
        operation: OutputOperation<'_>,
        stage: &mut dyn Write,
        workspace: &WriterScope,
    ) -> Result<JsonPending, FormatError> {
        workspace.check_cancelled()?;
        let config = &self.config.0;
        let mut pending = JsonPending {
            replacement: None,
            state: self.state,
            finalized: false,
        };
        let state = &mut pending.state;
        match operation {
            OutputOperation::Record(record) => {
                if config.envelope.is_some() && !state.document_open {
                    return Err(json_encoding_error(
                        0,
                        "document",
                        OutputEncodingKind::JsonDocumentClosed,
                    ));
                }
                if self
                    .cache
                    .as_ref()
                    .is_none_or(|c| !c.schema.matches(record.schema()))
                {
                    let identity = record.schema().try_identity(workspace.allocation())?;
                    let tree = json_tree(
                        record
                            .schema()
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| {
                                config.include_engine_stamped
                                    || !record.schema().is_engine_stamped(*i)
                            })
                            .map(|(i, name)| (i, name.as_ref())),
                        workspace,
                    )?;
                    pending.replacement = Some(JsonCache {
                        schema: identity,
                        tree,
                    });
                }
                let cache = pending
                    .replacement
                    .as_ref()
                    .or(self.cache.as_ref())
                    .ok_or_else(|| json_output_error(0, "schema"))?;
                for (i, value) in record.values().iter().enumerate() {
                    if config.include_engine_stamped || !record.schema().is_engine_stamped(i) {
                        validate_json_value(value, i, &record.schema().columns()[i], workspace, 0)?;
                    }
                }
                if config.envelope.is_some() {
                    if state.records > 0 {
                        stage.write_all(b",")?;
                    }
                } else if matches!(config.mode, JsonOutputMode::Array) {
                    stage.write_all(if state.records == 0 { b"[\n" } else { b",\n" })?;
                }
                json_serialize(
                    stage,
                    cache.tree.as_slice(),
                    &JsonValues::Record(record.values()),
                    config.preserve_nulls,
                    config.pretty
                        && (config.envelope.is_some()
                            || matches!(config.mode, JsonOutputMode::Array)),
                    workspace,
                )?;
                if config.envelope.is_none() && matches!(config.mode, JsonOutputMode::Ndjson) {
                    stage.write_all(b"\n")?;
                }
                state.records = state
                    .records
                    .checked_add(1)
                    .ok_or_else(|| json_output_error(0, "record count"))?;
            }
            OutputOperation::BeginDocument(doc) => {
                if let Some(envelope) = &config.envelope {
                    if state.document_open {
                        return Err(json_encoding_error(
                            0,
                            "document",
                            OutputEncodingKind::JsonDocumentOpen,
                        ));
                    }
                    match config.mode {
                        JsonOutputMode::Array => {
                            stage.write_all(if state.any_document { b",\n" } else { b"[\n" })?
                        }
                        JsonOutputMode::Ndjson if state.any_document => stage.write_all(b"\n")?,
                        JsonOutputMode::Ndjson => {}
                    }
                    stage.write_all(b"{")?;
                    if let Some(fields) = envelope.header_fields(doc) {
                        stage.write_all(b"\"header\":")?;
                        json_section(stage, fields, None, config.pretty, workspace)?;
                        stage.write_all(b",")?;
                    }
                    stage.write_all(b"\"body\":[")?;
                    state.document_open = true;
                    state.records = 0;
                }
            }
            OutputOperation::EndDocument(doc) => {
                if let Some(envelope) = &config.envelope
                    && state.document_open
                {
                    stage.write_all(b"]")?;
                    if let Some(fields) = envelope.footer_fields(doc) {
                        stage.write_all(b",\"footer\":")?;
                        let count = i64::try_from(state.records)
                            .map_err(|_| json_output_error(0, "record count"))?;
                        json_section(
                            stage,
                            fields,
                            envelope.count_name().map(|name| (name, count)),
                            config.pretty,
                            workspace,
                        )?;
                    }
                    stage.write_all(b"}")?;
                    state.document_open = false;
                    state.any_document = true;
                }
            }
            OutputOperation::Finalize => {
                if state.document_open {
                    return Err(json_encoding_error(
                        0,
                        "document",
                        OutputEncodingKind::JsonDocumentOpen,
                    ));
                }
                if matches!(config.mode, JsonOutputMode::Array) {
                    let nonempty = if config.envelope.is_some() {
                        state.any_document
                    } else {
                        state.records > 0
                    };
                    stage.write_all(if nonempty { b"\n]\n" } else { b"[]\n" })?;
                }
                pending.finalized = true;
            }
        }
        Ok(pending)
    }
    fn commit(&mut self, pending: JsonPending) {
        self.state = pending.state;
        if pending.finalized {
            self.cache = None;
        } else if let Some(cache) = pending.replacement {
            self.cache = Some(cache);
        }
    }
}

/// JSON output format mode.
#[derive(Debug, Clone, Copy, Default)]
pub enum JsonOutputMode {
    /// `[{...},{...},...]` — valid JSON array.
    #[default]
    Array,
    /// One JSON object per line, no wrapper.
    Ndjson,
}

#[derive(Clone)]
pub struct JsonWriterConfig {
    pub format: JsonOutputMode,
    pub pretty: bool,
    pub preserve_nulls: bool,
    /// Whether engine-stamped schema columns (`$ck.<field>` correlation
    /// snapshots) appear as keys in the emitted JSON object. Defaults
    /// to `false` to keep engine-internal namespaces out of output.
    pub include_engine_stamped: bool,
    /// Per-document envelope reconstruction. `None` (the default) keeps the
    /// whole-stream array / NDJSON framing byte-identical to today. `Some` is
    /// set by the executor only under `reconstruct_envelope: true` and
    /// reframes the output to one object per document.
    pub envelope: Option<OutputEnvelopeSpec>,
}

impl Default for JsonWriterConfig {
    fn default() -> Self {
        Self {
            format: JsonOutputMode::Array,
            pretty: false,
            preserve_nulls: false,
            include_engine_stamped: false,
            envelope: None,
        }
    }
}

/// Serializes a borrowed neutral value without an intermediate JSON tree.
/// Callers prevalidate keys, depth and finite numbers before publication.
pub(crate) struct ValueSer<'a>(pub(crate) &'a Value);

impl serde::Serialize for ValueSer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeMap, SerializeSeq};
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Integer(i) => serializer.serialize_i64(*i),
            Value::Float(f) => {
                // serde_json's default would silently coerce a non-finite float
                // to `null`, indistinguishable from a source null on read-back;
                // reject rather than losing the original scalar distinction.
                if !f.is_finite() {
                    return Err(S::Error::custom(format!(
                        "non-finite float {f} has no JSON representation; \
                         filter or replace the value before the JSON output"
                    )));
                }
                serializer.serialize_f64(*f)
            }
            // JSON has no exact-decimal type; emit the scale-preserving string
            // form used by the native format contract.
            Value::Decimal(d) => serializer.collect_str(d),
            Value::String(s) => serializer.serialize_str(s.as_str()),
            Value::Date(d) => serializer.collect_str(d),
            Value::DateTime(dt) => serializer.collect_str(dt),
            Value::Array(arr) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for v in arr {
                    seq.serialize_element(&ValueSer(v))?;
                }
                seq.end()
            }
            Value::Map(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    let key = clinker_record::nested_key::NestedKey::decode(k)
                        .map_err(S::Error::custom)?;
                    map.serialize_entry(key.text.as_ref(), &ValueSer(v))?;
                }
                map.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::reader::{JsonReader, JsonReaderConfig};
    use crate::preparation::MemoryOnlyResources;
    use crate::traits::FormatReader;
    use clinker_record::owned_storage::{OwnedMap, OwnedValues};
    use clinker_record::schema::{FieldMetadata, SchemaBuilder};
    use std::sync::Arc;

    fn assert_encoding_error(
        err: &FormatError,
        kind: OutputEncodingKind,
        field: usize,
        name: &str,
    ) {
        let FormatError::OutputEncoding {
            format,
            kind: actual,
            field: actual_field,
            field_name,
            ..
        } = err
        else {
            panic!("expected a bounded output error, got {err:?}");
        };
        assert_eq!(*format, "JSON");
        assert_eq!(*actual, kind);
        assert_eq!(*actual_field, field);
        assert_eq!(
            field_name.to_string(),
            crate::error::OutputFieldName::new(name).to_string()
        );
    }

    fn test_schema() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "name".into(),
            "age".into(),
            "active".into(),
        ])))
    }

    fn make_record(schema: &SharedStorage<Schema>, name: &str, age: i64, active: bool) -> Record {
        Record::new(
            schema.clone(),
            vec![
                Value::String(name.into()),
                Value::Integer(age),
                Value::Bool(active),
            ],
        )
    }

    fn write_records(
        config: JsonWriterConfig,
        records: &[Record],
        schema: &SharedStorage<Schema>,
    ) -> String {
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        String::from_utf8(buf).unwrap()
    }

    /// Build a record over a schema of plain columns, one value per column.
    fn record_of(schema: &SharedStorage<Schema>, values: Vec<Value>) -> Record {
        Record::new(schema.clone(), values)
    }

    fn schema_of(columns: &[&str]) -> SharedStorage<Schema> {
        columns.iter().copied().collect::<SchemaBuilder>().build()
    }

    /// Write one record as a single NDJSON line, so assertions can pin the
    /// exact bytes rather than a reparsed tree.
    fn write_one_line(
        config: JsonWriterConfig,
        schema: &SharedStorage<Schema>,
        values: Vec<Value>,
    ) -> String {
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..config
        };
        write_records(config, &[record_of(schema, values)], schema)
            .strip_suffix('\n')
            .expect("NDJSON record ends in one LF")
            .to_string()
    }

    /// Write one record and return the writer error plus everything that
    /// reached the sink, so a rejection can be checked to have emitted nothing.
    fn write_one_expecting_error(
        config: JsonWriterConfig,
        schema: &SharedStorage<Schema>,
        values: Vec<Value>,
    ) -> (FormatError, Vec<u8>) {
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = w
            .write_record(&record_of(schema, values))
            .expect_err("expected the record to be refused");
        drop(w);
        (err, buf)
    }

    #[test]
    fn test_json_write_array_mode() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
            make_record(&schema, "Carol", 35, true),
        ];
        let output = write_records(JsonWriterConfig::default(), &records, &schema);
        // Exact bytes: a flat schema must emit exactly what it did before
        // dotted names started expanding.
        assert_eq!(
            output,
            "[\n{\"name\":\"Alice\",\"age\":30,\"active\":true},\n\
             {\"name\":\"Bob\",\"age\":25,\"active\":false},\n\
             {\"name\":\"Carol\",\"age\":35,\"active\":true}\n]\n"
        );
    }

    #[test]
    fn test_json_write_ndjson_mode() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
            make_record(&schema, "Carol", 35, true),
        ];
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &records, &schema);
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["name"], "Alice");
    }

    #[test]
    fn test_json_write_pretty() {
        let schema = test_schema();
        let records = vec![make_record(&schema, "Alice", 30, true)];
        let config = JsonWriterConfig {
            pretty: true,
            ..Default::default()
        };
        let output = write_records(config, &records, &schema);
        // Pretty output has indentation within objects
        assert!(
            output.contains("  \"name\""),
            "Pretty output should be indented: {output}"
        );
    }

    #[test]
    fn test_json_write_omit_nulls() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a".into(), "b".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = JsonWriterConfig {
            preserve_nulls: false,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            !output.contains("\"b\""),
            "Null field 'b' should be omitted: {output}"
        );
        assert!(output.contains("\"a\""));
    }

    #[test]
    fn test_json_write_preserve_nulls() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a".into(), "b".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = JsonWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            output.contains("\"b\":null") || output.contains("\"b\": null"),
            "Null field 'b' should be present: {output}"
        );
    }

    #[test]
    fn test_json_write_field_ordering() {
        // Schema fields emit in schema order — the widened schema is
        // authoritative; there is no overflow ordering to reason about.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "z_field".into(),
            "a_field".into(),
            "m_field".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );

        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        let z_pos = output.find("z_field").unwrap();
        let a_pos = output.find("a_field").unwrap();
        let m_pos = output.find("m_field").unwrap();
        assert!(z_pos < a_pos, "schema field z comes before a");
        assert!(a_pos < m_pos, "schema fields emit in schema order");
    }

    #[test]
    fn test_json_roundtrip_reader_writer() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30, true),
            make_record(&schema, "Bob", 25, false),
        ];

        // Write as NDJSON
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            preserve_nulls: true,
            ..Default::default()
        };
        let written = write_records(config, &records, &schema);

        // Read back
        let mut reader = JsonReader::from_reader(
            std::io::Cursor::new(written.as_bytes().to_vec()),
            JsonReaderConfig::default(),
        )
        .unwrap();
        let _s = reader.schema().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("age"), Some(&Value::Integer(30)));
        assert_eq!(r1.get("active"), Some(&Value::Bool(true)));
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(r2.get("age"), Some(&Value::Integer(25)));
    }

    #[test]
    fn test_json_write_wide_and_long_value_roundtrip() {
        // Exercises the buffer-reuse serialize path across a reused writer with
        // a wide schema and a long string value, then reads back to confirm the
        // operation stage is fresh per record (no stale-byte bleed) and
        // the round-trip is faithful.
        let cols: Vec<OwnedKey> = (0..40).map(|i| format!("c{i}").into()).collect();
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(cols)));
        let long = "x".repeat(500);
        let mk = |seed: i64, tail: &str| {
            let mut vals: Vec<Value> = (0..40).map(|i| Value::Integer(seed + i as i64)).collect();
            // Overwrite one column with a long string to stress the value path.
            vals[17] = Value::String(format!("{long}-{tail}").into());
            Record::new(schema.clone(), vals)
        };
        let records = vec![mk(0, "first"), mk(100, "second")];
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            preserve_nulls: true,
            ..Default::default()
        };
        let written = write_records(config, &records, &schema);

        let mut reader = JsonReader::from_reader(
            std::io::Cursor::new(written.into_bytes()),
            JsonReaderConfig::default(),
        )
        .unwrap();
        let _s = reader.schema().unwrap();
        let r0 = reader.next_record().unwrap().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(r0.get("c0"), Some(&Value::Integer(0)));
        assert_eq!(r0.get("c39"), Some(&Value::Integer(39)));
        assert_eq!(
            r0.get("c17"),
            Some(&Value::String(format!("{long}-first").into()))
        );
        assert_eq!(r1.get("c0"), Some(&Value::Integer(100)));
        assert_eq!(
            r1.get("c17"),
            Some(&Value::String(format!("{long}-second").into()))
        );
    }

    #[test]
    fn test_json_write_finite_float_roundtrips_exactly() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["reading".into()])));
        let record = Record::new(schema.clone(), vec![Value::Float(2.5)]);
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(parsed["reading"], serde_json::json!(2.5));
    }

    #[test]
    fn test_json_write_rejects_non_finite_floats() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["reading".into()])));
        for val in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let record = Record::new(schema.clone(), vec![Value::Float(val)]);
            let mut buf = Vec::new();
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = JsonEncoder::new(
                schema.clone(),
                &JsonWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            let err = w.write_record(&record).unwrap_err();
            assert_encoding_error(&err, OutputEncodingKind::Json, 1, "reading");
            drop(w);
            assert!(
                buf.is_empty(),
                "no partial bytes for a rejected record, got: {:?}",
                String::from_utf8_lossy(&buf)
            );
        }
    }

    #[test]
    fn test_json_write_rejects_non_finite_float_nested_in_array() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["readings".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::Array(OwnedValues::from_vec(vec![
                Value::Float(1.0),
                Value::Float(f64::INFINITY),
            ]))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(
            schema.clone(),
            &JsonWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = w.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::Json, 1, "readings");
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    fn amount_record(schema: &SharedStorage<Schema>, n: i64) -> Record {
        Record::new(schema.clone(), vec![Value::Integer(n)])
    }

    #[test]
    fn json_envelope_record_with_no_open_document_errors_cleanly() {
        // Defense-in-depth: the plan-time E347 guard rejects pipelines that
        // route lineage-stripped (`<merged>`) records into an enveloped JSON
        // Output, so `begin_document` always fires first in production. If one
        // ever reached here without an open document, the writer must raise a
        // clean error rather than emit record bytes outside any `body` array
        // (which would be malformed JSON).
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        // No begin_document — write straight into the envelope writer.
        let err = w.write_record(&amount_record(&schema, 1)).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::JsonDocumentClosed, 1, "document");
        assert!(err.to_string().contains("no open document"));
        assert!(err.to_string().contains("begin_document"));
    }

    #[test]
    fn json_envelope_array_mode_frames_each_document_as_an_object() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc_a = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("SUM-A".into()))]),
        ]);
        let doc_b = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("B".into()))]),
            ("Foot", &[("checksum", Value::String("SUM-B".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc_a).unwrap();
            w.write_record(&amount_record(&schema, 10)).unwrap();
            w.write_record(&amount_record(&schema, 20)).unwrap();
            w.end_document(&doc_a).unwrap();
            w.begin_document(&doc_b).unwrap();
            w.write_record(&amount_record(&schema, 30)).unwrap();
            w.end_document(&doc_b).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        // Valid JSON: an outer array of two document objects.
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("valid JSON array");
        let arr = parsed.as_array().expect("outer array");
        assert_eq!(arr.len(), 2, "one object per document: {out}");
        assert_eq!(arr[0]["header"]["batch_id"], "A");
        assert_eq!(
            arr[0]["body"],
            serde_json::json!([{"amount":10},{"amount":20}])
        );
        assert_eq!(arr[0]["footer"]["checksum"], "SUM-A");
        assert_eq!(arr[0]["footer"]["count"], 2);
        assert_eq!(arr[1]["header"]["batch_id"], "B");
        assert_eq!(arr[1]["footer"]["count"], 1, "count resets per document");
    }

    #[test]
    fn json_envelope_ndjson_mode_one_document_object_per_line() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc_a = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let doc_b = doc_with_sections(&[("Head", &[("batch_id", Value::String("B".into()))])]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc_a).unwrap();
            w.write_record(&amount_record(&schema, 10)).unwrap();
            w.end_document(&doc_a).unwrap();
            w.begin_document(&doc_b).unwrap();
            w.write_record(&amount_record(&schema, 20)).unwrap();
            w.end_document(&doc_b).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "one document object per line: {out}");
        let d0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(d0["header"]["batch_id"], "A");
        assert_eq!(d0["body"], serde_json::json!([{"amount":10}]));
    }

    #[test]
    fn json_envelope_rejects_non_finite_float_in_section() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("ratio", Value::Float(f64::NAN))])]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = w.begin_document(&doc).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::Json, 1, "ratio");
    }

    #[test]
    fn json_envelope_rejects_non_finite_float_in_body_record() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = JsonWriterConfig {
            format: JsonOutputMode::Array,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[("Head", &[("batch_id", Value::String("A".into()))])]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        w.begin_document(&doc).unwrap();
        let record = Record::new(schema.clone(), vec![Value::Float(f64::NEG_INFINITY)]);
        let err = w.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::Json, 1, "amount");
    }

    // ── Dotted column name → nested object expansion ─────────────────

    #[test]
    fn dotted_columns_expand_into_one_nested_object() {
        let schema = schema_of(&["Address.City", "Address.State", "name"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![
                Value::String("Boston".into()),
                Value::String("MA".into()),
                Value::String("Ada".into()),
            ],
        );
        assert_eq!(
            out,
            r#"{"Address":{"City":"Boston","State":"MA"},"name":"Ada"}"#
        );
    }

    #[test]
    fn a_shared_prefix_groups_at_its_first_occurrence() {
        // Interleaved in the schema, grouped in the output, hoisted to where
        // the prefix first appeared — the same shape the XML writer produces.
        let schema = schema_of(&["A.x", "n", "A.y"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        assert_eq!(out, r#"{"A":{"x":1,"y":3},"n":2}"#);
    }

    #[test]
    fn expansion_nests_to_arbitrary_depth() {
        let schema = schema_of(&["a.b.c", "a.b.d", "a.e"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        assert_eq!(out, r#"{"a":{"b":{"c":1,"d":2},"e":3}}"#);
    }

    #[test]
    fn an_object_with_no_emitted_children_emits_no_key() {
        let schema = schema_of(&["a.b", "a.c", "d"]);
        let config = JsonWriterConfig {
            preserve_nulls: false,
            ..Default::default()
        };
        // Every descendant null: the parent key is absent, not `{}` — so it
        // reads back as the absent column it stands for.
        let all_null = write_one_line(
            config.clone(),
            &schema,
            vec![Value::Null, Value::Null, Value::Integer(9)],
        );
        assert_eq!(all_null, r#"{"d":9}"#);
        // One descendant present: the parent appears with only that child.
        let one_present = write_one_line(
            config,
            &schema,
            vec![Value::Null, Value::Integer(7), Value::Integer(9)],
        );
        assert_eq!(one_present, r#"{"a":{"c":7},"d":9}"#);
    }

    #[test]
    fn preserved_nulls_keep_the_nested_object() {
        let schema = schema_of(&["a.b"]);
        let config = JsonWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        assert_eq!(
            write_one_line(config, &schema, vec![Value::Null]),
            r#"{"a":{"b":null}}"#
        );
    }

    #[test]
    fn pretty_mode_keeps_ndjson_compact() {
        let schema = schema_of(&["Address.City", "name"]);
        let config = JsonWriterConfig {
            pretty: true,
            ..Default::default()
        };
        let out = write_one_line(
            config,
            &schema,
            vec![Value::String("Boston".into()), Value::String("Ada".into())],
        );
        assert_eq!(out, r#"{"Address":{"City":"Boston"},"name":"Ada"}"#);
    }

    #[test]
    fn a_structured_value_nests_below_the_expanded_leaf() {
        // Expansion adds structure above the leaf; the value model supplies
        // structure below it. The two never interact.
        let schema = schema_of(&["Items.Item", "meta.tags"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![
                Value::Array(OwnedValues::from_vec(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                ])),
                Value::Map(OwnedMap::from_map(
                    [("k".into(), Value::String("v".into()))]
                        .into_iter()
                        .collect(),
                )),
            ],
        );
        assert_eq!(out, r#"{"Items":{"Item":[1,2]},"meta":{"tags":{"k":"v"}}}"#);
    }

    #[test]
    fn a_column_that_is_also_a_container_is_refused_before_any_byte() {
        for columns in [
            // A value column and a container of the same name, either order.
            ["a", "a.b"],
            ["a.b", "a"],
            // The same clash one level deeper.
            ["a.b", "a.b.c"],
            ["a.b.c", "a.b"],
            // Two distinct spellings addressing the identical path.
            ["a[b", "a\\[b"],
        ] {
            let schema = schema_of(&columns);
            let (err, buf) = write_one_expecting_error(
                JsonWriterConfig::default(),
                &schema,
                vec![Value::Integer(1); columns.len()],
            );
            assert_encoding_error(&err, OutputEncodingKind::JsonPath, 2, columns[1]);
            assert!(
                buf.is_empty(),
                "a refused column set must emit no bytes, got: {:?}",
                String::from_utf8_lossy(&buf)
            );
        }
    }

    #[test]
    fn an_escaped_separator_keeps_the_dot_in_the_key() {
        // The replacement for the literal dotted key unconditional expansion
        // takes away: declare the column with the separator escaped.
        let schema = schema_of(&["a\\.b", "a"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        assert_eq!(out, r#"{"a.b":1,"a":2}"#);

        // An escaped and an unescaped separator address different paths, so
        // the two coexist rather than colliding.
        let schema = schema_of(&["a.b", "a\\.b"]);
        let out = write_one_line(
            JsonWriterConfig::default(),
            &schema,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        assert_eq!(out, r#"{"a":{"b":1},"a.b":2}"#);
    }

    #[test]
    fn a_malformed_escape_is_refused_before_any_byte() {
        let schema = schema_of(&["C:\\temp"]);
        let (err, buf) = write_one_expecting_error(
            JsonWriterConfig::default(),
            &schema,
            vec![Value::Integer(1)],
        );
        assert_encoding_error(&err, OutputEncodingKind::JsonPath, 1, r"C:\temp");
        assert!(err.to_string().contains(r"backslash as \\"), "{err}");
        assert!(buf.is_empty());
    }

    #[test]
    fn engine_stamped_columns_expand_by_the_same_rule() {
        // The rule is a function of the column-name string alone, with no
        // carve-out for engine-stamped namespaces.
        let schema = SchemaBuilder::new()
            .with_field("amount")
            .with_field_meta(
                "$ck.customer_id",
                FieldMetadata::source_correlation("customer_id"),
            )
            .build();
        let values = vec![Value::Integer(5), Value::String("C-1".into())];

        let stripped = write_one_line(JsonWriterConfig::default(), &schema, values.clone());
        assert_eq!(stripped, r#"{"amount":5}"#);

        let config = JsonWriterConfig {
            include_engine_stamped: true,
            ..Default::default()
        };
        let included = write_one_line(config, &schema, values);
        assert_eq!(included, r#"{"amount":5,"$ck":{"customer_id":"C-1"}}"#);
    }

    #[test]
    fn a_new_schema_identity_rebuilds_the_plan() {
        let flat = schema_of(&["a"]);
        let nested = schema_of(&["a.b"]);
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = JsonEncoder::new(flat.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.write_record(&record_of(&flat, vec![Value::Integer(1)]))
                .unwrap();
            w.write_record(&record_of(&nested, vec![Value::Integer(2)]))
                .unwrap();
            w.write_record(&record_of(&flat, vec![Value::Integer(3)]))
                .unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.trim_end(), "{\"a\":1}\n{\"a\":{\"b\":2}}\n{\"a\":3}");
    }

    #[test]
    fn a_nested_schema_stages_each_record_without_bleed() {
        // The nested walk writes into the same retained buffer every record;
        // a wide schema with a long value would surface any stale tail.
        let columns: Vec<String> = (0..40).map(|i| format!("g{}.c{i}", i % 4)).collect();
        let schema = schema_of(&columns.iter().map(String::as_str).collect::<Vec<_>>());
        let long = "x".repeat(500);
        let mk = |seed: i64, tail: &str| {
            let mut values: Vec<Value> = (0..40).map(|i| Value::Integer(seed + i)).collect();
            values[17] = Value::String(format!("{long}-{tail}").into());
            record_of(&schema, values)
        };
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            ..Default::default()
        };
        let written = write_records(config, &[mk(0, "first"), mk(100, "second")], &schema);
        let lines: Vec<&str> = written.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2);
        for (line, (seed, tail)) in lines.iter().zip([(0, "first"), (100, "second")]) {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["g0"]["c0"], seed);
            assert_eq!(v["g3"]["c39"], seed + 39);
            assert_eq!(v["g1"]["c17"], format!("{long}-{tail}"));
        }
    }

    #[test]
    fn envelope_section_fields_expand_too() {
        let schema = schema_of(&["amount"]);
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("totals.count".into()),
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[
            (
                "Head",
                &[
                    ("batch.id", Value::String("B-1".into())),
                    ("batch.source", Value::String("ftp".into())),
                ],
            ),
            ("Foot", &[("totals.checksum", Value::String("S".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc).unwrap();
            w.write_record(&record_of(&schema, vec![Value::Integer(1)]))
                .unwrap();
            w.end_document(&doc).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(out.trim_end()).unwrap();
        assert_eq!(parsed["header"]["batch"]["id"], "B-1");
        assert_eq!(parsed["header"]["batch"]["source"], "ftp");
        // The computed count rides the same expansion, joining the section
        // field that shares its prefix.
        assert_eq!(parsed["footer"]["totals"]["checksum"], "S");
        assert_eq!(parsed["footer"]["totals"]["count"], 1);
    }

    #[test]
    fn json_envelope_off_is_byte_identical() {
        // No envelope spec: array framing is the plain whole-stream array.
        let schema = test_schema();
        let records = vec![make_record(&schema, "Alice", 30, true)];
        let baseline = write_records(JsonWriterConfig::default(), &records, &schema);
        let parsed: serde_json::Value = serde_json::from_str(&baseline).unwrap();
        assert!(
            parsed.as_array().unwrap()[0]["body"].is_null(),
            "no per-doc body key"
        );
        assert_eq!(parsed.as_array().unwrap()[0]["name"], "Alice");
    }
}
