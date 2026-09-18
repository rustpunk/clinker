//! XML writer with configurable root/record elements, dotted field expansion
//! to nested elements, attribute-prefixed fields emitted as XML attributes,
//! null handling, and proper escaping.
//!
//! Column names are decoded with the shared record-space grammar in
//! [`clinker_record::field_path`] — the same grammar the JSON writer expands
//! objects with — so one column set produces the same tree in both formats.
//! What stays XML-specific is everything below the decode: attribute
//! classification against `attribute_prefix`, repeated-element naming, and
//! rejecting a segment that is not a well-formed XML `Name`.
//!
//! Under `reconstruct_envelope`, each document is wrapped in a `<Document>`
//! element inside the root: `begin_document` opens `<Document>` and emits the
//! header section as a `<header>` element; the body `<Record>` elements stream
//! between; `end_document` emits a `<footer>` element (section fields plus the
//! streaming record count) and closes `</Document>`. No document is buffered.
//!
//! Each record is processed in two borrowed passes. The first validates the
//! complete schema/value shape, XML names and scalar roles without touching the
//! destination. The second emits directly from the original [`Record`]. The
//! writer retains only a schema-derived tree plan across calls; it retains no
//! rendered record values or record-sized scalar capacity.

use clinker_record::owned_storage::{OwnedKey, SharedStorage};
use std::collections::BTreeSet;
use std::io::Write;

use quick_xml::Writer as XmlEmitter;
use quick_xml::events::{BytesEnd, BytesStart, Event};

use clinker_record::field_path;
use clinker_record::nested_key::NestedKey;
use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::OutputEnvelopeSpec;
use crate::error::{FormatError, OutputEncodingKind, OutputFieldName};
use crate::multi_value::JoinValues;
use crate::preparation::{
    FormatEncoder, OutputOperation, PreparedWriter, WriterResources, WriterScope,
};
use crate::reserved::{ReservedText, ReservedVec};
use crate::traits::FormatWriter;

/// Borrowed factory input; all retained policy is copied only after admission.
pub struct XmlEncoderOptions<'a> {
    pub root_element: &'a str,
    pub record_element: &'a str,
    pub attribute_prefix: &'a str,
    pub preserve_nulls: bool,
    pub include_engine_stamped: bool,
    pub join_values: &'a [JoinValues],
    pub declared_multiple: &'a BTreeSet<String>,
    pub envelope_header: Option<&'a str>,
    pub envelope_footer: Option<&'a str>,
    pub envelope_count: Option<&'a str>,
}
impl<'a> From<&'a XmlWriterConfig> for XmlEncoderOptions<'a> {
    fn from(c: &'a XmlWriterConfig) -> Self {
        Self {
            root_element: &c.root_element,
            record_element: &c.record_element,
            attribute_prefix: &c.attribute_prefix,
            preserve_nulls: c.preserve_nulls,
            include_engine_stamped: c.include_engine_stamped,
            join_values: &c.join_values,
            declared_multiple: &c.declared_multiple,
            envelope_header: c
                .envelope
                .as_ref()
                .and_then(|e| e.header_from_doc.as_deref()),
            envelope_footer: c
                .envelope
                .as_ref()
                .and_then(|e| e.footer_from_doc.as_deref()),
            envelope_count: c
                .envelope
                .as_ref()
                .and_then(|e| e.footer_record_count_field.as_deref()),
        }
    }
}
struct PreparedXmlJoin {
    field: ReservedText,
    repeat: Option<ReservedText>,
    wrap: Option<ReservedText>,
}
struct PreparedXmlConfig {
    root: ReservedText,
    record: ReservedText,
    prefix: ReservedText,
    preserve_nulls: bool,
    include_engine_stamped: bool,
    joins: ReservedVec<PreparedXmlJoin>,
    multiple: ReservedVec<ReservedText>,
    envelope: Option<crate::envelope_writer::PreparedEnvelope>,
}
/// Immutable admitted XML policy shared by physical writers from one factory.
#[derive(Clone)]
pub struct XmlEncoderConfig(SharedStorage<PreparedXmlConfig>);
fn xml_text(scope: &WriterScope, value: &str) -> Result<ReservedText, FormatError> {
    let mut text = ReservedText::new(scope.allocation().clone());
    text.push_str(value)?;
    Ok(text)
}
fn prepared_xml_error(field: usize, name: &str, kind: OutputEncodingKind) -> FormatError {
    FormatError::OutputEncoding {
        format: "XML",
        field: field + 1,
        offset: 0,
        kind,
        field_name: OutputFieldName::new(name),
        element: None,
    }
}
fn prepared_name(name: &str, field: usize) -> Result<(), FormatError> {
    if is_valid_xml_name(name) {
        Ok(())
    } else {
        Err(prepared_xml_error(field, name, OutputEncodingKind::XmlName))
    }
}
impl XmlEncoderConfig {
    /// Admit the shared backing, policy vectors and names before copying.
    /// The caller continues to own the compiled options and schema.
    pub fn new(
        options: XmlEncoderOptions<'_>,
        resources: &WriterResources,
    ) -> Result<Self, FormatError> {
        let scope = resources.scope()?;
        prepared_name(options.root_element, 0)?;
        prepared_name(options.record_element, 0)?;
        let mut joins = ReservedVec::new(scope.allocation().clone());
        joins.reserve_exact(options.join_values.len())?;
        for j in options.join_values {
            joins.push(PreparedXmlJoin {
                field: xml_text(&scope, &j.field)?,
                repeat: j
                    .repeat_as
                    .as_deref()
                    .map(|s| xml_text(&scope, s))
                    .transpose()?,
                wrap: j
                    .wrap_in
                    .as_deref()
                    .map(|s| xml_text(&scope, s))
                    .transpose()?,
            })?;
        }
        let mut multiple = ReservedVec::new(scope.allocation().clone());
        multiple.reserve_exact(options.declared_multiple.len())?;
        for name in options.declared_multiple {
            multiple.push(xml_text(&scope, name)?)?;
        }
        let envelope = crate::envelope_writer::PreparedEnvelope::from_names(
            options.envelope_header,
            options.envelope_footer,
            options.envelope_count,
            &scope,
        )?;
        Ok(Self(SharedStorage::try_new(
            PreparedXmlConfig {
                root: xml_text(&scope, options.root_element)?,
                record: xml_text(&scope, options.record_element)?,
                prefix: xml_text(&scope, options.attribute_prefix)?,
                preserve_nulls: options.preserve_nulls,
                include_engine_stamped: options.include_engine_stamped,
                joins,
                multiple,
                envelope,
            },
            scope.allocation(),
        )?))
    }
}

/// XML operation encoder. Schema/document values remain owned by the caller;
/// only admitted policy and schema-derived tree capacities are retained.
/// No escaped record or copied value tree is constructed.
/// Raw construction without a finite resource provider is unavailable.
///
/// ```compile_fail
/// use clinker_format::xml::writer::XmlWriter;
/// ```
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::sync::Arc;
/// use clinker_format::{FormatWriter, xml::writer::{XmlEncoder, XmlWriterConfig}};
/// use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
/// use clinker_record::{Record, Schema, Value, owned_storage::SharedStorage};
/// let resources = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
/// let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into()])));
/// let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
/// let encoder = XmlEncoder::new(schema, &XmlWriterConfig::default(), resources.resources())?;
/// let mut bytes = Vec::new();
/// let mut writer = PreparedWriter::new(&mut bytes, encoder, resources.resources())?;
/// writer.write_record(&record)?;
/// writer.flush()?;
/// drop(writer);
/// assert_eq!(bytes, b"<Root><Record><id>1</id></Record></Root>");
/// assert_eq!(resources.used(), 0);
/// # Ok::<(), clinker_format::FormatError>(())
/// ```
pub struct XmlEncoder {
    config: XmlEncoderConfig,
    plan_cache: Option<PreparedPlanCache>,
    header_written: bool,
    records: u64,
}
/// Replacement cache and framing counters become committed only after delivery.
/// Keeping the prior cache in the encoder admits both lifetimes at their peak.
pub struct XmlPending {
    replacement: Option<PreparedPlanCache>,
    header_written: bool,
    records: u64,
    finalized: bool,
}
// The sealed identity retains no dynamic schema columns or raw address.
struct PreparedPlanCache {
    schema: clinker_record::owned_storage::SharedStorageIdentity<Schema>,
    plan: TreePlan,
}
impl XmlEncoder {
    /// Admit retained policy before copying; schema values keep their caller owner.
    pub fn new(
        schema: SharedStorage<Schema>,
        config: &XmlWriterConfig,
        resources: WriterResources,
    ) -> Result<Self, FormatError> {
        Self::from_config(schema, XmlEncoderConfig::new(config.into(), &resources)?)
    }
    /// Borrow the schema through its existing owner and share admitted policy.
    pub fn from_config(
        _schema: SharedStorage<Schema>,
        config: XmlEncoderConfig,
    ) -> Result<Self, FormatError> {
        Ok(Self {
            config,
            plan_cache: None,
            header_written: false,
            records: 0,
        })
    }
    /// Admit the concrete writer until its actual backing is deallocated.
    /// The destination keeps its existing owner; drop never finalizes output.
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
impl<W: Write + Send> FormatWriter for PreparedWriter<W, XmlEncoder> {
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

fn admitted_body(scope: &WriterScope) -> PlanBody {
    PlanBody {
        attrs: ReservedVec::new(scope.allocation().clone()),
        children: ReservedVec::new(scope.allocation().clone()),
    }
}
fn admitted_tree<'a>(
    fields: impl Iterator<Item = (usize, &'a str)>,
    config: &PreparedXmlConfig,
    scope: &WriterScope,
) -> Result<TreePlan, FormatError> {
    let mut root = admitted_body(scope);
    for (field, name) in fields {
        scope.check_cancelled()?;
        let mut path = ReservedVec::new(scope.allocation().clone());
        for segment in field_path::segments(name) {
            scope.check_cancelled()?;
            let segment = segment
                .map_err(|_| prepared_xml_error(field, name, OutputEncodingKind::XmlPath))?;
            let mut text = ReservedText::new(scope.allocation().clone());
            segment.write_to(|chunk| text.push_str(chunk))?;
            path.push(text)?;
        }
        admitted_insert(&mut root, field, name, path.as_slice(), config, scope)?;
    }
    Ok(TreePlan { root })
}
fn admitted_insert(
    body: &mut PlanBody,
    field: usize,
    full: &str,
    path: &[ReservedText],
    config: &PreparedXmlConfig,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    scope.check_cancelled()?;
    let Some((segment, rest)) = path.split_first() else {
        return Err(prepared_xml_error(field, full, OutputEncodingKind::XmlPath));
    };
    let segment = segment.as_str();
    let prefix = config.prefix.as_str();
    let attr = !prefix.is_empty() && segment.starts_with(prefix);
    let name = if attr {
        &segment[prefix.len()..]
    } else {
        segment
    };
    prepared_name(name, field)?;
    if attr {
        if !rest.is_empty()
            || body
                .attrs
                .as_slice()
                .iter()
                .any(|a| a.name.as_str() == name)
        {
            return Err(prepared_xml_error(field, full, OutputEncodingKind::XmlPath));
        }
        body.attrs.push(PlanAttr {
            name: xml_text(scope, name)?,
            field,
        })?;
    } else if !rest.is_empty() {
        if let Some(node) = body.children.as_mut_slice().iter_mut().find(|n| match n {
            PlanNode::Leaf { name, .. } | PlanNode::Branch { name, .. } => name.as_str() == segment,
        }) {
            match node {
                PlanNode::Branch { body, .. } => {
                    admitted_insert(body, field, full, rest, config, scope)?
                }
                PlanNode::Leaf { .. } => {
                    return Err(prepared_xml_error(field, full, OutputEncodingKind::XmlPath));
                }
            }
        } else {
            let mut branch = admitted_body(scope);
            admitted_insert(&mut branch, field, full, rest, config, scope)?;
            body.children.push(PlanNode::Branch {
                name: xml_text(scope, segment)?,
                body: branch,
            })?;
        }
    } else {
        if body.children.as_slice().iter().any(|n| match n {
            PlanNode::Leaf { name, .. } | PlanNode::Branch { name, .. } => name.as_str() == segment,
        }) {
            return Err(prepared_xml_error(field, full, OutputEncodingKind::XmlPath));
        }
        let repeat = if let Some(j) = config
            .joins
            .as_slice()
            .iter()
            .find(|j| j.field.as_str() == full && (j.repeat.is_some() || j.wrap.is_some()))
        {
            let item = j.repeat.as_ref().map_or(segment, ReservedText::as_str);
            prepared_name(item, field)?;
            let wrap_in = j
                .wrap
                .as_ref()
                .map(|w| {
                    prepared_name(w.as_str(), field)?;
                    xml_text(scope, w.as_str())
                })
                .transpose()?;
            Some(XmlRepeat {
                item_name: xml_text(scope, item)?,
                wrap_in,
            })
        } else {
            None
        };
        body.children.push(PlanNode::Leaf {
            name: xml_text(scope, segment)?,
            field,
            declared_multiple: config
                .multiple
                .as_slice()
                .iter()
                .any(|n| n.as_str() == full),
            repeat,
        })?;
    }
    Ok(())
}
fn validate_prepared_scalar(
    value: &Value,
    field: usize,
    name: &str,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    let error = || prepared_xml_error(field, name, OutputEncodingKind::XmlValue);
    if matches!(value, Value::Map(_) | Value::Array(_)) {
        return Err(error());
    }
    let text = scalar_text("", value)?;
    for (offset, ch) in text.as_str().char_indices() {
        if offset % 4096 < 4 {
            scope.check_cancelled()?;
        }
        if !matches!(ch, '\u{9}' | '\u{A}' | '\u{D}' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
        {
            let mut error = error();
            if let FormatError::OutputEncoding { offset: at, .. } = &mut error {
                *at = offset;
            }
            return Err(error);
        }
    }
    Ok(())
}
fn validate_prepared_value(
    value: &Value,
    field: usize,
    name: &str,
    prefix: &str,
    depth: usize,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    scope.check_cancelled()?;
    let error = || prepared_xml_error(field, name, OutputEncodingKind::XmlValue);
    if matches!(value, Value::Map(_) | Value::Array(_))
        && depth >= clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH
    {
        return Err(error());
    }
    match value {
        Value::Array(values) => {
            for value in values.as_slice() {
                // Native XML has no anonymous array-item element.
                if matches!(value, Value::Array(_)) {
                    return Err(error());
                }
                validate_prepared_value(value, field, name, prefix, depth + 1, scope)?;
            }
        }
        Value::Map(values) => {
            for (index, (raw, child)) in values.as_map().iter().enumerate() {
                scope.check_cancelled()?;
                // The shared decoder borrows success and copies only malformed
                // keys. Admit that exact error-only layout until conversion.
                let layout = std::alloc::Layout::array::<u8>(raw.len()).map_err(|_| {
                    crate::preparation::ResourceError::new(
                        crate::preparation::ResourceErrorKind::Layout,
                        raw.len(),
                        0,
                    )
                })?;
                let diagnostic = scope.reserve(layout)?;
                let key = NestedKey::decode(raw).map_err(|_| error())?;
                for prior in values.as_map().keys().take(index) {
                    scope.check_cancelled()?;
                    let prior = NestedKey::decode(prior).map_err(|_| error())?;
                    if key.text == prior.text {
                        return Err(error());
                    }
                }
                drop(diagnostic);
                let attr = !key.escaped && !prefix.is_empty() && key.text.starts_with(prefix);
                let text = !key.escaped && key.text == "#text";
                if attr {
                    prepared_name(&key.text[prefix.len()..], field)?;
                } else if !text {
                    prepared_name(&key.text, field)?;
                }
                if attr || text {
                    validate_prepared_scalar(child, field, name, scope)?;
                } else {
                    validate_prepared_value(child, field, name, prefix, depth + 1, scope)?;
                }
            }
        }
        _ => validate_prepared_scalar(value, field, name, scope)?,
    }
    Ok(())
}
fn validate_prepared_body(
    body: &PlanBody,
    values: &impl FieldSource,
    config: &PreparedXmlConfig,
    scope: &WriterScope,
) -> Result<(), FormatError> {
    for attr in body.attrs.as_slice() {
        scope.check_cancelled()?;
        let (name, value) = values.field(attr.field);
        validate_prepared_scalar(value, attr.field, name, scope)?;
    }
    for child in body.children.as_slice() {
        scope.check_cancelled()?;
        match child {
            PlanNode::Branch { body, .. } => validate_prepared_body(body, values, config, scope)?,
            PlanNode::Leaf {
                field,
                declared_multiple,
                ..
            } => {
                let (name, value) = values.field(*field);
                if matches!(value, Value::Array(_)) && !declared_multiple {
                    return Err(prepared_xml_error(*field, name, OutputEncodingKind::Array));
                }
                validate_prepared_value(value, *field, name, config.prefix.as_str(), 0, scope)?;
            }
        }
    }
    Ok(())
}
fn prepared_element<W: Write>(
    writer: &mut XmlEmitter<W>,
    wrapper: &str,
    plan: &TreePlan,
    values: &impl FieldSource,
    config: &PreparedXmlConfig,
) -> Result<(), FormatError> {
    write_planned_start(writer, wrapper, plan.root.attrs.as_slice(), values, false)?;
    emit_body(
        writer,
        &plan.root,
        values,
        config.preserve_nulls,
        config.prefix.as_str(),
    )?;
    writer
        .write_event(Event::End(BytesEnd::new(wrapper)))
        .map_err(xml_err)
}
impl FormatEncoder for XmlEncoder {
    type Pending = XmlPending;
    fn prepare(
        &self,
        operation: OutputOperation<'_>,
        stage: &mut dyn Write,
        workspace: &WriterScope,
    ) -> Result<XmlPending, FormatError> {
        workspace.check_cancelled()?;
        let config = &self.config.0;
        let mut pending = XmlPending {
            replacement: None,
            header_written: self.header_written,
            records: self.records,
            finalized: false,
        };
        let mut writer = XmlEmitter::new(stage);
        let needs_root = matches!(
            operation,
            OutputOperation::Record(_) | OutputOperation::Finalize
        ) || matches!(operation, OutputOperation::BeginDocument(_))
            && config.envelope.is_some();
        if needs_root && !pending.header_written {
            writer
                .write_event(Event::Start(BytesStart::new(config.root.as_str())))
                .map_err(xml_err)?;
            pending.header_written = true;
        }
        match operation {
            OutputOperation::Record(record) => {
                if self
                    .plan_cache
                    .as_ref()
                    .is_none_or(|p| !p.schema.matches(record.schema()))
                {
                    let fields = record
                        .schema()
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| {
                            config.include_engine_stamped || !record.schema().is_engine_stamped(*i)
                        })
                        .map(|(i, n)| (i, n.as_ref()));
                    pending.replacement = Some(PreparedPlanCache {
                        schema: record.schema().try_identity(workspace.allocation())?,
                        plan: admitted_tree(fields, config, workspace)?,
                    });
                }
                let cache = pending
                    .replacement
                    .as_ref()
                    .or(self.plan_cache.as_ref())
                    .ok_or_else(|| {
                        crate::preparation::ResourceError::new(
                            crate::preparation::ResourceErrorKind::Authority,
                            0,
                            0,
                        )
                    })?;
                let values = RecordFields::new(record);
                validate_prepared_body(&cache.plan.root, &values, config, workspace)?;
                prepared_element(
                    &mut writer,
                    config.record.as_str(),
                    &cache.plan,
                    &values,
                    config,
                )?;
                pending.records = pending.records.saturating_add(1);
            }
            OutputOperation::BeginDocument(doc) => {
                if let Some(envelope) = &config.envelope {
                    writer
                        .write_event(Event::Start(BytesStart::new("Document")))
                        .map_err(xml_err)?;
                    if let Some(fields) = envelope.header_fields(doc) {
                        let values = SectionFields::new(fields, None);
                        let plan = admitted_tree(values.names(), config, workspace)?;
                        validate_prepared_body(&plan.root, &values, config, workspace)?;
                        prepared_element(&mut writer, "header", &plan, &values, config)?;
                    }
                    pending.records = 0;
                }
            }
            OutputOperation::EndDocument(doc) => {
                if let Some(envelope) = &config.envelope {
                    if let Some(fields) = envelope.footer_fields(doc) {
                        let count = envelope
                            .count_name()
                            .map(|name| (name, self.records as i64));
                        let values = SectionFields::new(fields, count);
                        let plan = admitted_tree(values.names(), config, workspace)?;
                        validate_prepared_body(&plan.root, &values, config, workspace)?;
                        prepared_element(&mut writer, "footer", &plan, &values, config)?;
                    }
                    writer
                        .write_event(Event::End(BytesEnd::new("Document")))
                        .map_err(xml_err)?;
                }
            }
            OutputOperation::Finalize => {
                writer
                    .write_event(Event::End(BytesEnd::new(config.root.as_str())))
                    .map_err(xml_err)?;
                pending.finalized = true;
            }
        }
        Ok(pending)
    }
    fn commit(&mut self, pending: XmlPending) {
        if pending.finalized {
            self.plan_cache = None;
        } else if let Some(cache) = pending.replacement {
            self.plan_cache = Some(cache);
        }
        self.header_written = pending.header_written;
        self.records = pending.records;
    }
}

#[derive(Clone)]
pub struct XmlWriterConfig {
    pub root_element: String,
    pub record_element: String,
    pub preserve_nulls: bool,
    /// Field-name prefix that marks a field as an XML attribute of its
    /// enclosing element rather than a child element. Mirrors the reader's
    /// `attribute_prefix` (default `@`) so attribute-derived fields
    /// round-trip: a top-level `@id` attaches to the record element's start
    /// tag, a nested `Address.@type` attaches to the `<Address>` branch.
    /// An empty prefix disables attribute classification entirely — every
    /// field emits as an element.
    pub attribute_prefix: String,
    /// Whether engine-stamped schema columns (`$ck.<field>` correlation
    /// snapshots) emit as nested elements. Defaults to `false` so
    /// engine-internal namespaces stay out of the XML output.
    pub include_engine_stamped: bool,
    /// Per-document envelope reconstruction. `None` (the default) keeps the
    /// flat `<Root><Record/>…</Root>` output byte-identical. `Some` is set by
    /// the executor under `reconstruct_envelope: true` and wraps each document
    /// in a `<Document>` frame with header/footer elements.
    pub envelope: Option<OutputEnvelopeSpec>,
    /// Per-field overrides for how a `multiple:` field's values are emitted as
    /// repeated child elements. A `Value::Array` at any element field emits one
    /// child element per value, named after the field, unless an entry here
    /// names the field and sets `repeat_as` (the per-item element name) and/or
    /// `wrap_in` (a container element). Empty by default; populated from the
    /// output's `join_values`. Mirrors the CSV writer's `join_values` field —
    /// the two writers read disjoint sub-vocabularies of the same declaration.
    pub join_values: Vec<JoinValues>,
    /// Exact output-facing column names whose schema declares
    /// `multiple: true`. A top-level array is repeated only for one of these
    /// columns; arrays nested inside a neutral map remain ordinary structured
    /// XML values.
    pub declared_multiple: BTreeSet<String>,
}

impl Default for XmlWriterConfig {
    fn default() -> Self {
        Self {
            root_element: "Root".into(),
            record_element: "Record".into(),
            preserve_nulls: false,
            attribute_prefix: "@".into(),
            include_engine_stamped: false,
            envelope: None,
            join_values: Vec::new(),
            declared_multiple: BTreeSet::new(),
        }
    }
}

/// Preserve emitter I/O identity, including governed-stage resource failures.
fn xml_err(e: std::io::Error) -> FormatError {
    FormatError::Io(e)
}

#[derive(Clone, Copy)]
enum EscapeContext {
    Text,
    Attribute,
}

/// Write borrowed XML character data without first materializing an escaped
/// copy. Attribute whitespace keeps the existing character-reference spelling
/// so conforming readers cannot normalize tabs or line endings to spaces.
fn write_escaped<W: Write>(
    writer: &mut XmlEmitter<W>,
    raw: &str,
    context: EscapeContext,
) -> Result<(), FormatError> {
    let mut copied_through = 0;
    for (offset, ch) in raw.char_indices() {
        if offset - copied_through >= 4096 {
            writer
                .get_mut()
                .write_all(&raw.as_bytes()[copied_through..offset])
                .map_err(xml_err)?;
            copied_through = offset;
        }
        let replacement = match ch {
            '&' => Some("&amp;"),
            '<' => Some("&lt;"),
            '>' => Some("&gt;"),
            '"' => Some("&quot;"),
            '\'' => Some("&apos;"),
            '\t' if matches!(context, EscapeContext::Attribute) => Some("&#9;"),
            '\n' if matches!(context, EscapeContext::Attribute) => Some("&#10;"),
            '\r' if matches!(context, EscapeContext::Attribute) => Some("&#13;"),
            _ => None,
        };
        let Some(replacement) = replacement else {
            continue;
        };
        writer
            .get_mut()
            .write_all(&raw.as_bytes()[copied_through..offset])
            .map_err(xml_err)?;
        writer
            .get_mut()
            .write_all(replacement.as_bytes())
            .map_err(xml_err)?;
        copied_through = offset + ch.len_utf8();
    }
    writer
        .get_mut()
        .write_all(&raw.as_bytes()[copied_through..])
        .map_err(xml_err)
}

fn begin_start_tag<W: Write>(writer: &mut XmlEmitter<W>, name: &str) -> Result<(), FormatError> {
    writer.get_mut().write_all(b"<").map_err(xml_err)?;
    writer.get_mut().write_all(name.as_bytes()).map_err(xml_err)
}

fn write_attribute<W: Write>(
    writer: &mut XmlEmitter<W>,
    name: &str,
    value: &str,
) -> Result<(), FormatError> {
    writer.get_mut().write_all(b" ").map_err(xml_err)?;
    writer
        .get_mut()
        .write_all(name.as_bytes())
        .map_err(xml_err)?;
    writer.get_mut().write_all(b"=\"").map_err(xml_err)?;
    write_escaped(writer, value, EscapeContext::Attribute)?;
    writer.get_mut().write_all(b"\"").map_err(xml_err)
}

fn finish_start_tag<W: Write>(writer: &mut XmlEmitter<W>, empty: bool) -> Result<(), FormatError> {
    writer
        .get_mut()
        .write_all(if empty { b"/>" } else { b">" })
        .map_err(xml_err)
}

/// True when `c` may begin an XML 1.0 `Name` (the `NameStartChar`
/// production). Covers the full Unicode ranges so an attribute name that
/// round-tripped from a source document with non-ASCII names is not
/// rejected on write-back.
fn is_xml_name_start_char(c: char) -> bool {
    matches!(c,
        ':' | 'A'..='Z' | '_' | 'a'..='z'
        | '\u{C0}'..='\u{D6}'
        | '\u{D8}'..='\u{F6}'
        | '\u{F8}'..='\u{2FF}'
        | '\u{370}'..='\u{37D}'
        | '\u{37F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}'
        | '\u{2070}'..='\u{218F}'
        | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}'
    )
}

/// True when `c` may appear after the first character of an XML 1.0 `Name`
/// (the `NameChar` production).
fn is_xml_name_char(c: char) -> bool {
    is_xml_name_start_char(c)
        || matches!(c,
            '-' | '.' | '0'..='9'
            | '\u{B7}'
            | '\u{0300}'..='\u{036F}'
            | '\u{203F}'..='\u{2040}'
        )
}

/// True when `name` is a well-formed XML 1.0 `Name`: a `NameStartChar`
/// followed by zero or more `NameChar`. Empty names are rejected.
///
/// quick-xml wraps attribute names as raw bytes (`QName`) without any
/// well-formedness check, so an illegal name (e.g. one containing a space
/// or `=`) would otherwise be written verbatim into the start tag and
/// corrupt the document.
///
/// Shared with the `record_path` grammar, which needs the same predicate to
/// reject a path segment no element can be named.
pub(crate) fn is_valid_xml_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if is_xml_name_start_char(first) => {}
        _ => return false,
    }
    chars.all(is_xml_name_char)
}

/// Reject a tag name quick-xml would otherwise write verbatim into a start
/// tag via `BytesStart::new`. Element (leaf / branch) names derive from
/// user field names and the configured root / record element names, so a
/// name with a leading digit, a space, or an illegal character (e.g. a
/// field literally named `1st` or `a b`) would emit malformed markup while
/// still reporting run success. `context` describes the name's origin for
/// the diagnostic (e.g. `"field 'X': element"`, `"root element"`).
const SCALAR_TEXT_CAPACITY: usize = 128;

struct ScalarBuffer {
    bytes: [u8; SCALAR_TEXT_CAPACITY],
    len: usize,
}

impl ScalarBuffer {
    fn new() -> Self {
        Self {
            bytes: [0; SCALAR_TEXT_CAPACITY],
            len: 0,
        }
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len])
            .expect("fmt::Write only accepts valid UTF-8 strings")
    }
}

impl std::fmt::Write for ScalarBuffer {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let end = self.len.checked_add(text.len()).ok_or(std::fmt::Error)?;
        let destination = self.bytes.get_mut(self.len..end).ok_or(std::fmt::Error)?;
        destination.copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

enum ScalarText<'a> {
    Borrowed(&'a str),
    Formatted(ScalarBuffer),
}

impl ScalarText<'_> {
    fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(text) => text,
            Self::Formatted(text) => text.as_str(),
        }
    }
}

/// Borrow authored strings and format every other scalar into fixed stack
/// storage. The largest supported scalar representation is bounded by its Rust
/// type, so record width and input string length cannot grow this scratch.
fn scalar_text<'a>(col: &str, val: &'a Value) -> Result<ScalarText<'a>, FormatError> {
    use std::fmt::Write as _;
    let mut buf = ScalarBuffer::new();
    let overflow = || {
        FormatError::Xml(format!(
            "field '{col}': scalar rendering exceeds the XML writer's fixed bound"
        ))
    };
    match val {
        Value::Null => return Ok(ScalarText::Borrowed("")),
        Value::Bool(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::Integer(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::Float(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::Decimal(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::String(value) => return Ok(ScalarText::Borrowed(value.as_str())),
        Value::Date(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::DateTime(value) => write!(&mut buf, "{value}").map_err(|_| overflow())?,
        Value::Array(_) => {
            return Err(FormatError::UnserializableArrayValue {
                format: "XML",
                column: col.to_string(),
            });
        }
        Value::Map(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "XML",
                column: col.to_string(),
            });
        }
    }
    Ok(ScalarText::Formatted(buf))
}

trait FieldSource {
    fn field(&self, index: usize) -> (&str, &Value);
}

struct RecordFields<'a> {
    record: &'a Record,
}

impl<'a> RecordFields<'a> {
    fn new(record: &'a Record) -> Self {
        Self { record }
    }
}

impl FieldSource for RecordFields<'_> {
    fn field(&self, index: usize) -> (&str, &Value) {
        (
            self.record.schema().columns()[index].as_ref(),
            &self.record.values()[index],
        )
    }
}

struct SectionFields<'a> {
    fields: &'a indexmap::IndexMap<OwnedKey, Value>,
    count: Option<(&'a str, Value)>,
}

impl<'a> SectionFields<'a> {
    fn new(fields: &'a indexmap::IndexMap<OwnedKey, Value>, count: Option<(&'a str, i64)>) -> Self {
        Self {
            fields,
            count: count.map(|(name, value)| (name, Value::Integer(value))),
        }
    }

    fn names(&self) -> impl Iterator<Item = (usize, &str)> + Clone {
        self.fields
            .keys()
            .enumerate()
            .map(|(index, name)| (index, name.as_ref()))
            .chain(
                self.count
                    .iter()
                    .map(|(name, _)| (self.fields.len(), *name)),
            )
    }
}

impl FieldSource for SectionFields<'_> {
    fn field(&self, index: usize) -> (&str, &Value) {
        if let Some((name, value)) = self.fields.get_index(index) {
            return (name.as_ref(), value);
        }
        let (name, value) = self
            .count
            .as_ref()
            .expect("the schema plan references the optional count field");
        (*name, value)
    }
}

// ── Precompiled record tree plan ─────────────────────────────────────

/// A record's precompiled element-tree shape: the record element's body
/// (attributes + child nodes) with per-node element names and the field index
/// each terminal reads its value from. Built once per schema identity.
struct TreePlan {
    root: PlanBody,
}

/// One element's precompiled body: the attributes on its start tag plus its
/// child nodes. Values are not stored — each terminal carries the field index
/// to borrow from the current record.
struct PlanBody {
    attrs: ReservedVec<PlanAttr>,
    children: ReservedVec<PlanNode>,
}

/// A precompiled attribute: its (validated) XML name and the field index whose
/// value it borrows.
struct PlanAttr {
    name: ReservedText,
    field: usize,
}

/// A precompiled child node: a leaf element or a nested branch.
enum PlanNode {
    Leaf {
        name: ReservedText,
        field: usize,
        /// Whether this top-level schema field declares `multiple: true`.
        /// Used only to admit a top-level array; nested arrays inside maps use
        /// the neutral recursive writer contract instead.
        declared_multiple: bool,
        /// Per-item naming when this leaf's value is a `Value::Array`. `None`
        /// emits bare repeats named after the leaf; `Some` carries the
        /// `repeat_as` / `wrap_in` overrides from the field's `join_values`
        /// entry (validated at plan build). A scalar value ignores this.
        repeat: Option<XmlRepeat>,
    },
    Branch {
        name: ReservedText,
        body: PlanBody,
    },
}

/// How a `multiple:` field's repeated child elements are named, resolved from a
/// `join_values` entry at plan build. Both names are validated as legal XML
/// names when the plan is built, so a malformed override fails the write cleanly
/// before any byte is emitted — the same point the element/attribute names are
/// checked.
struct XmlRepeat {
    /// Element name emitted per array item (a `repeat_as`, or the leaf's own
    /// element name when the entry did not set one).
    item_name: ReservedText,
    /// Optional container element wrapping the repeated items (`wrap_in`).
    wrap_in: Option<ReservedText>,
}

fn field_emits(value: &Value, is_attribute: bool, preserve_nulls: bool) -> bool {
    if is_attribute {
        return !value.is_null();
    }
    match value {
        Value::Null => preserve_nulls,
        Value::Array(items) => !items.is_empty(),
        Value::Map(_) => true,
        _ => true,
    }
}

fn write_planned_start<W: Write, S: FieldSource>(
    writer: &mut XmlEmitter<W>,
    name: &str,
    attrs: &[PlanAttr],
    values: &S,
    empty: bool,
) -> Result<(), FormatError> {
    begin_start_tag(writer, name)?;
    for attr in attrs {
        let (field, value) = values.field(attr.field);
        if value.is_null() {
            continue;
        }
        let text = scalar_text(field, value)?;
        write_attribute(writer, attr.name.as_str(), text.as_str())?;
    }
    finish_start_tag(writer, empty)
}

/// Emit a body's child nodes directly from borrowed field values. A branch is
/// emitted only when it has at least one emitting attribute or child, and
/// self-closes when it has no emitting children, preserving the existing null
/// and empty-container decisions byte for byte.
fn emit_body<W: Write, S: FieldSource>(
    writer: &mut XmlEmitter<W>,
    body: &PlanBody,
    values: &S,
    preserve_nulls: bool,
    attribute_prefix: &str,
) -> Result<(), FormatError> {
    for child in body.children.as_slice() {
        match child {
            PlanNode::Leaf {
                name,
                field,
                repeat,
                ..
            } => {
                let (field_name, value) = values.field(*field);
                if !field_emits(value, false, preserve_nulls) {
                    continue;
                }
                if matches!(value, Value::Array(_) | Value::Map(_)) {
                    emit_structured_leaf(
                        writer,
                        name.as_str(),
                        repeat,
                        value,
                        preserve_nulls,
                        attribute_prefix,
                    )?;
                    continue;
                }
                // A scalar on a field carrying a `repeat_as` / `wrap_in` override
                // is a one-element sequence: apply the same naming an array of
                // length one would get, so the emitted shape does not depend on
                // whether a lone value arrived wrapped (`[x]`) or bare (`x`) —
                // symmetric with the reader normalizing a lone scalar to a
                // one-element array. A field with no override (`repeat` is
                // `None`) keeps the plain `<name>text</name>` rendering.
                if repeat.is_some() {
                    emit_scalar_leaf(writer, name.as_str(), repeat, field_name, value)?;
                    continue;
                }
                emit_scalar_element(writer, name.as_str(), field_name, value)?;
            }
            PlanNode::Branch { name, body } => {
                let has_attrs = body
                    .attrs
                    .as_slice()
                    .iter()
                    .any(|attr| field_emits(values.field(attr.field).1, true, preserve_nulls));
                let has_children = body
                    .children
                    .as_slice()
                    .iter()
                    .any(|child| node_emits(child, values, preserve_nulls));
                if !has_attrs && !has_children {
                    continue;
                }
                if has_children {
                    write_planned_start(
                        writer,
                        name.as_str(),
                        body.attrs.as_slice(),
                        values,
                        false,
                    )?;
                    emit_body(writer, body, values, preserve_nulls, attribute_prefix)?;
                    writer
                        .write_event(Event::End(BytesEnd::new(name.as_str())))
                        .map_err(xml_err)?;
                } else {
                    write_planned_start(
                        writer,
                        name.as_str(),
                        body.attrs.as_slice(),
                        values,
                        true,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Emit one schema leaf whose value is a native map or array. Array items reuse
/// the leaf (or configured `repeat_as`) name; a map is one structured item.
fn emit_structured_leaf<W: Write>(
    writer: &mut XmlEmitter<W>,
    leaf_name: &str,
    repeat: &Option<XmlRepeat>,
    value: &Value,
    preserve_nulls: bool,
    attribute_prefix: &str,
) -> Result<(), FormatError> {
    let item_name = repeat.as_ref().map_or(leaf_name, |r| r.item_name.as_str());
    let wrap_in = repeat
        .as_ref()
        .and_then(|r| r.wrap_in.as_ref().map(ReservedText::as_str));
    if let Some(container) = wrap_in {
        writer
            .write_event(Event::Start(BytesStart::new(container)))
            .map_err(xml_err)?;
    }

    match value {
        Value::Array(items) => {
            for item in items {
                emit_named_value(writer, item_name, item, preserve_nulls, attribute_prefix)?;
            }
        }
        Value::Map(_) => {
            emit_named_value(writer, item_name, value, preserve_nulls, attribute_prefix)?
        }
        _ => unreachable!("structured slots contain only maps and arrays"),
    }

    if let Some(container) = wrap_in {
        writer
            .write_event(Event::End(BytesEnd::new(container)))
            .map_err(xml_err)?;
    }
    Ok(())
}

/// Emit a named native value recursively. Validation has already established
/// legal names, scalar-only attributes/text, canonical keys, and bounded depth.
fn emit_named_value<W: Write>(
    writer: &mut XmlEmitter<W>,
    name: &str,
    value: &Value,
    preserve_nulls: bool,
    attribute_prefix: &str,
) -> Result<(), FormatError> {
    match value {
        Value::Null if !preserve_nulls => Ok(()),
        Value::Null => writer
            .write_event(Event::Empty(BytesStart::new(name)))
            .map_err(xml_err),
        Value::Array(items) => {
            for item in items {
                emit_named_value(writer, name, item, preserve_nulls, attribute_prefix)?;
            }
            Ok(())
        }
        Value::Map(entries) => {
            begin_start_tag(writer, name)?;
            for (raw_key, child) in entries.iter() {
                let key = NestedKey::decode(raw_key).expect("keys validated before emission");
                let is_attribute = !key.escaped
                    && !attribute_prefix.is_empty()
                    && key.text.starts_with(attribute_prefix);
                if is_attribute && !child.is_null() {
                    let attr_name = &key.text[attribute_prefix.len()..];
                    let text = scalar_text(raw_key, child)?;
                    write_attribute(writer, attr_name, text.as_str())?;
                }
            }

            if !map_has_content(entries, preserve_nulls, attribute_prefix) {
                return finish_start_tag(writer, true);
            }
            finish_start_tag(writer, false)?;
            for (raw_key, child) in entries.iter() {
                let key = NestedKey::decode(raw_key).expect("keys validated before emission");
                let is_attribute = !key.escaped
                    && !attribute_prefix.is_empty()
                    && key.text.starts_with(attribute_prefix);
                if is_attribute {
                    continue;
                }
                if !key.escaped && key.text == "#text" {
                    if !child.is_null() {
                        let text = scalar_text(raw_key, child)?;
                        if !text.as_str().is_empty() {
                            write_escaped(writer, text.as_str(), EscapeContext::Text)?;
                        }
                    }
                    continue;
                }
                emit_named_value(writer, &key.text, child, preserve_nulls, attribute_prefix)?;
            }
            writer
                .write_event(Event::End(BytesEnd::new(name)))
                .map_err(xml_err)
        }
        _ => emit_scalar_element(writer, name, name, value),
    }
}

fn map_has_content(
    entries: &indexmap::IndexMap<OwnedKey, Value>,
    preserve_nulls: bool,
    attribute_prefix: &str,
) -> bool {
    entries.iter().any(|(raw_key, child)| {
        let key = NestedKey::decode(raw_key).expect("keys validated before emission");
        let is_attribute =
            !key.escaped && !attribute_prefix.is_empty() && key.text.starts_with(attribute_prefix);
        if is_attribute {
            return false;
        }
        if !key.escaped && key.text == "#text" {
            return match child {
                Value::Null => false,
                Value::String(text) => !text.is_empty(),
                _ => true,
            };
        }
        value_emits(child, preserve_nulls)
    })
}

fn value_emits(value: &Value, preserve_nulls: bool) -> bool {
    match value {
        Value::Null => preserve_nulls,
        Value::Array(items) => items.iter().any(|item| value_emits(item, preserve_nulls)),
        Value::Map(_) => true,
        _ => true,
    }
}

fn emit_scalar_element<W: Write>(
    writer: &mut XmlEmitter<W>,
    element_name: &str,
    field_name: &str,
    value: &Value,
) -> Result<(), FormatError> {
    let text = scalar_text(field_name, value)?;
    if text.as_str().is_empty() {
        writer
            .write_event(Event::Empty(BytesStart::new(element_name)))
            .map_err(xml_err)
    } else {
        writer
            .write_event(Event::Start(BytesStart::new(element_name)))
            .map_err(xml_err)?;
        write_escaped(writer, text.as_str(), EscapeContext::Text)?;
        writer
            .write_event(Event::End(BytesEnd::new(element_name)))
            .map_err(xml_err)
    }
}

/// Emit one scalar through the same item/wrapper naming used for a one-element
/// array, without cloning its rendered text into a temporary collection.
fn emit_scalar_leaf<W: Write>(
    writer: &mut XmlEmitter<W>,
    leaf_name: &str,
    repeat: &Option<XmlRepeat>,
    field_name: &str,
    value: &Value,
) -> Result<(), FormatError> {
    let item_name = repeat.as_ref().map_or(leaf_name, |r| r.item_name.as_str());
    let wrap_in = repeat
        .as_ref()
        .and_then(|r| r.wrap_in.as_ref().map(ReservedText::as_str));
    if let Some(container) = wrap_in {
        writer
            .write_event(Event::Start(BytesStart::new(container)))
            .map_err(xml_err)?;
    }
    emit_scalar_element(writer, item_name, field_name, value)?;
    if let Some(container) = wrap_in {
        writer
            .write_event(Event::End(BytesEnd::new(container)))
            .map_err(xml_err)?;
    }
    Ok(())
}

/// Whether a node contributes any output for the current record: a leaf emits
/// from its borrowed value; a branch emits when any attribute or descendant
/// does.
fn node_emits<S: FieldSource>(node: &PlanNode, values: &S, preserve_nulls: bool) -> bool {
    match node {
        PlanNode::Leaf { field, .. } => field_emits(values.field(*field).1, false, preserve_nulls),
        PlanNode::Branch { body, .. } => {
            body.attrs
                .as_slice()
                .iter()
                .any(|attr| field_emits(values.field(attr.field).1, true, preserve_nulls))
                || body
                    .children
                    .as_slice()
                    .iter()
                    .any(|child| node_emits(child, values, preserve_nulls))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preparation::MemoryOnlyResources;
    use crate::traits::FormatReader;
    use crate::xml::reader::{XmlReader, XmlReaderConfig};
    use clinker_record::owned_storage::{OwnedMap, OwnedValues};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct ByteCounter(Arc<AtomicUsize>);

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
        assert_eq!(*format, "XML");
        assert_eq!(*actual, kind);
        assert_eq!(*actual_field, field);
        assert_eq!(
            field_name.to_string(),
            crate::error::OutputFieldName::new(name).to_string()
        );
    }

    impl ByteCounter {
        fn bytes(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl Write for ByteCounter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.fetch_add(buf.len(), Ordering::Relaxed);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn test_schema() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into(), "age".into()])))
    }

    fn make_record(schema: &SharedStorage<Schema>, name: &str, age: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![Value::String(name.into()), Value::Integer(age)],
        )
    }

    fn write_records(
        config: XmlWriterConfig,
        records: &[Record],
        schema: &SharedStorage<Schema>,
    ) -> String {
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
        drop(w);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_xml_write_basic_structure() {
        let schema = test_schema();
        let records = vec![
            make_record(&schema, "Alice", 30),
            make_record(&schema, "Bob", 25),
        ];
        let output = write_records(XmlWriterConfig::default(), &records, &schema);
        assert!(output.contains("<Root>"));
        assert!(output.contains("</Root>"));
        assert!(output.contains("<Record>"));
        assert!(output.contains("</Record>"));
        assert!(output.contains("<name>Alice</name>"));
        assert!(output.contains("<age>30</age>"));
        // Should be valid XML — parse it
        let _ = quick_xml::Reader::from_str(&output);
    }

    #[test]
    fn test_xml_write_custom_elements() {
        let schema = test_schema();
        let records = vec![make_record(&schema, "Alice", 30)];
        let config = XmlWriterConfig {
            root_element: "Data".into(),
            record_element: "Row".into(),
            ..Default::default()
        };
        let output = write_records(config, &records, &schema);
        assert!(output.contains("<Data>"));
        assert!(output.contains("<Row>"));
        assert!(output.contains("</Row>"));
        assert!(output.contains("</Data>"));
    }

    #[test]
    fn test_xml_write_nested_expansion() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "Address.City".into(),
            "name".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("NYC".into()), Value::String("Alice".into())],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert!(
            output.contains("<Address><City>NYC</City></Address>"),
            "Dotted field should expand to nested elements: {output}"
        );
    }

    #[test]
    fn an_escaped_separator_stays_inside_one_element_name() {
        // `.` is a legal XML NameChar, so an escaped separator produces one
        // element rather than a nesting level. This is the replacement for the
        // literal dotted name that unconditional expansion takes away.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![r"a\.b".into()])));
        let record = Record::new(schema.clone(), vec![Value::String("v".into())]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert!(output.contains("<a.b>v</a.b>"), "{output}");

        // The reader flattens `<a.b>` back to the column `a.b` — unescaped, so
        // the name it hands back would nest on a second write. Closing that is
        // the read side's half of the grammar, tracked by
        // https://github.com/rustpunk/clinker/issues/920; pinned here so the
        // flip is deliberate.
        let mut reader = XmlReader::from_reader(
            std::io::Cursor::new(output.into_bytes()),
            XmlReaderConfig {
                record_path: Some("Root/Record".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let _s = reader.schema().unwrap();
        let back = reader.next_record().unwrap().unwrap();
        assert_eq!(back.get("a.b"), Some(&Value::String("v".into())));
    }

    #[test]
    fn a_column_that_is_also_a_container_is_refused() {
        // Previously emitted two sibling `<a>` elements, which this crate's own
        // reader then refused on the way back in.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a".into(), "a.b".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1), Value::Integer(2)]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = w.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlPath, 2, "a.b");
        drop(w);
        assert!(
            buf.is_empty(),
            "a refused column set emits no bytes, got: {:?}",
            String::from_utf8_lossy(&buf)
        );
    }

    #[test]
    fn test_xml_write_shared_prefix_grouping() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "Address.City".into(),
            "Address.State".into(),
            "name".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::String("NYC".into()),
                Value::String("NY".into()),
                Value::String("Alice".into()),
            ],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        // Should have ONE <Address> parent with two children
        assert_eq!(
            output.matches("<Address>").count(),
            1,
            "Should have exactly one <Address> parent: {output}"
        );
        assert!(output.contains("<City>NYC</City>"));
        assert!(output.contains("<State>NY</State>"));
    }

    #[test]
    fn test_xml_write_preserve_nulls_true() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a".into(), "b".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = XmlWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            output.contains("<b/>"),
            "Null field should be self-closing: {output}"
        );
    }

    #[test]
    fn test_xml_write_preserve_nulls_false() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["a".into(), "b".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("hello".into()), Value::Null],
        );
        let config = XmlWriterConfig {
            preserve_nulls: false,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert!(
            !output.contains("<b"),
            "Null field should be omitted: {output}"
        );
    }

    #[test]
    fn test_xml_write_escaping() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["val".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("a & b < c > d \"e\" 'f'".into())],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert!(output.contains("&amp;"), "& should be escaped: {output}");
        assert!(output.contains("&lt;"), "< should be escaped: {output}");
        assert!(output.contains("&gt;"), "> should be escaped: {output}");
        assert!(
            !output.contains("a & b"),
            "Raw & should not appear: {output}"
        );
    }

    #[test]
    fn test_xml_roundtrip_reader_writer() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into(), "value".into()])));
        let records = vec![
            Record::new(
                schema.clone(),
                vec![Value::String("Alice".into()), Value::Integer(42)],
            ),
            Record::new(
                schema.clone(),
                vec![Value::String("Bob".into()), Value::Integer(99)],
            ),
        ];

        // Write
        let output = write_records(
            XmlWriterConfig {
                preserve_nulls: true,
                ..Default::default()
            },
            &records,
            &schema,
        );

        // Read back
        let cursor = std::io::Cursor::new(output.as_bytes().to_vec());
        let mut reader = XmlReader::from_reader(
            cursor,
            XmlReaderConfig {
                record_path: Some("Root/Record".into()),
                ..Default::default()
            },
        )
        .expect("XML buffer read");
        let _s = reader.schema().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("value"), Some(&Value::Integer(42)));
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
    }

    /// Native maps use reserved `@...` and `#text` keys while ordinary keys
    /// become child elements. Arrays repeat their containing child name and
    /// preserve author insertion order.
    #[test]
    fn test_xml_writer_emits_recursive_map_and_array_values() {
        use indexmap::IndexMap;
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "payload".into()])));

        let mut first: IndexMap<OwnedKey, Value> = IndexMap::new();
        first.insert("@id".into(), Value::Integer(1));
        first.insert("#text".into(), Value::String("alpha".into()));
        let mut second: IndexMap<OwnedKey, Value> = IndexMap::new();
        second.insert("@id".into(), Value::Integer(2));
        second.insert("#text".into(), Value::String("beta".into()));

        let mut payload: IndexMap<OwnedKey, Value> = IndexMap::new();
        payload.insert("@kind".into(), Value::String("event".into()));
        payload.insert("#text".into(), Value::String("before".into()));
        payload.insert(
            "item".into(),
            Value::Array(OwnedValues::from_vec(vec![
                Value::Map(OwnedMap::from_map(first)),
                Value::Map(OwnedMap::from_map(second)),
            ])),
        );
        payload.insert("tail".into(), Value::String("after".into()));
        let record = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::Map(OwnedMap::from_map(payload))],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            "<Root><Record><id>7</id><payload kind=\"event\">before<item id=\"1\">alpha</item><item id=\"2\">beta</item><tail>after</tail></payload></Record></Root>"
        );
    }

    #[test]
    fn large_authored_text_does_not_grow_retained_preparation_state() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "@kind".into(),
            "payload".into(),
        ])));
        let large = "large <&> \"quoted\"\n".repeat(64 * 1024);
        let record = Record::new(
            schema.clone(),
            vec![
                Value::String(large.clone().into()),
                Value::String(large.into()),
            ],
        );
        let sink = ByteCounter::default();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(sink, encoder, provider.resources()).unwrap();

        writer
            .write_record(&Record::new(
                schema.clone(),
                vec![Value::String("small".into()), Value::String("small".into())],
            ))
            .unwrap();
        let retained = provider.used();
        writer.write_record(&record).expect("large record writes");

        assert_eq!(
            provider.used(),
            retained,
            "record-sized scalar preparation must not survive write_record",
        );
    }

    #[test]
    fn complete_validation_failures_add_no_record_bytes() {
        use indexmap::IndexMap;

        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["payload".into()])));
        let mut invalid_name = IndexMap::new();
        invalid_name.insert("1bad".into(), Value::Integer(1));

        let mut collision = IndexMap::new();
        collision.insert("@id".into(), Value::Integer(1));
        collision.insert("\\@id".into(), Value::Integer(2));

        let mut too_deep = Value::Null;
        for _ in 0..=clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH {
            too_deep = Value::Map(OwnedMap::from_map(IndexMap::from([(
                "next".into(),
                too_deep,
            )])));
        }

        for (case, invalid) in [
            (
                "malformed name",
                Value::Map(OwnedMap::from_map(invalid_name)),
            ),
            ("invalid text", Value::String("bad\u{1}".into())),
            ("excess depth", too_deep),
            (
                "decoded-key collision",
                Value::Map(OwnedMap::from_map(collision)),
            ),
        ] {
            let sink = ByteCounter::default();
            let observation = sink.clone();
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(
                schema.clone(),
                &XmlWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut writer = PreparedWriter::new(sink, encoder, provider.resources()).unwrap();
            writer
                .write_record(&Record::new(
                    schema.clone(),
                    vec![Value::String("valid".into())],
                ))
                .expect("control record writes");
            let before = observation.bytes();

            writer
                .write_record(&Record::new(schema.clone(), vec![invalid]))
                .expect_err(case);

            assert_eq!(
                observation.bytes(),
                before,
                "{case} must add zero bytes after an earlier valid record",
            );
        }
    }

    #[test]
    fn repeated_records_never_accumulate_preparation_state() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["payload".into()])));
        let sink = ByteCounter::default();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(sink, encoder, provider.resources()).unwrap();

        let mut retained = None;
        for width in [1, 4096, 17, 128 * 1024, 2] {
            let record = Record::new(
                schema.clone(),
                vec![Value::String("<&".repeat(width).into())],
            );
            writer.write_record(&record).expect("record writes");
            if let Some(retained) = retained {
                assert_eq!(provider.used(), retained);
            } else {
                retained = Some(provider.used());
            }
        }
    }

    #[test]
    fn test_xml_writer_rejects_duplicate_decoded_map_keys_before_output() {
        use indexmap::IndexMap;
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["payload".into()])));
        let mut payload: IndexMap<OwnedKey, Value> = IndexMap::new();
        payload.insert("@id".into(), Value::Integer(1));
        payload.insert("\\@id".into(), Value::Integer(2));
        let record = Record::new(
            schema.clone(),
            vec![Value::Map(OwnedMap::from_map(payload))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlValue, 1, "payload");
        drop(writer);
        assert!(buf.is_empty(), "rejected record must emit no partial XML");
    }

    #[test]
    fn test_xml_writer_rejects_collection_valued_nested_attribute_before_output() {
        use indexmap::IndexMap;
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["payload".into()])));
        let mut payload: IndexMap<OwnedKey, Value> = IndexMap::new();
        payload.insert(
            "@ids".into(),
            Value::Array(OwnedValues::from_vec(vec![Value::Integer(1)])),
        );
        let record = Record::new(
            schema.clone(),
            vec![Value::Map(OwnedMap::from_map(payload))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlValue, 1, "payload");
        drop(writer);
        assert!(buf.is_empty(), "rejected record must emit no partial XML");
    }

    /// Build a `[id, tags]` record whose `tags` field carries `values`.
    fn record_with_tags(schema: &SharedStorage<Schema>, id: i64, values: Vec<Value>) -> Record {
        Record::new(
            schema.clone(),
            vec![
                Value::Integer(id),
                Value::Array(OwnedValues::from_vec(values)),
            ],
        )
    }

    /// An XML writer config that admits arrays only for the named fields.
    fn xml_multiple_config(fields: &[&str]) -> XmlWriterConfig {
        XmlWriterConfig {
            declared_multiple: fields.iter().map(|field| (*field).to_string()).collect(),
            ..Default::default()
        }
    }

    /// A `join_values` config naming one field with the given XML overrides.
    fn xml_join_config(
        field: &str,
        repeat_as: Option<&str>,
        wrap_in: Option<&str>,
    ) -> XmlWriterConfig {
        XmlWriterConfig {
            declared_multiple: [field.to_string()].into_iter().collect(),
            join_values: vec![JoinValues {
                field: field.into(),
                repeat_as: repeat_as.map(str::to_string),
                wrap_in: wrap_in.map(str::to_string),
                ..JoinValues::bare(field)
            }],
            ..Default::default()
        }
    }

    /// A `multiple:` field emits one child element per value, in order, named
    /// after the field — the XML counterpart to CSV's delimited join (#916).
    #[test]
    fn test_xml_write_multi_value_emits_repeated_elements() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::String("a".into()), Value::String("b".into())],
        );
        let output = write_records(xml_multiple_config(&["tags"]), &[record], &schema);
        assert_eq!(
            output,
            "<Root><Record><id>7</id><tags>a</tags><tags>b</tags></Record></Root>"
        );
    }

    /// A single-element array yields exactly one element, byte-identical to a
    /// scalar field's output (criterion 3).
    #[test]
    fn test_xml_write_multi_value_single_element_matches_scalar() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let array = record_with_tags(&schema, 7, vec![Value::String("a".into())]);
        let array_out = write_records(xml_multiple_config(&["tags"]), &[array], &schema);
        let scalar = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::String("a".into())],
        );
        let scalar_out = write_records(XmlWriterConfig::default(), &[scalar], &schema);
        assert_eq!(array_out, scalar_out);
        assert_eq!(
            array_out,
            "<Root><Record><id>7</id><tags>a</tags></Record></Root>"
        );
    }

    /// An empty multi-value field emits nothing — no items and, with `wrap_in`
    /// set, no container either (criterion 3).
    #[test]
    fn test_xml_write_multi_value_empty_array_emits_nothing() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let bare = record_with_tags(&schema, 7, vec![]);
        assert_eq!(
            write_records(xml_multiple_config(&["tags"]), &[bare], &schema),
            "<Root><Record><id>7</id></Record></Root>"
        );
        let wrapped = record_with_tags(&schema, 7, vec![]);
        assert_eq!(
            write_records(
                xml_join_config("tags", Some("Tag"), Some("Tags")),
                &[wrapped],
                &schema
            ),
            "<Root><Record><id>7</id></Record></Root>",
            "an empty array emits no container even when wrap_in is set"
        );
    }

    /// An empty-string value renders to a self-closing item element, so a run of
    /// mixed present/empty values round-trips its per-item shape.
    #[test]
    fn test_xml_write_multi_value_empty_string_value_self_closes() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::String("a".into()), Value::String("".into())],
        );
        let output = write_records(xml_multiple_config(&["tags"]), &[record], &schema);
        assert_eq!(
            output,
            "<Root><Record><id>7</id><tags>a</tags><tags/></Record></Root>"
        );
    }

    /// `repeat_as` renames the per-item element; the field name is no longer the
    /// element name.
    #[test]
    fn test_xml_write_multi_value_repeat_as_renames_item() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::String("a".into()), Value::String("b".into())],
        );
        let output = write_records(
            xml_join_config("tags", Some("Tag"), None),
            &[record],
            &schema,
        );
        assert_eq!(
            output,
            "<Root><Record><id>7</id><Tag>a</Tag><Tag>b</Tag></Record></Root>"
        );
    }

    /// `wrap_in` alone adds a container around items still named after the field.
    #[test]
    fn test_xml_write_multi_value_wrap_in_adds_container() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::String("a".into()), Value::String("b".into())],
        );
        let output = write_records(
            xml_join_config("tags", None, Some("Tags")),
            &[record],
            &schema,
        );
        assert_eq!(
            output,
            "<Root><Record><id>7</id><Tags><tags>a</tags><tags>b</tags></Tags></Record></Root>"
        );
    }

    /// `repeat_as` and `wrap_in` together produce a named container with named
    /// items (criterion 2).
    #[test]
    fn test_xml_write_multi_value_repeat_as_and_wrap_in_together() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::String("a".into()), Value::String("b".into())],
        );
        let output = write_records(
            xml_join_config("tags", Some("Tag"), Some("Tags")),
            &[record],
            &schema,
        );
        assert_eq!(
            output,
            "<Root><Record><id>7</id><Tags><Tag>a</Tag><Tag>b</Tag></Tags></Record></Root>"
        );
    }

    /// A SCALAR value on a field carrying `repeat_as` / `wrap_in` is treated as
    /// a one-element sequence and gets the same container/item naming an array of
    /// length one would — so the output shape does not depend on whether a lone
    /// value arrived bare (`a`) or wrapped (`[a]`).
    #[test]
    fn test_xml_write_multi_value_scalar_applies_repeat_and_wrap() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let scalar = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::String("a".into())],
        );
        let scalar_out = write_records(
            xml_join_config("tags", Some("Tag"), Some("Tags")),
            &[scalar],
            &schema,
        );
        assert_eq!(
            scalar_out,
            "<Root><Record><id>7</id><Tags><Tag>a</Tag></Tags></Record></Root>"
        );
        // Byte-identical to the same lone value delivered as a one-element array.
        let array = record_with_tags(&schema, 7, vec![Value::String("a".into())]);
        let array_out = write_records(
            xml_join_config("tags", Some("Tag"), Some("Tags")),
            &[array],
            &schema,
        );
        assert_eq!(scalar_out, array_out);
    }

    /// An illegal `repeat_as` / `wrap_in` name fails the write with
    /// a bounded `OutputEncoding` error, leaving no partial output — element names are
    /// validated as legal XML names (criterion 5), like the record/root names.
    #[test]
    fn test_xml_write_multi_value_invalid_override_name_rejected() {
        for (repeat_as, wrap_in, bad) in [(Some("1bad"), None, "1bad"), (None, Some("a b"), "a b")]
        {
            let schema =
                SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
            let record = record_with_tags(&schema, 7, vec![Value::String("a".into())]);
            let mut buf = Vec::new();
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(
                schema.clone(),
                &xml_join_config("tags", repeat_as, wrap_in),
                provider.resources(),
            )
            .unwrap();
            let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            let err = writer.write_record(&record).unwrap_err();
            assert_encoding_error(&err, OutputEncodingKind::XmlName, 2, bad);
            drop(writer);
            assert!(
                buf.is_empty(),
                "no partial output before a rejected override name"
            );
        }
    }

    /// An array reaching an attribute-classified field is rejected — an XML
    /// attribute holds a single value and cannot repeat.
    #[test]
    fn test_xml_write_multi_value_array_on_attribute_rejected() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["@tags".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::Array(OwnedValues::from_vec(vec![Value::String(
                "a".into(),
            )]))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlValue, 1, "@tags");
        drop(writer);
        assert!(
            buf.is_empty(),
            "no partial output before a rejected attribute array"
        );
    }

    /// A nested collection inside a `multiple:` field (an array element that is
    /// itself an array) has no element body and is still rejected.
    #[test]
    fn test_xml_write_multi_value_nested_collection_element_rejected() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::Array(OwnedValues::from_vec(vec![Value::String(
                "a".into(),
            )]))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &xml_multiple_config(&["tags"]),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlValue, 2, "tags");
    }

    /// Read a document with repeated child elements into a `multiple:` column and
    /// write it back to XML: the repeated elements reappear byte-identically
    /// (criterion 4).
    #[test]
    fn test_xml_write_multi_value_read_write_round_trip() {
        let input = "<Root><Order><id>1</id><tags>a</tags><tags>b</tags></Order></Root>";
        let cursor = std::io::Cursor::new(input.as_bytes().to_vec());
        let mut reader = XmlReader::from_reader(
            cursor,
            XmlReaderConfig {
                record_path: Some("Root/Order".into()),
                multi_value_fields: vec!["tags".into()],
                ..Default::default()
            },
        )
        .expect("XML buffer read");
        let schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(
            record.get("tags"),
            Some(&Value::Array(OwnedValues::from_vec(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ])))
        );

        let output = write_records(
            XmlWriterConfig {
                record_element: "Order".into(),
                declared_multiple: ["tags".to_string()].into_iter().collect(),
                ..Default::default()
            },
            &[record],
            &schema,
        );
        assert_eq!(output, input, "repeated elements round-trip byte-for-byte");
    }

    /// Assert that writing a single record whose only field is `field`
    /// fails with `OutputEncoding` naming the offending segment and explaining the
    /// malformed element name, leaving no partial bytes behind.
    fn assert_element_name_rejected(field: &str) {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![field.into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(
            &err,
            OutputEncodingKind::XmlName,
            1,
            field.split('.').next().unwrap(),
        );
        drop(writer);
        assert!(
            buf.is_empty(),
            "rejected record must not leave partial output behind"
        );
    }

    #[test]
    fn test_xml_write_leaf_element_name_starting_with_digit_rejected() {
        // A digit is a NameChar but not a NameStartChar, so a field literally
        // named `1st` cannot become an XML element `<1st>`.
        assert_element_name_rejected("1st");
    }

    #[test]
    fn test_xml_write_leaf_element_name_with_space_rejected() {
        // A space is not an XML NameChar; writing it unvalidated would emit
        // `<first name>`, corrupting the start tag.
        assert_element_name_rejected("first name");
    }

    #[test]
    fn test_xml_write_branch_element_name_invalid_rejected() {
        // The dotted branch segment `1bad` cannot begin an XML name, so
        // `1bad.city` is rejected before an `<1bad>` branch is emitted.
        assert_element_name_rejected("1bad.city");
    }

    #[test]
    fn test_xml_write_unicode_leaf_element_name_accepted() {
        // `café` is a well-formed XML name (`é` is a NameChar), so the new
        // element-name validation does not over-reject non-ASCII field names.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["café".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, "<Root><Record><café>1</café></Record></Root>");
    }

    #[test]
    fn test_xml_write_invalid_record_element_name_rejected() {
        // The configured record element name flows straight into
        // `BytesStart::new`; a malformed one fails loud before any output.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into()])));
        let config = XmlWriterConfig {
            record_element: "1record".into(),
            ..Default::default()
        };
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let err = XmlEncoder::new(schema, &config, provider.resources())
            .err()
            .expect("invalid configured name is rejected before destination creation");
        assert_encoding_error(&err, OutputEncodingKind::XmlName, 1, "1record");
        assert_eq!(
            provider.used(),
            0,
            "failed construction releases its allocations"
        );
    }

    #[test]
    fn test_xml_write_invalid_root_element_name_rejected() {
        // The configured root element name is validated during construction, so a
        // malformed one fails loud rather than emitting `<1root>`.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into()])));
        let config = XmlWriterConfig {
            root_element: "1root".into(),
            ..Default::default()
        };
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let err = XmlEncoder::new(schema, &config, provider.resources())
            .err()
            .expect("invalid configured name is rejected before destination creation");
        assert_encoding_error(&err, OutputEncodingKind::XmlName, 1, "1root");
        assert_eq!(
            provider.used(),
            0,
            "failed construction releases its allocations"
        );
    }

    #[test]
    fn test_xml_write_attribute_prefixed_field_as_record_attribute() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["@id".into(), "name".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::String("A".into())],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record id="7"><name>A</name></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_nested_attribute_attaches_to_branch() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "Address.@type".into(),
            "Address.City".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("home".into()), Value::String("NYC".into())],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record><Address type="home"><City>NYC</City></Address></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_attribute_only_branch_self_closes() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["Address.@type".into()])));
        let record = Record::new(schema.clone(), vec![Value::String("home".into())]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record><Address type="home"/></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_custom_attribute_prefix() {
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["_id".into(), "name".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::Integer(7), Value::String("A".into())],
        );
        let config = XmlWriterConfig {
            attribute_prefix: "_".into(),
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record id="7"><name>A</name></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_default_prefix_leaves_underscore_field_as_element() {
        // Only the configured prefix classifies a field as an attribute;
        // `_id` is a valid element name under the default `@` prefix.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["_id".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(7)]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, "<Root><Record><_id>7</_id></Record></Root>");
    }

    #[test]
    fn test_xml_write_empty_prefix_disables_attribute_classification() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["_id".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(7)]);
        let config = XmlWriterConfig {
            attribute_prefix: String::new(),
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert_eq!(output, "<Root><Record><_id>7</_id></Record></Root>");
    }

    #[test]
    fn test_xml_write_null_attribute_dropped_even_with_preserve_nulls() {
        // A null element round-trips as a self-closing tag; an attribute
        // has no form that reads back as null, so it is dropped instead of
        // being emitted as an empty string.
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["@id".into(), "name".into()])));
        let record = Record::new(schema.clone(), vec![Value::Null, Value::Null]);
        let config = XmlWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert_eq!(output, "<Root><Record><name/></Record></Root>");
    }

    #[test]
    fn test_xml_write_attribute_value_escaped() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["@note".into()])));
        let record = Record::new(
            schema.clone(),
            vec![Value::String("a & \"b\" <c>\td\ne".into())],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record note="a &amp; &quot;b&quot; &lt;c&gt;&#9;d&#10;e"></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_attribute_whitespace_roundtrips_exactly() {
        // Literal tab / LF in an attribute value are written as character
        // references — a conformant parser would collapse the raw characters
        // to spaces (attribute-value normalization), but references resolve
        // back to the exact bytes.
        let schema =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["@note".into(), "name".into()])));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::String("line1\nline2\tend".into()),
                Value::String("A".into()),
            ],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);

        let cursor = std::io::Cursor::new(output.into_bytes());
        let mut reader = XmlReader::from_reader(
            cursor,
            XmlReaderConfig {
                record_path: Some("Root/Record".into()),
                ..Default::default()
            },
        )
        .expect("XML buffer read");
        let _s = reader.schema().unwrap();
        let read_back = reader.next_record().unwrap().unwrap();
        assert_eq!(
            read_back.get("@note"),
            Some(&Value::String("line1\nline2\tend".into()))
        );
    }

    #[test]
    fn test_xml_write_map_valued_attribute_rejected() {
        use indexmap::IndexMap;
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["@meta".into()])));
        let mut sidecar: IndexMap<OwnedKey, Value> = IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        let record = Record::new(
            schema.clone(),
            vec![Value::Map(OwnedMap::from_map(sidecar))],
        );
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlValue, 1, "@meta");
    }

    #[test]
    fn test_xml_write_attribute_segment_with_children_rejected() {
        // `@a.b` would need `@a` to be an element to hold `b` — an
        // attribute is a leaf, so the field is rejected instead of
        // emitting an `<@a>` element (invalid XML name).
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["@a.b".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlPath, 1, "@a.b");
        drop(writer);
        assert!(
            buf.is_empty(),
            "rejected record must not leave partial output behind"
        );
    }

    /// Assert that writing a single record whose only field is `field`
    /// fails with `OutputEncoding` identifying the field position and the
    /// stripped attribute name, and leaves no partial bytes behind.
    fn assert_attribute_name_rejected(field: &str, attr_name: &str) {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![field.into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let provider =
            MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        let err = writer.write_record(&record).unwrap_err();
        assert_encoding_error(&err, OutputEncodingKind::XmlName, 1, attr_name);
        drop(writer);
        assert!(
            buf.is_empty(),
            "rejected record must not leave partial output behind"
        );
    }

    #[test]
    fn test_xml_write_attribute_name_with_whitespace_rejected() {
        // A space is not an XML NameChar; writing it unvalidated would emit
        // `<Record foo bar="1">`, splitting one attribute into two tokens.
        assert_attribute_name_rejected("@foo bar", "foo bar");
    }

    #[test]
    fn test_xml_write_attribute_name_with_metacharacters_rejected() {
        for (field, name) in [
            ("@a=b", "a=b"),
            ("@a\"b", "a\"b"),
            ("@a/b", "a/b"),
            ("@a>b", "a>b"),
        ] {
            assert_attribute_name_rejected(field, name);
        }
    }

    #[test]
    fn test_xml_write_attribute_name_starting_with_digit_rejected() {
        // A digit is a NameChar but not a NameStartChar, so `1st` cannot
        // begin an XML name.
        assert_attribute_name_rejected("@1st", "1st");
    }

    #[test]
    fn test_xml_write_attribute_name_with_unicode_start_char_accepted() {
        // `é` (U+00E9) is a valid NameStartChar, so a non-ASCII attribute
        // name that round-tripped from a source document writes back
        // unchanged rather than being rejected.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["@café".into()])));
        let record = Record::new(schema.clone(), vec![Value::Integer(1)]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, r#"<Root><Record café="1"></Record></Root>"#);
    }

    #[test]
    fn test_xml_attribute_roundtrip_reader_writer() {
        // The reader flattens attributes to `@`-prefixed fields; writing
        // those records back must restore them as attributes, never emit
        // an `@`-named element.
        let input = r#"<Root><Record id="7" status="open"><name>A</name><Address type="home"><City>NYC</City></Address></Record></Root>"#;
        let cursor = std::io::Cursor::new(input.as_bytes().to_vec());
        let mut reader = XmlReader::from_reader(
            cursor,
            XmlReaderConfig {
                record_path: Some("Root/Record".into()),
                ..Default::default()
            },
        )
        .expect("XML buffer read");
        let schema = reader.schema().unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());

        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, input);
        assert!(
            !output.contains("<@"),
            "no @-named element may be emitted: {output}"
        );
    }

    #[test]
    fn test_xml_write_wide_dotted_and_attribute_golden() {
        // Golden byte-exact output for a wide schema mixing top-level fields,
        // record attributes, and shared-prefix dotted branches with their own
        // attributes — the shape the precompiled plan targets.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "@id".into(),
            "name".into(),
            "Address.@type".into(),
            "Address.City".into(),
            "Address.State".into(),
            "Contact.Email".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::Integer(7),
                Value::String("Alice".into()),
                Value::String("home".into()),
                Value::String("NYC".into()),
                Value::String("NY".into()),
                Value::String("a@example.com".into()),
            ],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record id="7"><name>Alice</name><Address type="home"><City>NYC</City><State>NY</State></Address><Contact><Email>a@example.com</Email></Contact></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_plan_reused_across_records() {
        // The plan is memoized by schema identity, so many records of one
        // schema reuse it. Each record must still render its own values.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "Address.City".into(),
            "name".into(),
        ])));
        let records: Vec<Record> = [("NYC", "Alice"), ("LA", "Bob"), ("SF", "Carol")]
            .into_iter()
            .map(|(city, name)| {
                Record::new(
                    schema.clone(),
                    vec![Value::String(city.into()), Value::String(name.into())],
                )
            })
            .collect();
        let output = write_records(XmlWriterConfig::default(), &records, &schema);
        assert_eq!(
            output,
            "<Root>\
             <Record><Address><City>NYC</City></Address><name>Alice</name></Record>\
             <Record><Address><City>LA</City></Address><name>Bob</name></Record>\
             <Record><Address><City>SF</City></Address><name>Carol</name></Record>\
             </Root>"
        );
    }

    #[test]
    fn test_xml_write_all_null_branch_suppressed_across_records() {
        // Under preserve_nulls:false a branch whose descendants are all null is
        // never opened; a later record filling the same branch still emits it.
        // Exercises per-record presence pruning over the shared plan.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "Address.City".into(),
            "Address.State".into(),
            "name".into(),
        ])));
        let all_null_branch = Record::new(
            schema.clone(),
            vec![Value::Null, Value::Null, Value::String("Alice".into())],
        );
        let branch_present = Record::new(
            schema.clone(),
            vec![
                Value::String("NYC".into()),
                Value::Null,
                Value::String("Bob".into()),
            ],
        );
        let output = write_records(
            XmlWriterConfig::default(),
            &[all_null_branch, branch_present],
            &schema,
        );
        assert_eq!(
            output,
            "<Root>\
             <Record><name>Alice</name></Record>\
             <Record><Address><City>NYC</City></Address><name>Bob</name></Record>\
             </Root>"
        );
    }

    /// XML self-describes each record from its OWN schema, so a column that
    /// appears only on a later record is emitted on that record and absent on
    /// the earlier one — no shared header to pin, nothing dropped. This is why
    /// XML needs no batch-union pass (unlike the CSV writer, issue #805): its
    /// per-record projection is already lossless under `auto_widen` drift.
    #[test]
    fn test_xml_write_late_widening_is_lossless_per_record() {
        let schema1 = SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into()])));
        let schema2 =
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "region".into()])));
        let r1 = Record::new(schema1.clone(), vec![Value::Integer(1)]);
        let r2 = Record::new(
            schema2.clone(),
            vec![Value::Integer(2), Value::String("US".into())],
        );
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(
                schema1.clone(),
                &XmlWriterConfig::default(),
                provider.resources(),
            )
            .unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.write_record(&r1).unwrap();
            w.write_record(&r2).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(
            out,
            "<Root>\
             <Record><id>1</id></Record>\
             <Record><id>2</id><region>US</region></Record>\
             </Root>",
            "the `region` column appears only on the record that carries it"
        );
    }

    /// A dotted group whose members are NON-CONTIGUOUS in the schema
    /// (`[A.x, b, A.y]`) groups under one `<A>` at the group's FIRST schema
    /// position — before `<b>` — even when the group's leading member (`A.x`)
    /// is null and drops out under `preserve_nulls: false`. Pins the
    /// deterministic element order the shared plan produces regardless of
    /// per-record null pruning.
    #[test]
    fn test_xml_write_non_contiguous_group_null_leader_keeps_group_position() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "A.x".into(),
            "b".into(),
            "A.y".into(),
        ])));
        let record = Record::new(
            schema.clone(),
            vec![
                Value::Null,
                Value::String("B".into()),
                Value::String("Y".into()),
            ],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output, "<Root><Record><A><y>Y</y></A><b>B</b></Record></Root>",
            "the <A> group stays at its first-member schema position, before <b>"
        );
    }

    use crate::envelope_writer::test_doc_with_sections as doc_with_sections;

    #[test]
    fn xml_envelope_wraps_each_document_with_header_and_footer() {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = XmlWriterConfig {
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
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
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc).unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(10)]))
                .unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(20)]))
                .unwrap();
            w.end_document(&doc).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        // Each document is a <Document> with a <header>, the body <Record>s,
        // and a <footer> carrying the section field plus the computed count.
        assert!(out.contains("<Document>"), "got: {out}");
        assert!(
            out.contains("<header><batch_id>A</batch_id></header>"),
            "got: {out}"
        );
        assert!(
            out.contains("<Record><amount>10</amount></Record>"),
            "got: {out}"
        );
        assert!(
            out.contains("<footer><checksum>SUM</checksum><count>2</count></footer>"),
            "got: {out}"
        );
        assert!(out.contains("</Document>"), "got: {out}");
        // The whole thing is valid XML wrapped in the root.
        assert!(
            out.contains("<Root>") && out.contains("</Root>"),
            "got: {out}"
        );
    }

    #[test]
    fn xml_envelope_section_attribute_field_attaches_to_wrapper() {
        // Attribute-prefixed section fields (an XML envelope section read
        // with attributes) attach to the section wrapper's start tag.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = XmlWriterConfig {
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: None,
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let doc = doc_with_sections(&[(
            "Head",
            &[
                ("@version", Value::String("1.1".into())),
                ("batch_id", Value::String("A".into())),
            ],
        )]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc).unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(10)]))
                .unwrap();
            w.end_document(&doc).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(
            out.contains(r#"<header version="1.1"><batch_id>A</batch_id></header>"#),
            "got: {out}"
        );
    }

    #[test]
    fn xml_envelope_section_writes_native_nested_value() {
        use indexmap::IndexMap;

        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = XmlWriterConfig {
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: None,
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let mut metadata: IndexMap<OwnedKey, Value> = IndexMap::new();
        metadata.insert("@kind".into(), Value::String("batch".into()));
        metadata.insert("name".into(), Value::String("A".into()));
        let doc = doc_with_sections(&[(
            "Head",
            &[("metadata", Value::Map(OwnedMap::from_map(metadata)))],
        )]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc).unwrap();
            w.end_document(&doc).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(
            out.contains(r#"<header><metadata kind="batch"><name>A</name></metadata></header>"#),
            "got: {out}"
        );
    }

    #[test]
    fn xml_envelope_two_documents_each_reframed_with_reset_count() {
        // Two documents in one stream: each carries its own header/footer
        // rendered from its own `$doc` sections, and the streaming record count
        // resets per document (1, then 2). Exercises the per-document framing
        // across `begin_document` / `end_document` more than once — the section
        // maps are rendered in place off the framer's borrow into each
        // DocumentContext.
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["amount".into()])));
        let config = XmlWriterConfig {
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: Some("Foot".into()),
                footer_record_count_field: Some("count".into()),
            }),
            ..Default::default()
        };
        let doc1 = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("A".into()))]),
            ("Foot", &[("checksum", Value::String("S1".into()))]),
        ]);
        let doc2 = doc_with_sections(&[
            ("Head", &[("batch_id", Value::String("B".into()))]),
            ("Foot", &[("checksum", Value::String("S2".into()))]),
        ]);
        let mut buf = Vec::new();
        {
            let provider =
                MemoryOnlyResources::new(std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap());
            let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
            let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
            w.begin_document(&doc1).unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(10)]))
                .unwrap();
            w.end_document(&doc1).unwrap();
            w.begin_document(&doc2).unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(20)]))
                .unwrap();
            w.write_record(&Record::new(schema.clone(), vec![Value::Integer(30)]))
                .unwrap();
            w.end_document(&doc2).unwrap();
            w.flush().unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(
            out,
            "<Root>\
             <Document><header><batch_id>A</batch_id></header>\
             <Record><amount>10</amount></Record>\
             <footer><checksum>S1</checksum><count>1</count></footer></Document>\
             <Document><header><batch_id>B</batch_id></header>\
             <Record><amount>20</amount></Record><Record><amount>30</amount></Record>\
             <footer><checksum>S2</checksum><count>2</count></footer></Document>\
             </Root>"
        );
    }
}
