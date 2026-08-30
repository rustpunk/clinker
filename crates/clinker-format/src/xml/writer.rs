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

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use quick_xml::Writer as XmlEmitter;
use quick_xml::events::{BytesEnd, BytesStart, Event};

use clinker_record::field_path::{self, FieldPathError};
use clinker_record::nested_key::{NestedKey, validate_nested_depth, validate_nested_keys};
use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::multi_value::JoinValues;
use crate::traits::FormatWriter;

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

/// Streaming XML writer with complete-record validation before record bytes.
///
/// The writer borrows values twice per call and retains no per-record
/// preparation state. Its only memoized heap state is the schema-derived XML
/// tree plan, whose size is independent of record value widths.
pub struct XmlWriter<W: Write> {
    writer: XmlEmitter<W>,
    /// Schema pinned for the writer's lifetime. The borrowed emit path walks
    /// each record positionally, while this ownership keeps factory callers
    /// honest about the stream's declared schema.
    _schema: Arc<Schema>,
    config: XmlWriterConfig,
    header_written: bool,
    /// Per-document envelope framer, present only when `config.envelope` is.
    framer: Option<EnvelopeFramer>,
    /// Precompiled element-tree shape for the record body, memoized by schema
    /// identity. The tree SHAPE (dotted-branch nesting, attribute-vs-element
    /// classification, element names, ordering, attribute-name validity) is a
    /// pure function of the schema's column names + config, independent of
    /// per-record values, so it is built once and reused across records. Built
    /// lazily on the first `write_record` from `record.schema()` and rebuilt on
    /// a schema-identity change, so a multi-schema output stays correct.
    plan_cache: Option<PlanCache>,
}

impl<W: Write> XmlWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: XmlWriterConfig) -> Self {
        let framer = config
            .envelope
            .clone()
            .and_then(OutputEnvelopeSpec::into_framer);
        Self {
            writer: XmlEmitter::new(writer),
            _schema: schema,
            config,
            header_written: false,
            framer,
            plan_cache: None,
        }
    }

    /// Ensure `plan_cache` holds a tree plan for this record's schema. The plan
    /// is rebuilt only when the schema identity changes (`Arc::ptr_eq`), so a
    /// single-schema stream builds it once. Attribute-name validation happens
    /// here (at build time), before any bytes are written, so a malformed
    /// attribute name still fails `write_record` cleanly.
    fn ensure_plan(&mut self, record: &Record) -> Result<(), FormatError> {
        let schema = record.schema();
        let current = self
            .plan_cache
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(&c.schema, schema));
        if !current {
            self.plan_cache = Some(build_plan_cache(record, &self.config)?);
        }
        Ok(())
    }

    /// Emit a `<wrapper>…</wrapper>` element whose children are the section's
    /// fields rendered as nested elements (reusing the dotted-name expansion),
    /// optionally appending a `<count_field>N</count_field>` child. Called only
    /// for a section the document actually carries (a missing section emits no
    /// wrapper at all).
    fn write_section_element(
        writer: &mut XmlEmitter<W>,
        config: &XmlWriterConfig,
        wrapper: &str,
        fields: &indexmap::IndexMap<Box<str>, Value>,
        count: Option<(&str, i64)>,
    ) -> Result<(), FormatError> {
        let values = SectionFields::new(fields, count);
        let plan = build_tree_plan(values.names(), config)?;
        validate_body(&plan.root, &values, config)?;
        write_planned_start(writer, wrapper, &plan.root.attrs, &values, false)?;
        emit_body(
            writer,
            &plan.root,
            &values,
            config.preserve_nulls,
            &config.attribute_prefix,
        )?;
        let end = BytesEnd::new(wrapper);
        writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<(), FormatError> {
        if !self.header_written {
            // The root and record element names come straight from config into
            // `BytesStart::new`; validate them before opening the root so a
            // malformed configured name fails loud rather than corrupting the
            // document. Both are checked here, once, before any record element
            // is emitted (every write path opens the header first).
            check_xml_name(&self.config.root_element, "root element")?;
            check_xml_name(&self.config.record_element, "record element")?;
            self.header_written = true;
            let start = BytesStart::new(&self.config.root_element);
            self.writer
                .write_event(Event::Start(start))
                .map_err(|e| FormatError::Xml(e.to_string()))?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn retained_preparation_bytes(&self) -> usize {
        0
    }
}

impl<W: Write + Send> FormatWriter for XmlWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // The schema plan and the complete borrowed-value validation pass both
        // finish before the record start tag. Pass two can therefore emit
        // directly from `record` without a prepared value tree or retained
        // scalar strings, while a structural failure adds no record bytes.
        self.ensure_plan(record)?;
        let values = RecordFields::new(record);
        let plan = &self.plan_cache.as_ref().expect("plan built above").plan;
        validate_body(&plan.root, &values, &self.config)?;

        self.write_header()?;

        // Disjoint field borrows let the sink, cached schema plan, and borrowed
        // record values remain live together throughout direct emission.
        let Self {
            writer,
            plan_cache,
            config,
            framer,
            ..
        } = self;
        let plan = &plan_cache.as_ref().expect("plan built above").plan;

        write_planned_start(
            writer,
            &config.record_element,
            &plan.root.attrs,
            &values,
            false,
        )?;

        emit_body(
            writer,
            &plan.root,
            &values,
            config.preserve_nulls,
            &config.attribute_prefix,
        )?;

        writer
            .write_event(Event::End(BytesEnd::new(&*config.record_element)))
            .map_err(xml_err)?;

        if let Some(framer) = framer.as_mut() {
            framer.count_record();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.write_header()?; // Ensure root is opened even for 0 records
        let end = BytesEnd::new(&*self.config.root_element);
        self.writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        self.writer.get_mut().flush().map_err(FormatError::Io)?;
        Ok(())
    }

    /// Drain the underlying sink without emitting the closing root element, so
    /// byte-limit split accounting can observe the size mid-document. The
    /// closing root tag is written only by [`Self::flush`] at end of file /
    /// rotation.
    fn flush_bytes(&mut self) -> Result<(), FormatError> {
        self.writer.get_mut().flush().map_err(FormatError::Io)
    }

    fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        if self.framer.is_none() {
            return Ok(());
        }
        // Ensure the root element is open before the first document.
        self.write_header()?;
        // Open the per-document wrapper.
        let start = BytesStart::new("Document");
        self.writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        // Reset the per-document counter, then render the header directly off
        // the framer's borrow into the DocumentContext. `write_section_element`
        // takes the disjoint `writer` field, so it runs while the framer borrow
        // is live. `None` (the document lacks the configured section) emits no
        // `<header>`.
        let framer = self.framer.as_mut().expect("framer checked above");
        framer.begin();
        if let Some(fields) = framer.header_fields(doc) {
            Self::write_section_element(&mut self.writer, &self.config, "header", fields, None)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        // Render the footer directly off the framer's borrow: the section map
        // and the computed count stay borrowed while the disjoint `writer` field
        // is written. `None` (the document lacks the configured footer section)
        // emits no `<footer>` — the count rides a present section only.
        if let Some(fields) = framer.footer_fields(doc) {
            let count = framer.footer_count();
            Self::write_section_element(&mut self.writer, &self.config, "footer", fields, count)?;
        }
        let end = BytesEnd::new("Document");
        self.writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        Ok(())
    }
}

/// Wrap a field-name grammar failure as this writer's error.
fn field_path_error(source: FieldPathError) -> FormatError {
    FormatError::field_path("XML", source)
}

/// Map an emitter error into [`FormatError::Xml`]. The emitter surfaces
/// `std::io::Error`; its `Display` carries the underlying cause.
fn xml_err<E: std::fmt::Display>(e: E) -> FormatError {
    FormatError::Xml(e.to_string())
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
fn check_xml_name(name: &str, context: &str) -> Result<(), FormatError> {
    if is_valid_xml_name(name) {
        Ok(())
    } else {
        Err(FormatError::Xml(format!(
            "{context} name '{name}' is not a well-formed XML name"
        )))
    }
}

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
    fields: &'a indexmap::IndexMap<Box<str>, Value>,
    count: Option<(&'a str, Value)>,
}

impl<'a> SectionFields<'a> {
    fn new(fields: &'a indexmap::IndexMap<Box<str>, Value>, count: Option<(&'a str, i64)>) -> Self {
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
#[derive(Default)]
struct PlanBody {
    attrs: Vec<PlanAttr>,
    children: Vec<PlanNode>,
}

/// A precompiled attribute: its (validated) XML name and the field index whose
/// value it borrows.
struct PlanAttr {
    name: String,
    field: usize,
}

/// A precompiled child node: a leaf element or a nested branch.
enum PlanNode {
    Leaf {
        name: String,
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
        name: String,
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
    item_name: String,
    /// Optional container element wrapping the repeated items (`wrap_in`).
    wrap_in: Option<String>,
}

/// The memoized schema-derived plan plus the identity it was built for. Field
/// nodes retain only raw schema indices and validated XML names; record values
/// remain borrowed from the caller throughout validation and emission.
struct PlanCache {
    schema: Arc<Schema>,
    plan: TreePlan,
}

/// Build the tree plan for a record's schema. Walks the fields in iterator
/// order (position = field index) and classifies each into the element tree,
/// validating attribute names up front.
fn build_plan_cache(record: &Record, config: &XmlWriterConfig) -> Result<PlanCache, FormatError> {
    let schema = record.schema();
    let fields = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(index, _)| config.include_engine_stamped || !schema.is_engine_stamped(*index))
        .map(|(index, name)| (index, name.as_ref()));
    let plan = build_tree_plan(fields, config)?;
    Ok(PlanCache {
        schema: Arc::clone(record.schema()),
        plan,
    })
}

fn build_tree_plan<'a>(
    fields: impl IntoIterator<Item = (usize, &'a str)> + Clone,
    config: &XmlWriterConfig,
) -> Result<TreePlan, FormatError> {
    field_path::check_expandable(fields.clone().into_iter().map(|(_, name)| name))
        .map_err(field_path_error)?;
    let mut root = PlanBody::default();
    for (field_index, name) in fields {
        let path = field_path::decode(name).map_err(field_path_error)?;
        plan_insert_field(
            &mut root,
            field_index,
            name,
            &path,
            &config.attribute_prefix,
            &config.join_values,
            &config.declared_multiple,
        )?;
    }
    Ok(TreePlan { root })
}

/// Insert a decoded field path into the plan, creating branches as needed —
/// the plan-time twin of [`insert_field`], storing the field index in place of
/// a rendered value. Attribute-prefixed segments must be terminal, and
/// attribute names are validated here (once) rather than per record.
fn plan_insert_field(
    body: &mut PlanBody,
    field_index: usize,
    field: &str,
    path: &[Cow<'_, str>],
    attribute_prefix: &str,
    join_values: &[JoinValues],
    declared_multiple: &BTreeSet<String>,
) -> Result<(), FormatError> {
    let (segment, rest) = path
        .split_first()
        .expect("a decoded field path has at least one segment");
    if !rest.is_empty() {
        if !attribute_prefix.is_empty() && segment.starts_with(attribute_prefix) {
            return Err(FormatError::Xml(format!(
                "field '{field}': attribute-prefixed segment '{segment}' \
                 cannot have fields nested under it — an XML attribute is a leaf"
            )));
        }
        check_xml_name(segment, &format!("field '{field}': element"))?;
        let branch = body.children.iter_mut().find(
            |n| matches!(n, PlanNode::Branch { name, .. } if name.as_str() == segment.as_ref()),
        );
        if let Some(PlanNode::Branch {
            body: branch_body, ..
        }) = branch
        {
            plan_insert_field(
                branch_body,
                field_index,
                field,
                rest,
                attribute_prefix,
                join_values,
                declared_multiple,
            )
        } else {
            let mut branch_body = PlanBody::default();
            plan_insert_field(
                &mut branch_body,
                field_index,
                field,
                rest,
                attribute_prefix,
                join_values,
                declared_multiple,
            )?;
            body.children.push(PlanNode::Branch {
                name: segment.to_string(),
                body: branch_body,
            });
            Ok(())
        }
    } else if !attribute_prefix.is_empty() && segment.starts_with(attribute_prefix) {
        let attr_name = &segment[attribute_prefix.len()..];
        if attr_name.is_empty() {
            return Err(FormatError::Xml(format!(
                "field '{field}': attribute prefix '{attribute_prefix}' \
                 carries no attribute name"
            )));
        }
        if !is_valid_xml_name(attr_name) {
            return Err(FormatError::Xml(format!(
                "field '{field}': attribute name '{attr_name}' is not a \
                 well-formed XML name"
            )));
        }
        body.attrs.push(PlanAttr {
            name: attr_name.to_string(),
            field: field_index,
        });
        Ok(())
    } else {
        check_xml_name(segment, &format!("field '{field}': element"))?;
        let repeat = build_repeat_spec(field, segment, join_values)?;
        body.children.push(PlanNode::Leaf {
            name: segment.to_string(),
            field: field_index,
            declared_multiple: declared_multiple.contains(field),
            repeat,
        });
        Ok(())
    }
}

/// Resolve a leaf's repeated-element naming from the output's `join_values`.
///
/// The entry is matched by the leaf's full flattened field name (the same
/// name the CSV writer matches on). Returns `None` when no entry names the field
/// or the entry carries neither XML override — the array then emits bare repeats
/// named after the leaf. When an override is present, `repeat_as` / `wrap_in` are
/// validated as legal XML names here, before any byte is written, so a malformed
/// name fails the write cleanly the same way an illegal element name does.
fn build_repeat_spec(
    field: &str,
    leaf_name: &str,
    join_values: &[JoinValues],
) -> Result<Option<XmlRepeat>, FormatError> {
    let Some(entry) = join_values.iter().find(|j| j.field == field) else {
        return Ok(None);
    };
    if entry.repeat_as.is_none() && entry.wrap_in.is_none() {
        return Ok(None);
    }
    let item_name = match &entry.repeat_as {
        Some(name) => {
            check_xml_name(name, &format!("field '{field}': `repeat_as` element"))?;
            name.clone()
        }
        None => leaf_name.to_string(),
    };
    let wrap_in = match &entry.wrap_in {
        Some(name) => {
            check_xml_name(name, &format!("field '{field}': `wrap_in` element"))?;
            Some(name.clone())
        }
        None => None,
    };
    Ok(Some(XmlRepeat { item_name, wrap_in }))
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

fn validate_body<S: FieldSource>(
    body: &PlanBody,
    values: &S,
    config: &XmlWriterConfig,
) -> Result<(), FormatError> {
    for attr in &body.attrs {
        let (name, value) = values.field(attr.field);
        validate_field_value(
            value,
            true,
            false,
            config.preserve_nulls,
            &config.attribute_prefix,
            name,
        )?;
    }
    for child in &body.children {
        match child {
            PlanNode::Leaf {
                field,
                declared_multiple,
                ..
            } => {
                let (name, value) = values.field(*field);
                validate_field_value(
                    value,
                    false,
                    *declared_multiple,
                    config.preserve_nulls,
                    &config.attribute_prefix,
                    name,
                )?;
            }
            PlanNode::Branch { body, .. } => validate_body(body, values, config)?,
        }
    }
    Ok(())
}

fn validate_field_value(
    val: &Value,
    is_attr: bool,
    declared_multiple: bool,
    _preserve_nulls: bool,
    attribute_prefix: &str,
    name: &str,
) -> Result<(), FormatError> {
    match val {
        Value::Array(_) if !is_attr && declared_multiple => {
            validate_xml_nested_value(val, name, attribute_prefix)
        }
        Value::Array(_) if !is_attr => Err(FormatError::UnserializableArrayValue {
            format: "XML",
            column: name.to_string(),
        }),
        Value::Array(_) => Err(FormatError::Xml(format!(
            "field '{name}': an XML attribute cannot hold multiple values — a \
                 `multiple:` field maps to repeated elements, not an attribute"
        ))),
        Value::Map(_) if is_attr => Err(FormatError::UnserializableMapValue {
            format: "XML",
            column: name.to_string(),
        }),
        Value::Map(_) => validate_xml_nested_value(val, name, attribute_prefix),
        _ => validate_scalar_value(name, val),
    }
}

fn validate_scalar_value(field: &str, value: &Value) -> Result<(), FormatError> {
    let text = scalar_text(field, value)?;
    if let Some(character) = text.as_str().chars().find(|character| {
        !matches!(
            *character,
            '\u{9}' | '\u{A}' | '\u{D}' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{10FFFF}'
        )
    }) {
        return Err(FormatError::Xml(format!(
            "field '{field}': character U+{:04X} is not permitted in XML 1.0 text",
            character as u32
        )));
    }
    Ok(())
}

/// Validate every recursive XML decision before any bytes for the record are
/// written. Neutral maps preserve insertion order; unescaped reserved keys
/// become attributes or text, while escaped keys remain ordinary element
/// names. Arrays repeat their containing element name and therefore cannot be
/// nested directly inside another array without an intervening map key.
fn validate_xml_nested_value(
    value: &Value,
    field: &str,
    attribute_prefix: &str,
) -> Result<(), FormatError> {
    validate_nested_depth(value)
        .map_err(|error| FormatError::Xml(format!("field '{field}': {error}")))?;
    validate_nested_keys(value)
        .map_err(|error| FormatError::Xml(format!("field '{field}': {error}")))?;

    fn visit(value: &Value, field: &str, attribute_prefix: &str) -> Result<(), FormatError> {
        match value {
            Value::Array(items) => {
                for item in items {
                    if matches!(item, Value::Array(_)) {
                        return Err(FormatError::UnserializableArrayValue {
                            format: "XML",
                            column: field.to_string(),
                        });
                    }
                    visit(item, field, attribute_prefix)?;
                }
            }
            Value::Map(entries) => {
                for (raw_key, child) in entries.iter() {
                    let key = NestedKey::decode(raw_key).expect("keys validated above");
                    let is_attribute = !key.escaped
                        && !attribute_prefix.is_empty()
                        && key.text.starts_with(attribute_prefix);
                    let is_text = !key.escaped && key.text == "#text";
                    if is_attribute {
                        let name = &key.text[attribute_prefix.len()..];
                        if name.is_empty() {
                            return Err(FormatError::Xml(format!(
                                "field '{field}': attribute prefix '{attribute_prefix}' carries no attribute name"
                            )));
                        }
                        check_xml_name(name, &format!("field '{field}': nested attribute"))?;
                        if matches!(child, Value::Array(_) | Value::Map(_)) {
                            return Err(FormatError::Xml(format!(
                                "field '{field}': nested XML attribute '{}' must hold a scalar value",
                                key.text
                            )));
                        }
                        validate_scalar_value(field, child)?;
                    } else if is_text {
                        if matches!(child, Value::Array(_) | Value::Map(_)) {
                            return Err(FormatError::Xml(format!(
                                "field '{field}': nested XML #text must hold a scalar value"
                            )));
                        }
                        validate_scalar_value(field, child)?;
                    } else {
                        check_xml_name(&key.text, &format!("field '{field}': nested element"))?;
                        visit(child, field, attribute_prefix)?;
                    }
                }
            }
            _ => validate_scalar_value(field, value)?,
        }
        Ok(())
    }

    visit(value, field, attribute_prefix)
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
        write_attribute(writer, &attr.name, text.as_str())?;
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
    for child in &body.children {
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
                        name,
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
                    emit_scalar_leaf(writer, name, repeat, field_name, value)?;
                    continue;
                }
                emit_scalar_element(writer, name, field_name, value)?;
            }
            PlanNode::Branch { name, body } => {
                let has_attrs = body
                    .attrs
                    .iter()
                    .any(|attr| field_emits(values.field(attr.field).1, true, preserve_nulls));
                let has_children = body
                    .children
                    .iter()
                    .any(|child| node_emits(child, values, preserve_nulls));
                if !has_attrs && !has_children {
                    continue;
                }
                if has_children {
                    write_planned_start(writer, name, &body.attrs, values, false)?;
                    emit_body(writer, body, values, preserve_nulls, attribute_prefix)?;
                    writer
                        .write_event(Event::End(BytesEnd::new(name.as_str())))
                        .map_err(xml_err)?;
                } else {
                    write_planned_start(writer, name, &body.attrs, values, true)?;
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
    let wrap_in = repeat.as_ref().and_then(|r| r.wrap_in.as_deref());
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
    entries: &indexmap::IndexMap<Box<str>, Value>,
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
    let wrap_in = repeat.as_ref().and_then(|r| r.wrap_in.as_deref());
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
                .iter()
                .any(|attr| field_emits(values.field(attr.field).1, true, preserve_nulls))
                || body
                    .children
                    .iter()
                    .any(|child| node_emits(child, values, preserve_nulls))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::FormatReader;
    use crate::xml::reader::{XmlReader, XmlReaderConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct ByteCounter(Arc<AtomicUsize>);

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

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["name".into(), "age".into()]))
    }

    fn make_record(schema: &Arc<Schema>, name: &str, age: i64) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![Value::String(name.into()), Value::Integer(age)],
        )
    }

    fn write_records(config: XmlWriterConfig, records: &[Record], schema: &Arc<Schema>) -> String {
        let mut buf = Vec::new();
        let mut w = XmlWriter::new(&mut buf, Arc::clone(schema), config);
        for r in records {
            w.write_record(r).unwrap();
        }
        w.flush().unwrap();
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
        let schema = Arc::new(Schema::new(vec!["Address.City".into(), "name".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec![r"a\.b".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::String("v".into())]);
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
        let schema = Arc::new(Schema::new(vec!["a".into(), "a.b".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(1), Value::Integer(2)],
        );
        let mut buf = Vec::new();
        let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = w.write_record(&record).unwrap_err();
        assert!(
            matches!(err, FormatError::FieldPath { format: "XML", .. }),
            "{err:?}"
        );
        drop(w);
        assert!(
            buf.is_empty(),
            "a refused column set emits no bytes, got: {:?}",
            String::from_utf8_lossy(&buf)
        );
    }

    #[test]
    fn test_xml_write_shared_prefix_grouping() {
        let schema = Arc::new(Schema::new(vec![
            "Address.City".into(),
            "Address.State".into(),
            "name".into(),
        ]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["a".into(), "b".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["a".into(), "b".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["val".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["name".into(), "value".into()]));
        let records = vec![
            Record::new(
                Arc::clone(&schema),
                vec![Value::String("Alice".into()), Value::Integer(42)],
            ),
            Record::new(
                Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "payload".into()]));

        let mut first: IndexMap<Box<str>, Value> = IndexMap::new();
        first.insert("@id".into(), Value::Integer(1));
        first.insert("#text".into(), Value::String("alpha".into()));
        let mut second: IndexMap<Box<str>, Value> = IndexMap::new();
        second.insert("@id".into(), Value::Integer(2));
        second.insert("#text".into(), Value::String("beta".into()));

        let mut payload: IndexMap<Box<str>, Value> = IndexMap::new();
        payload.insert("@kind".into(), Value::String("event".into()));
        payload.insert("#text".into(), Value::String("before".into()));
        payload.insert(
            "item".into(),
            Value::Array(vec![
                Value::Map(Box::new(first)),
                Value::Map(Box::new(second)),
            ]),
        );
        payload.insert("tail".into(), Value::String("after".into()));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(7), Value::Map(Box::new(payload))],
        );
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            "<Root><Record><id>7</id><payload kind=\"event\">before<item id=\"1\">alpha</item><item id=\"2\">beta</item><tail>after</tail></payload></Record></Root>"
        );
    }

    #[test]
    fn large_authored_text_does_not_grow_retained_preparation_state() {
        let schema = Arc::new(Schema::new(vec!["@kind".into(), "payload".into()]));
        let large = "large <&> \"quoted\"\n".repeat(64 * 1024);
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::String(large.clone().into()),
                Value::String(large.into()),
            ],
        );
        let sink = ByteCounter::default();
        let mut writer = XmlWriter::new(sink, Arc::clone(&schema), XmlWriterConfig::default());

        writer.write_record(&record).expect("large record writes");

        assert_eq!(
            writer.retained_preparation_bytes(),
            0,
            "record-sized scalar preparation must not survive write_record",
        );
    }

    #[test]
    fn complete_validation_failures_add_no_record_bytes() {
        use indexmap::IndexMap;

        let schema = Arc::new(Schema::new(vec!["payload".into()]));
        let mut invalid_name = IndexMap::new();
        invalid_name.insert("1bad".into(), Value::Integer(1));

        let mut collision = IndexMap::new();
        collision.insert("@id".into(), Value::Integer(1));
        collision.insert("\\@id".into(), Value::Integer(2));

        let mut too_deep = Value::Null;
        for _ in 0..=clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH {
            too_deep = Value::Map(Box::new(IndexMap::from([("next".into(), too_deep)])));
        }

        for (case, invalid) in [
            ("malformed name", Value::Map(Box::new(invalid_name))),
            ("invalid text", Value::String("bad\u{1}".into())),
            ("excess depth", too_deep),
            ("decoded-key collision", Value::Map(Box::new(collision))),
        ] {
            let sink = ByteCounter::default();
            let observation = sink.clone();
            let mut writer = XmlWriter::new(sink, Arc::clone(&schema), XmlWriterConfig::default());
            writer
                .write_record(&Record::new(
                    Arc::clone(&schema),
                    vec![Value::String("valid".into())],
                ))
                .expect("control record writes");
            let before = observation.bytes();

            writer
                .write_record(&Record::new(Arc::clone(&schema), vec![invalid]))
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
        let schema = Arc::new(Schema::new(vec!["payload".into()]));
        let sink = ByteCounter::default();
        let mut writer = XmlWriter::new(sink, Arc::clone(&schema), XmlWriterConfig::default());

        for width in [1, 4096, 17, 128 * 1024, 2] {
            let record = Record::new(
                Arc::clone(&schema),
                vec![Value::String("<&".repeat(width).into())],
            );
            writer.write_record(&record).expect("record writes");
            assert_eq!(writer.retained_preparation_bytes(), 0);
        }
    }

    #[test]
    fn test_xml_writer_rejects_duplicate_decoded_map_keys_before_output() {
        use indexmap::IndexMap;
        let schema = Arc::new(Schema::new(vec!["payload".into()]));
        let mut payload: IndexMap<Box<str>, Value> = IndexMap::new();
        payload.insert("@id".into(), Value::Integer(1));
        payload.insert("\\@id".into(), Value::Integer(2));
        let record = Record::new(Arc::clone(&schema), vec![Value::Map(Box::new(payload))]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::Xml(message) if message.contains("duplicate logical nested key \"@id\"")),
            "unexpected error: {err:?}"
        );
        drop(writer);
        assert!(buf.is_empty(), "rejected record must emit no partial XML");
    }

    #[test]
    fn test_xml_writer_rejects_collection_valued_nested_attribute_before_output() {
        use indexmap::IndexMap;
        let schema = Arc::new(Schema::new(vec!["payload".into()]));
        let mut payload: IndexMap<Box<str>, Value> = IndexMap::new();
        payload.insert("@ids".into(), Value::Array(vec![Value::Integer(1)]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Map(Box::new(payload))]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::Xml(message) if message.contains("attribute '@ids' must hold a scalar")),
            "unexpected error: {err:?}"
        );
        drop(writer);
        assert!(buf.is_empty(), "rejected record must emit no partial XML");
    }

    /// Build a `[id, tags]` record whose `tags` field carries `values`.
    fn record_with_tags(schema: &Arc<Schema>, id: i64, values: Vec<Value>) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![Value::Integer(id), Value::Array(values)],
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
        let array = record_with_tags(&schema, 7, vec![Value::String("a".into())]);
        let array_out = write_records(xml_multiple_config(&["tags"]), &[array], &schema);
        let scalar = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
        let scalar = Record::new(
            Arc::clone(&schema),
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
    /// `FormatError::Xml`, leaving no partial output — element names are
    /// validated as legal XML names (criterion 5), like the record/root names.
    #[test]
    fn test_xml_write_multi_value_invalid_override_name_rejected() {
        for (repeat_as, wrap_in, bad) in [(Some("1bad"), None, "1bad"), (None, Some("a b"), "a b")]
        {
            let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
            let record = record_with_tags(&schema, 7, vec![Value::String("a".into())]);
            let mut buf = Vec::new();
            let mut writer = XmlWriter::new(
                &mut buf,
                Arc::clone(&schema),
                xml_join_config("tags", repeat_as, wrap_in),
            );
            let err = writer.write_record(&record).unwrap_err();
            match err {
                FormatError::Xml(msg) => {
                    assert!(msg.contains(bad), "message names the bad name: {msg}");
                    assert!(
                        msg.contains("well-formed XML name"),
                        "message explains the malformed name: {msg}"
                    );
                }
                other => panic!("expected FormatError::Xml, got {other:?}"),
            }
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
        let schema = Arc::new(Schema::new(vec!["@tags".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Array(vec![Value::String("a".into())])],
        );
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(
                    msg.contains("@tags") && msg.contains("cannot hold multiple values"),
                    "message names the field and the reason: {msg}"
                );
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
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
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
        let record = record_with_tags(
            &schema,
            7,
            vec![Value::Array(vec![Value::String("a".into())])],
        );
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(
            &mut buf,
            Arc::clone(&schema),
            xml_multiple_config(&["tags"]),
        );
        let err = writer.write_record(&record).unwrap_err();
        assert!(
            matches!(&err, FormatError::UnserializableArrayValue { column, .. } if column == "tags"),
            "expected UnserializableArrayValue for the nested array, got {err:?}"
        );
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
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ]))
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
    /// fails with `FormatError::Xml` naming the field and explaining the
    /// malformed element name, leaving no partial bytes behind.
    fn assert_element_name_rejected(field: &str) {
        let schema = Arc::new(Schema::new(vec![field.into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(msg.contains(field), "message names the field: {msg}");
                assert!(
                    msg.contains("well-formed XML name"),
                    "message explains the failure is a malformed name: {msg}"
                );
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
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
        let schema = Arc::new(Schema::new(vec!["café".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, "<Root><Record><café>1</café></Record></Root>");
    }

    #[test]
    fn test_xml_write_invalid_record_element_name_rejected() {
        // The configured record element name flows straight into
        // `BytesStart::new`; a malformed one fails loud before any output.
        let schema = Arc::new(Schema::new(vec!["name".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::String("A".into())]);
        let config = XmlWriterConfig {
            record_element: "1record".into(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(
                    msg.contains("record element"),
                    "message names the record element: {msg}"
                );
                assert!(msg.contains("1record"), "message names the value: {msg}");
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
        drop(writer);
        assert!(
            buf.is_empty(),
            "no output before a rejected record element name"
        );
    }

    #[test]
    fn test_xml_write_invalid_root_element_name_rejected() {
        // The configured root element name is validated at header open, so a
        // malformed one fails loud rather than emitting `<1root>`.
        let schema = Arc::new(Schema::new(vec!["name".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::String("A".into())]);
        let config = XmlWriterConfig {
            root_element: "1root".into(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(
                    msg.contains("root element"),
                    "message names the root element: {msg}"
                );
                assert!(msg.contains("1root"), "message names the value: {msg}");
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
        drop(writer);
        assert!(
            buf.is_empty(),
            "no output before a rejected root element name"
        );
    }

    #[test]
    fn test_xml_write_attribute_prefixed_field_as_record_attribute() {
        let schema = Arc::new(Schema::new(vec!["@id".into(), "name".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec![
            "Address.@type".into(),
            "Address.City".into(),
        ]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["Address.@type".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::String("home".into())]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(
            output,
            r#"<Root><Record><Address type="home"/></Record></Root>"#
        );
    }

    #[test]
    fn test_xml_write_custom_attribute_prefix() {
        let schema = Arc::new(Schema::new(vec!["_id".into(), "name".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["_id".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(7)]);
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert_eq!(output, "<Root><Record><_id>7</_id></Record></Root>");
    }

    #[test]
    fn test_xml_write_empty_prefix_disables_attribute_classification() {
        let schema = Arc::new(Schema::new(vec!["_id".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(7)]);
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
        let schema = Arc::new(Schema::new(vec!["@id".into(), "name".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Null, Value::Null]);
        let config = XmlWriterConfig {
            preserve_nulls: true,
            ..Default::default()
        };
        let output = write_records(config, &[record], &schema);
        assert_eq!(output, "<Root><Record><name/></Record></Root>");
    }

    #[test]
    fn test_xml_write_attribute_value_escaped() {
        let schema = Arc::new(Schema::new(vec!["@note".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["@note".into(), "name".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["@meta".into()]));
        let mut sidecar: IndexMap<Box<str>, Value> = IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        let record = Record::new(Arc::clone(&schema), vec![Value::Map(Box::new(sidecar))]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableMapValue { format, column } => {
                assert_eq!(format, "XML");
                assert_eq!(column, "@meta");
            }
            other => panic!("expected UnserializableMapValue, got {other:?}"),
        }
    }

    #[test]
    fn test_xml_write_attribute_segment_with_children_rejected() {
        // `@a.b` would need `@a` to be an element to hold `b` — an
        // attribute is a leaf, so the field is rejected instead of
        // emitting an `<@a>` element (invalid XML name).
        let schema = Arc::new(Schema::new(vec!["@a.b".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(
                    msg.contains("'@a.b'") && msg.contains("'@a'"),
                    "message names the field and offending segment: {msg}"
                );
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
        drop(writer);
        assert!(
            buf.is_empty(),
            "rejected record must not leave partial output behind"
        );
    }

    /// Assert that writing a single record whose only field is `field`
    /// fails with `FormatError::Xml` mentioning both the field and the
    /// stripped attribute name, and leaves no partial bytes behind.
    fn assert_attribute_name_rejected(field: &str, attr_name: &str) {
        let schema = Arc::new(Schema::new(vec![field.into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::Xml(msg) => {
                assert!(
                    msg.contains(field) && msg.contains(attr_name),
                    "message names the field and offending attribute name: {msg}"
                );
                assert!(
                    msg.contains("well-formed XML name"),
                    "message explains the failure is a malformed name: {msg}"
                );
            }
            other => panic!("expected FormatError::Xml, got {other:?}"),
        }
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
        let schema = Arc::new(Schema::new(vec!["@café".into()]));
        let record = Record::new(Arc::clone(&schema), vec![Value::Integer(1)]);
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
        let schema = Arc::new(Schema::new(vec![
            "@id".into(),
            "name".into(),
            "Address.@type".into(),
            "Address.City".into(),
            "Address.State".into(),
            "Contact.Email".into(),
        ]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["Address.City".into(), "name".into()]));
        let records: Vec<Record> = [("NYC", "Alice"), ("LA", "Bob"), ("SF", "Carol")]
            .into_iter()
            .map(|(city, name)| {
                Record::new(
                    Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec![
            "Address.City".into(),
            "Address.State".into(),
            "name".into(),
        ]));
        let all_null_branch = Record::new(
            Arc::clone(&schema),
            vec![Value::Null, Value::Null, Value::String("Alice".into())],
        );
        let branch_present = Record::new(
            Arc::clone(&schema),
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
        let schema1 = Arc::new(Schema::new(vec!["id".into()]));
        let schema2 = Arc::new(Schema::new(vec!["id".into(), "region".into()]));
        let r1 = Record::new(Arc::clone(&schema1), vec![Value::Integer(1)]);
        let r2 = Record::new(
            Arc::clone(&schema2),
            vec![Value::Integer(2), Value::String("US".into())],
        );
        let mut buf = Vec::new();
        {
            let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema1), XmlWriterConfig::default());
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
        let schema = Arc::new(Schema::new(vec!["A.x".into(), "b".into(), "A.y".into()]));
        let record = Record::new(
            Arc::clone(&schema),
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
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
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
            let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
            w.begin_document(&doc).unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(10)]))
                .unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(20)]))
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
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
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
            let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
            w.begin_document(&doc).unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(10)]))
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

        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let config = XmlWriterConfig {
            envelope: Some(crate::envelope_writer::OutputEnvelopeSpec {
                header_from_doc: Some("Head".into()),
                footer_from_doc: None,
                footer_record_count_field: None,
            }),
            ..Default::default()
        };
        let mut metadata: IndexMap<Box<str>, Value> = IndexMap::new();
        metadata.insert("@kind".into(), Value::String("batch".into()));
        metadata.insert("name".into(), Value::String("A".into()));
        let doc = doc_with_sections(&[("Head", &[("metadata", Value::Map(Box::new(metadata)))])]);
        let mut buf = Vec::new();
        {
            let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
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
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
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
            let mut w = XmlWriter::new(&mut buf, Arc::clone(&schema), config);
            w.begin_document(&doc1).unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(10)]))
                .unwrap();
            w.end_document(&doc1).unwrap();
            w.begin_document(&doc2).unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(20)]))
                .unwrap();
            w.write_record(&Record::new(Arc::clone(&schema), vec![Value::Integer(30)]))
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
