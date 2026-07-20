//! XML writer with configurable root/record elements, dotted field expansion
//! to nested elements, attribute-prefixed fields emitted as XML attributes,
//! null handling, and proper escaping.
//!
//! Under `reconstruct_envelope`, each document is wrapped in a `<Document>`
//! element inside the root: `begin_document` opens `<Document>` and emits the
//! header section as a `<header>` element; the body `<Record>` elements stream
//! between; `end_document` emits a `<footer>` element (section fields plus the
//! streaming record count) and closes `</Document>`. No document is buffered.

use std::borrow::Cow;
use std::io::Write;
use std::sync::Arc;

use quick_xml::Writer as XmlEmitter;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::name::QName;

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
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
        }
    }
}

pub struct XmlWriter<W: Write> {
    writer: XmlEmitter<W>,
    /// Schema pinned for the writer's lifetime. After the overflow rip
    /// the emit path walks `Record::iter_all_fields` (schema-positional),
    /// so the writer does not consult the schema per record — but the
    /// Arc ownership here keeps factory callers honest.
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
    /// Reusable per-record value buffer, one slot per field in iterator order.
    /// Each slot's `String` capacity is retained across records so filling a
    /// record's values reuses the allocation rather than building a fresh owned
    /// tree. Bounded by the widest single record.
    fill: FillBuf,
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
            fill: FillBuf::new(),
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

    /// Fill the reusable `fill` buffer with this record's rendered field values
    /// and per-field presence, in the same iterator order the plan indexes.
    /// Runs before any bytes are emitted, so a `Value::Map` field (rejected by
    /// [`write_value_text`]) fails the record without leaving partial output.
    fn fill_values(&mut self, record: &Record) -> Result<(), FormatError> {
        let Self {
            plan_cache,
            fill,
            config,
            ..
        } = self;
        let cache = plan_cache.as_ref().expect("plan built before fill_values");
        let flags = &cache.field_is_attr;
        let preserve = config.preserve_nulls;
        fill.reset(flags.len());
        if config.include_engine_stamped {
            for (i, (name, val)) in record.iter_all_fields().enumerate() {
                fill_slot(&mut fill.slots[i], flags[i], preserve, name, val)?;
            }
        } else {
            for (i, (name, val)) in record.iter_user_fields().enumerate() {
                fill_slot(&mut fill.slots[i], flags[i], preserve, name, val)?;
            }
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
        let mut pairs: Vec<(&str, &Value)> = Vec::new();
        for (name, value) in fields {
            pairs.push((name.as_ref(), value));
        }
        // The computed count is held as an owned Value so it can be borrowed
        // into the same pair list the section fields use.
        let count_value = count.map(|(field, n)| (field, Value::Integer(n)));
        if let Some((field, ref v)) = count_value {
            pairs.push((field, v));
        }
        let tree = build_field_tree(&pairs, config.preserve_nulls, &config.attribute_prefix)?;
        let mut start = BytesStart::new(wrapper);
        push_attributes(&mut start, &tree.attrs);
        writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        write_field_tree(writer, &tree.children)?;
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
}

/// Recursively write a field tree as nested XML elements. Takes the emitter
/// directly rather than `&mut self`, so a caller can render an envelope section
/// while holding a disjoint borrow of the framer.
fn write_field_tree<W: Write>(
    writer: &mut XmlEmitter<W>,
    nodes: &[FieldNode],
) -> Result<(), FormatError> {
    for node in nodes {
        match node {
            FieldNode::Leaf { name, text } => {
                if text.is_empty() {
                    // Self-closing empty element (for preserve_nulls: true)
                    let elem = BytesStart::new(name.as_str());
                    writer
                        .write_event(Event::Empty(elem))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                } else {
                    let start = BytesStart::new(name.as_str());
                    writer
                        .write_event(Event::Start(start))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                    let text_event = BytesText::new(text);
                    writer
                        .write_event(Event::Text(text_event))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                    let end = BytesEnd::new(name.as_str());
                    writer
                        .write_event(Event::End(end))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                }
            }
            FieldNode::Branch { name, body } => {
                let mut start = BytesStart::new(name.as_str());
                push_attributes(&mut start, &body.attrs);
                if body.children.is_empty() {
                    // Attribute-only branch: nothing nests inside, so the
                    // element self-closes carrying its attributes.
                    writer
                        .write_event(Event::Empty(start))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                } else {
                    writer
                        .write_event(Event::Start(start))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                    write_field_tree(writer, &body.children)?;
                    let end = BytesEnd::new(name.as_str());
                    writer
                        .write_event(Event::End(end))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

impl<W: Write + Send> FormatWriter for XmlWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // Both the plan build and the value fill run before the record start
        // tag is emitted: attribute-classified fields must be known when the
        // tag opens, and a rejected field (map payload, malformed attribute
        // name) must not leave a dangling half-written record behind.
        self.ensure_plan(record)?;
        self.fill_values(record)?;

        self.write_header()?;

        // Disjoint field borrows: the sink writes (`writer`) need the plan
        // (`plan_cache`) and the filled values (`fill`) live at the same time.
        let Self {
            writer,
            plan_cache,
            fill,
            config,
            framer,
            ..
        } = self;
        let plan = &plan_cache.as_ref().expect("plan built above").plan;

        let mut start = BytesStart::new(&*config.record_element);
        for attr in &plan.root.attrs {
            if fill.emits(attr.field) {
                push_one_attribute(&mut start, &attr.name, fill.text(attr.field));
            }
        }
        writer.write_event(Event::Start(start)).map_err(xml_err)?;

        emit_body(writer, &plan.root, fill)?;

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

// ── Field tree for dotted name → nested element expansion ────────────

/// One element's content: the attributes attached to its start tag plus its
/// child nodes. The whole body is materialized before the element's start
/// tag is emitted, because attributes have to be known at that point.
#[derive(Default)]
struct ElementBody {
    attrs: Vec<(String, String)>,
    children: Vec<FieldNode>,
}

enum FieldNode {
    Leaf { name: String, text: String },
    Branch { name: String, body: ElementBody },
}

/// Build an element body from dotted field names. Fields with shared
/// prefixes are grouped under a single parent branch
/// (`Address.City` + `Address.State` → one `Address` branch with two leaf
/// children), and a field whose final segment carries the attribute prefix
/// becomes an attribute of its enclosing element instead of a child
/// (`@id` → attribute on the returned body, `Address.@type` → attribute on
/// the `Address` branch).
///
/// Null attribute fields are dropped even under `preserve_nulls` — a null
/// element round-trips as a self-closing tag, but an attribute has no
/// present-but-empty form that reads back as null.
///
/// Returns `FormatError::UnserializableMapValue` if any field carries
/// a `Value::Map` payload — XML elements have no canonical scalar
/// serialization for a map, and `value_to_text` raises from the Map
/// arm directly. Returns `FormatError::Xml` for a malformed attribute
/// path (a prefixed segment with children nested under it, or a bare
/// prefix with no attribute name).
fn build_field_tree(
    fields: &[(&str, &Value)],
    preserve_nulls: bool,
    attribute_prefix: &str,
) -> Result<ElementBody, FormatError> {
    let mut root = ElementBody::default();

    for &(name, val) in fields {
        if val.is_null() && (!preserve_nulls || is_attribute_path(name, attribute_prefix)) {
            continue;
        }
        let text = value_to_text(name, val)?;
        insert_field(&mut root, name, name, &text, attribute_prefix)?;
    }

    Ok(root)
}

/// Push name/value attributes onto an element's start tag, escaping each
/// value.
///
/// quick-xml's `(&str, &str)` attribute conversion escapes only the five
/// predefined entities, leaving literal tab / CR / LF untouched — a
/// conformant parser then collapses those to spaces (XML attribute-value
/// normalization), silently altering values that carry literal whitespace.
/// They are written as character references instead, which both clinker's
/// reader and any conformant parser resolve back to the exact bytes.
fn push_attributes(start: &mut BytesStart, attrs: &[(String, String)]) {
    for (key, value) in attrs {
        push_one_attribute(start, key, value);
    }
}

/// Push a single name/value attribute onto an element's start tag, escaping the
/// value (see [`push_attributes`] for why literal whitespace is emitted as
/// character references).
fn push_one_attribute(start: &mut BytesStart, key: &str, value: &str) {
    start.push_attribute(Attribute {
        key: QName(key.as_bytes()),
        value: Cow::Owned(escape_attribute_value(value).into_bytes()),
    });
}

/// Map an emitter error into [`FormatError::Xml`]. The emitter surfaces
/// `std::io::Error`; its `Display` carries the underlying cause.
fn xml_err<E: std::fmt::Display>(e: E) -> FormatError {
    FormatError::Xml(e.to_string())
}

/// Escape an attribute value: the five predefined entities plus literal
/// tab / CR / LF as character references (see [`push_attributes`]).
fn escape_attribute_value(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            '\t' => escaped.push_str("&#9;"),
            '\n' => escaped.push_str("&#10;"),
            '\r' => escaped.push_str("&#13;"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// True when the path's final segment is attribute-classified — i.e. a
/// non-empty prefix marks it as an attribute of its enclosing element.
fn is_attribute_path(path: &str, attribute_prefix: &str) -> bool {
    !attribute_prefix.is_empty()
        && path
            .rsplit('.')
            .next()
            .is_some_and(|segment| segment.starts_with(attribute_prefix))
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
fn is_valid_xml_name(name: &str) -> bool {
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

/// Insert a dotted field name into the tree, creating branches as needed.
/// An attribute-prefixed segment must be the path's final segment (an
/// attribute is a leaf — nothing can nest under it); `field` is the full
/// original field name, carried for error messages.
fn insert_field(
    body: &mut ElementBody,
    field: &str,
    path: &str,
    text: &str,
    attribute_prefix: &str,
) -> Result<(), FormatError> {
    if let Some((first, rest)) = path.split_once('.') {
        if !attribute_prefix.is_empty() && first.starts_with(attribute_prefix) {
            return Err(FormatError::Xml(format!(
                "field '{field}': attribute-prefixed segment '{first}' \
                 cannot have fields nested under it — an XML attribute is a leaf"
            )));
        }
        check_xml_name(first, &format!("field '{field}': element"))?;
        // Find or create a branch for `first`
        let branch = body
            .children
            .iter_mut()
            .find(|n| matches!(n, FieldNode::Branch { name, .. } if name == first));
        if let Some(FieldNode::Branch {
            body: branch_body, ..
        }) = branch
        {
            insert_field(branch_body, field, rest, text, attribute_prefix)
        } else {
            let mut branch_body = ElementBody::default();
            insert_field(&mut branch_body, field, rest, text, attribute_prefix)?;
            body.children.push(FieldNode::Branch {
                name: first.to_string(),
                body: branch_body,
            });
            Ok(())
        }
    } else if !attribute_prefix.is_empty() && path.starts_with(attribute_prefix) {
        let attr_name = &path[attribute_prefix.len()..];
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
        body.attrs.push((attr_name.to_string(), text.to_string()));
        Ok(())
    } else {
        check_xml_name(path, &format!("field '{field}': element"))?;
        body.children.push(FieldNode::Leaf {
            name: path.to_string(),
            text: text.to_string(),
        });
        Ok(())
    }
}

/// Convert a clinker Value to XML text content.
///
/// `Value::Map` and `Value::Array` have no canonical scalar serialization
/// for an XML element body (silently JSON-encoding a map, or comma-joining
/// an array, hides routing bugs — e.g. a `$widened` sidecar reaching the
/// writer without `include_unmapped: true` expansion for a map, or a
/// `match: collect` combine output misrouted to XML for an array), so this
/// returns `FormatError::UnserializableMapValue` /
/// `FormatError::UnserializableArrayValue` carrying the offending column.
/// XML is the single point of truth for collection rejection on this path;
/// there is no upstream pre-walk.
fn value_to_text(col: &str, val: &Value) -> Result<String, FormatError> {
    let mut buf = String::new();
    write_value_text(&mut buf, col, val)?;
    Ok(buf)
}

/// Render a clinker Value as XML text content into `buf` (appending). Shares
/// its logic with [`value_to_text`] so the record fast path and the envelope
/// section path stay byte-identical. `Value::Map` and `Value::Array` are
/// rejected exactly as `value_to_text` documents.
fn write_value_text(buf: &mut String, col: &str, val: &Value) -> Result<(), FormatError> {
    use std::fmt::Write as _;
    match val {
        Value::Null => {}
        Value::Bool(b) => {
            let _ = write!(buf, "{b}");
        }
        Value::Integer(i) => {
            let _ = write!(buf, "{i}");
        }
        Value::Float(f) => {
            let _ = write!(buf, "{f}");
        }
        Value::Decimal(d) => {
            let _ = write!(buf, "{d}");
        }
        Value::String(s) => buf.push_str(s.as_str()),
        Value::Date(d) => {
            let _ = write!(buf, "{d}");
        }
        Value::DateTime(dt) => {
            let _ = write!(buf, "{dt}");
        }
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
    Ok(())
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
/// to read from the per-record fill buffer.
#[derive(Default)]
struct PlanBody {
    attrs: Vec<PlanAttr>,
    children: Vec<PlanNode>,
}

/// A precompiled attribute: its (validated) XML name and the field index whose
/// value fills it.
struct PlanAttr {
    name: String,
    field: usize,
}

/// A precompiled child node: a leaf element or a nested branch.
enum PlanNode {
    Leaf { name: String, field: usize },
    Branch { name: String, body: PlanBody },
}

/// The memoized plan plus the identity it was built for. `field_is_attr` marks,
/// per field iterator position, whether that field is an attribute (drops null
/// unconditionally) versus an element leaf (drops null only when
/// `preserve_nulls` is false).
struct PlanCache {
    schema: Arc<Schema>,
    plan: TreePlan,
    field_is_attr: Vec<bool>,
}

/// Build the tree plan for a record's schema. Walks the fields in iterator
/// order (position = field index) and classifies each into the element tree,
/// validating attribute names up front. Mirrors [`build_field_tree`]'s shape
/// exactly so output stays byte-identical.
fn build_plan_cache(record: &Record, config: &XmlWriterConfig) -> Result<PlanCache, FormatError> {
    let names: Vec<&str> = if config.include_engine_stamped {
        record.iter_all_fields().map(|(name, _)| name).collect()
    } else {
        record.iter_user_fields().map(|(name, _)| name).collect()
    };
    let mut root = PlanBody::default();
    let mut field_is_attr = vec![false; names.len()];
    for (i, name) in names.iter().enumerate() {
        plan_insert_field(
            &mut root,
            i,
            name,
            name,
            &config.attribute_prefix,
            &mut field_is_attr,
        )?;
    }
    Ok(PlanCache {
        schema: Arc::clone(record.schema()),
        plan: TreePlan { root },
        field_is_attr,
    })
}

/// Insert a dotted field name into the plan, creating branches as needed —
/// the plan-time twin of [`insert_field`], storing the field index in place of
/// a rendered value. Attribute-prefixed segments must be terminal, and
/// attribute names are validated here (once) rather than per record.
fn plan_insert_field(
    body: &mut PlanBody,
    field_index: usize,
    field: &str,
    path: &str,
    attribute_prefix: &str,
    field_is_attr: &mut [bool],
) -> Result<(), FormatError> {
    if let Some((first, rest)) = path.split_once('.') {
        if !attribute_prefix.is_empty() && first.starts_with(attribute_prefix) {
            return Err(FormatError::Xml(format!(
                "field '{field}': attribute-prefixed segment '{first}' \
                 cannot have fields nested under it — an XML attribute is a leaf"
            )));
        }
        check_xml_name(first, &format!("field '{field}': element"))?;
        let branch = body
            .children
            .iter_mut()
            .find(|n| matches!(n, PlanNode::Branch { name, .. } if name == first));
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
                field_is_attr,
            )
        } else {
            let mut branch_body = PlanBody::default();
            plan_insert_field(
                &mut branch_body,
                field_index,
                field,
                rest,
                attribute_prefix,
                field_is_attr,
            )?;
            body.children.push(PlanNode::Branch {
                name: first.to_string(),
                body: branch_body,
            });
            Ok(())
        }
    } else if !attribute_prefix.is_empty() && path.starts_with(attribute_prefix) {
        let attr_name = &path[attribute_prefix.len()..];
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
        field_is_attr[field_index] = true;
        body.attrs.push(PlanAttr {
            name: attr_name.to_string(),
            field: field_index,
        });
        Ok(())
    } else {
        check_xml_name(path, &format!("field '{field}': element"))?;
        body.children.push(PlanNode::Leaf {
            name: path.to_string(),
            field: field_index,
        });
        Ok(())
    }
}

/// Per-record scratch, one slot per field in iterator order. Slot `String`
/// capacity is retained across records (only the length is reset) so filling a
/// record reuses allocations. `emits` records whether the field appears in
/// output for this record (null handling); `text` holds its rendered value.
struct FillBuf {
    slots: Vec<FillSlot>,
}

#[derive(Default)]
struct FillSlot {
    text: String,
    emits: bool,
}

impl FillBuf {
    fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Ensure at least `n` slots exist (reusing existing `String` capacity),
    /// ready to be overwritten for the current record. Every index in `0..n`
    /// is written by `fill_values` before it is read, so stale contents beyond
    /// a shrink never surface.
    fn reset(&mut self, n: usize) {
        if self.slots.len() < n {
            self.slots.resize_with(n, FillSlot::default);
        }
    }

    fn emits(&self, field: usize) -> bool {
        self.slots[field].emits
    }

    fn text(&self, field: usize) -> &str {
        &self.slots[field].text
    }
}

/// Fill a single slot with a field's presence and rendered text. A field is
/// dropped (`emits = false`) when it is a null attribute, or a null element
/// under `preserve_nulls: false`; otherwise its text is rendered (a preserved
/// null element renders to the empty string, emitted as a self-closing tag).
fn fill_slot(
    slot: &mut FillSlot,
    is_attr: bool,
    preserve_nulls: bool,
    name: &str,
    val: &Value,
) -> Result<(), FormatError> {
    let skip = if is_attr {
        val.is_null()
    } else {
        val.is_null() && !preserve_nulls
    };
    slot.emits = !skip;
    slot.text.clear();
    if !skip {
        write_value_text(&mut slot.text, name, val)?;
    }
    Ok(())
}

/// Emit a body's child nodes (its start-tag attributes are handled by the
/// caller). A branch is emitted only when it has at least one emitting
/// attribute or child, and self-closes when it has no emitting children —
/// matching the pruning `build_field_tree` performs by never inserting a
/// dropped field.
fn emit_body<W: Write>(
    writer: &mut XmlEmitter<W>,
    body: &PlanBody,
    fill: &FillBuf,
) -> Result<(), FormatError> {
    for child in &body.children {
        match child {
            PlanNode::Leaf { name, field } => {
                if !fill.emits(*field) {
                    continue;
                }
                let text = fill.text(*field);
                if text.is_empty() {
                    writer
                        .write_event(Event::Empty(BytesStart::new(name.as_str())))
                        .map_err(xml_err)?;
                } else {
                    writer
                        .write_event(Event::Start(BytesStart::new(name.as_str())))
                        .map_err(xml_err)?;
                    writer
                        .write_event(Event::Text(BytesText::new(text)))
                        .map_err(xml_err)?;
                    writer
                        .write_event(Event::End(BytesEnd::new(name.as_str())))
                        .map_err(xml_err)?;
                }
            }
            PlanNode::Branch { name, body } => {
                let has_attrs = body.attrs.iter().any(|a| fill.emits(a.field));
                let has_children = body.children.iter().any(|c| node_emits(c, fill));
                if !has_attrs && !has_children {
                    continue;
                }
                let mut start = BytesStart::new(name.as_str());
                for attr in &body.attrs {
                    if fill.emits(attr.field) {
                        push_one_attribute(&mut start, &attr.name, fill.text(attr.field));
                    }
                }
                if has_children {
                    writer.write_event(Event::Start(start)).map_err(xml_err)?;
                    emit_body(writer, body, fill)?;
                    writer
                        .write_event(Event::End(BytesEnd::new(name.as_str())))
                        .map_err(xml_err)?;
                } else {
                    writer.write_event(Event::Empty(start)).map_err(xml_err)?;
                }
            }
        }
    }
    Ok(())
}

/// Whether a node contributes any output for the current record: a leaf emits
/// per its fill slot; a branch emits when any attribute or descendant does.
fn node_emits(node: &PlanNode, fill: &FillBuf) -> bool {
    match node {
        PlanNode::Leaf { field, .. } => fill.emits(*field),
        PlanNode::Branch { body, .. } => {
            body.attrs.iter().any(|a| fill.emits(a.field))
                || body.children.iter().any(|c| node_emits(c, fill))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::FormatReader;
    use crate::xml::reader::{XmlReader, XmlReaderConfig};

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

    /// XML writer rejects `Value::Map` payloads with
    /// `FormatError::UnserializableMapValue`. The pre-walk in
    /// `write_fields` catches the misroute before the value-to-text
    /// function silently JSON-encodes the map inside an element.
    #[test]
    fn test_xml_writer_rejects_map_value() {
        use indexmap::IndexMap;
        let schema = Arc::new(Schema::new(vec!["id".into(), "payload".into()]));
        let mut sidecar: IndexMap<Box<str>, Value> = IndexMap::new();
        sidecar.insert("a".into(), Value::Integer(1));
        let record = Record::new(
            Arc::clone(&schema),
            vec![Value::Integer(7), Value::Map(Box::new(sidecar))],
        );
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableMapValue { format, column } => {
                assert_eq!(format, "XML");
                assert_eq!(column, "payload");
            }
            other => panic!("expected UnserializableMapValue, got {other:?}"),
        }
    }

    /// XML writer rejects `Value::Array` payloads with
    /// `FormatError::UnserializableArrayValue`, parallel to the map
    /// rejection. The prior behavior comma-joined the array's elements
    /// inside a single element, silently hiding a misroute (e.g. a
    /// `match: collect` combine output sent to XML). `fill_values` runs
    /// before any bytes, so the rejected record leaves no partial output.
    #[test]
    fn test_xml_writer_rejects_array_value() {
        let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
        let record = Record::new(
            Arc::clone(&schema),
            vec![
                Value::Integer(7),
                Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            ],
        );
        let mut buf = Vec::new();
        let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
        let err = writer.write_record(&record).unwrap_err();
        match err {
            FormatError::UnserializableArrayValue { format, column } => {
                assert_eq!(format, "XML");
                assert_eq!(column, "tags");
            }
            other => panic!("expected UnserializableArrayValue, got {other:?}"),
        }
        drop(writer);
        assert!(
            buf.is_empty(),
            "rejected record must not leave partial output behind"
        );
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
