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
        }
    }

    /// Emit a `<wrapper>…</wrapper>` element whose children are the section's
    /// fields rendered as nested elements (reusing the dotted-name expansion),
    /// optionally appending a `<count_field>N</count_field>` child. Called only
    /// for a section the document actually carries (a missing section emits no
    /// wrapper at all).
    fn write_section_element(
        &mut self,
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
        let tree = build_field_tree(
            &pairs,
            self.config.preserve_nulls,
            &self.config.attribute_prefix,
        )?;
        let mut start = BytesStart::new(wrapper);
        push_attributes(&mut start, &tree.attrs);
        self.writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        self.write_field_tree(&tree.children)?;
        let end = BytesEnd::new(wrapper);
        self.writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<(), FormatError> {
        if !self.header_written {
            self.header_written = true;
            let start = BytesStart::new(&self.config.root_element);
            self.writer
                .write_event(Event::Start(start))
                .map_err(|e| FormatError::Xml(e.to_string()))?;
        }
        Ok(())
    }

    /// Collects schema fields as (name, value) pairs and builds the record's
    /// element body — attributes plus child nodes — before any bytes are
    /// emitted, so record-level attributes can ride the record start tag.
    /// Engine-stamped columns are stripped from the default output; callers
    /// opt in via `include_engine_stamped`.
    fn build_record_tree(&self, record: &Record) -> Result<ElementBody, FormatError> {
        let fields: Vec<(&str, &Value)> = if self.config.include_engine_stamped {
            record.iter_all_fields().collect()
        } else {
            record.iter_user_fields().collect()
        };

        build_field_tree(
            &fields,
            self.config.preserve_nulls,
            &self.config.attribute_prefix,
        )
    }

    /// Recursively write a field tree as nested XML elements.
    fn write_field_tree(&mut self, nodes: &[FieldNode]) -> Result<(), FormatError> {
        for node in nodes {
            match node {
                FieldNode::Leaf { name, text } => {
                    if text.is_empty() {
                        // Self-closing empty element (for preserve_nulls: true)
                        let elem = BytesStart::new(name.as_str());
                        self.writer
                            .write_event(Event::Empty(elem))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                    } else {
                        let start = BytesStart::new(name.as_str());
                        self.writer
                            .write_event(Event::Start(start))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                        let text_event = BytesText::new(text);
                        self.writer
                            .write_event(Event::Text(text_event))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                        let end = BytesEnd::new(name.as_str());
                        self.writer
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
                        self.writer
                            .write_event(Event::Empty(start))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                    } else {
                        self.writer
                            .write_event(Event::Start(start))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                        self.write_field_tree(&body.children)?;
                        let end = BytesEnd::new(name.as_str());
                        self.writer
                            .write_event(Event::End(end))
                            .map_err(|e| FormatError::Xml(e.to_string()))?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for XmlWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        // The tree is built before the record start tag is emitted:
        // attribute-classified fields must be known when the tag opens, and
        // a rejected field (map payload, malformed attribute path) must not
        // leave a dangling half-written record behind.
        let tree = self.build_record_tree(record)?;

        self.write_header()?;

        let mut start = BytesStart::new(&*self.config.record_element);
        push_attributes(&mut start, &tree.attrs);
        self.writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;

        self.write_field_tree(&tree.children)?;

        let end = BytesEnd::new(&*self.config.record_element);
        self.writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;

        if let Some(framer) = self.framer.as_mut() {
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
        // Reset the per-document counter and clone the header section's fields
        // out (so the immutable framer borrow ends before the `&mut self`
        // write below). `None` when the document lacks the configured section
        // — then no `<header>` is emitted.
        let header_owned: Option<indexmap::IndexMap<Box<str>, Value>> = {
            let framer = self.framer.as_mut().expect("framer checked above");
            framer.begin();
            framer.header_fields(doc).cloned()
        };
        if let Some(fields) = header_owned.as_ref() {
            self.write_section_element("header", fields, None)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        // `None` when the document lacks the configured footer section — then
        // no `<footer>` is emitted (the count rides the present section only).
        let footer_owned: Option<indexmap::IndexMap<Box<str>, Value>> =
            framer.footer_fields(doc).cloned();
        let count_owned: Option<(String, i64)> = framer
            .footer_count()
            .map(|(field, n)| (field.to_string(), n));
        if let Some(fields) = footer_owned.as_ref() {
            let count_ref = count_owned.as_ref().map(|(f, n)| (f.as_str(), *n));
            self.write_section_element("footer", fields, count_ref)?;
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
        start.push_attribute(Attribute {
            key: QName(key.as_bytes()),
            value: Cow::Owned(escape_attribute_value(value).into_bytes()),
        });
    }
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
        body.attrs.push((attr_name.to_string(), text.to_string()));
        Ok(())
    } else {
        body.children.push(FieldNode::Leaf {
            name: path.to_string(),
            text: text.to_string(),
        });
        Ok(())
    }
}

/// Convert a clinker Value to XML text content.
///
/// `Value::Map` has no canonical scalar serialization for an XML
/// element body (silently JSON-encoding hides routing bugs — e.g. a
/// `$widened` sidecar reaching the writer without
/// `include_unmapped: true` expansion), so this returns
/// `FormatError::UnserializableMapValue` carrying the offending
/// column. XML is the single point of truth for map rejection on
/// this path; there is no upstream pre-walk.
///
/// Array elements that themselves contain a `Value::Map` propagate
/// the same rejection (the array is joined element-wise).
fn value_to_text(col: &str, val: &Value) -> Result<String, FormatError> {
    Ok(match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Array(arr) => arr
            .iter()
            .map(|v| value_to_text(col, v))
            .collect::<Result<Vec<_>, _>>()?
            .join(","),
        Value::Map(_) => {
            return Err(FormatError::UnserializableMapValue {
                format: "XML",
                column: col.to_string(),
            });
        }
    })
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
}
