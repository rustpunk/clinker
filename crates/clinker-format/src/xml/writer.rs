//! XML writer with configurable root/record elements, dotted field expansion
//! to nested elements, null handling, and proper escaping.
//!
//! Under `reconstruct_envelope`, each document is wrapped in a `<Document>`
//! element inside the root: `begin_document` opens `<Document>` and emits the
//! header section as a `<header>` element; the body `<Record>` elements stream
//! between; `end_document` emits a `<footer>` element (section fields plus the
//! streaming record count) and closes `</Document>`. No document is buffered.

use std::io::Write;
use std::sync::Arc;

use quick_xml::Writer as XmlEmitter;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};

use clinker_record::{DocumentContext, Record, Schema, Value};

use crate::envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
use crate::error::FormatError;
use crate::traits::FormatWriter;

#[derive(Clone)]
pub struct XmlWriterConfig {
    pub root_element: String,
    pub record_element: String,
    pub preserve_nulls: bool,
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
            .filter(|s| !s.is_empty())
            .map(EnvelopeFramer::new);
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
    /// optionally appending a `<count_field>N</count_field>` child. A section
    /// with no fields and no count emits an empty `<wrapper/>`.
    fn write_section_element(
        &mut self,
        wrapper: &str,
        fields: Option<&indexmap::IndexMap<Box<str>, Value>>,
        count: Option<(&str, i64)>,
    ) -> Result<(), FormatError> {
        let mut pairs: Vec<(&str, &Value)> = Vec::new();
        if let Some(fields) = fields {
            for (name, value) in fields {
                pairs.push((name.as_ref(), value));
            }
        }
        // The computed count is held as an owned Value so it can be borrowed
        // into the same pair list the section fields use.
        let count_value = count.map(|(field, n)| (field, Value::Integer(n)));
        if let Some((field, ref v)) = count_value {
            pairs.push((field, v));
        }
        let tree = build_field_tree(&pairs, self.config.preserve_nulls)?;
        let start = BytesStart::new(wrapper);
        self.writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        self.write_field_tree(&tree)?;
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

    /// Collects schema fields as (name, value) pairs, then writes them as
    /// nested XML elements. Engine-stamped columns are stripped from the
    /// default output; callers opt in via `include_engine_stamped`.
    fn write_fields(&mut self, record: &Record) -> Result<(), FormatError> {
        let fields: Vec<(&str, &Value)> = if self.config.include_engine_stamped {
            record.iter_all_fields().collect()
        } else {
            record.iter_user_fields().collect()
        };

        let tree = build_field_tree(&fields, self.config.preserve_nulls)?;
        self.write_field_tree(&tree)?;

        Ok(())
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
                FieldNode::Branch { name, children } => {
                    let start = BytesStart::new(name.as_str());
                    self.writer
                        .write_event(Event::Start(start))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                    self.write_field_tree(children)?;
                    let end = BytesEnd::new(name.as_str());
                    self.writer
                        .write_event(Event::End(end))
                        .map_err(|e| FormatError::Xml(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}

impl<W: Write + Send> FormatWriter for XmlWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        self.write_header()?;

        let start = BytesStart::new(&*self.config.record_element);
        self.writer
            .write_event(Event::Start(start))
            .map_err(|e| FormatError::Xml(e.to_string()))?;

        self.write_fields(record)?;

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
        // Reset the per-document counter and emit the header element.
        let (has_header, header_owned) = {
            let framer = self.framer.as_mut().expect("framer checked above");
            framer.begin();
            // Clone the header section's fields out so the immutable borrow of
            // `framer` ends before the `&mut self` write below.
            let owned: Option<indexmap::IndexMap<Box<str>, Value>> =
                framer.header_fields(doc).cloned();
            (framer.has_header(), owned)
        };
        if has_header {
            self.write_section_element("header", header_owned.as_ref(), None)?;
        }
        Ok(())
    }

    fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
        let Some(framer) = self.framer.as_ref() else {
            return Ok(());
        };
        let has_footer = framer.has_footer();
        let footer_owned: Option<indexmap::IndexMap<Box<str>, Value>> =
            framer.footer_fields(doc).cloned();
        let count_owned: Option<(String, i64)> = framer
            .footer_count()
            .map(|(field, n)| (field.to_string(), n));
        if has_footer {
            let count_ref = count_owned.as_ref().map(|(f, n)| (f.as_str(), *n));
            self.write_section_element("footer", footer_owned.as_ref(), count_ref)?;
        }
        let end = BytesEnd::new("Document");
        self.writer
            .write_event(Event::End(end))
            .map_err(|e| FormatError::Xml(e.to_string()))?;
        Ok(())
    }
}

// ── Field tree for dotted name → nested element expansion ────────────

enum FieldNode {
    Leaf {
        name: String,
        text: String,
    },
    Branch {
        name: String,
        children: Vec<FieldNode>,
    },
}

/// Build a tree from dotted field names. Fields with shared prefixes
/// are grouped under a single parent branch.
/// E.g., `Address.City` and `Address.State` → Branch("Address", [Leaf("City"), Leaf("State")])
///
/// Returns `FormatError::UnserializableMapValue` if any field carries
/// a `Value::Map` payload — XML elements have no canonical scalar
/// serialization for a map, and `value_to_text` raises from the Map
/// arm directly.
fn build_field_tree(
    fields: &[(&str, &Value)],
    preserve_nulls: bool,
) -> Result<Vec<FieldNode>, FormatError> {
    let mut roots: Vec<FieldNode> = Vec::new();

    for &(name, val) in fields {
        if val.is_null() && !preserve_nulls {
            continue;
        }
        let text = value_to_text(name, val)?;
        insert_field(&mut roots, name, &text);
    }

    Ok(roots)
}

/// Insert a dotted field name into the tree, creating branches as needed.
fn insert_field(nodes: &mut Vec<FieldNode>, path: &str, text: &str) {
    if let Some((first, rest)) = path.split_once('.') {
        // Find or create a branch for `first`
        let branch = nodes
            .iter_mut()
            .find(|n| matches!(n, FieldNode::Branch { name, .. } if name == first));
        if let Some(FieldNode::Branch { children, .. }) = branch {
            insert_field(children, rest, text);
        } else {
            let mut children = Vec::new();
            insert_field(&mut children, rest, text);
            nodes.push(FieldNode::Branch {
                name: first.to_string(),
                children,
            });
        }
    } else {
        nodes.push(FieldNode::Leaf {
            name: path.to_string(),
            text: text.to_string(),
        });
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

    fn doc_with_sections(
        sections: &[(&str, &[(&str, Value)])],
    ) -> Arc<clinker_record::DocumentContext> {
        use indexmap::IndexMap;
        let mut map: IndexMap<Box<str>, Value> = IndexMap::new();
        for (name, fields) in sections {
            let mut inner: IndexMap<Box<str>, Value> = IndexMap::new();
            for (k, v) in *fields {
                inner.insert(Box::from(*k), v.clone());
            }
            map.insert(Box::from(*name), Value::Map(Box::new(inner)));
        }
        Arc::new(clinker_record::DocumentContext::new(
            clinker_record::DocumentId::next(),
            Arc::from("f.xml"),
            map,
        ))
    }

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
}
