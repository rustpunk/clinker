//! XML writer with configurable root/record elements, dotted field expansion
//! to nested elements, null handling, and proper escaping.

use std::io::Write;
use std::sync::Arc;

use quick_xml::Writer as XmlEmitter;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatWriter;

#[derive(Clone)]
pub struct XmlWriterConfig {
    pub root_element: String,
    pub record_element: String,
    pub preserve_nulls: bool,
}

impl Default for XmlWriterConfig {
    fn default() -> Self {
        Self {
            root_element: "Root".into(),
            record_element: "Record".into(),
            preserve_nulls: false,
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
}

impl<W: Write> XmlWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: XmlWriterConfig) -> Self {
        Self {
            writer: XmlEmitter::new(writer),
            _schema: schema,
            config,
            header_written: false,
        }
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

    /// Collect schema fields as (name, value) pairs, then write them as
    /// nested XML elements. Metadata (`$meta.*`) is stripped from the
    /// default output; the Output-node `include_metadata` flag is the
    /// opt-in for layers that want to surface it.
    fn write_fields(&mut self, record: &Record) -> Result<(), FormatError> {
        let fields: Vec<(&str, &Value)> = record.iter_all_fields().collect();

        // Build a tree of field segments for nested expansion
        let tree = build_field_tree(&fields, self.config.preserve_nulls);
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
fn build_field_tree(fields: &[(&str, &Value)], preserve_nulls: bool) -> Vec<FieldNode> {
    let mut roots: Vec<FieldNode> = Vec::new();

    for &(name, val) in fields {
        if val.is_null() && !preserve_nulls {
            continue;
        }
        let text = value_to_text(val);
        insert_field(&mut roots, name, &text);
    }

    roots
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
fn value_to_text(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Array(arr) => arr.iter().map(value_to_text).collect::<Vec<_>>().join(","),
        Value::Map(m) => serde_json::to_string(m.as_ref()).unwrap_or_default(),
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
        let mut reader = XmlReader::new(
            std::io::BufReader::new(cursor),
            XmlReaderConfig {
                record_path: Some("Root/Record".into()),
                ..Default::default()
            },
        );
        let _s = reader.schema().unwrap();
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());

        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("value"), Some(&Value::Integer(42)));
        assert_eq!(r2.get("name"), Some(&Value::String("Bob".into())));
    }

    /// Default XML writer output contains only schema columns; metadata
    /// stays in `$meta.*` unless a higher layer opts in via
    /// `include_metadata: true` (which route-rewrites the Record
    /// upstream of the writer).
    #[test]
    fn test_xml_writer_emits_schema_fields_only() {
        let schema = test_schema();
        let mut record = make_record(&schema, "Alice", 30);
        record
            .set_meta("audit", Value::String("flagged".into()))
            .unwrap();
        let output = write_records(XmlWriterConfig::default(), &[record], &schema);
        assert!(output.contains("<name>Alice</name>"));
        assert!(output.contains("<age>30</age>"));
        assert!(
            !output.contains("audit"),
            "default writer must strip metadata; got: {output}"
        );
        assert!(
            !output.contains("flagged"),
            "default writer must strip metadata values; got: {output}"
        );
    }
}
