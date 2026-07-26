//! The JSON and XML writers expand one column set the same way.
//!
//! Both decode column names with the shared record-space grammar, so grouping,
//! key order, null pruning, escaping, and the rejection of an unexpandable
//! column set are properties of the grammar rather than of either writer. A
//! change that made one writer disagree with the other would fail here.

use std::sync::Arc;

use clinker_format::json::writer::{JsonOutputMode, JsonWriter, JsonWriterConfig};
use clinker_format::xml::writer::{XmlWriter, XmlWriterConfig};
use clinker_format::{FormatError, FormatWriter};
use clinker_record::schema::FieldMetadata;
use clinker_record::{Record, Schema, SchemaBuilder, Value};

fn schema_of(columns: &[&str]) -> Arc<Schema> {
    columns.iter().copied().collect::<SchemaBuilder>().build()
}

fn write_json(schema: &Arc<Schema>, values: Vec<Value>, preserve_nulls: bool) -> String {
    let config = JsonWriterConfig {
        format: JsonOutputMode::Ndjson,
        preserve_nulls,
        ..Default::default()
    };
    let mut buf = Vec::new();
    {
        let mut w = JsonWriter::new(&mut buf, Arc::clone(schema), config);
        w.write_record(&Record::new(Arc::clone(schema), values))
            .expect("json record writes");
        w.flush().expect("json writer flushes");
    }
    String::from_utf8(buf)
        .expect("utf-8")
        .trim_end()
        .to_string()
}

fn write_xml(schema: &Arc<Schema>, values: Vec<Value>, preserve_nulls: bool) -> String {
    let config = XmlWriterConfig {
        preserve_nulls,
        ..Default::default()
    };
    let mut buf = Vec::new();
    {
        let mut w = XmlWriter::new(&mut buf, Arc::clone(schema), config);
        w.write_record(&Record::new(Arc::clone(schema), values))
            .expect("xml record writes");
        w.flush().expect("xml writer flushes");
    }
    String::from_utf8(buf).expect("utf-8")
}

/// The error each writer raises for the same column set, or `None` when it
/// accepted the set.
fn refusal(schema: &Arc<Schema>, values: Vec<Value>) -> (Option<FormatError>, Option<FormatError>) {
    let mut json_buf = Vec::new();
    let json = {
        let mut w = JsonWriter::new(
            &mut json_buf,
            Arc::clone(schema),
            JsonWriterConfig::default(),
        );
        w.write_record(&Record::new(Arc::clone(schema), values.clone()))
            .err()
    };
    let mut xml_buf = Vec::new();
    let xml = {
        let mut w = XmlWriter::new(&mut xml_buf, Arc::clone(schema), XmlWriterConfig::default());
        w.write_record(&Record::new(Arc::clone(schema), values))
            .err()
    };
    (json, xml)
}

#[test]
fn both_writers_group_a_shared_prefix_at_its_first_occurrence() {
    let schema = schema_of(&["A.x", "n", "A.y"]);
    let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    assert_eq!(
        write_json(&schema, values.clone(), false),
        r#"{"A":{"x":1,"y":3},"n":2}"#
    );
    assert!(
        write_xml(&schema, values, false).contains("<A><x>1</x><y>3</y></A><n>2</n>"),
        "XML groups and hoists the same way"
    );
}

#[test]
fn both_writers_nest_to_the_same_depth() {
    let schema = schema_of(&["a.b.c", "a.b.d"]);
    let values = vec![Value::Integer(1), Value::Integer(2)];
    assert_eq!(
        write_json(&schema, values.clone(), false),
        r#"{"a":{"b":{"c":1,"d":2}}}"#
    );
    assert!(write_xml(&schema, values, false).contains("<a><b><c>1</c><d>2</d></b></a>"));
}

#[test]
fn both_writers_omit_a_container_whose_children_are_all_absent() {
    let schema = schema_of(&["a.b", "a.c", "d"]);
    let values = vec![Value::Null, Value::Null, Value::Integer(9)];
    assert_eq!(write_json(&schema, values.clone(), false), r#"{"d":9}"#);
    let xml = write_xml(&schema, values, false);
    assert!(
        !xml.contains("<a"),
        "XML omits the empty container too: {xml}"
    );
    assert!(xml.contains("<d>9</d>"));
}

#[test]
fn both_writers_keep_an_escaped_separator_in_the_name() {
    // `.` is a legal XML NameChar, so the escaped column lands as one element
    // named `a.b` — the same single key JSON emits.
    let schema = schema_of(&["a\\.b"]);
    let values = vec![Value::Integer(1)];
    assert_eq!(write_json(&schema, values.clone(), false), r#"{"a.b":1}"#);
    assert!(write_xml(&schema, values, false).contains("<a.b>1</a.b>"));
}

#[test]
fn both_writers_refuse_a_column_set_that_cannot_be_expanded() {
    // Before the shared grammar the XML writer silently emitted two sibling
    // `<a>` elements for this set, which its own reader then refused on the way
    // back in. Both writers now refuse it up front, with the same error.
    for columns in [["a", "a.b"], ["a.b", "a"], ["a.b", "a.b.c"]] {
        let schema = schema_of(&columns);
        let values = vec![Value::Integer(1); columns.len()];
        let (json, xml) = refusal(&schema, values);
        let (Some(json), Some(xml)) = (json, xml) else {
            panic!("both writers must refuse {columns:?}");
        };
        assert!(
            matches!(json, FormatError::FieldPath { format: "JSON", .. }),
            "JSON: {json:?}"
        );
        assert!(
            matches!(xml, FormatError::FieldPath { format: "XML", .. }),
            "XML: {xml:?}"
        );
        // Only the format label differs — the diagnostic itself is the
        // grammar's, so both name the same two columns and the same remedy.
        assert_eq!(
            json.to_string().replace("JSON", ""),
            xml.to_string().replace("XML", "")
        );
    }
}

#[test]
fn an_engine_stamped_column_expands_by_the_same_rule() {
    // The rule reads the column-name string and nothing else, so it applies to
    // engine-stamped columns with no carve-out. JSON nests `$ck.customer_id`
    // and reads back cleanly.
    //
    // XML cannot follow it here, and did not before this rule either: `$` is
    // not a legal XML name start character, so the segment `$ck` is refused at
    // the format boundary — after the shared decode, not instead of it. That is
    // a format constraint rather than a second grammar, and it means enabling
    // correlation-key output on an XML sink fails outright. Pinned so the
    // divergence stays a known, located fact.
    let schema = SchemaBuilder::new()
        .with_field("amount")
        .with_field_meta(
            "$ck.customer_id",
            FieldMetadata::source_correlation("customer_id"),
        )
        .build();
    let values = vec![Value::Integer(5), Value::String("C-1".into())];
    let record = || Record::new(Arc::clone(&schema), values.clone());

    let mut json_buf = Vec::new();
    {
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            include_engine_stamped: true,
            ..Default::default()
        };
        let mut w = JsonWriter::new(&mut json_buf, Arc::clone(&schema), config);
        w.write_record(&record()).expect("json accepts `$ck`");
        w.flush().expect("json flushes");
    }
    assert_eq!(
        String::from_utf8(json_buf).expect("utf-8").trim_end(),
        r#"{"amount":5,"$ck":{"customer_id":"C-1"}}"#
    );

    let mut xml_buf = Vec::new();
    let config = XmlWriterConfig {
        include_engine_stamped: true,
        ..Default::default()
    };
    let mut w = XmlWriter::new(&mut xml_buf, Arc::clone(&schema), config);
    let err = w
        .write_record(&record())
        .expect_err("XML has no well-formed name for `$ck`");
    assert!(
        matches!(err, FormatError::Xml(ref m) if m.contains("not a well-formed XML name")),
        "{err:?}"
    );
}

#[test]
fn both_writers_refuse_a_malformed_escape() {
    let schema = schema_of(&["C:\\temp"]);
    let (json, xml) = refusal(&schema, vec![Value::Integer(1)]);
    for err in [json, xml] {
        let err = err.expect("a malformed escape is refused by both writers");
        assert!(matches!(err, FormatError::FieldPath { .. }), "{err:?}");
    }
}
