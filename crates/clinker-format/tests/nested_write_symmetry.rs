//! The JSON and XML writers expand one column set the same way.
//!
//! Both decode column names with the shared record-space grammar, so grouping,
//! key order, null pruning, escaping, and the rejection of an unexpandable
//! column set are properties of the grammar rather than of either writer. A
//! change that made one writer disagree with the other would fail here.

use clinker_record::owned_storage::{OwnedMap, OwnedValues, SharedStorage};

use clinker_format::error::OutputEncodingKind;
use clinker_format::json::writer::{JsonEncoder, JsonOutputMode, JsonWriterConfig};
use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
use clinker_format::{FormatError, FormatWriter};
use clinker_record::schema::FieldMetadata;
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

fn schema_of(columns: &[&str]) -> SharedStorage<Schema> {
    columns.iter().copied().collect::<SchemaBuilder>().build()
}

fn write_json(schema: &SharedStorage<Schema>, values: Vec<Value>, preserve_nulls: bool) -> String {
    let config = JsonWriterConfig {
        format: JsonOutputMode::Ndjson,
        preserve_nulls,
        ..Default::default()
    };
    let mut buf = Vec::new();
    {
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        w.write_record(&Record::new(schema.clone(), values))
            .expect("json record writes");
        w.flush().expect("json writer flushes");
    }
    String::from_utf8(buf).expect("utf-8")
}

fn write_xml(schema: &SharedStorage<Schema>, values: Vec<Value>, preserve_nulls: bool) -> String {
    let config = XmlWriterConfig {
        preserve_nulls,
        ..Default::default()
    };
    let mut buf = Vec::new();
    {
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        w.write_record(&Record::new(schema.clone(), values))
            .expect("xml record writes");
        w.flush().expect("xml writer flushes");
    }
    String::from_utf8(buf).expect("utf-8")
}

/// The error each writer raises for the same column set, or `None` when it
/// accepted the set.
fn refusal(
    schema: &SharedStorage<Schema>,
    values: Vec<Value>,
) -> (Option<FormatError>, Option<FormatError>) {
    let mut json_buf = Vec::new();
    let json = {
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(
            schema.clone(),
            &JsonWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut w = PreparedWriter::new(&mut json_buf, encoder, provider.resources()).unwrap();
        w.write_record(&Record::new(schema.clone(), values.clone()))
            .err()
    };
    let mut xml_buf = Vec::new();
    let xml = {
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(
            schema.clone(),
            &XmlWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut w = PreparedWriter::new(&mut xml_buf, encoder, provider.resources()).unwrap();
        w.write_record(&Record::new(schema.clone(), values)).err()
    };
    assert!(json_buf.is_empty(), "refused JSON cannot publish framing");
    assert!(xml_buf.is_empty(), "refused XML cannot publish framing");
    (json, xml)
}

#[test]
fn both_writers_group_a_shared_prefix_at_its_first_occurrence() {
    let schema = schema_of(&["A.x", "n", "A.y"]);
    let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    assert_eq!(
        write_json(&schema, values.clone(), false),
        concat!(r#"{"A":{"x":1,"y":3},"n":2}"#, "\n")
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
        concat!(r#"{"a":{"b":{"c":1,"d":2}}}"#, "\n")
    );
    assert!(write_xml(&schema, values, false).contains("<a><b><c>1</c><d>2</d></b></a>"));
}

#[test]
fn both_writers_omit_a_container_whose_children_are_all_absent() {
    let schema = schema_of(&["a.b", "a.c", "d"]);
    let values = vec![Value::Null, Value::Null, Value::Integer(9)];
    assert_eq!(
        write_json(&schema, values.clone(), false),
        concat!(r#"{"d":9}"#, "\n")
    );
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
    assert_eq!(
        write_json(&schema, values.clone(), false),
        concat!(r#"{"a.b":1}"#, "\n")
    );
    assert!(write_xml(&schema, values, false).contains("<a.b>1</a.b>"));
}

#[test]
fn both_writers_refuse_a_column_set_that_cannot_be_expanded() {
    // Before the shared grammar the XML writer silently emitted two sibling
    // `<a>` elements for this set, which its own reader then refused on the way
    // back in. Both writers now refuse it up front, with bounded format-local reasons.
    for columns in [["a", "a.b"], ["a.b", "a"], ["a.b", "a.b.c"]] {
        let schema = schema_of(&columns);
        let values = vec![Value::Integer(1); columns.len()];
        let (json, xml) = refusal(&schema, values);
        let (Some(json), Some(xml)) = (json, xml) else {
            panic!("both writers must refuse {columns:?}");
        };
        assert!(
            matches!(&json, FormatError::OutputEncoding { format: "JSON", field: 2, kind: OutputEncodingKind::JsonPath, field_name, .. } if field_name.to_string() == columns[1]),
            "JSON: {json:?}"
        );
        assert!(
            matches!(&xml, FormatError::OutputEncoding { format: "XML", field: 2, kind: OutputEncodingKind::XmlPath, field_name, .. } if field_name.to_string() == columns[1]),
            "XML: {xml:?}"
        );
        assert!(json.to_string().contains("use distinct leaf paths"));
        assert!(xml.to_string().contains("use distinct leaf paths"));
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
    let record = || Record::new(schema.clone(), values.clone());

    let mut json_buf = Vec::new();
    {
        let config = JsonWriterConfig {
            format: JsonOutputMode::Ndjson,
            include_engine_stamped: true,
            ..Default::default()
        };
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut w = PreparedWriter::new(&mut json_buf, encoder, provider.resources()).unwrap();
        w.write_record(&record()).expect("json accepts `$ck`");
        w.flush().expect("json flushes");
    }
    assert_eq!(
        String::from_utf8(json_buf).expect("utf-8"),
        concat!(r#"{"amount":5,"$ck":{"customer_id":"C-1"}}"#, "\n")
    );

    let mut xml_buf = Vec::new();
    let config = XmlWriterConfig {
        include_engine_stamped: true,
        ..Default::default()
    };
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
    let mut w = PreparedWriter::new(&mut xml_buf, encoder, provider.resources()).unwrap();
    let err = w
        .write_record(&record())
        .expect_err("XML has no well-formed name for `$ck`");
    assert!(
        matches!(&err, FormatError::OutputEncoding { format: "XML", field: 2, kind: OutputEncodingKind::XmlName, field_name, .. } if field_name.to_string() == "$ck"),
        "{err:?}"
    );
}

#[test]
fn both_writers_refuse_a_malformed_escape() {
    let schema = schema_of(&["C:\\temp"]);
    let (json, xml) = refusal(&schema, vec![Value::Integer(1)]);
    for (error, expected_format, expected_kind) in [
        (json, "JSON", OutputEncodingKind::JsonPath),
        (xml, "XML", OutputEncodingKind::XmlPath),
    ] {
        let error = error.expect("a malformed escape is refused by both writers");
        assert!(
            matches!(&error, FormatError::OutputEncoding { format, field: 1, kind, field_name, .. } if *format == expected_format && *kind == expected_kind && field_name.to_string() == "C:\\temp"),
            "{error:?}"
        );
        assert!(error.to_string().contains(r"backslash as \\"));
    }
}

#[test]
fn native_nested_values_keep_order_and_format_specific_xml_roles() {
    let schema = schema_of(&["payload"]);
    let mut first = IndexMap::new();
    first.insert("@id".into(), Value::Integer(1));
    first.insert("#text".into(), Value::String("alpha".into()));
    let mut second = IndexMap::new();
    second.insert("@id".into(), Value::Integer(2));
    second.insert("#text".into(), Value::String("beta".into()));
    let mut payload = IndexMap::new();
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
    let value = Value::Map(OwnedMap::from_map(payload));

    assert_eq!(
        write_json(&schema, vec![value.clone()], false),
        concat!(
            r##"{"payload":{"@kind":"event","#text":"before","item":[{"@id":1,"#text":"alpha"},{"@id":2,"#text":"beta"}],"tail":"after"}}"##,
            "\n"
        )
    );
    assert_eq!(
        write_xml(&schema, vec![value], false),
        "<Root><Record><payload kind=\"event\">before<item id=\"1\">alpha</item><item id=\"2\">beta</item><tail>after</tail></payload></Record></Root>"
    );
}

#[test]
fn escaped_nested_keys_decode_for_json_and_are_validated_before_output() {
    let schema = schema_of(&["payload"]);
    let mut payload = IndexMap::new();
    payload.insert("\\@literal".into(), Value::Integer(1));
    assert_eq!(
        write_json(
            &schema,
            vec![Value::Map(OwnedMap::from_map(payload))],
            false
        ),
        concat!(r#"{"payload":{"@literal":1}}"#, "\n")
    );

    let mut duplicate = IndexMap::new();
    duplicate.insert("@id".into(), Value::Integer(1));
    duplicate.insert("\\@id".into(), Value::Integer(2));
    let record = Record::new(
        schema.clone(),
        vec![Value::Map(OwnedMap::from_map(duplicate))],
    );
    let mut json_buf = Vec::new();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = JsonEncoder::new(
        schema.clone(),
        &JsonWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut json = PreparedWriter::new(&mut json_buf, encoder, provider.resources()).unwrap();
    assert!(json.write_record(&record).is_err());
    drop(json);
    assert!(json_buf.is_empty());

    let mut xml_buf = Vec::new();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = XmlEncoder::new(
        schema.clone(),
        &XmlWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut xml = PreparedWriter::new(&mut xml_buf, encoder, provider.resources()).unwrap();
    assert!(xml.write_record(&record).is_err());
    drop(xml);
    assert!(xml_buf.is_empty());
}

#[test]
fn both_recursive_writers_reject_depth_cap_plus_one_before_output() {
    let schema = schema_of(&["payload"]);
    let mut value = Value::Null;
    for _ in 0..=clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH {
        value = Value::Array(OwnedValues::from_vec(vec![value]));
    }
    let record = Record::new(schema.clone(), vec![value]);

    let mut json_buf = Vec::new();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = JsonEncoder::new(
        schema.clone(),
        &JsonWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut json = PreparedWriter::new(&mut json_buf, encoder, provider.resources()).unwrap();
    assert!(json.write_record(&record).is_err());
    drop(json);
    assert!(json_buf.is_empty());

    let mut xml_buf = Vec::new();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = XmlEncoder::new(
        schema.clone(),
        &XmlWriterConfig::default(),
        provider.resources(),
    )
    .unwrap();
    let mut xml = PreparedWriter::new(&mut xml_buf, encoder, provider.resources()).unwrap();
    assert!(xml.write_record(&record).is_err());
    drop(xml);
    assert!(xml_buf.is_empty());
}
