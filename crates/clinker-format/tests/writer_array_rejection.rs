//! Crate-boundary handling of `Value::Array` payloads in the non-JSON writers.
//!
//! The XML and fixed-width writers still reject a stray array — most often a
//! `match: collect` combine output misrouted to a positional format — as an
//! explicit `FormatError::UnserializableArrayValue` naming the offending column,
//! listing the two remedies (coerce to a scalar in CXL, or route to JSON). The
//! CSV writer instead JOINS an array into one delimited cell (#917): with
//! multi-value CSV output a supported shape, an array is expected there, so it
//! is encoded rather than rejected. JSON output serializes arrays natively.
//! See #46, #917.

use std::sync::Arc;

use clinker_format::csv::{CsvWriter, CsvWriterConfig};
use clinker_format::fixed_width::{FixedWidthWriter, FixedWidthWriterConfig};
use clinker_format::json::writer::{JsonWriter, JsonWriterConfig};
use clinker_format::xml::writer::{XmlWriter, XmlWriterConfig};
use clinker_format::{Column, FormatError, FormatWriter};
use clinker_record::{Record, Schema, Value};
use cxl::typecheck::Type;

/// A two-column record whose second column (`tags`) carries a `Value::Array`,
/// the shape a `match: collect` combine build side produces.
fn record_with_array() -> (Arc<Schema>, Record) {
    let schema = Arc::new(Schema::new(vec!["id".into(), "tags".into()]));
    let record = Record::new(
        Arc::clone(&schema),
        vec![
            Value::Integer(7),
            Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
        ],
    );
    (schema, record)
}

/// Both user remedies named in the acceptance criteria must appear in the
/// surfaced message: CXL coercion (`to_string`) and JSON output.
fn assert_lists_remedies(err: &FormatError) {
    let msg = err.to_string();
    assert!(
        msg.contains("to_string"),
        "message lists the CXL-coercion remedy: {msg}"
    );
    assert!(
        msg.contains("JSON"),
        "message lists the JSON-output remedy: {msg}"
    );
}

/// The CSV writer now JOINS an array into one delimited cell (#917) rather than
/// rejecting it: with `multiple:` CSV output supported, an array cell is the
/// expected shape. The default `;` delimiter applies with no configuration.
#[test]
fn csv_writer_joins_array_into_delimited_cell() {
    let (schema, record) = record_with_array();
    let mut buf = Vec::new();
    {
        let mut writer = CsvWriter::new(&mut buf, Arc::clone(&schema), CsvWriterConfig::default());
        writer
            .write_record(&record)
            .expect("CSV writer joins a scalar array into one cell");
        writer.flush().expect("flush succeeds");
    }
    let out = String::from_utf8(buf).expect("CSV output is UTF-8");
    assert_eq!(out, "id,tags\n7,a;b\n");
}

#[test]
fn xml_writer_rejects_array_payload() {
    let (schema, record) = record_with_array();
    let mut buf = Vec::new();
    let mut writer = XmlWriter::new(&mut buf, Arc::clone(&schema), XmlWriterConfig::default());
    let err = writer.write_record(&record).unwrap_err();
    assert!(
        matches!(&err, FormatError::UnserializableArrayValue { format, column }
            if *format == "XML" && column == "tags"),
        "expected UnserializableArrayValue for XML/tags, got {err:?}"
    );
    assert_lists_remedies(&err);
    // The XML writer fills and validates every value before emitting any byte,
    // so a rejected record leaves no partial output.
    drop(writer);
    assert!(
        buf.is_empty(),
        "rejected record leaves no partial XML output"
    );
}

#[test]
fn fixed_width_writer_rejects_array_payload() {
    let (_schema, record) = record_with_array();
    let mut id = Column::bare("id", Type::Int);
    id.start = Some(0);
    id.width = Some(5);
    let mut tags = Column::bare("tags", Type::String);
    tags.start = Some(5);
    tags.width = Some(10);
    let mut buf = Vec::new();
    let mut writer =
        FixedWidthWriter::new(&mut buf, vec![id, tags], FixedWidthWriterConfig::default())
            .expect("fixed-width writer constructs from a valid layout");
    let err = writer.write_record(&record).unwrap_err();
    assert!(
        matches!(&err, FormatError::UnserializableArrayValue { format, column }
            if *format == "fixed-width" && column == "tags"),
        "expected UnserializableArrayValue for fixed-width/tags, got {err:?}"
    );
    assert_lists_remedies(&err);
}

/// A JSON writer serializes an array natively — the rejection is specific to
/// the non-self-describing formats, so JSON output is unchanged (criterion 3).
#[test]
fn json_writer_serializes_array_natively() {
    let (schema, record) = record_with_array();
    let mut buf = Vec::new();
    {
        let mut writer =
            JsonWriter::new(&mut buf, Arc::clone(&schema), JsonWriterConfig::default());
        writer
            .write_record(&record)
            .expect("JSON writer serializes an array natively");
        writer.flush().expect("flush succeeds");
    }
    let out = String::from_utf8(buf).expect("JSON output is UTF-8");
    assert!(
        out.contains('[') && out.contains("\"a\"") && out.contains("\"b\""),
        "the array serializes as a native JSON array: {out}"
    );
}
