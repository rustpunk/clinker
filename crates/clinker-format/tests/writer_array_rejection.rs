//! Crate-boundary handling of `Value::Array` payloads across the writers.
//!
//! The fixed-width writer still rejects a stray array — most often a
//! `match: collect` combine output misrouted to a positional format — as an
//! explicit the bounded fixed-width scalar error naming the offending column,
//! listing the schema, scalar-coercion, and JSON remedies. The
//! CSV and XML encode arrays only for columns whose schema declares
//! `multiple: true`; an undeclared array remains a loud routing error. JSON
//! output serializes arrays natively. See #46, #917, #916, and #944.

use clinker_record::owned_storage::{OwnedValues, SharedStorage};
use std::collections::BTreeSet;
use std::sync::Arc;

use clinker_format::csv::writer::{CsvEncoder, CsvWriterConfig};
use clinker_format::error::OutputEncodingKind;
use clinker_format::fixed_width::{FixedWidthEncoder, FixedWidthWriterConfig};
use clinker_format::json::writer::{JsonEncoder, JsonWriterConfig};
use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
use clinker_format::xml::writer::{XmlEncoder, XmlWriterConfig};
use clinker_format::{Column, FormatError, FormatWriter};
use clinker_record::{Record, Schema, Value};
use cxl::typecheck::Type;

/// A two-column record whose second column (`tags`) carries a `Value::Array`,
/// the shape a `match: collect` combine build side produces.
fn record_with_array() -> (SharedStorage<Schema>, Record) {
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "tags".into()])));
    let record = Record::new(
        schema.clone(),
        vec![
            Value::Integer(7),
            Value::Array(OwnedValues::from_vec(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ])),
        ],
    );
    (schema, record)
}

/// Every user remedy must appear in the surfaced message: a repeated-field
/// declaration, CXL coercion (`to_string`), and JSON output.
fn assert_lists_remedies(err: &FormatError) {
    let msg = err.to_string();
    assert!(
        msg.contains("multiple: true"),
        "message lists the repeated-field declaration remedy: {msg}"
    );
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
        let config = CsvWriterConfig {
            declared_multiple: BTreeSet::from(["tags".to_string()]),
            ..CsvWriterConfig::default()
        };
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = CsvEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        writer
            .write_record(&record)
            .expect("CSV writer joins a scalar array into one cell");
        writer.flush().expect("flush succeeds");
    }
    let out = String::from_utf8(buf).expect("CSV output is UTF-8");
    assert_eq!(out, "id,tags\n7,a;b\n");
}

#[test]
fn csv_writer_rejects_array_in_undeclared_column() {
    let (schema, record) = record_with_array();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder =
        CsvEncoder::new(schema, &CsvWriterConfig::default(), provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let err = writer.write_record(&record).unwrap_err();
    assert!(
        matches!(&err, FormatError::OutputEncoding { format: "CSV", field: 2, offset: 0, kind: OutputEncodingKind::Array, field_name, element: None }
            if field_name.to_string() == "tags"),
        "expected undeclared CSV array rejection, got {err:?}"
    );
    assert_lists_remedies(&err);
}

/// The XML writer now emits an array as repeated child elements (#916) rather
/// than rejecting it: with `multiple:` XML output supported, a repeated element
/// is the expected shape. The default (no `join_values` config) names each
/// element after the field.
#[test]
fn xml_writer_emits_repeated_elements_for_array() {
    let (schema, record) = record_with_array();
    let mut buf = Vec::new();
    {
        let config = XmlWriterConfig {
            declared_multiple: BTreeSet::from(["tags".to_string()]),
            ..XmlWriterConfig::default()
        };
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = XmlEncoder::new(schema.clone(), &config, provider.resources()).unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        writer
            .write_record(&record)
            .expect("XML writer emits repeated elements for a scalar array");
        writer.flush().expect("flush succeeds");
    }
    let out = String::from_utf8(buf).expect("XML output is UTF-8");
    assert_eq!(
        out,
        "<Root><Record><id>7</id><tags>a</tags><tags>b</tags></Record></Root>"
    );
}

#[test]
fn xml_writer_rejects_array_in_undeclared_column() {
    let (schema, record) = record_with_array();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder =
        XmlEncoder::new(schema, &XmlWriterConfig::default(), provider.resources()).unwrap();
    let mut writer = PreparedWriter::new(Vec::new(), encoder, provider.resources()).unwrap();
    let err = writer.write_record(&record).unwrap_err();
    assert!(
        matches!(&err, FormatError::OutputEncoding { format: "XML", field: 2, offset: 0, kind: OutputEncodingKind::Array, field_name, element: None }
            if field_name.to_string() == "tags"),
        "expected undeclared XML array rejection, got {err:?}"
    );
    assert_lists_remedies(&err);
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
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
    let encoder = FixedWidthEncoder::new(
        &[id, tags],
        &FixedWidthWriterConfig::default(),
        provider.resources(),
    )
    .expect("fixed-width encoder admits a valid layout");
    let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
    let err = writer.write_record(&record).unwrap_err();
    assert!(
        matches!(&err, FormatError::OutputEncoding { format: "fixed-width", field: 2, offset: 0, kind: OutputEncodingKind::FixedWidthScalar, field_name, element: None }
            if field_name.to_string() == "tags"),
        "expected bounded array rejection for fixed-width/tags, got {err:?}"
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
        let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1024 * 1024).unwrap());
        let encoder = JsonEncoder::new(
            schema.clone(),
            &JsonWriterConfig::default(),
            provider.resources(),
        )
        .unwrap();
        let mut writer = PreparedWriter::new(&mut buf, encoder, provider.resources()).unwrap();
        writer
            .write_record(&record)
            .expect("JSON writer serializes an array natively");
        writer.flush().expect("flush succeeds");
    }
    let out = String::from_utf8(buf).expect("JSON output is UTF-8");
    assert_eq!(out, "[\n{\"id\":7,\"tags\":[\"a\",\"b\"]}\n]\n");
}

#[test]
fn native_writer_construction_refuses_insufficient_finite_resources() {
    use clinker_format::preparation::ResourceErrorKind;
    let (schema, _) = record_with_array();
    let provider = MemoryOnlyResources::new(std::num::NonZeroUsize::new(1).unwrap());
    let json = JsonEncoder::new(
        schema.clone(),
        &JsonWriterConfig::default(),
        provider.resources(),
    );
    assert!(
        matches!(json, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
    );
    assert_eq!(provider.used(), 0);
    let xml = XmlEncoder::new(schema, &XmlWriterConfig::default(), provider.resources());
    assert!(
        matches!(xml, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Budget)
    );
    assert_eq!(provider.used(), 0);
}
