use std::sync::Arc;

use clinker_format::fixed_width::{
    FixedWidthReader, FixedWidthReaderConfig, FixedWidthWriter, FixedWidthWriterConfig,
};
use clinker_format::{
    Column, FixedWidthCountField, FixedWidthFill, FixedWidthOccurs, FixedWidthOverflow,
    FormatReader, FormatWriter,
};
use clinker_record::{Record, Schema, Value};
use cxl::typecheck::Type;
use indexmap::IndexMap;

fn child(name: &str, start: usize, width: usize) -> Column {
    Column {
        start: Some(start),
        width: Some(width),
        ..Column::bare(name, Type::String)
    }
}

fn group(name: &str, start: usize, max: usize, with_count: bool) -> Column {
    Column {
        start: Some(start),
        multiple: Some(true),
        fields: Some(vec![child("kind", 0, 1), child("code", 1, 2)]),
        occurs: Some(FixedWidthOccurs {
            min: 0,
            max,
            fill: FixedWidthFill::Pad,
            on_overflow: FixedWidthOverflow::Error,
            keep: None,
        }),
        count_field: with_count.then(|| FixedWidthCountField {
            name: format!("{name}_count"),
            width: 1,
        }),
        ..Column::bare(name, Type::Map)
    }
}

fn nested_record(fields: &[(&str, Value)]) -> Value {
    let values = fields
        .iter()
        .map(|(name, value)| ((*name).into(), value.clone()))
        .collect::<IndexMap<Box<str>, Value>>();
    Value::Map(Box::new(values))
}

fn record(name: &str, value: Value) -> Record {
    Record::new(Arc::new(Schema::new(vec![name.into()])), vec![value])
}

#[test]
fn tracer_group_round_trip() {
    let layout = vec![group("transactions", 0, 2, true)];
    let input = Value::Array(vec![
        nested_record(&[
            ("kind", Value::String("A".into())),
            ("code", Value::String("01".into())),
        ]),
        nested_record(&[
            ("kind", Value::String("B".into())),
            ("code", Value::String("02".into())),
        ]),
    ]);

    let mut bytes = Vec::new();
    {
        let mut writer = FixedWidthWriter::new(
            &mut bytes,
            layout.clone(),
            FixedWidthWriterConfig::default(),
        )
        .expect("bounded group layout should resolve");
        writer
            .write_record(&record("transactions", input.clone()))
            .expect("two occurrences should fit");
        writer.flush().expect("flush output");
    }
    assert_eq!(bytes, b"2A01B02\n");

    let mut reader = FixedWidthReader::new(
        bytes.as_slice(),
        layout,
        FixedWidthReaderConfig::default(),
    )
    .expect("same layout should resolve for reading");
    let round_trip = reader
        .next_record()
        .expect("read result")
        .expect("one record");
    assert_eq!(round_trip.get("transactions"), Some(&input));

    let invalid = Value::Array(vec![
        nested_record(&[
            ("kind", Value::String("A".into())),
            ("code", Value::String("01".into())),
        ]),
        Value::String("not a nested record".into()),
    ]);
    let mut destination = Vec::new();
    let mut writer = FixedWidthWriter::new(
        &mut destination,
        vec![group("transactions", 0, 2, true)],
        FixedWidthWriterConfig::default(),
    )
    .expect("layout should resolve");
    let error = writer
        .write_record(&record("transactions", invalid))
        .expect_err("an invalid later occurrence must reject the record");
    assert!(error.to_string().contains("transactions"), "{error}");
    drop(writer);
    assert!(destination.is_empty(), "record writes must be atomic");
}
