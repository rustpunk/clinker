use std::sync::Arc;

use clinker_format::fixed_width::field::ResolvedRepeatingGroup;
use clinker_format::fixed_width::{
    FixedWidthReader, FixedWidthReaderConfig, FixedWidthWriter, FixedWidthWriterConfig,
};
use clinker_format::{
    Column, FixedWidthCountField, FixedWidthFill, FixedWidthOccurs, FixedWidthOverflow,
    FixedWidthTruncateKeep, FormatError, FormatReader, FormatWriter,
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

fn occurrence(kind: &str, code: &str) -> Value {
    nested_record(&[
        ("kind", Value::String(kind.into())),
        ("code", Value::String(code.into())),
    ])
}

fn record(name: &str, value: Value) -> Record {
    Record::new(Arc::new(Schema::new(vec![name.into()])), vec![value])
}

fn record_fields(fields: &[(&str, Value)]) -> Record {
    Record::new(
        Arc::new(Schema::new(
            fields.iter().map(|(name, _)| (*name).into()).collect(),
        )),
        fields.iter().map(|(_, value)| value.clone()).collect(),
    )
}

fn scalar(name: &str, start: usize, width: usize) -> Column {
    Column {
        start: Some(start),
        width: Some(width),
        ..Column::bare(name, Type::String)
    }
}

fn write(layout: Vec<Column>, record: &Record) -> Result<Vec<u8>, FormatError> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            FixedWidthWriter::new(&mut bytes, layout, FixedWidthWriterConfig::default())?;
        writer.write_record(record)?;
        writer.flush()?;
    }
    Ok(bytes)
}

fn read(layout: Vec<Column>, bytes: &[u8]) -> Result<Record, FormatError> {
    let mut reader = FixedWidthReader::new(bytes, layout, FixedWidthReaderConfig::default())?;
    reader
        .next_record()?
        .ok_or_else(|| FormatError::InvalidRecord {
            row: 0,
            message: "expected one test record".to_string(),
        })
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

    let resolved = ResolvedRepeatingGroup::from_column_at(&layout[0], 0)
        .expect("layout resolves without consuming input");
    assert_eq!(resolved.occurrence_width(), 3);
    assert_eq!(resolved.max_width(), 7, "count plus two occurrences");

    let mut reader =
        FixedWidthReader::new(bytes.as_slice(), layout, FixedWidthReaderConfig::default())
            .expect("same layout should resolve for reading");
    let round_trip = reader
        .next_record()
        .expect("read result")
        .expect("one record");
    assert_eq!(round_trip.get("transactions"), Some(&input));
    let logical_schema = reader.schema().expect("logical schema");
    assert_eq!(logical_schema.columns().len(), 1);
    assert_eq!(logical_schema.columns()[0].as_ref(), "transactions");

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

    let missing_max = serde_json::from_str::<Column>(
        r#"{
            "name":"transactions","type":"map","multiple":true,"start":0,
            "fields":[{"name":"kind","type":"string","width":1}],
            "occurs":{}
        }"#,
    )
    .expect_err("occurs.max is required");
    assert!(missing_max.to_string().contains("max"), "{missing_max}");

    let mut zero_max = group("transactions", 0, 1, false);
    zero_max.occurs.as_mut().expect("occurs").max = 0;
    let zero_error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![zero_max],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("zero max must fail before I/O");
    assert!(zero_error.to_string().contains("positive"), "{zero_error}");

    let overflow = group("transactions", 0, usize::MAX, false);
    let overflow_error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![overflow],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("overflowing layout must fail before allocation");
    assert!(
        overflow_error.to_string().contains("overflows"),
        "{overflow_error}"
    );

    let mut nested = group("transactions", 0, 2, false);
    nested.fields.as_mut().expect("children")[0].fields = Some(vec![child("nested", 0, 1)]);
    let nested_error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![nested],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("recursive groups must fail before I/O");
    assert!(
        nested_error.to_string().contains("flatten"),
        "{nested_error}"
    );

    let count_position = serde_json::from_str::<Column>(
        r#"{
            "name":"transactions","type":"map","multiple":true,"start":0,
            "fields":[{"name":"kind","type":"string","width":1}],
            "occurs":{"max":2},
            "count_field":{"name":"transaction_count","width":1,"start":1}
        }"#,
    )
    .expect_err("a count position cannot overlap or bypass the derived leading cell");
    assert!(
        count_position.to_string().contains("start"),
        "{count_position}"
    );
}

#[test]
fn zero_null_one_min_and_max_have_exact_pad_bytes() {
    let layout = vec![group("transactions", 0, 2, false)];
    for zero in [Value::Null, Value::Array(Vec::new())] {
        let bytes = write(layout.clone(), &record("transactions", zero)).expect("zero writes");
        assert_eq!(bytes, b"      \n");
        assert_eq!(
            read(layout.clone(), &bytes)
                .expect("zero reads")
                .get("transactions"),
            Some(&Value::Array(Vec::new()))
        );
    }
    let absent = Record::new(Arc::new(Schema::new(Vec::new())), Vec::new());
    let absent_bytes = write(layout.clone(), &absent).expect("absent group writes as zero");
    assert_eq!(absent_bytes, b"      \n");

    let one = Value::Array(vec![occurrence("A", "01")]);
    let one_bytes = write(layout.clone(), &record("transactions", one.clone())).expect("one");
    assert_eq!(one_bytes, b"A01   \n");
    assert_eq!(
        read(layout.clone(), &one_bytes)
            .expect("one reads")
            .get("transactions"),
        Some(&one)
    );

    let maximum = Value::Array(vec![occurrence("A", "01"), occurrence("B", "02")]);
    let max_bytes =
        write(layout.clone(), &record("transactions", maximum.clone())).expect("maximum");
    assert_eq!(max_bytes, b"A01B02\n");
    assert_eq!(
        read(layout, &max_bytes)
            .expect("maximum reads")
            .get("transactions"),
        Some(&maximum)
    );

    let mut minimum_layout = group("transactions", 0, 2, false);
    minimum_layout.occurs.as_mut().expect("occurs").min = 1;
    let error = write(
        vec![minimum_layout.clone()],
        &record("transactions", Value::Null),
    )
    .expect_err("min minus one rejects");
    assert!(error.to_string().contains("minimum is 1"), "{error}");
    write(
        vec![minimum_layout],
        &record("transactions", Value::Array(vec![occurrence("A", "01")])),
    )
    .expect("minimum writes");
}

#[test]
fn pad_and_shift_keep_adjacent_fields_at_deterministic_offsets() {
    let pad_layout = vec![group("transactions", 0, 2, false), scalar("tail", 6, 1)];
    let value = Value::Array(vec![occurrence("A", "01")]);
    let pad_record = record_fields(&[
        ("transactions", value.clone()),
        ("tail", Value::String("Z".into())),
    ]);
    let pad_bytes = write(pad_layout.clone(), &pad_record).expect("pad writes");
    assert_eq!(pad_bytes, b"A01   Z\n");
    let pad_read = read(pad_layout, &pad_bytes).expect("pad reads");
    assert_eq!(pad_read.get("transactions"), Some(&value));
    assert_eq!(pad_read.get("tail"), Some(&Value::String("Z".into())));

    let mut shifted = group("transactions", 0, 2, true);
    shifted.occurs.as_mut().expect("occurs").fill = FixedWidthFill::Shift;
    let shift_layout = vec![shifted, scalar("tail", 7, 1)];
    let shift_bytes = write(shift_layout.clone(), &pad_record).expect("shift writes");
    assert_eq!(shift_bytes, b"1A01Z\n");
    let shift_read = read(shift_layout, &shift_bytes).expect("shift reads");
    assert_eq!(shift_read.get("transactions"), Some(&value));
    assert_eq!(shift_read.get("tail"), Some(&Value::String("Z".into())));

    let mut last_without_count = group("transactions", 0, 2, false);
    last_without_count.occurs.as_mut().expect("occurs").fill = FixedWidthFill::Shift;
    let short = write(
        vec![last_without_count.clone()],
        &record("transactions", value.clone()),
    )
    .expect("last shifted group writes");
    assert_eq!(short, b"A01\n");
    assert_eq!(
        read(vec![last_without_count], &short)
            .expect("last shifted group reads")
            .get("transactions"),
        Some(&value)
    );

    let mut ambiguous_shift = group("transactions", 0, 2, false);
    ambiguous_shift.occurs.as_mut().expect("occurs").fill = FixedWidthFill::Shift;
    let error = FixedWidthReader::new(
        &b"A01Z\n"[..],
        vec![ambiguous_shift, scalar("tail", 6, 1)],
        FixedWidthReaderConfig::default(),
    )
    .err()
    .expect("shift before another field needs a count");
    assert!(error.to_string().contains("count_field"), "{error}");
}

#[test]
fn overflow_errors_or_retains_the_selected_end_atomically() {
    let values = Value::Array(vec![
        occurrence("A", "01"),
        occurrence("B", "02"),
        occurrence("C", "03"),
    ]);
    let error = write(
        vec![group("transactions", 0, 2, false)],
        &record("transactions", values.clone()),
    )
    .expect_err("max plus one rejects by default");
    let message = error.to_string();
    assert!(message.contains("transactions"), "{message}");
    assert!(message.contains("maximum is 2"), "{message}");
    assert!(message.contains("contains 3"), "{message}");

    let mut destination = Vec::new();
    let mut writer = FixedWidthWriter::new(
        &mut destination,
        vec![group("transactions", 0, 2, false)],
        FixedWidthWriterConfig::default(),
    )
    .expect("layout");
    writer
        .write_record(&record("transactions", values.clone()))
        .expect_err("overflow rejects before destination write");
    drop(writer);
    assert!(destination.is_empty(), "overflow writes must be atomic");

    for (keep, expected) in [
        (FixedWidthTruncateKeep::First, &b"A01B02\n"[..]),
        (FixedWidthTruncateKeep::Last, &b"B02C03\n"[..]),
    ] {
        let mut layout = group("transactions", 0, 2, false);
        let occurs = layout.occurs.as_mut().expect("occurs");
        occurs.on_overflow = FixedWidthOverflow::Truncate;
        occurs.keep = Some(keep);
        assert_eq!(
            write(vec![layout], &record("transactions", values.clone())).expect("truncate"),
            expected
        );
    }
}

#[test]
fn count_field_controls_cardinality_and_detects_extra_payload() {
    let layout = vec![group("transactions", 0, 2, true)];
    let one = read(layout.clone(), b"1A01   \n").expect("count one");
    assert_eq!(
        one.get("transactions"),
        Some(&Value::Array(vec![occurrence("A", "01")]))
    );

    let two = read(layout.clone(), b"2A01   \n").expect("count two");
    assert_eq!(
        two.get("transactions"),
        Some(&Value::Array(vec![
            occurrence("A", "01"),
            nested_record(&[("kind", Value::Null), ("code", Value::Null)]),
        ]))
    );

    let error = read(layout, b"1A01B02\n").expect_err("count/payload mismatch");
    let message = error.to_string();
    assert!(message.contains("transactions"), "{message}");
    assert!(message.contains("declares 1"), "{message}");
    assert!(message.contains("slot 2"), "{message}");
}

#[test]
fn ambiguous_padding_and_non_record_shapes_fail_before_destination_write() {
    let blank = Value::Array(vec![nested_record(&[
        ("kind", Value::Null),
        ("code", Value::Null),
    ])]);
    let error = write(
        vec![group("transactions", 0, 2, false)],
        &record("transactions", blank),
    )
    .expect_err("blank occurrence is ambiguous without a count");
    assert!(error.to_string().contains("count_field"), "{error}");

    for invalid in [
        Value::String("not an array".into()),
        Value::Array(vec![Value::String("not a record".into())]),
    ] {
        let mut destination = Vec::new();
        let mut writer = FixedWidthWriter::new(
            &mut destination,
            vec![group("transactions", 0, 2, true)],
            FixedWidthWriterConfig::default(),
        )
        .expect("layout");
        writer
            .write_record(&record("transactions", invalid))
            .expect_err("shape rejects");
        drop(writer);
        assert!(destination.is_empty(), "invalid record is atomic");
    }
}

#[test]
fn adjacent_groups_preserve_equal_occurrences_and_never_exchange_bytes() {
    let first = group("first", 0, 2, true);
    let second = group("second", 7, 2, true);
    let equal = occurrence("A", "01");
    let record = record_fields(&[
        ("first", Value::Array(vec![equal.clone(), equal.clone()])),
        ("second", Value::Array(vec![occurrence("B", "02")])),
    ]);
    let layout = vec![first, second];
    let bytes = write(layout.clone(), &record).expect("adjacent groups write");
    assert_eq!(bytes, b"2A01A011B02   \n");
    let round_trip = read(layout, &bytes).expect("adjacent groups read");
    assert_eq!(round_trip.get("first"), record.get("first"));
    assert_eq!(round_trip.get("second"), record.get("second"));
}

#[test]
fn malformed_occurrence_policies_fail_during_layout_resolution() {
    let mut minimum = group("transactions", 0, 2, false);
    minimum.occurs.as_mut().expect("occurs").min = 3;
    let error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![minimum],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("minimum cannot exceed maximum");
    assert!(error.to_string().contains("greater than"), "{error}");

    let mut truncate_without_end = group("transactions", 0, 2, false);
    truncate_without_end
        .occurs
        .as_mut()
        .expect("occurs")
        .on_overflow = FixedWidthOverflow::Truncate;
    let error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![truncate_without_end],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("truncation must select a retained end");
    assert!(error.to_string().contains("keep: first"), "{error}");

    let mut retained_end_with_error = group("transactions", 0, 2, false);
    retained_end_with_error
        .occurs
        .as_mut()
        .expect("occurs")
        .keep = Some(FixedWidthTruncateKeep::First);
    let error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![retained_end_with_error],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("error overflow policy cannot select a retained end");
    assert!(error.to_string().contains("remove `keep`"), "{error}");

    let mut narrow_count = group("transactions", 0, 10, true);
    narrow_count.count_field.as_mut().expect("count").width = 1;
    let error = FixedWidthWriter::new(
        Vec::<u8>::new(),
        vec![narrow_count],
        FixedWidthWriterConfig::default(),
    )
    .err()
    .expect("count width must fit the maximum");
    assert!(error.to_string().contains("at least 2"), "{error}");
}
