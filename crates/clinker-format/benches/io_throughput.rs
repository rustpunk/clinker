use clinker_bench_support::{CsvPayload, MEDIUM, RecordFactory, SMALL};
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig};
use clinker_format::json::reader::{JsonReader, JsonReaderConfig};
use clinker_format::json::writer::{JsonWriter, JsonWriterConfig};
use clinker_format::traits::{FormatReader, FormatWriter};
use clinker_format::xml::writer::{XmlWriter, XmlWriterConfig};
use clinker_record::{Record, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::io::Cursor;
use std::sync::Arc;

// ── CSV Read ───────────────────────────────────────────────────────

fn bench_csv_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_read");
    for (record_count, field_count) in [(SMALL, 5), (SMALL, 20), (MEDIUM, 10)] {
        let csv_bytes = CsvPayload::generate(
            record_count,
            &clinker_bench_support::FieldKind::default_layout(field_count),
            16,
            42,
        );
        let byte_len = csv_bytes.len();

        group.throughput(Throughput::Bytes(byte_len as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{record_count}r_{field_count}f"), record_count),
            &csv_bytes,
            |b, data| {
                b.iter(|| {
                    let cursor = Cursor::new(data.clone());
                    let mut reader = CsvReader::from_reader(cursor, CsvReaderConfig::default());
                    let _schema = reader.schema().unwrap();
                    let mut count = 0u64;
                    while let Some(_rec) = reader.next_record().unwrap() {
                        count += 1;
                    }
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

// ── CSV Write ──────────────────────────────────────────────────────

fn bench_csv_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_write");
    for (record_count, field_count) in [(SMALL, 5), (SMALL, 20), (MEDIUM, 10)] {
        let mut factory = RecordFactory::new(field_count, 16, 0.0, 42);
        let records = factory.generate(record_count);
        let schema = factory.schema().clone();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{record_count}r_{field_count}f"), record_count),
            &records,
            |b, recs| {
                b.iter(|| {
                    let buf = Vec::with_capacity(record_count * field_count * 20);
                    let mut writer =
                        CsvWriter::new(buf, Arc::clone(&schema), CsvWriterConfig::default());
                    for rec in recs {
                        writer.write_record(rec).unwrap();
                    }
                    writer.flush().unwrap();
                    black_box(writer);
                });
            },
        );
    }
    group.finish();
}

// ── JSON NDJSON Read ───────────────────────────────────────────────

fn bench_json_ndjson_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_ndjson_read");
    for record_count in [SMALL, MEDIUM] {
        let json_bytes = clinker_bench_support::generate_ndjson(
            record_count,
            &clinker_bench_support::FieldKind::default_layout(10),
            16,
            42,
        );
        let byte_len = json_bytes.len();

        group.throughput(Throughput::Bytes(byte_len as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &json_bytes,
            |b, data| {
                b.iter(|| {
                    let cursor = Cursor::new(data.clone());
                    let mut reader =
                        JsonReader::from_reader(cursor, JsonReaderConfig::default()).unwrap();
                    let _schema = reader.schema().unwrap();
                    let mut count = 0u64;
                    while let Some(_rec) = reader.next_record().unwrap() {
                        count += 1;
                    }
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

// ── JSON Array Read ────────────────────────────────────────────────

fn bench_json_array_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_array_read");
    for record_count in [SMALL, MEDIUM] {
        let json_bytes = clinker_bench_support::generate_json_array(
            record_count,
            &clinker_bench_support::FieldKind::default_layout(10),
            16,
            42,
        );
        let byte_len = json_bytes.len();

        group.throughput(Throughput::Bytes(byte_len as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &json_bytes,
            |b, data| {
                b.iter(|| {
                    let cursor = Cursor::new(data.clone());
                    let mut reader =
                        JsonReader::from_reader(cursor, JsonReaderConfig::default()).unwrap();
                    let _schema = reader.schema().unwrap();
                    let mut count = 0u64;
                    while let Some(_rec) = reader.next_record().unwrap() {
                        count += 1;
                    }
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

// ── JSON Write ─────────────────────────────────────────────────────

fn bench_json_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_write");
    // (label, record_count, field_count, string_len). The first two rows keep
    // the original 10-field/16-char shape as the no-regression oracle; the
    // wide (many-field) and long-value rows are where eliminating the
    // per-record `serde_json::Value` tree pays off.
    for (label, record_count, field_count, string_len) in [
        ("small_10f_16c", SMALL, 10, 16),
        ("medium_10f_16c", MEDIUM, 10, 16),
        ("medium_50f_16c", MEDIUM, 50, 16),
        ("medium_10f_256c", MEDIUM, 10, 256),
    ] {
        let mut factory = RecordFactory::new(field_count, string_len, 0.0, 42);
        let records = factory.generate(record_count);
        let schema = factory.schema().clone();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &records, |b, recs| {
            b.iter(|| {
                let buf = Vec::with_capacity(record_count * field_count * string_len);
                let mut writer =
                    JsonWriter::new(buf, Arc::clone(&schema), JsonWriterConfig::default());
                for rec in recs {
                    writer.write_record(rec).unwrap();
                }
                writer.flush().unwrap();
                black_box(writer);
            });
        });
    }
    group.finish();
}

// ── XML Write ──────────────────────────────────────────────────────

/// Build `count` records over a dotted + attribute schema (shared-prefix
/// branches with their own attributes), the shape the precompiled tree plan
/// targets. Values vary per record so the fill path does real work.
fn dotted_records(count: usize) -> (Arc<Schema>, Vec<Record>) {
    let schema = Arc::new(Schema::new(vec![
        "@id".into(),
        "name".into(),
        "Address.@type".into(),
        "Address.City".into(),
        "Address.State".into(),
        "Contact.Email".into(),
        "Contact.Phone".into(),
    ]));
    let records = (0..count)
        .map(|i| {
            Record::new(
                Arc::clone(&schema),
                vec![
                    Value::Integer(i as i64),
                    Value::String(format!("name{i}").into()),
                    Value::String("home".into()),
                    Value::String(format!("city{i}").into()),
                    Value::String("NY".into()),
                    Value::String(format!("user{i}@example.com").into()),
                    Value::String("555-0100".into()),
                ],
            )
        })
        .collect();
    (schema, records)
}

fn bench_xml_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("xml_write");

    // Flat-wide: many top-level fields. The old per-record tree build paid an
    // O(fields^2) sibling scan here, which the plan eliminates.
    let mut factory = RecordFactory::new(50, 16, 0.0, 42);
    let flat_records = factory.generate(MEDIUM);
    let flat_schema = factory.schema().clone();
    group.throughput(Throughput::Elements(MEDIUM as u64));
    group.bench_with_input(
        BenchmarkId::from_parameter("flat_50f"),
        &flat_records,
        |b, recs| {
            b.iter(|| {
                let buf = Vec::with_capacity(MEDIUM * 50 * 20);
                let mut writer =
                    XmlWriter::new(buf, Arc::clone(&flat_schema), XmlWriterConfig::default());
                for rec in recs {
                    writer.write_record(rec).unwrap();
                }
                writer.flush().unwrap();
                black_box(writer);
            });
        },
    );

    // Dotted + attribute: nested branches with attributes, the classic XML
    // shape whose per-node name allocation the plan removes.
    let (dotted_schema, dotted_records) = dotted_records(MEDIUM);
    group.bench_with_input(
        BenchmarkId::from_parameter("dotted_7f"),
        &dotted_records,
        |b, recs| {
            b.iter(|| {
                let buf = Vec::with_capacity(MEDIUM * 200);
                let mut writer =
                    XmlWriter::new(buf, Arc::clone(&dotted_schema), XmlWriterConfig::default());
                for rec in recs {
                    writer.write_record(rec).unwrap();
                }
                writer.flush().unwrap();
                black_box(writer);
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_csv_read,
    bench_csv_write,
    bench_json_ndjson_read,
    bench_json_array_read,
    bench_json_write,
    bench_xml_write,
);
criterion_main!(benches);
