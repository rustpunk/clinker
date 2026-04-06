use clinker_bench_support::{CsvPayload, MEDIUM, RecordFactory, SMALL};
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig};
use clinker_format::json::reader::{JsonReader, JsonReaderConfig};
use clinker_format::json::writer::{JsonWriter, JsonWriterConfig};
use clinker_format::traits::{FormatReader, FormatWriter};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::io::Cursor;
use std::sync::Arc;

// ── CSV Read ───────────────────────────────────────────────────────

fn bench_csv_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_read");
    for (record_count, field_count) in [(SMALL, 5), (SMALL, 20), (MEDIUM, 10)] {
        let csv_bytes = CsvPayload::generate(record_count, field_count, 16);
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
        let json_bytes = clinker_bench_support::generate_ndjson(record_count, 10, 16);
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
        let json_bytes = clinker_bench_support::generate_json_array(record_count, 10, 16);
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
    for record_count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(record_count);
        let schema = factory.schema().clone();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &records,
            |b, recs| {
                b.iter(|| {
                    let buf = Vec::with_capacity(record_count * 200);
                    let mut writer =
                        JsonWriter::new(buf, Arc::clone(&schema), JsonWriterConfig::default());
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

criterion_group!(
    benches,
    bench_csv_read,
    bench_csv_write,
    bench_json_ndjson_read,
    bench_json_array_read,
    bench_json_write,
);
criterion_main!(benches);
