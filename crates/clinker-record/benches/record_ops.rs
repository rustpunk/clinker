use clinker_bench_support::RecordFactory;
use clinker_record::{Record, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::Arc;

// ── Helpers ────────────────────────────────────────────────────────

fn make_schema(n: usize) -> Arc<Schema> {
    let cols: Vec<Box<str>> = (0..n).map(|i| format!("f{i}").into_boxed_str()).collect();
    Arc::new(Schema::new(cols))
}

fn make_values(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            if i % 2 == 0 {
                Value::Integer(i as i64 * 42)
            } else {
                Value::String(format!("value_{i}").into_boxed_str())
            }
        })
        .collect()
}

// ── Record construction ────────────────────────────────────────────

fn bench_record_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_create");
    for field_count in [5, 20, 50] {
        let schema = make_schema(field_count);
        let values = make_values(field_count);
        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            &field_count,
            |b, _| {
                b.iter(|| Record::new(Arc::clone(&schema), values.clone()));
            },
        );
    }
    group.finish();
}

// ── Field get ──────────────────────────────────────────────────────

fn bench_record_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_get");
    for field_count in [5, 20, 50] {
        let schema = make_schema(field_count);
        let values = make_values(field_count);
        let record = Record::new(Arc::clone(&schema), values);
        // Lookup the middle field to avoid branch-predictor bias
        let target = format!("f{}", field_count / 2);
        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            &field_count,
            |b, _| {
                b.iter(|| black_box(record.get(black_box(&target))));
            },
        );
    }
    group.finish();
}

// ── Field set ──────────────────────────────────────────────────────

fn bench_record_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_set");
    for field_count in [5, 20, 50] {
        let schema = make_schema(field_count);
        let values = make_values(field_count);
        let mut record = Record::new(Arc::clone(&schema), values);
        let target = format!("f{}", field_count / 2);
        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            &field_count,
            |b, _| {
                b.iter(|| {
                    record.set(black_box(&target), Value::Integer(999));
                });
            },
        );
    }
    group.finish();
}

// ── Record clone ───────────────────────────────────────────────────

fn bench_record_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_clone");
    for (field_count, string_len) in [(5, 16), (20, 16), (20, 128), (50, 64)] {
        let mut factory = RecordFactory::new(field_count, string_len, 0.0, 42);
        let record = factory.next_record();
        group.bench_with_input(
            BenchmarkId::new(format!("{field_count}f_{string_len}sl"), field_count),
            &field_count,
            |b, _| {
                b.iter(|| black_box(record.clone()));
            },
        );
    }
    group.finish();
}

// ── Value heap_size ────────────────────────────────────────────────

fn bench_value_heap_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_heap_size");

    let scalar = Value::Integer(42);
    group.bench_function("scalar", |b| {
        b.iter(|| black_box(scalar.heap_size()));
    });

    let string = Value::String("hello world, this is a medium length string".into());
    group.bench_function("string", |b| {
        b.iter(|| black_box(string.heap_size()));
    });

    let nested_map =
        Value::map((0..20).map(|i| (format!("key_{i}").into_boxed_str(), Value::Integer(i))));
    group.bench_function("map_20_keys", |b| {
        b.iter(|| black_box(nested_map.heap_size()));
    });

    group.finish();
}

// ── Value::String construction ─────────────────────────────────────

fn bench_value_from_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_from_string");
    for len in [8, 64, 512] {
        let s: String = "x".repeat(len);
        group.bench_with_input(BenchmarkId::from_parameter(len), &len, |b, _| {
            b.iter(|| Value::String(black_box(s.clone()).into_boxed_str()));
        });
    }
    group.finish();
}

// ── Schema index lookup ────────────────────────────────────────────

fn bench_schema_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_index_lookup");
    for col_count in [10, 50, 200] {
        let schema = make_schema(col_count);
        // Lookup near the middle
        let target = format!("f{}", col_count / 2);
        group.bench_with_input(
            BenchmarkId::from_parameter(col_count),
            &col_count,
            |b, _| {
                b.iter(|| black_box(schema.index(black_box(&target))));
            },
        );
    }
    group.finish();
}

// ── Metadata set + get ─────────────────────────────────────────────

fn bench_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_set_get");
    for meta_count in [1, 16, 64] {
        let schema = make_schema(5);
        group.bench_with_input(
            BenchmarkId::from_parameter(meta_count),
            &meta_count,
            |b, &count| {
                b.iter(|| {
                    let mut record = Record::new(Arc::clone(&schema), vec![Value::Null; 5]);
                    for i in 0..count {
                        let key = format!("meta_{i}");
                        let _ = record.set_meta(&key, Value::Integer(i as i64));
                    }
                    // Read back the last key
                    if count > 0 {
                        black_box(record.get_meta(&format!("meta_{}", count - 1)));
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Batch record generation throughput ─────────────────────────────

fn bench_factory_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("factory_throughput");
    for count in [1_000, 10_000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let mut factory = RecordFactory::new(10, 16, 0.0, 42);
                black_box(factory.generate(count));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_record_create,
    bench_record_get,
    bench_record_set,
    bench_record_clone,
    bench_value_heap_size,
    bench_value_from_string,
    bench_schema_index,
    bench_metadata,
    bench_factory_throughput,
);
criterion_main!(benches);
