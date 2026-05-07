use clinker_bench_support::{LARGE, MEDIUM, RecordFactory, SMALL};
use clinker_core::pipeline::arena::Arena;
use clinker_core::pipeline::index::SecondaryIndex;
use clinker_record::{MinimalRecord, RecordStorage, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;

// ── Arena construction ─────────────────────────────────────────────

fn bench_arena_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_build");
    for (record_count, field_count) in [(SMALL, 5), (SMALL, 20), (MEDIUM, 10), (LARGE, 5)] {
        let mut factory = RecordFactory::new(field_count, 16, 0.0, 42);
        let records = factory.generate(record_count);
        let schema = factory.schema().clone();

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{record_count}r_{field_count}f"), record_count),
            &records,
            |b, recs| {
                b.iter(|| {
                    let minimals: Vec<MinimalRecord> = recs
                        .iter()
                        .map(|r| MinimalRecord::new(r.values().to_vec()))
                        .collect();
                    let arena = Arena::from_parts(Arc::clone(&schema), minimals);
                    black_box(&arena);
                });
            },
        );
    }
    group.finish();
}

// ── Arena field access ─────────────────────────────────────────────

fn bench_arena_field_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_field_access");
    for record_count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(record_count);
        let schema = factory.schema().clone();
        let minimals: Vec<MinimalRecord> = records
            .iter()
            .map(|r| MinimalRecord::new(r.values().to_vec()))
            .collect();
        let arena = Arena::from_parts(Arc::clone(&schema), minimals);

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    for pos in 0..record_count as u64 {
                        black_box(arena.resolve_field(pos, "f4"));
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Secondary index construction ───────────────────────────────────

fn bench_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");

    for (record_count, cardinality_label) in [(MEDIUM, "low_card"), (MEDIUM, "high_card")] {
        // Low cardinality: 100 distinct groups; High: 5000 distinct groups
        let num_groups: usize = if cardinality_label == "low_card" {
            100
        } else {
            5000
        };
        let schema = Arc::new(Schema::new(vec!["group".into(), "value".into()]));
        let minimals: Vec<MinimalRecord> = (0..record_count)
            .map(|i| {
                MinimalRecord::new(vec![
                    Value::String(format!("g{}", i % num_groups).into_boxed_str()),
                    Value::Integer(i as i64),
                ])
            })
            .collect();
        let arena = Arena::from_parts(Arc::clone(&schema), minimals);

        group.throughput(Throughput::Elements(record_count as u64));
        group.bench_with_input(
            BenchmarkId::new(cardinality_label, record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let idx =
                        SecondaryIndex::build(&arena, &["group".to_string()], &HashMap::new())
                            .unwrap();
                    black_box(&idx);
                });
            },
        );
    }
    group.finish();
}

// ── Secondary index lookup ─────────────────────────────────────────

fn bench_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_lookup");
    let num_groups = 100;
    let record_count = MEDIUM;

    let schema = Arc::new(Schema::new(vec!["group".into(), "value".into()]));
    let minimals: Vec<MinimalRecord> = (0..record_count)
        .map(|i| {
            MinimalRecord::new(vec![
                Value::String(format!("g{}", i % num_groups).into_boxed_str()),
                Value::Integer(i as i64),
            ])
        })
        .collect();
    let arena = Arena::from_parts(schema, minimals);
    let idx = SecondaryIndex::build(&arena, &["group".to_string()], &HashMap::new()).unwrap();

    // Build lookup keys
    let keys: Vec<Vec<clinker_record::GroupByKey>> = (0..num_groups)
        .map(|i| {
            vec![clinker_record::GroupByKey::Str(
                format!("g{i}").into_boxed_str(),
            )]
        })
        .collect();

    group.throughput(Throughput::Elements(num_groups as u64));
    group.bench_function("100_groups", |b| {
        b.iter(|| {
            for key in &keys {
                black_box(idx.get(key));
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_arena_build,
    bench_arena_field_access,
    bench_index_build,
    bench_index_lookup,
);
criterion_main!(benches);
