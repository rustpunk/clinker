use clinker_bench_support::{LARGE, MEDIUM, RecordFactory, SMALL};
use clinker_core::config::{NullOrder, SortField, SortOrder};
use clinker_core::pipeline::arena::Arena;
use clinker_record::{MinimalRecord, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::Arc;

/// Build an Arena from generated records for sort benchmarks.
fn build_arena(record_count: usize, field_count: usize, null_ratio: f64) -> Arena {
    let mut factory = RecordFactory::new(field_count, 16, null_ratio, 42);
    let records = factory.generate(record_count);
    let schema = factory.schema().clone();

    let minimals: Vec<MinimalRecord> = records
        .into_iter()
        .map(|r| MinimalRecord::new(r.values().to_vec()))
        .collect();
    Arena::from_parts(schema, minimals)
}

fn sort_field(name: &str, order: SortOrder, null_order: Option<NullOrder>) -> SortField {
    SortField {
        field: name.to_string(),
        order,
        null_order,
    }
}

// ── Single-field sort ──────────────────────────────────────────────

fn bench_sort_single_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_single_field");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, None)];

    for count in [SMALL, MEDIUM, LARGE] {
        let arena = build_arena(count, 10, 0.0);
        let positions_template: Vec<u32> = (0..count as u32).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_core::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Multi-field sort (3 keys) ──────────────────────────────────────

fn bench_sort_multi_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_multi_field");
    let sort_by = vec![
        sort_field("f0", SortOrder::Asc, None),
        sort_field("f2", SortOrder::Desc, None),
        sort_field("f4", SortOrder::Asc, None),
    ];

    for count in [SMALL, MEDIUM, LARGE] {
        let arena = build_arena(count, 10, 0.0);
        let positions_template: Vec<u32> = (0..count as u32).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_core::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Sort with nulls ────────────────────────────────────────────────

fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, Some(NullOrder::Last))];

    for null_pct in [0, 10, 50] {
        let null_ratio = null_pct as f64 / 100.0;
        let arena = build_arena(MEDIUM, 10, null_ratio);
        let positions_template: Vec<u32> = (0..MEDIUM as u32).collect();

        group.throughput(Throughput::Elements(MEDIUM as u64));
        group.bench_with_input(BenchmarkId::new("null_pct", null_pct), &null_pct, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_core::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Pre-sorted input (best case) ──────────────────────────────────

fn bench_sort_presorted(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_presorted");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, None)];

    for count in [SMALL, MEDIUM, LARGE] {
        // Build a pre-sorted arena: sequential integers in f0
        let schema = Arc::new(Schema::new(vec!["f0".into(), "f1".into()]));
        let minimals: Vec<MinimalRecord> = (0..count)
            .map(|i| MinimalRecord::new(vec![Value::Integer(i as i64), Value::Null]))
            .collect();
        let arena = Arena::from_parts(schema, minimals);
        let positions_template: Vec<u32> = (0..count as u32).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_core::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Reverse-sorted input (worst case) ─────────────────────────────

fn bench_sort_reverse(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_reverse");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, None)];

    for count in [SMALL, MEDIUM, LARGE] {
        let schema = Arc::new(Schema::new(vec!["f0".into(), "f1".into()]));
        let minimals: Vec<MinimalRecord> = (0..count)
            .rev()
            .map(|i| MinimalRecord::new(vec![Value::Integer(i as i64), Value::Null]))
            .collect();
        let arena = Arena::from_parts(schema, minimals);
        let positions_template: Vec<u32> = (0..count as u32).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_core::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sort_single_field,
    bench_sort_multi_field,
    bench_sort_with_nulls,
    bench_sort_presorted,
    bench_sort_reverse,
);
criterion_main!(benches);
