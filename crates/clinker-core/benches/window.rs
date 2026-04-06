use clinker_core::pipeline::arena::Arena;
use clinker_core::pipeline::window_context::PartitionWindowContext;
use clinker_record::{MinimalRecord, Schema, Value, WindowContext};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

/// Build an Arena with numeric fields for window benchmarks.
fn build_numeric_arena(partition_size: usize) -> (Arena, Vec<u32>) {
    let schema = Arc::new(Schema::new(vec!["amount".into(), "category".into()]));
    let mut rng = fastrand::Rng::with_seed(42);
    let mut minimals = Vec::with_capacity(partition_size);
    for i in 0..partition_size {
        minimals.push(MinimalRecord::new(vec![
            Value::Float(rng.f64() * 10_000.0),
            Value::String(format!("cat_{}", i % 50).into_boxed_str()),
        ]));
    }
    let positions: Vec<u32> = (0..partition_size as u32).collect();
    (Arena::from_parts(schema, minimals), positions)
}

// ── Window sum ─────────────────────────────────────────────────────

fn bench_window_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_sum");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::from_parameter(partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.sum("amount"));
                });
            },
        );
    }
    group.finish();
}

// ── Window avg ─────────────────────────────────────────────────────

fn bench_window_avg(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_avg");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::from_parameter(partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.avg("amount"));
                });
            },
        );
    }
    group.finish();
}

// ── Window min/max ─────────────────────────────────────────────────

fn bench_window_min_max(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_min_max");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::new("min", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.min("amount"));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("max", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.max("amount"));
                });
            },
        );
    }
    group.finish();
}

// ── Window count ───────────────────────────────────────────────────

fn bench_window_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_count");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::from_parameter(partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.count());
                });
            },
        );
    }
    group.finish();
}

// ── Window first/last ──────────────────────────────────────────────

fn bench_window_first_last(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_first_last");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::new("first", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.first());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("last", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.last());
                });
            },
        );
    }
    group.finish();
}

// ── Window lag/lead ────────────────────────────────────────────────

fn bench_window_lag_lead(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_lag_lead");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::new("lag_1", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.lag(1));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lead_1", partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.lead(1));
                });
            },
        );
    }
    group.finish();
}

// ── Window collect_set ─────────────────────────────────────────────

fn bench_window_collect_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_collect_set");
    for partition_size in [100, 1_000, 10_000] {
        let (arena, positions) = build_numeric_arena(partition_size);
        let mid = partition_size / 2;

        group.bench_with_input(
            BenchmarkId::from_parameter(partition_size),
            &partition_size,
            |b, _| {
                b.iter(|| {
                    let ctx = PartitionWindowContext::new(&arena, &positions, mid);
                    black_box(ctx.collect("category"));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_window_sum,
    bench_window_avg,
    bench_window_min_max,
    bench_window_count,
    bench_window_first_last,
    bench_window_lag_lead,
    bench_window_collect_set,
);
criterion_main!(benches);
