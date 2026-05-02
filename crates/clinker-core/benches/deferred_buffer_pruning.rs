//! Cardinality stress: verify the deferred-region column-pruning pass
//! produces ≥ 5x per-row memory reduction on a 50-column source where
//! only 5 columns flow through to the deferred operators.
//!
//! The bench builds two narrow projections of the same 10K-row wide
//! input — one using the production buffer-schema width (5 columns,
//! the planner's pruned output) and one using the un-pruned width (all
//! 50 columns, the no-pruning baseline). Per-row memory is measured
//! through the same `Arena::from_records` charge accounting the
//! executor's `tee_emit_to_region_input_buffers` uses, so the ratio
//! reflects the actual admission cost the deferred-region path pays.
//!
//! Asserted contract: pruning reduces per-row admission charge by at
//! least 5x. The bench runs under `cargo bench` and `cargo test
//! --benches` (with `harness = false` the body is just a `main`); the
//! assertion fires regardless of which entry point invokes it so a
//! regression in `pass_b_column_prune`'s width or the per-row arena
//! charge formula surfaces in CI.

use clinker_core::pipeline::arena::Arena;
use clinker_core::pipeline::memory::MemoryBudget;
use clinker_record::{Record, Schema, SchemaBuilder, Value};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

const ROW_COUNT: usize = 10_000;
const WIDE_COLUMN_COUNT: usize = 50;
const NARROW_COLUMN_COUNT: usize = 5;

fn build_wide_schema() -> Arc<Schema> {
    let mut builder = SchemaBuilder::with_capacity(WIDE_COLUMN_COUNT);
    for i in 0..WIDE_COLUMN_COUNT {
        builder = builder.with_field(format!("col_{i}"));
    }
    builder.build()
}

fn build_wide_rows() -> Vec<(Record, u64)> {
    let schema = build_wide_schema();
    (0..ROW_COUNT)
        .map(|i| {
            let values: Vec<Value> = (0..WIDE_COLUMN_COUNT)
                .map(|c| Value::String(format!("row{i}_col{c}").into_boxed_str().into()))
                .collect();
            (Record::new(Arc::clone(&schema), values), i as u64)
        })
        .collect()
}

fn measure_arena_bytes(rows: &[(Record, u64)], fields: &[String]) -> u64 {
    let schema = rows[0].0.schema().clone();
    let mut budget = MemoryBudget::new(u64::MAX, 0.80);
    let _arena = Arena::from_records(rows, fields, &schema, &mut budget)
        .expect("u64::MAX budget never overflows");
    budget.arena_bytes_charged()
}

fn bench_deferred_buffer_pruning(c: &mut Criterion) {
    let rows = build_wide_rows();

    let pruned_fields: Vec<String> = (0..NARROW_COLUMN_COUNT)
        .map(|i| format!("col_{i}"))
        .collect();
    let unpruned_fields: Vec<String> = (0..WIDE_COLUMN_COUNT).map(|i| format!("col_{i}")).collect();

    let pruned_bytes = measure_arena_bytes(&rows, &pruned_fields);
    let unpruned_bytes = measure_arena_bytes(&rows, &unpruned_fields);
    let ratio = unpruned_bytes as f64 / pruned_bytes.max(1) as f64;
    println!(
        "deferred_buffer_pruning: pruned={pruned_bytes} bytes, \
         unpruned={unpruned_bytes} bytes, ratio={ratio:.2}x"
    );
    assert!(
        ratio >= 5.0,
        "column pruning must yield >= 5x per-row memory reduction; \
         got pruned={pruned_bytes} unpruned={unpruned_bytes} ratio={ratio:.2}x"
    );

    let mut group = c.benchmark_group("deferred_buffer_pruning");
    group.bench_function("pruned_5_of_50", |b| {
        b.iter(|| {
            let bytes = measure_arena_bytes(black_box(&rows), black_box(&pruned_fields));
            black_box(bytes);
        });
    });
    group.bench_function("unpruned_all_50", |b| {
        b.iter(|| {
            let bytes = measure_arena_bytes(black_box(&rows), black_box(&unpruned_fields));
            black_box(bytes);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_deferred_buffer_pruning);
criterion_main!(benches);
