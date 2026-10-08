//! Sort benchmarks.
//!
//! The `sort_*` groups time the window partition sort (`sort_partition`) over
//! arena positions. `sort_buffer_in_memory` times a field-ordered
//! `SortBuffer` that holds every row: push, sort and finish, sequentially and
//! on a dedicated eight-thread pool, over key shapes that stress the
//! comparator differently (an integer, short and long strings, a string
//! sharing a 16-byte prefix, a three-field mixed key, a leading date-time, and
//! a low-cardinality integer with real ties).
//!
//! `sort_buffer_spilled` times the external sort: the same buffer with a
//! threshold that forms about sixteen runs, then the field-ordered k-way merge
//! of those runs. The merge entry point is a `test-utils` wrapper, so this group
//! builds only with `--features test-utils`; a default
//! `cargo bench -p clinker-exec --bench sort` measures every other group.
//! Spill files go to the OS temp directory (`TMPDIR`).

use chrono::{NaiveDate, TimeDelta};
use clinker_bench_support::{LARGE, MEDIUM, RecordFactory, SMALL};
use clinker_exec::executor::SourceRowId;
use clinker_exec::pipeline::arena::Arena;
use clinker_exec::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use clinker_format::preparation::MemoryOnlyResources;
use clinker_plan::config::{NullOrder, NullPlacement, OrderField, SortField, SortOrder};
use clinker_plan::plan::{EntityRef, PlanNodeId};
use clinker_record::owned_storage::{AllocationResources, SharedStorage};
use clinker_record::{MinimalRecord, Record, Schema, Value};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use std::num::NonZeroUsize;
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

fn sort_field(name: &str, order: SortOrder, null_order: NullPlacement) -> OrderField {
    OrderField {
        field: name.to_string(),
        order,
        null_order,
    }
}

// ── Single-field sort ──────────────────────────────────────────────

fn bench_sort_single_field(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_single_field");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, NullPlacement::Last)];

    for count in [SMALL, MEDIUM, LARGE] {
        let arena = build_arena(count, 10, 0.0);
        let positions_template: Vec<u64> = (0..count as u64).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_exec::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
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
        sort_field("f0", SortOrder::Asc, NullPlacement::Last),
        sort_field("f2", SortOrder::Desc, NullPlacement::Last),
        sort_field("f4", SortOrder::Asc, NullPlacement::Last),
    ];

    for count in [SMALL, MEDIUM, LARGE] {
        let arena = build_arena(count, 10, 0.0);
        let positions_template: Vec<u64> = (0..count as u64).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_exec::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Sort with nulls ────────────────────────────────────────────────

fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, NullPlacement::Last)];

    for null_pct in [0, 10, 50] {
        let null_ratio = null_pct as f64 / 100.0;
        let arena = build_arena(MEDIUM, 10, null_ratio);
        let positions_template: Vec<u64> = (0..MEDIUM as u64).collect();

        group.throughput(Throughput::Elements(MEDIUM as u64));
        group.bench_with_input(BenchmarkId::new("null_pct", null_pct), &null_pct, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_exec::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Pre-sorted input (best case) ──────────────────────────────────

fn bench_sort_presorted(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_presorted");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, NullPlacement::Last)];

    for count in [SMALL, MEDIUM, LARGE] {
        // Build a pre-sorted arena: sequential integers in f0
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["f0".into(), "f1".into()])));
        let minimals: Vec<MinimalRecord> = (0..count)
            .map(|i| MinimalRecord::new(vec![Value::Integer(i as i64), Value::Null]))
            .collect();
        let arena = Arena::from_parts(schema, minimals);
        let positions_template: Vec<u64> = (0..count as u64).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_exec::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── Reverse-sorted input (worst case) ─────────────────────────────

fn bench_sort_reverse(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_reverse");
    let sort_by = vec![sort_field("f0", SortOrder::Asc, NullPlacement::Last)];

    for count in [SMALL, MEDIUM, LARGE] {
        let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["f0".into(), "f1".into()])));
        let minimals: Vec<MinimalRecord> = (0..count)
            .rev()
            .map(|i| MinimalRecord::new(vec![Value::Integer(i as i64), Value::Null]))
            .collect();
        let arena = Arena::from_parts(schema, minimals);
        let positions_template: Vec<u64> = (0..count as u64).collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut positions = positions_template.clone();
                clinker_exec::pipeline::sort::sort_partition(&arena, &mut positions, &sort_by);
                black_box(&positions);
            });
        });
    }
    group.finish();
}

// ── SortBuffer: field-ordered, resident and spilled ────────────────

/// One `(record, payload)` input row, as a Sort node pushes it.
type BufferRow = (Record, SourceRowId);

/// An odd multiplier coprime with every row count below, so
/// `i * PERMUTE % rows` visits each of `0..rows` exactly once in a
/// scrambled order without a random-number dependency.
const PERMUTE: u64 = 0x9E37_79B1;

fn permuted(i: usize, rows: usize) -> u64 {
    (i as u64).wrapping_mul(PERMUTE) % rows as u64
}

/// Fixed lowercase base-36 digits of `n`, left-padded with `0` to `width`.
fn base36(mut n: u64, width: usize) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::with_capacity(width);
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    while out.len() < width {
        out.push(b'0');
    }
    out.reverse();
    String::from_utf8(out).expect("base-36 digits are ASCII")
}

fn string_value(s: String) -> Value {
    Value::String(s.into())
}

fn buffer_key(name: &str, order: SortOrder, null_order: Option<NullOrder>) -> SortField {
    SortField {
        field: name.to_string(),
        order,
        null_order,
    }
}

/// A sort-buffer input shape: its schema, sort fields and rows.
struct BufferShape {
    schema: SharedStorage<Schema>,
    sort_by: Vec<SortField>,
    rows: Vec<BufferRow>,
}

fn buffer_shape(shape: &str, rows: usize) -> BufferShape {
    let column_names: &[&str] = match shape {
        "mixed3" => &["s", "n", "f"],
        "datetime_lead" => &["t", "n"],
        _ => &["k"],
    };
    let schema = SharedStorage::from_arc(Arc::new(Schema::new(
        column_names.iter().map(|name| (*name).into()).collect(),
    )));
    let sort_by = match shape {
        "mixed3" => vec![
            buffer_key("s", SortOrder::Asc, Some(NullOrder::Last)),
            buffer_key("n", SortOrder::Desc, Some(NullOrder::First)),
            buffer_key("f", SortOrder::Asc, None),
        ],
        "datetime_lead" => vec![
            buffer_key("t", SortOrder::Asc, None),
            buffer_key("n", SortOrder::Asc, None),
        ],
        _ => vec![buffer_key("k", SortOrder::Asc, None)],
    };
    let epoch = NaiveDate::from_ymd_opt(2020, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("a valid instant");
    let source = <PlanNodeId as EntityRef>::new(1);
    let rows = (0..rows)
        .map(|i| {
            let p = permuted(i, rows);
            // A second scramble, independent of `p`, decides nulls so they do
            // not line up with the key order.
            let q = (i as u64).wrapping_mul(0x2545_F491) % 1_000;
            let values = match shape {
                "int" => vec![Value::Integer(p as i64)],
                "short_string" => vec![string_value(base36(p, 4 + i % 3))],
                "prefixed_string" => {
                    let mut s = String::from("shared-prefix-16");
                    s.push_str(&base36(p, 8));
                    s.extend(std::iter::repeat_n('x', i % 17));
                    vec![string_value(s)]
                }
                "mixed3" => vec![
                    if q % 10 == 0 {
                        Value::Null
                    } else {
                        string_value(format!("name-{}", p % 1_000))
                    },
                    if q % 10 == 1 {
                        Value::Null
                    } else {
                        Value::Integer((p % 100) as i64)
                    },
                    if q % 10 == 2 {
                        Value::Null
                    } else {
                        Value::Float(p as f64 / 7.0)
                    },
                ],
                "datetime_lead" => vec![
                    Value::DateTime(epoch + TimeDelta::seconds(p as i64)),
                    Value::Integer((i % 1_000) as i64),
                ],
                "low_card" => vec![Value::Integer((p % 16) as i64)],
                other => unreachable!("unknown sort-buffer shape {other}"),
            };
            let record = Record::new(schema.clone(), values);
            (record, SourceRowId::new(source, i as u64 + 1))
        })
        .collect();
    BufferShape {
        schema,
        sort_by,
        rows,
    }
}

fn buffer_resources() -> AllocationResources {
    MemoryOnlyResources::new(NonZeroUsize::new(1 << 30).expect("non-zero"))
        .resources()
        .allocation()
        .clone()
}

/// The sort's own kernel pool: eight workers, built once outside any timed
/// region.
fn eight_thread_pool() -> Arc<rayon::ThreadPool> {
    Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .expect("build the bench kernel pool"),
    )
}

fn new_buffer(
    shape: &BufferShape,
    threshold: usize,
    spill_dir: Option<std::path::PathBuf>,
    pool: Option<&Arc<rayon::ThreadPool>>,
    resources: &AllocationResources,
) -> SortBuffer<SourceRowId> {
    let buffer = SortBuffer::new(
        shape.sort_by.clone(),
        threshold,
        spill_dir,
        true,
        shape.schema.clone(),
        resources.clone(),
    );
    match pool {
        Some(pool) => buffer.with_kernel_pool(Arc::clone(pool)),
        None => buffer,
    }
}

const BUFFER_SHAPES: [&str; 6] = [
    "int",
    "short_string",
    "prefixed_string",
    "mixed3",
    "datetime_lead",
    "low_card",
];

fn bench_sort_buffer_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_buffer_in_memory");
    let pool = eight_thread_pool();
    let resources = buffer_resources();
    for shape_name in BUFFER_SHAPES {
        for rows in [10_000usize, 100_000] {
            let shape = buffer_shape(shape_name, rows);
            group.throughput(Throughput::Elements(rows as u64));
            for (mode, pool) in [("seq", None), ("pool", Some(&pool))] {
                group.bench_with_input(
                    BenchmarkId::new(format!("{shape_name}/{mode}"), rows),
                    &rows,
                    |b, _| {
                        b.iter_batched(
                            || shape.rows.clone(),
                            |input| {
                                let mut buffer =
                                    new_buffer(&shape, usize::MAX, None, pool, &resources);
                                for (record, payload) in input {
                                    buffer.push(record, payload);
                                }
                                match buffer.finish().expect("resident sort") {
                                    (SortedOutput::InMemory(sorted), _) => black_box(sorted),
                                    (SortedOutput::Spilled(_), _) => {
                                        unreachable!("an unbounded threshold spilled")
                                    }
                                };
                            },
                            BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

#[cfg(feature = "test-utils")]
fn bench_sort_buffer_spilled(c: &mut Criterion) {
    use clinker_exec::pipeline::spill_merge::merge_sorted_runs_for_testing;

    let mut group = c.benchmark_group("sort_buffer_spilled");
    group.sample_size(20);
    let pool = eight_thread_pool();
    let resources = buffer_resources();
    let rows = 100_000usize;
    for shape_name in ["int", "prefixed_string", "mixed3"] {
        let shape = buffer_shape(shape_name, rows);
        // Size the threshold from one row's charge so about sixteen runs form,
        // whatever a row costs the buffer.
        let mut probe = new_buffer(&shape, usize::MAX, None, None, &resources);
        let (record, payload) = shape.rows[0].clone();
        probe.push(record, payload);
        let threshold = probe.bytes_used() * rows / 16;
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{shape_name}/pool"), rows),
            &rows,
            |b, _| {
                b.iter_batched(
                    || shape.rows.clone(),
                    |input| {
                        let mut buffer = new_buffer(
                            &shape,
                            threshold,
                            Some(std::env::temp_dir()),
                            Some(&pool),
                            &resources,
                        );
                        for (record, payload) in input {
                            buffer.push(record, payload);
                            if buffer.should_spill() {
                                buffer.sort_and_spill().expect("spill a sorted run");
                            }
                        }
                        match buffer.finish().expect("finish the external sort") {
                            (SortedOutput::Spilled(files), _) => black_box(
                                merge_sorted_runs_for_testing(files, &shape.sort_by)
                                    .expect("merge the spilled runs"),
                            ),
                            (SortedOutput::InMemory(sorted), _) => black_box(sorted),
                        };
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

#[cfg(feature = "test-utils")]
criterion_group!(
    benches,
    bench_sort_single_field,
    bench_sort_multi_field,
    bench_sort_with_nulls,
    bench_sort_presorted,
    bench_sort_reverse,
    bench_sort_buffer_in_memory,
    bench_sort_buffer_spilled,
);
#[cfg(not(feature = "test-utils"))]
criterion_group!(
    benches,
    bench_sort_single_field,
    bench_sort_multi_field,
    bench_sort_with_nulls,
    bench_sort_presorted,
    bench_sort_reverse,
    bench_sort_buffer_in_memory,
);
criterion_main!(benches);
