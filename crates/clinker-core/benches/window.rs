use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::pipeline::arena::Arena;
use clinker_core::pipeline::window_context::PartitionWindowContext;
use clinker_record::{MinimalRecord, Schema, Value, WindowContext};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

/// Build an Arena with numeric fields for window benchmarks.
fn build_numeric_arena(partition_size: usize) -> (Arena, Vec<u64>) {
    let schema = Arc::new(Schema::new(vec!["amount".into(), "category".into()]));
    let mut rng = fastrand::Rng::with_seed(42);
    let mut minimals = Vec::with_capacity(partition_size);
    for i in 0..partition_size {
        minimals.push(MinimalRecord::new(vec![
            Value::Float(rng.f64() * 10_000.0),
            Value::String(format!("cat_{}", i % 50).into_boxed_str()),
        ]));
    }
    let positions: Vec<u64> = (0..partition_size as u64).collect();
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

// ── End-to-end node-rooted post-aggregate window ──────────────────

#[derive(Clone, Default)]
struct BenchBuffer(Arc<Mutex<Vec<u8>>>);

impl BenchBuffer {
    fn new() -> Self {
        Self::default()
    }
}

impl Write for BenchBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

/// End-to-end pipeline `Source → Aggregate(group_by=department) →
/// Transform($window.sum(total), partition_by=department) → Output`.
/// Pins throughput for the node-rooted post-aggregate window path.
fn bench_node_rooted_window(c: &mut Criterion) {
    let yaml = r#"
pipeline:
  name: node_rooted_window_bench
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("bench yaml parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("bench compile");
    let params = PipelineRunParams {
        execution_id: "bench-exec".to_string(),
        batch_id: "bench-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    };

    let mut group = c.benchmark_group("node_rooted_window");
    for row_count in [10_000usize, 100_000] {
        let mut csv = String::from("department,amount\n");
        for i in 0..row_count {
            csv.push_str(&format!("dept_{:03},{}\n", i % 50, i * 10));
        }
        let csv_bytes = csv.into_bytes();

        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let readers: HashMap<String, Box<dyn std::io::Read + Send>> =
                        HashMap::from([(
                            "src".to_string(),
                            Box::new(Cursor::new(csv_bytes.clone()))
                                as Box<dyn std::io::Read + Send>,
                        )]);
                    let buf = BenchBuffer::new();
                    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                        "out".to_string(),
                        Box::new(buf.clone()) as Box<dyn Write + Send>,
                    )]);
                    PipelineExecutor::run_plan_with_readers_writers_with_primary(
                        &plan, "src", readers, writers, &params,
                    )
                    .expect("bench pipeline runs");
                    black_box(buf);
                });
            },
        );
    }
    group.finish();
}

/// End-to-end pipeline with a `HashBuildProbe` Combine instead of an
/// Aggregate as the post-combine window's rooting operator.
fn bench_post_combine_window(c: &mut Criterion) {
    let yaml = r#"
pipeline:
  name: post_combine_window_bench
error_handling:
  strategy: continue
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: matched_amount, type: int }
- type: source
  name: depts
  config:
    name: depts
    path: depts.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: budget, type: int }
- type: combine
  name: enriched
  input:
    o: orders
    d: depts
  config:
    where: "o.department == d.department"
    match: first
    on_miss: skip
    cxl: |
      emit order_id = o.order_id
      emit department = o.department
      emit matched_amount = o.matched_amount
      emit budget = d.budget
    propagate_ck: driver
- type: transform
  name: running
  input: enriched
  config:
    cxl: |
      emit order_id = order_id
      emit department = department
      emit matched_amount = matched_amount
      emit dept_total = $window.sum(matched_amount)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("bench yaml parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("bench compile");
    let params = PipelineRunParams {
        execution_id: "bench-exec".to_string(),
        batch_id: "bench-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    };

    // Build dept lookup once.
    let depts_csv = {
        let mut s = String::from("department,budget\n");
        for i in 0..50 {
            s.push_str(&format!("dept_{i:03},{}\n", i * 1000));
        }
        s.into_bytes()
    };

    let mut group = c.benchmark_group("post_combine_window");
    for row_count in [10_000usize, 100_000] {
        let mut orders_csv = String::from("order_id,department,matched_amount\n");
        for i in 0..row_count {
            orders_csv.push_str(&format!("o{i},dept_{:03},{}\n", i % 50, i * 7));
        }
        let orders_bytes = orders_csv.into_bytes();

        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
                        (
                            "orders".to_string(),
                            Box::new(Cursor::new(orders_bytes.clone()))
                                as Box<dyn std::io::Read + Send>,
                        ),
                        (
                            "depts".to_string(),
                            Box::new(Cursor::new(depts_csv.clone()))
                                as Box<dyn std::io::Read + Send>,
                        ),
                    ]);
                    let buf = BenchBuffer::new();
                    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                        "out".to_string(),
                        Box::new(buf.clone()) as Box<dyn Write + Send>,
                    )]);
                    PipelineExecutor::run_plan_with_readers_writers_with_primary(
                        &plan, "orders", readers, writers, &params,
                    )
                    .expect("bench pipeline runs");
                    black_box(buf);
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
    bench_node_rooted_window,
    bench_post_combine_window,
);
criterion_main!(benches);
