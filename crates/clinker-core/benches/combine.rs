//! Benchmarks for the Combine node.
//!
//! Five criterion functions cover the performance targets from the
//! combine spec:
//!
//! | Bench                          | Strategy under measurement       |
//! |--------------------------------|----------------------------------|
//! | `bench_combine_equi_2input`    | end-to-end equi-join executor    |
//! | `bench_combine_iejoin`         | range/theta (IE-join)            |
//! | `bench_combine_nary_3input`    | N-ary cascade                    |
//! | `bench_predicate_decomposition`| where-predicate decomposition    |
//! | `bench_combine_grace_hash`     | Grace hash partitioning + spill  |
//!
//! `bench_combine_equi_2input` drives the full executor path
//! (CombineHashTable build + probe + CombineResolver + body eval +
//! output emit) over `CombineDataGen`-produced inputs at three sizes.
//! Its 10K×100K configuration is comparable to the preserved
//! `target/criterion/lookup_baseline/10k_x_100k/lookup-v1` saved
//! baseline (same row counts, same overlap ratio, same column width)
//! and is the regression gate against the prior lookup path.
//!
//! The four IE/N-ary/decomposition/grace functions remain scaffolds
//! awaiting their executor strategies — each spins up a
//! `CombineDataGen` so the bench touches the real data-generation
//! path, then `black_box`'s the generated rows.

use clinker_bench_support::CombineDataGen;
use clinker_core::config::parse_config;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

// ── Shared helpers ────────────────────────────────────────────────

/// Thread-safe in-memory writer (mirrors the helper in `pipeline.rs`).
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

fn bench_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "combine-bench".to_string(),
        batch_id: "combine-batch".to_string(),
        pipeline_vars: IndexMap::new(),
        shutdown_token: None,
    }
}

// ── End-to-end equi-combine bench (regression gate vs. lookup-v1) ─

/// Pipeline YAML for the 2-input equi combine bench. Two CSV sources
/// (`products` build-side, `orders` driving) joined on the integer
/// `key`, with `match: first` (build side has unique keys at the
/// configured cardinality) and `on_miss: null_fields` (preserve all
/// driver rows; null-fill misses). The CXL body emits four columns —
/// two from the driver and two coalesced from the build side via `??`
/// — mirroring the workload shape captured in the
/// `lookup_baseline/10k_x_100k/lookup-v1` saved baseline so the
/// fairness comparison measures the same units of work per probe row.
const COMBINE_EQUI_2INPUT_YAML: &str = r#"
pipeline:
  name: bench_combine_equi_2input
error_handling:
  strategy: continue
nodes:
- type: source
  name: products
  config:
    name: products
    path: products.csv
    type: csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
      - { name: c1, type: string }
      - { name: c2, type: string }

- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    options:
      has_header: true
    schema:
      - { name: key, type: int }
      - { name: c0, type: string }
      - { name: c1, type: string }
      - { name: c2, type: string }

- type: combine
  name: enriched
  input:
    orders: orders
    products: products
  config:
    where: "orders.key == products.key"
    match: first
    on_miss: null_fields
    cxl: |
      emit order_key = orders.key
      emit order_c0 = orders.c0
      emit product_c1 = products.c1 ?? "UNKNOWN"
      emit product_c2 = products.c2 ?? "UNKNOWN"

- type: output
  name: out
  input: enriched
  config:
    name: out
    path: out.csv
    type: csv
"#;

/// End-to-end equi-combine bench at three sizes. Each iteration
/// re-parses readers from cached CSV bytes, recompiles the plan, and
/// drives `PipelineExecutor::run_plan_with_readers_writers_with_primary`
/// with `orders` as the explicit driving source. This measures the
/// same boundary-to-boundary path as the preserved
/// `lookup_baseline/10k_x_100k/lookup-v1` saved baseline so a numeric
/// delta against that estimates.json is meaningful.
fn bench_combine_equi_2input(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine_equi_2input");
    let config =
        parse_config(COMBINE_EQUI_2INPUT_YAML).expect("combine equi_2input YAML must parse");
    let params = bench_params();

    for (label, build, probe, samples) in [
        ("small", 1_000usize, 10_000usize, 50usize),
        ("medium", 10_000, 100_000, 10),
        ("large", 10_000, 1_000_000, 10), // 1M driving × 10K build
    ] {
        // Generate CSV inputs once per size — re-running the generator
        // inside `b.iter` would dominate the measurement at the small
        // end and obscure executor cost at the large end.
        let data_gen = CombineDataGen {
            build_rows: build,
            probe_rows: probe,
            overlap_ratio: 0.9,
            key_cardinality: build,
            extra_columns: 3,
        };
        let build_csv = data_gen.build_csv();
        let probe_csv = data_gen.probe_csv();

        group.throughput(Throughput::Elements(probe as u64));
        group.sample_size(samples);
        group.bench_with_input(BenchmarkId::new("rows", label), &(build, probe), |b, _| {
            b.iter(|| {
                let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([
                    (
                        "products".to_string(),
                        Box::new(Cursor::new(build_csv.clone())) as Box<dyn std::io::Read + Send>,
                    ),
                    (
                        "orders".to_string(),
                        Box::new(Cursor::new(probe_csv.clone())) as Box<dyn std::io::Read + Send>,
                    ),
                ]);
                let out_buf = BenchBuffer::new();
                let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
                    "out".to_string(),
                    Box::new(out_buf.clone()) as Box<dyn Write + Send>,
                )]);
                let plan = clinker_core::config::PipelineConfig::compile(
                    &config,
                    &clinker_core::config::CompileContext::default(),
                )
                .expect("combine equi_2input must compile");
                let report = PipelineExecutor::run_plan_with_readers_writers_with_primary(
                    &plan, "orders", readers, writers, &params,
                )
                .expect("combine equi_2input must execute");
                black_box(report);
            });
        });
    }
    group.finish();
}

fn bench_combine_iejoin(c: &mut Criterion) {
    // C.3 fills this in with the IE-join (range + theta) strategy.
    let mut group = c.benchmark_group("combine_iejoin");
    let data_gen = CombineDataGen {
        build_rows: 10_000,
        probe_rows: 100_000,
        overlap_ratio: 0.5,
        key_cardinality: 1_000,
        extra_columns: 2,
    };
    let build_rows = data_gen.build_records();
    let probe_rows = data_gen.probe_records();
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("medium", |b| {
        b.iter(|| {
            // C.3 fills in.
            black_box(&build_rows);
            black_box(&probe_rows);
        });
    });
    group.finish();
}

fn bench_combine_nary_3input(c: &mut Criterion) {
    // C.4 fills this in with the N-ary cascade executor.
    let mut group = c.benchmark_group("combine_nary_3input");
    // Three correlated sets: build × mid × probe.
    let build = CombineDataGen {
        build_rows: 1_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    let mid = CombineDataGen {
        build_rows: 10_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    let probe = CombineDataGen {
        build_rows: 100_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000,
        extra_columns: 1,
    }
    .build_records();
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("1k_10k_100k", |b| {
        b.iter(|| {
            // C.4 fills in.
            black_box(&build);
            black_box(&mid);
            black_box(&probe);
        });
    });
    group.finish();
}

fn bench_predicate_decomposition(c: &mut Criterion) {
    // C.1 fills this in — decomposition of the where-predicate into
    // equality / range / residual conjuncts.
    let mut group = c.benchmark_group("predicate_decomposition");
    group.bench_function("scaffold", |b| {
        b.iter(|| {
            // C.1 fills in: parse a CXL predicate, call decompose(), assert
            // the number of equality/range/residual conjuncts.
            black_box(());
        });
    });
    group.finish();
}

fn bench_combine_grace_hash(c: &mut Criterion) {
    // C.4 fills this in — Grace hash partitioning with disk spill.
    let mut group = c.benchmark_group("combine_grace_hash");
    let data_gen = CombineDataGen {
        build_rows: 1_000_000,
        probe_rows: 0,
        overlap_ratio: 0.0,
        key_cardinality: 1_000_000,
        extra_columns: 4,
    };
    let build_rows = data_gen.build_records();
    group.throughput(Throughput::Elements(1_000_000));
    group.bench_function("1m_build", |b| {
        b.iter(|| {
            // C.4 fills in: force spill threshold low, run partitioned build,
            // probe with 100K, measure end-to-end time.
            black_box(&build_rows);
        });
    });
    group.finish();
}

criterion_group!(
    combine_benches,
    bench_combine_equi_2input,
    bench_combine_iejoin,
    bench_combine_nary_3input,
    bench_predicate_decomposition,
    bench_combine_grace_hash,
);
criterion_main!(combine_benches);
